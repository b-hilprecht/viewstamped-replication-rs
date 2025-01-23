use clap::Parser;
use std::{
    collections::HashMap,
    io::{self, Write},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};
use vsr::{Client as VsrClient, Message};
use vsr_example::{
    connection::{Connection, MessageHandler},
    state_machine::{AppendResult, LogAppendOp},
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    replicas: Vec<String>,

    #[arg(long, default_value_t = 1)]
    client_id: u64,

    #[arg(long, default_value_t = 1000)]
    retry_ms: u64,

    #[arg(long, default_value_t = 5000)]
    reconnect_ms: u64,

    #[arg(long, default_value_t = 100)]
    tick_interval_ms: u64,
}

#[derive(Debug)]
enum Command {
    Append(String),
    Quit,
}

impl Command {
    fn from_line(line: &str) -> Result<Command, String> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty command".to_string());
        }

        match parts[0] {
            "append" => {
                let data = parts.get(1..).unwrap_or_default().join(" ");
                if data.is_empty() {
                    Err("append requires data".to_string())
                } else {
                    Ok(Command::Append(data))
                }
            }
            "quit" | "exit" => Ok(Command::Quit),
            cmd => Err(format!("Unknown command: {}", cmd)),
        }
    }
}

struct Client {
    vsr_client: Arc<Mutex<VsrClient<String, Vec<LogAppendOp>, AppendResult>>>,
    replica_connections: Arc<RwLock<HashMap<usize, Connection>>>,
    tick_interval: Duration,
}

impl Client {
    async fn new(
        replicas: Vec<String>,
        client_id: usize,
        retry_interval: Duration,
        tick_interval: Duration,
    ) -> Arc<Self> {
        let vsr_client = VsrClient::new(client_id, replicas.len(), retry_interval);
        let connections = HashMap::new();

        // Create the client instance first
        let client = Arc::new(Self {
            vsr_client: Arc::new(Mutex::new(vsr_client)),
            replica_connections: Arc::new(RwLock::new(connections)),
            tick_interval,
        });

        // Then set up connections with message handlers
        let mut connections = HashMap::new();
        for (i, addr) in replicas.into_iter().enumerate() {
            let client_clone = Arc::clone(&client);
            let handler: MessageHandler = MessageHandler::new(move |message| {
                let client = client_clone.clone();
                Box::pin(async move {
                    if let Ok(msg) = serde_json::from_slice(&message) {
                        if let Err(e) = client.handle_message(msg).await {
                            eprintln!("Error handling message: {}", e);
                        }
                    } else {
                        eprintln!("Failed to deserialize message");
                    }
                })
            });
            let conn = Connection::new(addr, handler, true);

            connections.insert(i, conn);
        }

        *client.replica_connections.write().await = connections;

        client
    }

    async fn append(&self, data: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = self.vsr_client.lock().await;
        let request_num = client.queue_request(data.to_string());
        println!("Queued request {}", request_num);

        Ok(())
    }

    async fn send_message(
        &self,
        msg: &Message<String, Vec<LogAppendOp>, AppendResult>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let replica_id = match msg {
            Message::ClientRequest(req) => req.replica_id,
            _ => return Err("Invalid message type for client to send".into()),
        };

        let connections = self.replica_connections.read().await;
        let Some(conn) = connections.get(&replica_id) else {
            return Err(format!("No connection found for replica {}", replica_id).into());
        };

        let Ok(serialized_msg) = serde_json::to_vec(&msg) else {
            return Err("Failed to serialize message".into());
        };
        conn.send_message(&serialized_msg).await?;

        Ok(())
    }

    async fn run_background_tasks(self: Arc<Self>) {
        let tick_client = Arc::clone(&self);
        let tick_interval = self.tick_interval;

        // Spawn tick task
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            loop {
                interval.tick().await;
                let mut client = tick_client.vsr_client.lock().await;
                let messages = client.tick(Instant::now());
                drop(client);

                for msg in messages {
                    if let Err(e) = tick_client.send_message(&msg).await {
                        eprintln!("Error sending message: {}", e);
                    }
                }
            }
        });
    }

    async fn handle_message(
        &self,
        msg: Message<String, Vec<LogAppendOp>, AppendResult>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut client = self.vsr_client.lock().await;

        client.process_message(msg, Instant::now());

        if let Some((request_num, result)) = client.consume_result() {
            println!(
                "Request {} completed: log length is now {}",
                request_num, result.log_len
            );
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.replicas.is_empty() {
        eprintln!("Error: At least one replica address must be specified");
        eprintln!("Example: --replicas localhost:50051 localhost:50052 localhost:50053");
        std::process::exit(1);
    }

    let client = Client::new(
        args.replicas,
        args.client_id as usize,
        Duration::from_millis(args.retry_ms),
        Duration::from_millis(args.tick_interval_ms),
    )
    .await;

    println!("VSR Log Client (client_id: {})", args.client_id);

    // Start background tasks
    client.clone().run_background_tasks().await;

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut line = String::new();
        io::stdin().read_line(&mut line)?;

        let cmd = match Command::from_line(&line) {
            Ok(cmd) => cmd,
            Err(e) => {
                println!("Error: {}", e);
                continue;
            }
        };

        match cmd {
            Command::Append(data) => {
                if let Err(e) = client.append(&data).await {
                    println!("Error: {}", e);
                }
            }
            Command::Quit => break,
        }
    }

    Ok(())
}
