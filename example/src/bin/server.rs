use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info};
use tracing_subscriber::FmtSubscriber;
use vsr::{Message as VsrMessage, Replica as VsrReplica, ReplicaConfig};
use vsr_example::connection::{Connection, MessageHandler};
use vsr_example::state_machine::{AppendResult, LogAppendOp, LogStateMachine};

type Replica = VsrReplica<String, LogStateMachine, Vec<LogAppendOp>, AppendResult>;
type Message = VsrMessage<String, Vec<LogAppendOp>, AppendResult>;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    replica_id: usize,

    #[arg(long)]
    replicas: Vec<String>,

    #[arg(short, long, default_value_t = 1000)]
    tick_interval_ms: u64,

    #[arg(long)]
    recovery: bool,
}

struct Server {
    replicas: Vec<String>,
    replica: Arc<Mutex<Replica>>,
    replica_connections: Arc<RwLock<HashMap<usize, Connection>>>,
    client_connections: Arc<RwLock<HashMap<String, Arc<Connection>>>>,
    client_mappings: Arc<RwLock<HashMap<usize, String>>>,
    listener: TcpListener,
}

impl Server {
    async fn new(
        replicas: Vec<String>,
        replica_count: usize,
        id: usize,
        recovery: bool,
        config: ReplicaConfig,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let replica = match recovery {
            true => Replica::new_recovering(Instant::now(), 0, replica_count, id, config),
            false => Replica::new(Instant::now(), replica_count, id, config),
        };

        let replica = Arc::new(Mutex::new(replica));
        let our_addr = &replicas[id];
        let listener = TcpListener::bind(our_addr).await?;

        // Create the server instance first
        let server = Self {
            replicas: replicas.clone(),
            replica,
            replica_connections: Arc::new(RwLock::new(HashMap::new())),
            client_connections: Arc::new(RwLock::new(HashMap::new())),
            client_mappings: Arc::new(RwLock::new(HashMap::new())),
            listener,
        };

        let server = Arc::new(server);

        // Initialize connections with message handlers
        let connections: HashMap<usize, Connection> = replicas
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let handler = MessageHandler::new({
                    let server = Arc::clone(&server);
                    let addr = addr.clone();
                    move |message| {
                        let server = Arc::clone(&server);
                        let addr = addr.clone();
                        Box::pin(async move {
                            if let Err(e) = server.handle_message(message, addr).await {
                                error!("Error handling message: {}", e);
                            }
                        })
                    }
                });
                let conn = Connection::new(addr.clone(), handler, true);

                (i, conn)
            })
            .collect();

        *server.replica_connections.write().await = connections;

        Ok(server)
    }

    async fn send_message(&self, msg: Message) -> Result<(), Box<dyn std::error::Error>> {
        let serialized_msg = serde_json::to_vec(&msg)?;

        match &msg {
            VsrMessage::VsrMessage(vsr_message) => {
                let destination = vsr_message.to;
                debug!(
                    msg = ?msg,
                    destination = destination,
                    "Sending message to replica"
                );

                let connections = self.replica_connections.read().await;
                if let Some(conn) = connections.get(&destination) {
                    conn.send_message(&serialized_msg).await?;
                    debug!("Message sent to replica {}", destination);
                } else {
                    error!("No connection found for replica {}", destination);
                }
            }
            VsrMessage::ClientResponse(client_response) => {
                let client_id = client_response.client_id;
                debug!(
                    msg = ?msg,
                    client_id = client_id,
                    "Sending message to client"
                );

                let client_mappings = self.client_mappings.read().await;
                let Some(client_addr) = client_mappings.get(&client_id) else {
                    error!("No address found for client {}", client_id);
                    return Err("No address found for client".into());
                };
                let client_connections = self.client_connections.read().await;
                if let Some(conn) = client_connections.get(client_addr) {
                    conn.send_message(&serialized_msg).await?;
                } else {
                    error!("No connection found for client {}", client_id);
                }
            }
            VsrMessage::ClientRequest(_) => unreachable!(),
        }

        Ok(())
    }

    async fn handle_message(
        self: &Arc<Self>,
        message: Vec<u8>,
        addr: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let msg: Message = serde_json::from_slice(&message)?;
        debug!(msg = ?msg, "Received message");

        if let VsrMessage::ClientRequest(client_request) = &msg {
            let mut mappings = self.client_mappings.write().await;
            mappings.insert(client_request.client_id, addr);
        }

        let mut replica = self.replica.lock().await;
        let reply_messages = replica.process_message(msg, Instant::now());
        drop(replica);

        for reply_msg in reply_messages {
            if let Err(e) = self.send_message(reply_msg).await {
                error!("Error sending reply message: {}", e);
            }
        }

        Ok(())
    }

    async fn handle_incoming_connections(self: Arc<Self>) {
        loop {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted connection from: {}", addr);

                    let replica_idx = self.replicas.iter().position(|r| r == &addr.to_string());
                    let handler = MessageHandler::new({
                        let server = Arc::clone(&self);

                        move |message| {
                            let server = Arc::clone(&server);

                            Box::pin(async move {
                                if let Err(e) =
                                    server.handle_message(message, addr.to_string()).await
                                {
                                    error!("Error handling message: {}", e);
                                }
                            })
                        }
                    });
                    let conn = Connection::with_stream(
                        addr.to_string(),
                        stream,
                        handler,
                        replica_idx.is_some(),
                    );

                    match replica_idx {
                        Some(id) => {
                            *self.replica_connections.write().await = {
                                let mut connections = self.replica_connections.read().await.clone();
                                connections.insert(id, conn);
                                connections
                            };
                        }
                        None => {
                            *self.client_connections.write().await = {
                                let mut connections = self.client_connections.read().await.clone();
                                connections.insert(addr.to_string(), Arc::new(conn));
                                connections
                            };
                        }
                    }
                }
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }

    async fn run(self: Arc<Self>, tick_interval: Duration) {
        // Start handling incoming connections
        tokio::spawn(self.clone().handle_incoming_connections());

        // Tick the replica
        let mut interval = tokio::time::interval(tick_interval);
        loop {
            interval.tick().await;

            let mut replica = self.replica.lock().await;
            let messages = replica.tick(Instant::now());
            drop(replica);

            for msg in messages {
                if let Err(e) = self.send_message(msg).await {
                    error!("Failed to handle tick message: {}", e);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .pretty()
        .init();

    let args = Args::parse();

    // take it a bit slow to not get flooded with messages
    let config = ReplicaConfig {
        heartbeat_interval: Duration::from_millis(1000),
        prepare_retry_timeout: Duration::from_millis(1000),
        view_change_timeout: Duration::from_millis(10000),
        state_transfer_timeout: Duration::from_millis(10000),
    };

    let replica_count = args.replicas.len();
    let server = Server::new(
        args.replicas,
        replica_count,
        args.replica_id,
        args.recovery,
        config,
    )
    .await?;

    server
        .run(Duration::from_millis(args.tick_interval_ms))
        .await;

    Ok(())
}
