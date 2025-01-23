use bytes::Bytes;
use futures::SinkExt;
use futures::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{error, info, warn};

type MessageHandlerFn =
    Box<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Clone)]
pub struct MessageHandler(Arc<MessageHandlerFn>);

impl MessageHandler {
    pub fn new(
        handler: impl Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    ) -> Self {
        Self(Arc::new(Box::new(handler)))
    }
}

#[derive(Clone)]
pub struct Connection {
    pub address: String,
    stream: Arc<Mutex<Option<Framed<TcpStream, LengthDelimitedCodec>>>>,
    message_handler: MessageHandler,
    sender: mpsc::Sender<Vec<u8>>,
    try_reconnect: bool,
    shutdown: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl Connection {
    pub fn new(address: String, handler: MessageHandler, try_reconnect: bool) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let connection = Self {
            address: address.clone(),
            stream: Arc::new(Mutex::new(None)),
            message_handler: handler,
            sender,
            try_reconnect,
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
        };

        let conn_clone = connection.clone();
        tokio::spawn(async move {
            Self::run_connection_loop(conn_clone, receiver, shutdown_rx).await;
        });

        connection
    }

    pub fn with_stream(
        address: String,
        stream: TcpStream,
        handler: MessageHandler,
        try_reconnect: bool,
    ) -> Self {
        if let Err(e) = stream.set_nodelay(true) {
            warn!("Failed to set TCP_NODELAY: {}", e);
        }

        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        let (sender, receiver) = mpsc::channel(100);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let connection = Self {
            address: address.clone(),
            stream: Arc::new(Mutex::new(Some(framed))),
            message_handler: handler,
            sender,
            try_reconnect,
            shutdown: Arc::new(Mutex::new(Some(shutdown_tx))),
        };

        let conn_clone = connection.clone();
        tokio::spawn(async move {
            Self::run_connection_loop(conn_clone, receiver, shutdown_rx).await;
        });

        connection
    }

    async fn ensure_connected(
        address: &str,
        stream: &Arc<Mutex<Option<Framed<TcpStream, LengthDelimitedCodec>>>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut stream_guard = stream.lock().await;

        if stream_guard.is_some() {
            return Ok(());
        }

        match TcpStream::connect(address).await {
            Ok(socket) => {
                if let Err(e) = socket.set_nodelay(true) {
                    warn!("Failed to set TCP_NODELAY: {}", e);
                }

                info!("Connected to replica at {}", address);
                let framed = Framed::new(socket, LengthDelimitedCodec::new());
                *stream_guard = Some(framed);
                Ok(())
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    pub async fn send_message(&self, message: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.sender
            .send(message.to_vec())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }

    async fn run_connection_loop(
        connection: Connection,
        mut receiver: mpsc::Receiver<Vec<u8>>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) {
        loop {
            if Self::ensure_connected(&connection.address, &connection.stream)
                .await
                .is_err()
            {
                if !connection.try_reconnect {
                    return;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            let mut stream_guard = connection.stream.lock().await;
            let Some(ref mut framed) = *stream_guard else {
                warn!("No stream available");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            };

            tokio::select! {
                // Handle shutdown signal
                _ = &mut shutdown_rx => {
                    info!("Connection shutdown received");
                    return;
                }

                // Handle outgoing messages
                Some(message) = receiver.recv() => {
                    if let Err(e) = framed.send(Bytes::from(message)).await {
                        error!("Failed to send message: {}", e);
                        *stream_guard = None;
                        continue;
                    }
                    if let Err(e) = framed.flush().await {
                        error!("Failed to flush: {}", e);
                        *stream_guard = None;
                        continue;
                    }
                }

                // Handle incoming messages
                result = framed.next() => {
                    match result {
                        Some(Ok(bytes)) => {
                            (connection.message_handler.0)(bytes.to_vec()).await;
                        }
                        Some(Err(e)) => {
                            error!("Error reading message: {}", e);
                            *stream_guard = None;
                            continue;
                        }
                        None => {
                            error!("Connection closed");
                            *stream_guard = None;
                            continue;
                        }
                    }
                }
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self
            .shutdown
            .try_lock()
            .ok()
            .and_then(|mut guard| guard.take())
        {
            let _ = shutdown_tx.send(());
        }
    }
}
