use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::channel::{Sender, Receiver};
use std::sync::Arc;
use tokio_tungstenite::connect_async;
use futures_util::StreamExt;
use tracing::{debug, error};
use url::Url;

pub struct WebSocketSource {
    url: String,
}

impl WebSocketSource {
    pub fn new(url: String) -> Self {
        WebSocketSource { url }
    }
}

impl PipelineComponent for WebSocketSource {
    type Input = ();
    type Output = String;

    fn new() -> Self {
        WebSocketSource { 
            url: "ws://localhost:8080".to_string() 
        }
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("WebSocketSource starting");

        let url = match Url::parse(&self.url) {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to parse URL: {}", e);
                return;
            }
        };

        let (ws_stream, _) = match connect_async(url).await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to connect to WebSocket: {}", e);
                return;
            }
        };

        let (_, mut read) = ws_stream.split();

        debug!("WebSocket connection established");

        while let Some(message) = read.next().await {
            match message {
                Ok(msg) => {
                    if let Ok(text) = msg.to_text() {
                        if let Err(e) = output.send(Message::new(text.to_string())) {
                            error!("Failed to send message to output: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
            }
        }

        debug!("WebSocketSource completed");
    }
}
