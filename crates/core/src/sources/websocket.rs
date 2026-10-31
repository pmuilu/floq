use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use futures::StreamExt;
use tokio_tungstenite::connect_async;
use tracing::{debug, error};
use url::Url;

/// A pipeline source that reads text frames from a WebSocket endpoint.
#[derive(Clone, Debug)]
pub struct WebSocketSource {
    url: String,
}

impl Default for WebSocketSource {
    fn default() -> Self {
        Self {
            url: "ws://localhost:8080".to_string(),
        }
    }
}

impl WebSocketSource {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl Source for WebSocketSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("WebSocketSource starting");

        let url = Url::parse(&self.url)?;
        let (ws_stream, _) = connect_async(url).await?;
        let (_, mut read) = ws_stream.split();

        debug!("WebSocket connection established");

        while let Some(message) = read.next().await {
            match message {
                Ok(msg) => {
                    if let Ok(text) = msg.to_text() {
                        if output.send(Message::new(text.to_owned())).await.is_err() {
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    return Err(e.into());
                }
            }
        }

        debug!("WebSocketSource completed");
        Ok(())
    }
}

impl_source_component!(WebSocketSource, String);
