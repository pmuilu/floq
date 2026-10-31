use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use crate::sources::reconnect::ReconnectPolicy;
use futures::StreamExt;
use tokio_tungstenite::connect_async;
use tracing::{debug, info, warn};
use url::Url;

/// A pipeline source that reads text frames from a WebSocket endpoint.
#[derive(Clone, Debug)]
pub struct WebSocketSource {
    url: String,
    reconnect: ReconnectPolicy,
}

impl Default for WebSocketSource {
    fn default() -> Self {
        Self {
            url: "ws://localhost:8080".to_string(),
            reconnect: ReconnectPolicy::disabled(),
        }
    }
}

impl WebSocketSource {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            reconnect: ReconnectPolicy::disabled(),
        }
    }

    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }
}

impl Source for WebSocketSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("WebSocketSource starting");
        let url = Url::parse(&self.url)?;
        let mut attempt: usize = 0;

        loop {
            if output.is_closed() {
                break;
            }

            match connect_async(url.clone()).await {
                Ok((ws_stream, _)) => {
                    attempt = 0;
                    let (_, mut read) = ws_stream.split();
                    debug!("WebSocket connection established");

                    while let Some(message) = read.next().await {
                        match message {
                            Ok(msg) => {
                                if let Ok(text) = msg.to_text() {
                                    if output.send(Message::new(text.to_owned())).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("WebSocket error: {}", e);
                                if !self.reconnect.should_retry(attempt) {
                                    return Err(e.into());
                                }
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    if !self.reconnect.should_retry(attempt) {
                        return Err(e.into());
                    }
                    warn!("Failed to connect to WebSocket: {}", e);
                }
            }

            if !self.reconnect.should_retry(attempt) {
                break;
            }
            let delay = self.reconnect.backoff_for_attempt(attempt);
            attempt += 1;
            info!("Reconnecting WebSocket in {:?}...", delay);
            tokio::time::sleep(delay).await;
        }

        debug!("WebSocketSource completed");
        Ok(())
    }
}

impl_source_component!(WebSocketSource, String);
