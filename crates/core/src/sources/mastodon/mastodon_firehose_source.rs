use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::{Receiver, Sender};
use futures::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{debug, error, info};
use url::Url;


#[derive(Debug, Deserialize)]
struct MastodonEvent {
    event: String,
    payload: String,
}

#[derive(Debug, Deserialize)]
struct MastodonStatus {
    content: String,
}

pub struct MastodonFirehoseSource {
    server_url: String,
    access_token: Option<String>,
}

impl MastodonFirehoseSource {
    pub fn new(server_url: String) -> Self {
        MastodonFirehoseSource {
            server_url,
            access_token: None,
        }
    }

    pub fn with_token(server_url: String, access_token: String) -> Self {
        MastodonFirehoseSource {
            server_url,
            access_token: Some(access_token),
        }
    }

    fn build_websocket_url(&self) -> Result<Url, url::ParseError> {
        let mut url = Url::parse(&self.server_url)?;
        
        // Convert http(s) to ws(s)
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        debug!("Using scheme: {}", scheme);
        url.set_scheme(scheme).unwrap();
        
        // Extract the domain
        let domain = url.domain().unwrap_or("mastodon.social");
        debug!("Using domain: {}", domain);
        
        // Construct the streaming URL
        let streaming_domain = format!("streaming.{}", domain);
        debug!("Using streaming domain: {}", streaming_domain);
        url.set_host(Some(&streaming_domain))?;
        
        // Set the streaming API path
        url.set_path("/api/v1/streaming");
        
        // Add stream type and access token
        url.query_pairs_mut()
            .append_pair("stream", "public")
            .append_pair("access_token", self.access_token.as_deref().unwrap_or(""));
        
        debug!("Final WebSocket URL: {}", url);
        Ok(url)
    }
}

impl PipelineComponent for MastodonFirehoseSource {
    type Input = ();
    type Output = String;

    fn new() -> Self {
        MastodonFirehoseSource::new("https://mastodon.social".to_string())
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("MastodonFirehoseSource starting");

        let url = match self.build_websocket_url() {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to build WebSocket URL: {:?}", e);
                return;
            }
        };

        info!("Connecting to Mastodon streaming API at {}", url);

        let (ws_stream, _) = match connect_async(url).await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to connect to Mastodon: {:?}", e);
                return;
            }
        };

        let (_write, mut read) = ws_stream.split();

        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(msg) => {
                    match msg {
                        Message::Text(text) => {
                            debug!("Received text: {}", text);
                            // Try to parse as Mastodon event
                            if let Ok(event) = serde_json::from_str::<MastodonEvent>(&text) {
                                if event.event == "update" {
                                    // Parse the payload as a status
                                    if let Ok(status) = serde_json::from_str::<MastodonStatus>(&event.payload) {
                                        debug!("Parsed status: {}", status.content);
                                        if let Err(e) = output.send(status.content) {
                                            error!("Failed to send status: {:?}", e);
                                            break;
                                        }
                                    } else {
                                        debug!("Failed to parse payload as status: {}", event.payload);
                                    }
                                } else {
                                    debug!("Ignoring non-update event: {}", event.event);
                                }
                            } else {
                                debug!("Failed to parse message as event: {}", text);
                            }
                        }
                        Message::Close(_) => {
                            info!("Received close frame from Mastodon");
                            break;
                        }
                        _ => {
                            debug!("Ignoring non-text message");
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error: {:?}", e);
                    break;
                }
            }
        }

        debug!("MastodonFirehoseSource completed");
    }
} 