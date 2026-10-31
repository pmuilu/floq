use crate::error::{Error, Result};
use crate::impl_source_component;
use crate::pipeline::{Message as FloqMessage, Sender, Source};
use crate::sources::reconnect::ReconnectPolicy;
use futures::StreamExt;
use serde::Deserialize;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use url::Url;

#[derive(Debug, Deserialize)]
struct MastodonEvent {
    event: String,
    payload: String,
}

#[derive(Debug, Deserialize)]
struct MastodonStatus {
    content: String,
    created_at: String,
}

/// Streams public status updates from a Mastodon server's WebSocket streaming API
/// with configurable automatic reconnection.
#[derive(Clone, Debug)]
pub struct MastodonFirehoseSource {
    server_url: String,
    access_token: Option<String>,
    reconnect: ReconnectPolicy,
}

impl Default for MastodonFirehoseSource {
    fn default() -> Self {
        Self::new("https://mastodon.social")
    }
}

impl MastodonFirehoseSource {
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            access_token: None,
            reconnect: ReconnectPolicy::default(),
        }
    }

    pub fn with_token(server_url: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            access_token: Some(access_token.into()),
            reconnect: ReconnectPolicy::default(),
        }
    }

    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    pub fn without_reconnect(mut self) -> Self {
        self.reconnect = ReconnectPolicy::disabled();
        self
    }

    fn build_websocket_url(&self) -> Result<Url> {
        let mut url = Url::parse(&self.server_url)?;

        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme).map_err(|_| {
            Error::Configuration(format!("Invalid URL scheme in {}", self.server_url))
        })?;

        let domain = url.domain().unwrap_or("mastodon.social").to_owned();
        let streaming_domain = format!("streaming.{}", domain);
        url.set_host(Some(&streaming_domain))?;
        url.set_path("/api/v1/streaming");

        url.query_pairs_mut()
            .append_pair("stream", "public")
            .append_pair("access_token", self.access_token.as_deref().unwrap_or(""));

        debug!("Final Mastodon WebSocket URL: {}", url);
        Ok(url)
    }
}

impl Source for MastodonFirehoseSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("MastodonFirehoseSource starting");
        let url = self.build_websocket_url()?;
        let mut attempt: usize = 0;

        loop {
            if output.is_closed() {
                break;
            }

            info!("Connecting to Mastodon streaming API at {}", url);
            match connect_async(url.clone()).await {
                Ok((ws_stream, _)) => {
                    attempt = 0;
                    let (_write, mut read) = ws_stream.split();

                    while let Some(msg_result) = read.next().await {
                        if output.is_closed() {
                            return Ok(());
                        }
                        match msg_result {
                            Ok(Message::Text(text)) => {
                                if let Ok(event) = serde_json::from_str::<MastodonEvent>(&text) {
                                    if event.event == "update" {
                                        if let Ok(status) =
                                            serde_json::from_str::<MastodonStatus>(&event.payload)
                                        {
                                            if let Ok(timestamp) =
                                                chrono::DateTime::parse_from_rfc3339(
                                                    &status.created_at,
                                                )
                                            {
                                                let unix_ms =
                                                    timestamp.timestamp_millis().max(0) as u64;
                                                if output
                                                    .send(FloqMessage::with_event_time(
                                                        status.content,
                                                        unix_ms,
                                                    ))
                                                    .await
                                                    .is_err()
                                                {
                                                    return Ok(());
                                                }
                                            } else {
                                                error!(
                                                    "Failed to parse timestamp: {}",
                                                    status.created_at
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                info!("Received close frame from Mastodon");
                                break;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Mastodon WebSocket error: {}", e);
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
                    warn!("Failed to connect to Mastodon: {}", e);
                }
            }

            if !self.reconnect.should_retry(attempt) {
                break;
            }
            let delay = self.reconnect.backoff_for_attempt(attempt);
            attempt += 1;
            info!("Reconnecting to Mastodon in {:?}...", delay);
            tokio::time::sleep(delay).await;
        }

        debug!("MastodonFirehoseSource completed");
        Ok(())
    }
}

impl_source_component!(MastodonFirehoseSource, String);
