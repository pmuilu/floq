use crate::error::{Error, Result};
use crate::impl_source_component;
use crate::pipeline::{Message as FloqMessage, Sender, Source};
use crate::sources::reconnect::ReconnectPolicy;
use crate::sources::OutputFormat;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use url::Url;

#[derive(Debug, Deserialize)]
struct MastodonEvent {
    event: String,
    payload: String,
}

#[derive(Debug, Default, Deserialize)]
struct MastodonAccount {
    #[serde(default)]
    acct: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MastodonStatus {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    uri: Option<String>,
    #[serde(default)]
    url: Option<String>,
    content: String,
    created_at: String,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    account: MastodonAccount,
}

#[derive(Serialize)]
struct MastodonStatusJson<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acct: Option<&'a str>,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    language: Option<&'a str>,
    created_at: &'a str,
}

/// Streams public status updates from a Mastodon server's WebSocket streaming API
/// with configurable automatic reconnection and optional structured JSON output.
#[derive(Clone, Debug)]
pub struct MastodonFirehoseSource {
    server_url: String,
    access_token: Option<String>,
    reconnect: ReconnectPolicy,
    output_format: OutputFormat,
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
            output_format: OutputFormat::Text,
        }
    }

    pub fn with_token(server_url: impl Into<String>, access_token: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            access_token: Some(access_token.into()),
            reconnect: ReconnectPolicy::default(),
            output_format: OutputFormat::Text,
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

    /// Sets the output format (`OutputFormat::Text` or `OutputFormat::Json`).
    pub fn with_output_format(mut self, format: OutputFormat) -> Self {
        self.output_format = format;
        self
    }

    /// Configures the source to emit structured JSON strings (`id`, `uri`, `url`, `acct`, `text`, `language`, `created_at`).
    pub fn with_json_output(mut self) -> Self {
        self.output_format = OutputFormat::Json;
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
                                                let acct = status.account.acct.as_deref();
                                                let payload = match self.output_format {
                                                    OutputFormat::Text => status.content,
                                                    OutputFormat::Json => {
                                                        let rec = MastodonStatusJson {
                                                            id: status.id.as_deref(),
                                                            uri: status.uri.as_deref(),
                                                            url: status.url.as_deref(),
                                                            acct,
                                                            text: &status.content,
                                                            language: status.language.as_deref(),
                                                            created_at: &status.created_at,
                                                        };
                                                        serde_json::to_string(&rec)
                                                            .unwrap_or(status.content)
                                                    }
                                                };

                                                let mut msg =
                                                    FloqMessage::with_event_time(payload, unix_ms);
                                                if let Some(a) = acct {
                                                    if !a.is_empty() {
                                                        msg = msg.with_source(Arc::from(a));
                                                    }
                                                }

                                                if output.send(msg).await.is_err() {
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
