use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Sender, Source};
use crate::sources::bluesky::firehose_message::FirehoseMessage;
use crate::sources::reconnect::ReconnectPolicy;
use futures::stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;

const DEFAULT_BLUESKY_FIREHOSE_URL: &str =
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";

/// Streams live repository commit post records from the Bluesky ATProto firehose
/// with automatic cursor-resuming reconnection.
#[derive(Clone, Debug)]
pub struct BlueskyFirehoseSource {
    url: String,
    reconnect: ReconnectPolicy,
}

impl Default for BlueskyFirehoseSource {
    fn default() -> Self {
        Self::new()
    }
}

impl BlueskyFirehoseSource {
    pub fn new() -> Self {
        Self {
            url: DEFAULT_BLUESKY_FIREHOSE_URL.to_string(),
            reconnect: ReconnectPolicy::default(),
        }
    }

    pub fn with_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
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

    fn endpoint_url(&self, cursor: Option<i64>) -> Result<Url> {
        let mut url = Url::parse(&self.url)?;
        if let Some(seq) = cursor {
            url.query_pairs_mut().append_pair("cursor", &seq.to_string());
        }
        Ok(url)
    }
}

impl Source for BlueskyFirehoseSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        info!("Starting BlueskyFirehoseSource");
        let mut last_seq: Option<i64> = None;
        let mut attempt: usize = 0;

        loop {
            if output.is_closed() {
                break;
            }

            let url = self.endpoint_url(last_seq)?;
            match connect_async(url).await {
                Ok((ws_stream, _)) => {
                    info!("Connected to Bluesky WebSocket (cursor={:?})", last_seq);
                    attempt = 0;
                    let (_, mut read) = ws_stream.split();

                    while let Some(frame) = read.next().await {
                        if output.is_closed() {
                            return Ok(());
                        }
                        match frame {
                            Ok(Message::Binary(bytes)) => {
                                debug!("Processing binary message of {} bytes", bytes.len());
                                if let Some(firehose_msg) = FirehoseMessage::from_cbor(&bytes) {
                                    if let Some(seq) = firehose_msg.seq {
                                        last_seq = Some(seq);
                                    }
                                    if let Err(e) = firehose_msg.process_blocks(&output).await {
                                        error!("Error processing blocks: {}", e);
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                info!("Bluesky firehose connection closed by remote");
                                break;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Bluesky WebSocket error: {}", e);
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
                    warn!("Failed to connect to Bluesky firehose: {}", e);
                }
            }

            if !self.reconnect.should_retry(attempt) {
                break;
            }
            let delay = self.reconnect.backoff_for_attempt(attempt);
            attempt += 1;
            info!("Reconnecting to Bluesky firehose in {:?}...", delay);
            tokio::time::sleep(delay).await;
        }

        Ok(())
    }
}

impl_source_component!(BlueskyFirehoseSource, String);
