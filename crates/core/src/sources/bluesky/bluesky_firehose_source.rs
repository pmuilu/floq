use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Sender, Source};
use crate::sources::bluesky::firehose_message::FirehoseMessage;
use futures::stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};

const DEFAULT_BLUESKY_FIREHOSE_URL: &str =
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";

/// Streams live repository commit post records from the Bluesky ATProto firehose.
#[derive(Clone, Debug)]
pub struct BlueskyFirehoseSource {
    url: String,
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
        }
    }

    pub fn with_url(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl Source for BlueskyFirehoseSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        info!("Starting BlueskyFirehoseSource");
        let (ws_stream, _) = connect_async(&self.url).await?;
        info!("Connected to Bluesky WebSocket");

        let (_, mut read) = ws_stream.split();

        while let Some(frame) = read.next().await {
            match frame {
                Ok(Message::Binary(bytes)) => {
                    debug!("Processing binary message of {} bytes", bytes.len());
                    if let Some(firehose_msg) = FirehoseMessage::from_cbor(&bytes) {
                        if let Err(e) = firehose_msg.process_blocks(&output).await {
                            error!("Error processing blocks: {}", e);
                        }
                    } else {
                        debug!("Skipped non-commit CBOR frame");
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("Bluesky firehose connection closed");
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    error!("Bluesky WebSocket error: {}", e);
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }
}

impl_source_component!(BlueskyFirehoseSource, String);
