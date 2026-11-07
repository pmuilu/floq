#[cfg(feature = "sqlite")]
use crate::checkpoint::SqliteCheckpointStore;
use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Sender, Source};
use crate::sources::bluesky::firehose_message::FirehoseMessage;
use crate::sources::reconnect::ReconnectPolicy;
use crate::sources::OutputFormat;
use futures::stream::StreamExt;
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use url::Url;

const DEFAULT_BLUESKY_FIREHOSE_URL: &str =
    "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos";
const DEFAULT_CHECKPOINT_KEY: &str = "bluesky:firehose";

/// Streams live repository commit post records from the Bluesky ATProto firehose
/// with automatic cursor-resuming reconnection, optional structured JSON output,
/// and optional SQLite cursor checkpointing.
#[derive(Clone, Debug)]
pub struct BlueskyFirehoseSource {
    url: String,
    reconnect: ReconnectPolicy,
    output_format: OutputFormat,
    #[cfg(feature = "sqlite")]
    checkpoint: Option<(SqliteCheckpointStore, String)>,
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
            output_format: OutputFormat::Text,
            #[cfg(feature = "sqlite")]
            checkpoint: None,
        }
    }

    pub fn with_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Self::new()
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

    /// Configures the source to emit structured JSON strings (`uri`, `cid`, `did`, `text`, `langs`, `reply_to`, `created_at`, `seq`).
    pub fn with_json_output(mut self) -> Self {
        self.output_format = OutputFormat::Json;
        self
    }

    /// Attaches a SQLite checkpoint store to persist and resume the ATProto `seq` cursor across restarts.
    #[cfg(feature = "sqlite")]
    pub fn with_checkpoint(
        mut self,
        store: SqliteCheckpointStore,
        key: impl Into<String>,
    ) -> Self {
        self.checkpoint = Some((store, key.into()));
        self
    }

    /// Attaches a SQLite checkpoint store using the default key `"bluesky:firehose"`.
    #[cfg(feature = "sqlite")]
    pub fn with_default_checkpoint(self, store: SqliteCheckpointStore) -> Self {
        self.with_checkpoint(store, DEFAULT_CHECKPOINT_KEY)
    }

    fn endpoint_url(&self, cursor: Option<i64>) -> Result<Url> {
        let mut url = Url::parse(&self.url)?;
        if let Some(seq) = cursor {
            url.query_pairs_mut().append_pair("cursor", &seq.to_string());
        }
        Ok(url)
    }

    #[cfg(feature = "sqlite")]
    fn load_checkpoint(&self) -> Result<Option<i64>> {
        if let Some((store, key)) = &self.checkpoint {
            store.get_i64(key)
        } else {
            Ok(None)
        }
    }

    #[cfg(not(feature = "sqlite"))]
    fn load_checkpoint(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    #[cfg(feature = "sqlite")]
    fn save_checkpoint(&self, seq: Option<i64>) {
        if let (Some((store, key)), Some(seq_val)) = (&self.checkpoint, seq) {
            if let Err(e) = store.set_i64(key, seq_val) {
                warn!("Failed to persist Bluesky cursor checkpoint: {}", e);
            }
        }
    }

    #[cfg(not(feature = "sqlite"))]
    fn save_checkpoint(&self, _seq: Option<i64>) {}
}

impl Source for BlueskyFirehoseSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        info!("Starting BlueskyFirehoseSource");
        let mut last_seq: Option<i64> = self.load_checkpoint()?;
        let mut attempt: usize = 0;
        let mut last_checkpoint_save = Instant::now();
        let mut frames_since_save: usize = 0;

        loop {
            if output.is_closed() {
                self.save_checkpoint(last_seq);
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
                            self.save_checkpoint(last_seq);
                            return Ok(());
                        }
                        match frame {
                            Ok(Message::Binary(bytes)) => {
                                debug!("Processing binary message of {} bytes", bytes.len());
                                if let Some(firehose_msg) = FirehoseMessage::from_cbor(&bytes) {
                                    if let Some(seq) = firehose_msg.seq {
                                        last_seq = Some(seq);
                                        frames_since_save += 1;
                                        if frames_since_save >= 100
                                            || last_checkpoint_save.elapsed()
                                                >= Duration::from_secs(2)
                                        {
                                            self.save_checkpoint(last_seq);
                                            frames_since_save = 0;
                                            last_checkpoint_save = Instant::now();
                                        }
                                    }
                                    if let Err(e) = firehose_msg
                                        .process_blocks(&output, self.output_format)
                                        .await
                                    {
                                        error!("Error processing blocks: {}", e);
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                info!("Bluesky firehose connection closed by remote");
                                self.save_checkpoint(last_seq);
                                break;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("Bluesky WebSocket error: {}", e);
                                self.save_checkpoint(last_seq);
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

        self.save_checkpoint(last_seq);
        Ok(())
    }
}

impl_source_component!(BlueskyFirehoseSource, String);
