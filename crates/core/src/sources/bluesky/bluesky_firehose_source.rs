use crate::pipeline::{PipelineComponent, ComponentContext};
use futures::stream::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use crate::pipeline::channel::{Sender, Receiver};
use tracing::{info, debug, error};
use crate::sources::bluesky::firehose_message::FirehoseMessage;
use std::sync::Arc;


pub struct BlueskyFirehoseSource {
    url: String
}

impl PipelineComponent for BlueskyFirehoseSource {
    type Input = (); // No input for the source
    type Output = String;

    fn new() -> Self {
        BlueskyFirehoseSource {
            url: "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos".to_string()
        }
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        info!("Starting BlueskyFirehoseSource");
        let (ws_stream, _) = connect_async(&self.url).await.expect("Failed to connect");
        info!("Connected to WebSocket");

        let (_, mut read) = ws_stream.split();

        while let Some(Ok(msg)) = read.next().await {
            debug!("Received WebSocket message");
            if let Message::Binary(bytes) = msg {
                debug!("Processing binary message of {} bytes", bytes.len());
                if let Some(firehose_msg) = FirehoseMessage::from_cbor(&bytes) {
                    debug!("Successfully parsed CBOR message");
                    if let Err(e) = firehose_msg.process_blocks(&output).await {
                        error!("Error processing blocks: {:?}", e);
                    }
                } else {
                    error!("Failed to parse CBOR message");
                }
            }
        }
    }
}