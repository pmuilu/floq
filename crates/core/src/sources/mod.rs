#[cfg(feature = "bluesky")]
pub mod bluesky;
pub mod file_source;
#[cfg(feature = "mastodon")]
pub mod mastodon;
#[cfg(feature = "mqtt")]
pub mod websocket_mqtt;
#[cfg(feature = "websocket")]
pub mod websocket;
pub mod reconnect;

/// Output serialization format for built-in streaming sources.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OutputFormat {
    /// Emit only the primary human-readable text payload (default).
    #[default]
    Text,
    /// Emit a canonical compact JSON object containing payload text and source metadata.
    Json,
}

#[cfg(feature = "bluesky")]
pub use bluesky::bluesky_firehose_source::BlueskyFirehoseSource;
pub use file_source::FileSource;
#[cfg(feature = "mastodon")]
pub use mastodon::mastodon_firehose_source::MastodonFirehoseSource;
pub use reconnect::ReconnectPolicy;
#[cfg(feature = "websocket")]
pub use websocket::WebSocketSource;
#[cfg(feature = "mqtt")]
pub use websocket_mqtt::WebSocketMqttSource;
