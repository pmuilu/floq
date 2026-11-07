use thiserror::Error;

/// Unified error type for `floq` pipelines, stages, and connectors.
#[derive(Debug, Error)]
pub enum Error {
    #[error("pipeline channel closed")]
    ChannelClosed,

    #[error("invalid slot index {index} (available slots: {slots})")]
    InvalidSlot { index: usize, slots: usize },

    #[error("pipeline configuration error: {0}")]
    Configuration(String),

    #[error("pipeline task panicked or was cancelled: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),

    #[error("regex error: {0}")]
    Regex(#[from] regex::Error),

    #[cfg(any(
        feature = "adk",
        feature = "gemini",
        feature = "huggingface",
        feature = "webhook"
    ))]
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[cfg(feature = "websocket")]
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("CBOR decoding error: {0}")]
    Cbor(String),

    #[cfg(feature = "bluesky")]
    #[error("CAR archive error: {0}")]
    Car(#[from] rs_car::CarDecodeError),

    #[cfg(feature = "mqtt")]
    #[error("MQTT client error: {0}")]
    MqttClient(#[from] rumqttc::ClientError),

    #[cfg(feature = "mqtt")]
    #[error("MQTT connection error: {0}")]
    MqttConnection(#[from] rumqttc::ConnectionError),

    #[cfg(feature = "mqtt")]
    #[error("MQTT option error: {0}")]
    MqttOption(#[from] rumqttc::OptionError),

    #[error("missing environment variable `{0}`: {1}")]
    EnvVar(&'static str, #[source] std::env::VarError),

    #[error("external API error: {0}")]
    Api(String),

    #[error("{0}")]
    Custom(String),
}

/// Alias for `std::result::Result<T, floq::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
