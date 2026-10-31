pub mod channel_sink;
#[cfg(feature = "gemini")]
pub mod gemini_embeddings;
#[cfg(feature = "huggingface")]
pub mod huggingface_embeddings;
pub mod printer_sink;
#[cfg(feature = "webhook")]
pub mod webhook_sink;

pub use channel_sink::ChannelSink;
#[cfg(feature = "gemini")]
pub use gemini_embeddings::GeminiEmbeddings;
#[cfg(feature = "huggingface")]
pub use huggingface_embeddings::HuggingfaceEmbeddings;
pub use printer_sink::PrinterSink;
#[cfg(feature = "webhook")]
pub use webhook_sink::WebhookSink;
