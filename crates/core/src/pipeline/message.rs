use serde::Serialize;
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the current Unix epoch timestamp in milliseconds.
#[inline]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn serialize_source_id<S>(source_id: &Option<Arc<str>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    source_id.as_deref().serialize(serializer)
}

/// A timestamped pipeline envelope carrying a strongly-typed `payload` and stream metadata.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct Message<T> {
    pub payload: T,
    /// Event creation timestamp in Unix milliseconds.
    pub event_timestamp: u64,
    /// Pipeline ingestion timestamp in Unix milliseconds.
    pub ingestion_timestamp: u64,
    /// Optional identifier of the upstream source that produced this message.
    #[serde(serialize_with = "serialize_source_id")]
    pub source_id: Option<Arc<str>>,
}

impl<T: fmt::Display> fmt::Display for Message<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.payload)
    }
}

impl<T> Message<T> {
    /// Creates a new [`Message`] with both `event_timestamp` and `ingestion_timestamp` set to now.
    pub fn new(payload: T) -> Self {
        let now = now_millis();
        Self {
            payload,
            event_timestamp: now,
            ingestion_timestamp: now,
            source_id: None,
        }
    }

    /// Replaces the payload while preserving `event_timestamp`, `ingestion_timestamp`, and `source_id`.
    #[inline]
    pub fn with_new_payload<U>(self, new_payload: U) -> Message<U> {
        Message {
            payload: new_payload,
            event_timestamp: self.event_timestamp,
            ingestion_timestamp: self.ingestion_timestamp,
            source_id: self.source_id,
        }
    }

    /// Maps the payload using `f` while preserving message metadata.
    #[inline]
    pub fn map<U, F>(self, f: F) -> Message<U>
    where
        F: FnOnce(T) -> U,
    {
        let Message {
            payload,
            event_timestamp,
            ingestion_timestamp,
            source_id,
        } = self;
        Message {
            payload: f(payload),
            event_timestamp,
            ingestion_timestamp,
            source_id,
        }
    }

    /// Creates a [`Message`] with an explicit `event_time` (in Unix milliseconds).
    pub fn with_event_time(payload: T, event_time: u64) -> Self {
        Self {
            payload,
            event_timestamp: event_time,
            ingestion_timestamp: now_millis(),
            source_id: None,
        }
    }

    /// Attaches a `source_id` to this message.
    pub fn with_source(mut self, source_id: impl Into<Arc<str>>) -> Self {
        self.source_id = Some(source_id.into());
        self
    }
}
