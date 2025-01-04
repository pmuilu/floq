use std::time::{SystemTime, UNIX_EPOCH};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct Message<T> {
    pub payload: T,
    pub event_timestamp: u64,  // Unix timestamp in milliseconds
    pub ingestion_timestamp: u64,
    pub source_id: Option<String>,
    // Add other metadata fields as needed
}

impl<T> Message<T> {
    pub fn new(payload: T) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Message {
            payload,
            event_timestamp: now,
            ingestion_timestamp: now,
            source_id: None,
        }
    }

    pub fn with_event_time(payload: T, event_time: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Message {
            payload,
            event_timestamp: event_time,
            ingestion_timestamp: now,
            source_id: None,
        }
    }

    pub fn with_source(mut self, source_id: impl Into<String>) -> Self {
        self.source_id = Some(source_id.into());
        self
    }
} 