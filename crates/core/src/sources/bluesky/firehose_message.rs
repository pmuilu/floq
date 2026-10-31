use crate::error::Result;
use crate::pipeline::{Message, Sender};
use ciborium::value::Value;
use futures::stream::StreamExt;
use rs_car::CarReader;
use std::io::Cursor;
use tracing::{debug, error};

#[derive(Debug)]
pub(crate) struct FirehoseMessage {
    ops: Vec<Operation>,
    blocks: Vec<u8>,
}

#[derive(Debug)]
struct Operation {
    action: String,
}

fn map_get<'a>(entries: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    entries.iter().find_map(|(k, v)| match k {
        Value::Text(s) if s == key => Some(v),
        _ => None,
    })
}

impl FirehoseMessage {
    pub(crate) fn from_cbor(bytes: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(bytes);

        // First CBOR item is the frame header; second is the commit payload map.
        let _header: Value = ciborium::de::from_reader(&mut cursor).ok()?;
        let body: Value = ciborium::de::from_reader(&mut cursor).ok()?;

        let entries = match body {
            Value::Map(m) => m,
            _ => return None,
        };

        let ops = Self::extract_ops(&entries)?;
        let blocks = Self::extract_blocks(&entries)?;

        Some(Self { ops, blocks })
    }

    fn extract_ops(entries: &[(Value, Value)]) -> Option<Vec<Operation>> {
        let arr = match map_get(entries, "ops")? {
            Value::Array(arr) => arr,
            _ => return None,
        };

        let first_op = match arr.first()? {
            Value::Map(m) => m,
            _ => return None,
        };

        let action = match map_get(first_op, "action")? {
            Value::Text(action) => action.clone(),
            _ => return None,
        };

        Some(vec![Operation { action }])
    }

    fn extract_blocks(entries: &[(Value, Value)]) -> Option<Vec<u8>> {
        match map_get(entries, "blocks")? {
            Value::Bytes(blocks) => Some(blocks.clone()),
            _ => None,
        }
    }

    pub(crate) async fn process_blocks(&self, output: &Sender<String>) -> Result<()> {
        let Some(first_op) = self.ops.first() else {
            return Ok(());
        };

        debug!("Processing blocks with action: {}", first_op.action);

        if first_op.action != "create" {
            return Ok(());
        }

        let mut buffer = futures::io::Cursor::new(self.blocks.as_slice());
        let mut car_reader = CarReader::new(&mut buffer, false).await?;

        while let Some(item) = car_reader.next().await {
            if let Ok((_cid, block)) = item {
                Self::process_block(&block, output).await;
            }
        }

        Ok(())
    }

    async fn process_block(block: &[u8], output: &Sender<String>) {
        let Ok(Value::Map(entries)) = ciborium::de::from_reader::<Value, _>(block) else {
            return;
        };

        let Some(Value::Text(type_str)) = map_get(&entries, "$type") else {
            return;
        };

        if type_str == "app.bsky.feed.post" {
            debug!("Found a Bluesky post");
            if let (Some(Value::Text(text)), Some(Value::Text(created_at))) =
                (map_get(&entries, "text"), map_get(&entries, "createdAt"))
            {
                if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(created_at) {
                    let unix_ms = timestamp.timestamp_millis().max(0) as u64;
                    if let Err(e) = output
                        .send(Message::with_event_time(text.clone(), unix_ms))
                        .await
                    {
                        error!("Failed to send text to channel: {}", e);
                    } else {
                        debug!("Successfully sent post to channel");
                    }
                }
            }
        }
    }
}
