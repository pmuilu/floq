use serde_cbor::Value;
use std::collections::BTreeMap;
use crate::pipeline::channel::Sender;
use crate::pipeline::Message;
use tracing::{debug, error};
use futures::stream::StreamExt;
use rs_car::CarReader;
use serde::Deserialize;


// Types for CBOR structure
#[derive(Debug)]
pub(crate) struct FirehoseMessage {
    ops: Vec<Operation>,
    blocks: Vec<u8>,
}

#[derive(Debug)]
struct Operation {
    action: String,
}

impl FirehoseMessage {
    pub(crate) fn from_cbor(bytes: &[u8]) -> Option<Self> {
        let mut decoder = serde_cbor::Deserializer::from_slice(bytes);
        
        // Skip first value
        let _ = Value::deserialize(&mut decoder).ok()?;
        
        // Get second value which contains our data
        let value = Value::deserialize(&mut decoder).ok()?;
        let map = match value {
            Value::Map(m) => m,
            _ => return None,
        };

        // Extract ops
        let ops = Self::extract_ops(&map)?;
        let blocks = Self::extract_blocks(&map)?;

        Some(FirehoseMessage { ops, blocks })
    }

    fn extract_ops(map: &BTreeMap<Value, Value>) -> Option<Vec<Operation>> {
        let ops = map.get(&Value::Text("ops".to_string()))?;
        let arr = match ops {
            Value::Array(arr) => arr,
            _ => return None,
        };

        let first_op = arr.first()?;
        let op_map = match first_op {
            Value::Map(m) => m,
            _ => return None,
        };

        let action = match op_map.get(&Value::Text("action".to_string()))? {
            Value::Text(action) => action.clone(),
            _ => return None,
        };

        Some(vec![Operation { action }])
    }

    fn extract_blocks(map: &BTreeMap<Value, Value>) -> Option<Vec<u8>> {
        match map.get(&Value::Text("blocks".to_string()))? {
            Value::Bytes(blocks) => Some(blocks.clone()),
            _ => None,
        }
    }

    pub(crate) async fn process_blocks(&self, output: &Sender<String>) -> Result<(), Box<dyn std::error::Error>> {
        let first_op = self.ops.first()
            .ok_or("No operations found")?;

        debug!("Processing blocks with action: {}", first_op.action);

        if first_op.action != "create" {
            return Ok(());
        }

        let mut buffer = futures::io::Cursor::new(self.blocks.as_slice());
        let mut car_reader = CarReader::new(&mut buffer, false).await?;

        while let Some(item) = car_reader.next().await {
            if let Ok((_cid, block)) = item {
                Self::process_block(&block, output);
            }
        }

        Ok(())
    }

    fn process_block(block: &[u8], output: &Sender<String>) {
        if let Ok(Value::Map(map)) = serde_cbor::from_slice::<Value>(block) {
            if let Some(Value::Text(type_str)) = map.get(&Value::Text("$type".to_string())) {
                if type_str == "app.bsky.feed.post" {
                    debug!("Found a post!");
                    if let Some(Value::Text(text)) = map.get(&Value::Text("text".to_string())) {
                        if let Err(e) = output.send(Message::new(text.clone())) {
                            error!("Failed to send text to channel: {:?}", e);
                        } else {
                            debug!("Successfully sent post to channel");
                        }
                    }
                }
            }
        }
    }
}