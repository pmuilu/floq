use crate::error::Result;
use crate::pipeline::{Message, Sender};
use crate::sources::OutputFormat;
use ciborium::value::Value;
use futures::stream::StreamExt;
use rs_car::CarReader;
use serde::Serialize;
use std::io::Cursor;
use std::sync::Arc;
use tracing::{debug, error};

#[derive(Debug)]
pub(crate) struct FirehoseMessage {
    pub(crate) seq: Option<i64>,
    pub(crate) repo: Option<Arc<str>>,
    ops: Vec<Operation>,
    blocks: Vec<u8>,
}

#[derive(Debug)]
struct Operation {
    action: String,
    path: Option<String>,
}

#[derive(Serialize)]
struct BlueskyPostJson<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
    cid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    did: Option<&'a str>,
    text: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    langs: Vec<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<&'a str>,
    created_at: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<i64>,
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

        let seq = match map_get(&entries, "seq") {
            Some(Value::Integer(i)) => i128::from(*i).try_into().ok(),
            _ => None,
        };
        let repo = match map_get(&entries, "repo") {
            Some(Value::Text(s)) => Some(Arc::from(s.as_str())),
            _ => None,
        };
        let ops = Self::extract_ops(&entries)?;
        let blocks = Self::extract_blocks(&entries)?;

        Some(Self {
            seq,
            repo,
            ops,
            blocks,
        })
    }

    fn extract_ops(entries: &[(Value, Value)]) -> Option<Vec<Operation>> {
        let arr = match map_get(entries, "ops")? {
            Value::Array(arr) => arr,
            _ => return None,
        };

        let mut ops = Vec::with_capacity(arr.len());
        for item in arr {
            if let Value::Map(m) = item {
                if let Some(Value::Text(action)) = map_get(m, "action") {
                    let path = match map_get(m, "path") {
                        Some(Value::Text(p)) => Some(p.clone()),
                        _ => None,
                    };
                    ops.push(Operation {
                        action: action.clone(),
                        path,
                    });
                }
            }
        }

        if ops.is_empty() {
            None
        } else {
            Some(ops)
        }
    }

    fn extract_blocks(entries: &[(Value, Value)]) -> Option<Vec<u8>> {
        match map_get(entries, "blocks")? {
            Value::Bytes(blocks) => Some(blocks.clone()),
            _ => None,
        }
    }

    pub(crate) async fn process_blocks(
        &self,
        output: &Sender<String>,
        format: OutputFormat,
    ) -> Result<()> {
        let create_op = self.ops.iter().find(|op| op.action == "create");
        let Some(op) = create_op else {
            return Ok(());
        };

        debug!("Processing blocks with action: {}", op.action);

        let mut buffer = futures::io::Cursor::new(self.blocks.as_slice());
        let mut car_reader = CarReader::new(&mut buffer, false).await?;

        while let Some(item) = car_reader.next().await {
            if let Ok((cid, block)) = item {
                Self::process_block(
                    &cid.to_string(),
                    &block,
                    self.repo.as_ref(),
                    op.path.as_deref(),
                    self.seq,
                    format,
                    output,
                )
                .await;
            }
        }

        Ok(())
    }

    pub(crate) async fn process_block(
        cid_str: &str,
        block: &[u8],
        repo: Option<&Arc<str>>,
        op_path: Option<&str>,
        seq: Option<i64>,
        format: OutputFormat,
        output: &Sender<String>,
    ) {
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
                    let payload = match format {
                        OutputFormat::Text => text.clone(),
                        OutputFormat::Json => {
                            let langs = match map_get(&entries, "langs") {
                                Some(Value::Array(arr)) => arr
                                    .iter()
                                    .filter_map(|v| match v {
                                        Value::Text(s) => Some(s.as_str()),
                                        _ => None,
                                    })
                                    .collect(),
                                _ => Vec::new(),
                            };
                            let reply_to = map_get(&entries, "reply")
                                .and_then(|v| match v {
                                    Value::Map(m) => map_get(m, "parent"),
                                    _ => None,
                                })
                                .and_then(|v| match v {
                                    Value::Map(m) => map_get(m, "uri"),
                                    _ => None,
                                })
                                .and_then(|v| match v {
                                    Value::Text(s) => Some(s.as_str()),
                                    _ => None,
                                });
                            let uri = match (repo, op_path) {
                                (Some(did), Some(path)) => Some(format!("at://{did}/{path}")),
                                _ => None,
                            };
                            let record = BlueskyPostJson {
                                uri,
                                cid: cid_str.to_string(),
                                did: repo.map(|r| r.as_ref()),
                                text: text.as_str(),
                                langs,
                                reply_to,
                                created_at: created_at.as_str(),
                                seq,
                            };
                            serde_json::to_string(&record).unwrap_or_else(|_| text.clone())
                        }
                    };

                    let mut msg = Message::with_event_time(payload, unix_ms);
                    if let Some(did) = repo {
                        msg = msg.with_source(Arc::clone(did));
                    }

                    if let Err(e) = output.send(msg).await {
                        error!("Failed to send post to channel: {}", e);
                    } else {
                        debug!("Successfully sent post to channel");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::channel::bounded;

    #[test]
    fn test_from_cbor_with_tag_42() {
        let mut buf = Vec::new();
        let header = Value::Map(vec![
            (Value::Text("op".into()), Value::Integer(1.into())),
            (Value::Text("t".into()), Value::Text("#commit".into())),
        ]);
        let body = Value::Map(vec![
            (Value::Text("seq".into()), Value::Integer(999.into())),
            (
                Value::Text("repo".into()),
                Value::Text("did:plc:alice123".into()),
            ),
            (
                Value::Text("commit".into()),
                Value::Tag(42, Box::new(Value::Bytes(vec![0, 1, 85, 18, 32]))),
            ),
            (
                Value::Text("ops".into()),
                Value::Array(vec![Value::Map(vec![
                    (Value::Text("action".into()), Value::Text("create".into())),
                    (
                        Value::Text("path".into()),
                        Value::Text("app.bsky.feed.post/3kxyz".into()),
                    ),
                    (
                        Value::Text("cid".into()),
                        Value::Tag(42, Box::new(Value::Bytes(vec![0, 1, 85, 18, 32]))),
                    ),
                ])]),
            ),
            (Value::Text("blocks".into()), Value::Bytes(vec![1, 2, 3])),
        ]);
        ciborium::ser::into_writer(&header, &mut buf).unwrap();
        ciborium::ser::into_writer(&body, &mut buf).unwrap();

        let parsed = FirehoseMessage::from_cbor(&buf).expect("Failed to parse CBOR with Tag 42");
        assert_eq!(parsed.seq, Some(999));
        assert_eq!(parsed.repo.as_deref(), Some("did:plc:alice123"));
        assert_eq!(
            parsed.ops[0].path.as_deref(),
            Some("app.bsky.feed.post/3kxyz")
        );
    }

    #[tokio::test]
    async fn test_process_block_json_and_text_formats() {
        let mut block = Vec::new();
        let post_map = Value::Map(vec![
            (
                Value::Text("$type".into()),
                Value::Text("app.bsky.feed.post".into()),
            ),
            (
                Value::Text("text".into()),
                Value::Text("Hello Rust streaming!".into()),
            ),
            (
                Value::Text("createdAt".into()),
                Value::Text("2026-11-07T12:00:00Z".into()),
            ),
            (
                Value::Text("langs".into()),
                Value::Array(vec![Value::Text("en".into())]),
            ),
        ]);
        ciborium::ser::into_writer(&post_map, &mut block).unwrap();

        let (sender, mut rx) = bounded(4);
        let repo: Arc<str> = Arc::from("did:plc:alice123");

        FirehoseMessage::process_block(
            "bafyreiblockcid",
            &block,
            Some(&repo),
            Some("app.bsky.feed.post/3kxyz"),
            Some(1042),
            OutputFormat::Json,
            &sender,
        )
        .await;

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.source_id.as_deref(), Some("did:plc:alice123"));
        let parsed: serde_json::Value = serde_json::from_str(&msg.payload).unwrap();
        assert_eq!(
            parsed["uri"],
            "at://did:plc:alice123/app.bsky.feed.post/3kxyz"
        );
        assert_eq!(parsed["cid"], "bafyreiblockcid");
        assert_eq!(parsed["did"], "did:plc:alice123");
        assert_eq!(parsed["text"], "Hello Rust streaming!");
        assert_eq!(parsed["langs"][0], "en");
        assert_eq!(parsed["seq"], 1042);
    }
}
