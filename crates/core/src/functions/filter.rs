use crate::error::Result;
use crate::pipeline::{Operator, PipelineComponent, Receiver, Sender};
use regex::Regex;
use std::sync::Arc;
use tracing::debug;

type PredicateFn<T> = dyn Fn(&T) -> bool + Send + Sync;

enum FilterCondition<T> {
    Regex(Regex),
    JsonRegex { pointer: Arc<str>, regex: Regex },
    Predicate(Arc<PredicateFn<T>>),
}

impl<T> Clone for FilterCondition<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Regex(r) => Self::Regex(r.clone()),
            Self::JsonRegex { pointer, regex } => Self::JsonRegex {
                pointer: Arc::clone(pointer),
                regex: regex.clone(),
            },
            Self::Predicate(p) => Self::Predicate(Arc::clone(p)),
        }
    }
}

fn normalize_json_pointer(path: &str) -> Arc<str> {
    if path.is_empty() || path.starts_with('/') {
        Arc::from(path)
    } else {
        Arc::from(format!("/{path}"))
    }
}

fn match_json_pointer(raw_json: &str, pointer: &str, regex: &Regex) -> bool {
    let Ok(val) = serde_json::from_str::<serde_json::Value>(raw_json) else {
        return false;
    };
    let Some(target) = val.pointer(pointer) else {
        return false;
    };
    match target {
        serde_json::Value::String(s) => regex.is_match(s),
        serde_json::Value::Array(items) => items.iter().any(|item| match item {
            serde_json::Value::String(s) => regex.is_match(s),
            other => regex.is_match(&other.to_string()),
        }),
        serde_json::Value::Null => false,
        other => regex.is_match(&other.to_string()),
    }
}

/// Filtering operator that forwards messages matching a regex pattern, a JSON-pointer field regex,
/// or a custom predicate closure.
pub struct Filter<T = String> {
    condition: FilterCondition<T>,
}

impl<T> Clone for Filter<T> {
    fn clone(&self) -> Self {
        Self {
            condition: self.condition.clone(),
        }
    }
}

impl Filter<String> {
    /// Creates a string filter from a regular expression `pattern`.
    pub fn with_pattern(pattern: &str) -> std::result::Result<Self, regex::Error> {
        Ok(Self {
            condition: FilterCondition::Regex(Regex::new(pattern)?),
        })
    }

    /// Creates a JSON-aware filter that evaluates `pattern` against the JSON field at `json_pointer`
    /// (e.g. `"/text"` or `"text"`, `"/langs"`, `"/user/did"`) while forwarding the full JSON string intact.
    pub fn with_json_pattern(
        json_pointer: &str,
        pattern: &str,
    ) -> std::result::Result<Self, regex::Error> {
        Ok(Self {
            condition: FilterCondition::JsonRegex {
                pointer: normalize_json_pointer(json_pointer),
                regex: Regex::new(pattern)?,
            },
        })
    }

    /// Creates a string filter from a closure taking `&str`.
    pub fn with_lambda<F>(f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self {
            condition: FilterCondition::Predicate(Arc::new(move |s: &String| f(s.as_str()))),
        }
    }
}

impl<T> Filter<T>
where
    T: Send + 'static,
{
    /// Creates a generic typed filter from a predicate closure `Fn(&T) -> bool`.
    pub fn with_predicate<F>(f: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self {
            condition: FilterCondition::Predicate(Arc::new(f)),
        }
    }
}

impl Default for Filter<String> {
    fn default() -> Self {
        Self::with_lambda(|_| true)
    }
}

impl<T> Operator for Filter<T>
where
    T: AsRef<str> + Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn run(&self, mut input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("Filter starting");

        while let Some(msg) = input.recv().await {
            let matches = match &self.condition {
                FilterCondition::Regex(pattern) => pattern.is_match(msg.payload.as_ref()),
                FilterCondition::JsonRegex { pointer, regex } => {
                    match_json_pointer(msg.payload.as_ref(), pointer, regex)
                }
                FilterCondition::Predicate(f) => f(&msg.payload),
            };

            if matches {
                debug!("Message matches filter condition, forwarding");
                output.send(msg).await?;
            } else {
                debug!("Message does not match filter condition, dropping");
            }
        }

        debug!("Filter completed");
        Ok(())
    }
}

impl<T> PipelineComponent for Filter<T>
where
    T: AsRef<str> + Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<crate::pipeline::ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::channel::bounded;
    use crate::pipeline::Message;

    #[tokio::test]
    async fn test_filter_with_json_pattern_preserves_full_json() {
        // Note: "rust" appears in `cid` of the second post, but NOT in `text`.
        // `with_json_pattern("/text", ...)` must only match against `/text`.
        let post_matching =
            r#"{"uri":"at://did:plc:1/app.bsky.feed.post/1","cid":"bafy123","text":"Building agents in Rust","langs":["en"]}"#;
        let post_cid_false_positive =
            r#"{"uri":"at://did:plc:2/app.bsky.feed.post/2","cid":"bafyrust999","text":"Cooking pasta tonight","langs":["en"]}"#;

        let filter = Filter::with_json_pattern("text", r"(?i)\brust\b").unwrap();
        let (in_tx, in_rx) = bounded(8);
        let (out_tx, mut out_rx) = bounded(8);

        in_tx
            .send(Message::new(post_matching.to_string()))
            .await
            .unwrap();
        in_tx
            .send(Message::new(post_cid_false_positive.to_string()))
            .await
            .unwrap();
        drop(in_tx);

        Operator::run(&filter, in_rx, out_tx).await.unwrap();

        let forwarded = out_rx.recv().await.unwrap();
        assert_eq!(forwarded.payload, post_matching);
        assert!(out_rx.recv().await.is_none());
    }
}
