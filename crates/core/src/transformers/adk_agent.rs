use crate::error::{Error, Result};
use crate::pipeline::{
    now_millis, ComponentContext, Message, Operator, PipelineComponent, Receiver, Sender,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error};

/// Trait implemented by pipeline payload types (`String` and `Vec<String>`) that can be
/// formatted into a prompt for a Google ADK agent without requiring an intermediate `Map` stage.
pub trait AdkInput: Send + Sync + 'static {
    fn format_prompt(&self, prefix: Option<&str>, separator: &str) -> Option<String>;
}

impl AdkInput for String {
    fn format_prompt(&self, prefix: Option<&str>, _separator: &str) -> Option<String> {
        let trimmed = self.trim();
        if trimmed.is_empty() {
            return None;
        }
        match prefix {
            Some(p) if !p.is_empty() => Some(format!("{}{}", p, self)),
            _ => Some(self.clone()),
        }
    }
}

impl AdkInput for Vec<String> {
    fn format_prompt(&self, prefix: Option<&str>, separator: &str) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        let joined = self.join(separator);
        if joined.trim().is_empty() {
            return None;
        }
        match prefix {
            Some(p) if !p.is_empty() => Some(format!("{}{}", p, joined)),
            _ => Some(joined),
        }
    }
}

type KeyExtractor<I> = dyn Fn(&I) -> String + Send + Sync;

/// Session management strategy for [`AdkAgent`].
pub enum AdkSessionMode<I> {
    /// Uses a single shared `session_id` across all messages/batches for continuous multi-turn memory.
    Shared(String),
    /// Routes messages to per-key ADK sessions using a key extractor closure.
    Keyed(Arc<KeyExtractor<I>>),
    /// Uses `Message::source_id` as the ADK `session_id` (falling back to `fallback` if `None`).
    FromSourceId { fallback: String },
    /// Creates a fresh, stateless ADK `session_id` for every message or windowed batch.
    Ephemeral { prefix: String },
}

impl<I> Clone for AdkSessionMode<I> {
    fn clone(&self) -> Self {
        match self {
            Self::Shared(s) => Self::Shared(s.clone()),
            Self::Keyed(f) => Self::Keyed(Arc::clone(f)),
            Self::FromSourceId { fallback } => Self::FromSourceId {
                fallback: fallback.clone(),
            },
            Self::Ephemeral { prefix } => Self::Ephemeral {
                prefix: prefix.clone(),
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct AdkEvent {
    #[serde(default)]
    content: Option<AdkContent>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AdkContent {
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    parts: Vec<AdkPart>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AdkPart {
    #[serde(default)]
    text: Option<String>,
}

/// Operator that connects a `floq` streaming pipeline to a Google ADK (`google-adk`) agent.
///
/// Accepts either `String` (single events) or `Vec<String>` (windowed batches) and emits
/// the agent's text response downstream as a `String`.
pub struct AdkAgent<I = Vec<String>> {
    client: reqwest::Client,
    base_url: String,
    app_name: String,
    user_id: String,
    prompt_prefix: Option<String>,
    separator: String,
    session_mode: AdkSessionMode<I>,
    initialized_sessions: Arc<Mutex<HashSet<String>>>,
    ephemeral_counter: Arc<AtomicU64>,
    _phantom: PhantomData<fn(I)>,
}

impl<I> Clone for AdkAgent<I> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            app_name: self.app_name.clone(),
            user_id: self.user_id.clone(),
            prompt_prefix: self.prompt_prefix.clone(),
            separator: self.separator.clone(),
            session_mode: self.session_mode.clone(),
            initialized_sessions: Arc::clone(&self.initialized_sessions),
            ephemeral_counter: Arc::clone(&self.ephemeral_counter),
            _phantom: PhantomData,
        }
    }
}

impl<I: AdkInput> AdkAgent<I> {
    /// Creates a new [`AdkAgent`] operator targeting an ADK server (e.g. `http://localhost:8000`)
    /// and `app_name`.
    pub fn new(base_url: impl Into<String>, app_name: impl Into<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            client,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            app_name: app_name.into(),
            user_id: "floq_user".to_string(),
            prompt_prefix: None,
            separator: "\n---\n".to_string(),
            session_mode: AdkSessionMode::Shared("floq_default_session".to_string()),
            initialized_sessions: Arc::new(Mutex::new(HashSet::new())),
            ephemeral_counter: Arc::new(AtomicU64::new(0)),
            _phantom: PhantomData,
        }
    }

    /// Sets the ADK `user_id` (defaults to `"floq_user"`).
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }

    /// Prepends a static instruction or prompt prefix to each message/batch sent to the agent.
    pub fn with_prompt_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prompt_prefix = Some(prefix.into());
        self
    }

    /// Sets the delimiter used when joining `Vec<String>` batches (defaults to `"\n---\n"`).
    pub fn with_separator(mut self, separator: impl Into<String>) -> Self {
        self.separator = separator.into();
        self
    }

    /// Sets the [`AdkSessionMode`] directly.
    pub fn with_session_mode(mut self, mode: AdkSessionMode<I>) -> Self {
        self.session_mode = mode;
        self
    }

    /// Uses a single fixed `session_id` for continuous multi-turn agent memory.
    pub fn with_shared_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_mode = AdkSessionMode::Shared(session_id.into());
        self
    }

    /// Uses a fresh, ephemeral `session_id` for every incoming message/batch (stateless triage).
    pub fn with_ephemeral_sessions(mut self) -> Self {
        self.session_mode = AdkSessionMode::Ephemeral {
            prefix: "floq_ephemeral".to_string(),
        };
        self
    }

    /// Derives the ADK `session_id` from `Message::source_id` (with a fallback if `source_id` is `None`).
    pub fn with_source_id_session(mut self, fallback: impl Into<String>) -> Self {
        self.session_mode = AdkSessionMode::FromSourceId {
            fallback: fallback.into(),
        };
        self
    }

    /// Derives the ADK `session_id` dynamically from each payload using `key_fn`.
    pub fn with_keyed_session<F>(mut self, key_fn: F) -> Self
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        self.session_mode = AdkSessionMode::Keyed(Arc::new(key_fn));
        self
    }

    fn resolve_session_id(&self, msg: &Message<I>) -> String {
        match &self.session_mode {
            AdkSessionMode::Shared(id) => id.clone(),
            AdkSessionMode::Keyed(f) => f(&msg.payload),
            AdkSessionMode::FromSourceId { fallback } => msg
                .source_id
                .as_deref()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| fallback.clone()),
            AdkSessionMode::Ephemeral { prefix } => {
                let seq = self.ephemeral_counter.fetch_add(1, Ordering::Relaxed);
                format!("{}_{}_{}", prefix, now_millis(), seq)
            }
        }
    }

    async fn ensure_session(&self, session_id: &str) -> Result<()> {
        let already_created = self
            .initialized_sessions
            .lock()
            .map(| set| set.contains(session_id))
            .unwrap_or(false);

        if already_created {
            return Ok(());
        }

        let url = format!(
            "{}/apps/{}/users/{}/sessions/{}",
            self.base_url, self.app_name, self.user_id, session_id
        );

        let response = self.client.post(&url).json(&json!({})).send().await?;
        // 200 OK or 409/400 (already exists) are both acceptable for session initialization
        if response.status().is_success() || response.status().as_u16() == 409 {
            if let Ok(mut set) = self.initialized_sessions.lock() {
                set.insert(session_id.to_string());
            }
        }
        Ok(())
    }

    async fn invoke_agent(&self, session_id: &str, prompt: &str) -> Result<String> {
        self.ensure_session(session_id).await?;

        let url = format!("{}/run", self.base_url);
        let body = json!({
            "app_name": &self.app_name,
            "user_id": &self.user_id,
            "session_id": session_id,
            "new_message": {
                "role": "user",
                "parts": [{ "text": prompt }]
            }
        });

        let response = self.client.post(&url).json(&body).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let err_text = response.text().await.unwrap_or_default();
            return Err(Error::Api(format!(
                "ADK /run request failed ({}): {}",
                status, err_text
            )));
        }

        let events: Vec<AdkEvent> = response.json().await?;
        Ok(Self::extract_response_text(&events))
    }

    fn extract_response_text(events: &[AdkEvent]) -> String {
        let mut texts = Vec::new();
        for event in events {
            if let Some(content) = &event.content {
                for part in &content.parts {
                    if let Some(text) = &part.text {
                        if !text.trim().is_empty() {
                            texts.push(text.as_str());
                        }
                    }
                }
            }
        }
        texts.join("\n")
    }
}

impl<I: AdkInput> Operator for AdkAgent<I> {
    type Input = I;
    type Output = String;

    async fn run(
        &self,
        mut input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
    ) -> Result<()> {
        debug!("AdkAgent starting for app '{}'", self.app_name);

        while let Some(msg) = input.recv().await {
            let Some(prompt) = msg
                .payload
                .format_prompt(self.prompt_prefix.as_deref(), &self.separator)
            else {
                continue;
            };

            let session_id = self.resolve_session_id(&msg);
            match self.invoke_agent(&session_id, &prompt).await {
                Ok(reply) if !reply.is_empty() => {
                    output.send(msg.with_new_payload(reply)).await?;
                }
                Ok(_) => {
                    debug!("AdkAgent received empty response for session {}", session_id);
                }
                Err(e) => {
                    error!("AdkAgent invocation failed: {}", e);
                }
            }
        }

        debug!("AdkAgent completed");
        Ok(())
    }
}

impl<I: AdkInput> PipelineComponent for AdkAgent<I> {
    type Input = I;
    type Output = String;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adk_input_formatting() {
        let single = "hello world".to_string();
        assert_eq!(
            single.format_prompt(Some("Analyze:\n"), "\n---\n"),
            Some("Analyze:\nhello world".to_string())
        );
        assert_eq!("   ".to_string().format_prompt(None, "\n"), None);

        let batch = vec!["post 1".to_string(), "post 2".to_string()];
        assert_eq!(
            batch.format_prompt(Some("Summarize:\n"), "\n---\n"),
            Some("Summarize:\npost 1\n---\npost 2".to_string())
        );
        assert_eq!(Vec::<String>::new().format_prompt(None, "\n"), None);
    }

    #[test]
    fn test_adk_session_modes() {
        let shared = AdkAgent::<String>::new("http://localhost:8000", "my_app")
            .with_shared_session("continuous_1");
        let msg = Message::new("event".to_string()).with_source("sensor_42");
        assert_eq!(shared.resolve_session_id(&msg), "continuous_1");

        let from_source = AdkAgent::<String>::new("http://localhost:8000", "my_app")
            .with_source_id_session("fallback_session");
        assert_eq!(from_source.resolve_session_id(&msg), "sensor_42");
        assert_eq!(
            from_source.resolve_session_id(&Message::new("no_source".to_string())),
            "fallback_session"
        );

        let keyed = AdkAgent::<Vec<String>>::new("http://localhost:8000", "my_app")
            .with_keyed_session(|batch: &Vec<String>| {
                batch.first().cloned().unwrap_or_else(|| "empty".to_string())
            });
        let batch_msg = Message::new(vec!["tenant_a".to_string(), "data".to_string()]);
        assert_eq!(keyed.resolve_session_id(&batch_msg), "tenant_a");

        let ephemeral =
            AdkAgent::<String>::new("http://localhost:8000", "my_app").with_ephemeral_sessions();
        let s1 = ephemeral.resolve_session_id(&msg);
        let s2 = ephemeral.resolve_session_id(&msg);
        assert_ne!(s1, s2);
        assert!(s1.starts_with("floq_ephemeral_"));
    }

    #[test]
    fn test_extract_response_text() {
        let raw_json = r#"[
            {"content": {"role": "model", "parts": [{"text": "First insight."}]}},
            {"content": {"role": "model", "parts": [{"text": ""}, {"text": "Second insight."}]}}
        ]"#;
        let events: Vec<AdkEvent> = serde_json::from_str(raw_json).unwrap();
        assert_eq!(
            AdkAgent::<String>::extract_response_text(&events),
            "First insight.\nSecond insight."
        );
    }
}
