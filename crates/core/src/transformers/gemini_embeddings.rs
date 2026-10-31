use crate::error::{Error, Result};
use crate::impl_operator_component;
use crate::pipeline::{Operator, Receiver, Sender};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{debug, error};

const GEMINI_EMBEDDING_MODEL: &str = "models/text-embedding-004";

#[derive(Deserialize)]
struct BatchEmbedResponse {
    embeddings: Vec<EmbeddingValues>,
}

#[derive(Deserialize)]
struct EmbeddingValues {
    values: Vec<f32>,
}

/// Operator that computes batched text embeddings via the Google Gemini API.
#[derive(Clone, Debug)]
pub struct GeminiEmbeddings {
    client: reqwest::Client,
    api_key: String,
}

impl Default for GeminiEmbeddings {
    fn default() -> Self {
        Self::new()
    }
}

impl GeminiEmbeddings {
    /// Creates a new [`GeminiEmbeddings`] instance reading `GEMINI_API_KEY` from the environment.
    pub fn new() -> Self {
        Self::try_new().expect("GEMINI_API_KEY environment variable not set")
    }

    /// Fallible constructor reading `GEMINI_API_KEY` from the environment.
    pub fn try_new() -> Result<Self> {
        let api_key =
            std::env::var("GEMINI_API_KEY").map_err(|e| Error::EnvVar("GEMINI_API_KEY", e))?;
        Ok(Self::with_api_key(api_key))
    }

    /// Creates a new [`GeminiEmbeddings`] instance with an explicit `api_key`.
    pub fn with_api_key(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
        }
    }

    async fn get_embeddings(&self, body: &Value) -> Result<Vec<Vec<f32>>> {
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/{}:batchEmbedContents?key={}",
            GEMINI_EMBEDDING_MODEL, self.api_key
        );

        let response = self.client.post(&url).json(body).send().await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(Error::Api(format!(
                "Gemini API request failed: {}",
                error_text
            )));
        }

        let parsed: BatchEmbedResponse = response.json().await?;
        Ok(parsed.embeddings.into_iter().map(|e| e.values).collect())
    }
}

impl Operator for GeminiEmbeddings {
    type Input = Vec<String>;
    type Output = Vec<Vec<f32>>;

    async fn run(&self, mut input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("GeminiEmbeddings starting");

        while let Some(msg) = input.recv().await {
            debug!("Processing {} texts for embeddings", msg.payload.len());

            let requests: Vec<_> = msg
                .payload
                .iter()
                .map(|t| {
                    json!({
                        "model": GEMINI_EMBEDDING_MODEL,
                        "content": {
                            "parts": [{ "text": t }]
                        }
                    })
                })
                .collect();

            let body = json!({ "requests": requests });

            match self.get_embeddings(&body).await {
                Ok(embeddings) => {
                    output.send(msg.with_new_payload(embeddings)).await?;
                }
                Err(e) => {
                    error!("Failed to get embeddings: {}", e);
                }
            }
        }

        debug!("GeminiEmbeddings completed");
        Ok(())
    }
}

impl_operator_component!(GeminiEmbeddings, Vec<String>, Vec<Vec<f32>>);
