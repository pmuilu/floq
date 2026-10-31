use crate::error::{Error, Result};
use crate::impl_operator_component;
use crate::pipeline::{Operator, Receiver, Sender};
use serde_json::{json, Value};
use tracing::{debug, error};

const HF_MODEL_URL: &str =
    "https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2";

/// Operator that computes sentence embeddings via the Hugging Face Inference API.
#[derive(Clone, Debug)]
pub struct HuggingfaceEmbeddings {
    client: reqwest::Client,
    api_key: String,
}

impl Default for HuggingfaceEmbeddings {
    fn default() -> Self {
        Self::new()
    }
}

impl HuggingfaceEmbeddings {
    /// Creates a new [`HuggingfaceEmbeddings`] instance reading `HUGGINGFACE_API_TOKEN` from the environment.
    pub fn new() -> Self {
        Self::try_new().expect("HUGGINGFACE_API_TOKEN environment variable not set")
    }

    /// Fallible constructor reading `HUGGINGFACE_API_TOKEN` from the environment.
    pub fn try_new() -> Result<Self> {
        let api_key = std::env::var("HUGGINGFACE_API_TOKEN")
            .map_err(|e| Error::EnvVar("HUGGINGFACE_API_TOKEN", e))?;
        Ok(Self::with_api_key(api_key))
    }

    /// Creates a new [`HuggingfaceEmbeddings`] instance with an explicit `api_key`.
    pub fn with_api_key(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
        }
    }

    async fn get_embeddings(&self, body: &Value) -> Result<Vec<f32>> {
        let response = self
            .client
            .post(HF_MODEL_URL)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(Error::Api(format!(
                "HuggingFace API request failed: {}",
                error_text
            )));
        }

        let embeddings: Vec<f32> = response.json().await?;
        Ok(embeddings)
    }
}

impl Operator for HuggingfaceEmbeddings {
    type Input = String;
    type Output = Vec<f32>;

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("HuggingfaceEmbeddings starting");

        while let Some(msg) = input.recv().await {
            debug!("Processing text for embeddings");

            let body = json!({
                "inputs": &msg.payload,
                "options": {
                    "wait_for_model": true
                }
            });

            match self.get_embeddings(&body).await {
                Ok(embeddings) => {
                    output.send(msg.with_new_payload(embeddings)).await?;
                }
                Err(e) => {
                    error!("Failed to get embeddings: {}", e);
                }
            }
        }

        debug!("HuggingfaceEmbeddings completed");
        Ok(())
    }
}

impl_operator_component!(HuggingfaceEmbeddings, String, Vec<f32>);
