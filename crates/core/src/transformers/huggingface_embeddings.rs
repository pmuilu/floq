use crate::pipeline::{PipelineComponent, ComponentContext};
use crossbeam_channel::{Receiver, Sender};
use tracing::{debug, error};
use serde_json::{json, Value};
use std::sync::Arc;

pub struct HuggingfaceEmbeddings {
    client: reqwest::Client,
    api_key: String,
}

impl PipelineComponent for HuggingfaceEmbeddings {
    type Input = String;
    type Output = Vec<f32>;

    fn new() -> Self {
        HuggingfaceEmbeddings {
            client: reqwest::Client::new(),
            api_key: std::env::var("HUGGINGFACE_API_TOKEN")
                .expect("HUGGINGFACE_API_KEY environment variable not set"),
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("HuggingfaceEmbeddings starting");
        while let Ok(text) = input.recv() {
            debug!("Processing text for embeddings");
            
            // Format for feature-extraction pipeline
            let body = json!({
                "inputs": text,
                "options": {
                    "wait_for_model": true
                }
            });

            match self.get_embeddings(body).await {
                Ok(embeddings) => {
                    if let Err(e) = output.send(embeddings) {
                        error!("Failed to send embeddings: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to get embeddings: {:?}", e);
                }
            }
        }
        debug!("HuggingfaceEmbeddings completed");
    }
}

impl HuggingfaceEmbeddings {
    async fn get_embeddings(&self, body: Value) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
        let response = self.client
            .post("https://api-inference.huggingface.co/pipeline/feature-extraction/sentence-transformers/all-MiniLM-L6-v2")  // Changed back to feature-extraction endpoint
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")  // Added explicit content type
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(format!("API request failed: {}", error_text).into());
        }

        let embeddings: Vec<f32> = response.json().await?;
        Ok(embeddings)
    }
}
