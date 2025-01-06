use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::channel::{Receiver, Sender};
use tracing::{debug, error};
use serde_json::{json, Value};
use std::sync::Arc;


pub struct GeminiEmbeddings {
    client: reqwest::Client,
    api_key: String,
}

impl PipelineComponent for GeminiEmbeddings {
    type Input = Vec<String>;
    type Output = Vec<Vec<f32>>;

    fn new() -> Self {
        GeminiEmbeddings {
            client: reqwest::Client::new(),
            api_key: std::env::var("GEMINI_API_KEY")
                .expect("GEMINI_API_KEY environment variable not set"),
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("GeminiEmbeddings starting");
        while let Ok(msg) = input.recv() {
            let texts = msg.payload.clone();
            debug!("Processing {} texts for embeddings", texts.len());
            
            // Create individual requests for each text
            let requests = texts.iter().map(|t| json!({
                "model": "models/text-embedding-004",
                "content": {
                    "parts": [{
                        "text": t
                    }]
                }
            })).collect::<Vec<_>>();

            // Batch them in a single request
            let body = json!({
                "requests": requests
            });

            match self.get_embeddings(body).await {
                Ok(embeddings) => {
                    if let Err(e) = output.send(Message::with_new_payload(msg, embeddings)) {
                        error!("Failed to send embeddings: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to get embeddings: {:?}", e);
                }
            }
        }
        debug!("GeminiEmbeddings completed");
    }
}

impl GeminiEmbeddings {
    async fn get_embeddings(&self, body: Value) -> Result<Vec<Vec<f32>>, Box<dyn std::error::Error>> {
        let url = format!("https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents?key={}", self.api_key);
        let response = self.client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(format!("API request failed: {}", error_text).into());
        }
        
        let json_response: serde_json::Value = response.json().await?;

        // Parse multiple embeddings
        let embeddings = json_response["embeddings"]
            .as_array()
            .ok_or("Missing embeddings array in response")?
            .iter()
            .map(|embedding| {
                embedding["values"]
                    .as_array()
                    .ok_or("Missing values in embedding")
                    .map(|values| {
                        values.iter()
                            .map(|v| v.as_f64().unwrap_or(0.0) as f32)
                            .collect::<Vec<f32>>()
                    })
            })
            .collect::<Result<Vec<Vec<f32>>, _>>()?;

        Ok(embeddings)
    }
}