use crate::error::{Error, Result};
use crate::pipeline::{ComponentContext, PipelineComponent, Receiver, Sender, Sink};
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::{debug, error};

/// A sink that POSTs each received [` crate::pipeline::Message<T> `] as JSON to an HTTP webhook endpoint.
#[derive(Clone, Debug)]
pub struct WebhookSink<T> {
    client: reqwest::Client,
    url: String,
    bearer_token: Option<String>,
    _phantom: PhantomData<fn(T)>,
}

impl<T> WebhookSink<T> {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            bearer_token: None,
            _phantom: PhantomData,
        }
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }
}

impl<T> Sink for WebhookSink<T>
where
    T: Serialize + Send + Sync + 'static,
{
    type Input = T;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        debug!("WebhookSink starting for {}", self.url);

        while let Some(msg) = input.recv().await {
            let mut req = self.client.post(&self.url).json(&msg);
            if let Some(token) = &self.bearer_token {
                req = req.bearer_auth(token);
            }

            match req.send().await {
                Ok(resp) if !resp.status().is_success() => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!("Webhook POST failed ({}): {}", status, body);
                    return Err(Error::Api(format!("Webhook failed ({}): {}", status, body)));
                }
                Ok(_) => {}
                Err(e) => {
                    error!("Webhook request error: {}", e);
                    return Err(e.into());
                }
            }
        }

        debug!("WebhookSink completed");
        Ok(())
    }
}

impl<T> PipelineComponent for WebhookSink<T>
where
    T: Serialize + Send + Sync + 'static,
{
    type Input = T;
    type Output = ();

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        _output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Sink>::run(self, input).await
    }
}
