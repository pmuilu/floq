use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use std::time::Duration;
use tracing::debug;

#[derive(Clone, Debug, Default)]
pub struct DelayedStringSource {
    items: Vec<(String, Duration)>,
}

impl DelayedStringSource {
    pub fn new(items: Vec<(String, Duration)>) -> Self {
        Self { items }
    }
}

impl Source for DelayedStringSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("DelayedStringSource starting");

        for (item, delay) in &self.items {
            tokio::time::sleep(*delay).await;
            if output.send(Message::new(item.clone())).await.is_err() {
                break;
            }
        }

        debug!("DelayedStringSource completed");
        Ok(())
    }
}

impl_source_component!(DelayedStringSource, String);
