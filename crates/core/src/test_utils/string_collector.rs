use crate::error::Result;
use crate::impl_sink_component;
use crate::pipeline::{Receiver, Sink};
use std::sync::{Arc, Mutex};
use tracing::debug;

#[derive(Clone, Debug, Default)]
pub struct StringCollector {
    pub results: Arc<Mutex<Vec<String>>>,
}

impl StringCollector {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sink for StringCollector {
    type Input = String;

    async fn run(&self, input: Receiver<Self::Input>) -> Result<()> {
        debug!("StringCollector starting");
        while let Some(msg) = input.recv().await {
            debug!("StringCollector received {}", msg.payload);
            if let Ok(mut results) = self.results.lock() {
                results.push(msg.payload);
            }
        }
        debug!("StringCollector completed");
        Ok(())
    }
}

impl_sink_component!(StringCollector, String);
