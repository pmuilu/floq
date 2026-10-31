use crate::error::Result;
use crate::impl_sink_component;
use crate::pipeline::{Receiver, Sink};
use std::sync::{Arc, Mutex};
use tracing::debug;

#[derive(Clone, Debug, Default)]
pub struct NumberCollector {
    pub results: Arc<Mutex<Vec<i32>>>,
}

impl NumberCollector {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Sink for NumberCollector {
    type Input = i32;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        debug!("NumberCollector starting");
        while let Some(msg) = input.recv().await {
            let num = msg.payload;
            debug!("NumberCollector received {}", num);
            if let Ok(mut results) = self.results.lock() {
                results.push(num);
            }
        }
        debug!("NumberCollector completed");
        Ok(())
    }
}

impl_sink_component!(NumberCollector, i32);
