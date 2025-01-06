use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use tracing::debug;

pub struct StringCollector {
    pub results: Arc<Mutex<Vec<String>>>,
}

impl PipelineComponent for StringCollector {
    type Input = String;
    type Output = ();

    fn new() -> Self {
        StringCollector {
            results: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("StringCollector starting");
        while let Ok(msg) = input.recv() {
            let text = msg.payload;
            debug!("StringCollector received {}", text);
            if let Ok(mut results) = self.results.lock() {
                results.push(text.clone());
                debug!("StringCollector stored {}", text);
            }
        }
        debug!("StringCollector completed");
    }
} 