use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::Arc;
use tracing::debug;

pub struct StringSource {
    strings: Vec<String>,
}

impl PipelineComponent for StringSource {
    type Input = ();
    type Output = String;

    fn new() -> Self {
        StringSource {
            strings: vec!["0".to_string(), "1".to_string(), "2".to_string()],
        }
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("StringSource starting");
        for s in &self.strings {
            debug!("StringSource sending {}", s);
            if output.send(s.clone()).is_err() {
                debug!("StringSource failed to send {}", s);
                break;
            }
            debug!("StringSource sent {}", s);
        }
        debug!("StringSource completed");
    }
} 