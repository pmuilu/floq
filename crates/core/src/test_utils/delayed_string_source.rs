use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::channel::{Receiver, Sender};

use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

pub struct DelayedStringSource {
    items: Vec<(String, Duration)>,
}

impl DelayedStringSource {
    pub fn new(items: Vec<(String, Duration)>) -> Self {
        DelayedStringSource { items }
    }
}

impl PipelineComponent for DelayedStringSource {
    type Input = ();
    type Output = String;

    fn new() -> Self {
        DelayedStringSource { items: Vec::new() }
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("DelayedStringSource starting");
        
        for (item, delay) in &self.items {
            // Wait for the specified delay
            tokio::time::sleep(*delay).await;
            
            // Send the item
            if let Err(_) = output.send(Message::new(item.clone())) {
                break;
            }
        }
        
        debug!("DelayedStringSource completed");
    }
} 