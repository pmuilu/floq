use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Sender, Receiver};
use std::sync::Arc;
use tracing::{info, error};

/// A sink component that prints each received message to stdout
#[derive(Clone)]
pub struct PrinterSink {
    prefix: String,
}

impl PrinterSink {
    pub fn new(prefix: impl Into<String>) -> Self {
        PrinterSink {
            prefix: prefix.into(),
        }
    }
}

impl PipelineComponent for PrinterSink {
    type Input = String;
    type Output = String; // No real output, just for compatibility

    fn new() -> Self {
        PrinterSink {
            prefix: "PrinterSink: ".to_string(),
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        info!("PrinterSink starting with prefix: {}", self.prefix);
        
        while let Ok(msg) = input.recv() {
            println!("{}{}", self.prefix, msg.payload);
        }
        
        info!("PrinterSink completed");
    }
} 