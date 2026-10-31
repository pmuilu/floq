use crate::error::Result;
use crate::impl_sink_component;
use crate::pipeline::{Receiver, Sink};
use tracing::info;

/// A sink component that prints each received message to stdout.
#[derive(Clone, Debug)]
pub struct PrinterSink {
    prefix: String,
}

impl Default for PrinterSink {
    fn default() -> Self {
        Self {
            prefix: "PrinterSink: ".to_string(),
        }
    }
}

impl PrinterSink {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }
}

impl Sink for PrinterSink {
    type Input = String;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        info!("PrinterSink starting with prefix: {}", self.prefix);

        while let Some(msg) = input.recv().await {
            println!("{}{}", self.prefix, msg.payload);
        }

        info!("PrinterSink completed");
        Ok(())
    }
}

impl_sink_component!(PrinterSink, String);
