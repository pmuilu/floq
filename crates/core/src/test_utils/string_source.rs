use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use tracing::debug;

#[derive(Clone, Debug)]
pub struct StringSource {
    strings: Vec<String>,
}

impl Default for StringSource {
    fn default() -> Self {
        Self::new()
    }
}

impl StringSource {
    pub fn new() -> Self {
        Self {
            strings: vec!["0".to_string(), "1".to_string(), "2".to_string()],
        }
    }

    pub fn with_items(strings: Vec<String>) -> Self {
        Self { strings }
    }
}

impl Source for StringSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("StringSource starting");
        for s in &self.strings {
            debug!("StringSource sending {}", s);
            if output.send(Message::new(s.clone())).await.is_err() {
                break;
            }
        }
        debug!("StringSource completed");
        Ok(())
    }
}

impl_source_component!(StringSource, String);
