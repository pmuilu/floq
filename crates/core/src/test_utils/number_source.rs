use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use tracing::debug;

#[derive(Clone, Debug)]
pub struct NumberSource {
    count: usize,
}

impl Default for NumberSource {
    fn default() -> Self {
        Self::new()
    }
}

impl NumberSource {
    pub fn new() -> Self {
        Self { count: 3 }
    }

    pub fn with_count(count: usize) -> Self {
        Self { count }
    }
}

impl Source for NumberSource {
    type Output = i32;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("NumberSource starting");
        for i in 0..self.count {
            debug!("NumberSource sending {}", i);
            if output.send(Message::new(i as i32)).await.is_err() {
                debug!("NumberSource failed to send {}", i);
                break;
            }
        }
        debug!("NumberSource completed");
        Ok(())
    }
}

impl_source_component!(NumberSource, i32);
