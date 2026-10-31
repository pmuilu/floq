use crate::error::Result;
use crate::impl_operator_component;
use crate::pipeline::{Operator, Receiver, Sender};
use tracing::debug;

#[derive(Clone, Copy, Debug, Default)]
pub struct NumberDoubler;

impl NumberDoubler {
    pub fn new() -> Self {
        Self
    }
}

impl Operator for NumberDoubler {
    type Input = i32;
    type Output = i32;

    async fn run(&self, mut input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("NumberDoubler starting");
        while let Some(msg) = input.recv().await {
            let doubled = msg.payload * 2;
            debug!("NumberDoubler received {}, sending {}", msg.payload, doubled);
            output.send(msg.with_new_payload(doubled)).await?;
        }
        debug!("NumberDoubler completed");
        Ok(())
    }
}

impl_operator_component!(NumberDoubler, i32, i32);
