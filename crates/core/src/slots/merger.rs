use crate::error::Result;
use crate::pipeline::{ComponentContext, Operator, PipelineComponent, Receiver, Sender};
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::debug;

/// Merges messages from multiple upstream slots into the downstream stage.
///
/// Note: Connecting a multi-slot stage to a single-slot stage automatically merges channels,
/// so [`Merger`] is optional and kept for explicit topology definitions.
#[derive(Debug, Clone)]
pub struct Merger<T> {
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Default for Merger<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Merger<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + 'static> Operator for Merger<T> {
    type Input = T;
    type Output = T;

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("Merger starting");

        while let Some(item) = input.recv().await {
            debug!("Merger received item");
            output.send(item).await?;
            debug!("Merger forwarded item");
        }

        debug!("Merger completed");
        Ok(())
    }
}

impl<T: Send + 'static> PipelineComponent for Merger<T> {
    type Input = T;
    type Output = T;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}
