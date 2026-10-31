use crate::error::Result;
use crate::pipeline::{Operator, Receiver, Sender};
use std::sync::Arc;
use tracing::debug;

type TransformFn<I, O> = dyn Fn(I) -> O + Send + Sync;

/// Stateless transformation operator that maps each `Message<I>` to `Message<O>` by value.
pub struct Map<I, O> {
    transform: Arc<TransformFn<I, O>>,
}

impl<I, O> Clone for Map<I, O> {
    fn clone(&self) -> Self {
        Self {
            transform: Arc::clone(&self.transform),
        }
    }
}

impl<I, O> Map<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    pub fn new<F>(transform: F) -> Self
    where
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        Self {
            transform: Arc::new(transform),
        }
    }
}

impl<I, O> Operator for Map<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("Map starting");

        while let Some(msg) = input.recv().await {
            debug!("Map received item");
            let mapped = msg.map(|payload| (self.transform)(payload));
            output.send(mapped).await?;
            debug!("Map sent transformed item");
        }

        debug!("Map completed");
        Ok(())
    }
}

impl<I, O> crate::pipeline::PipelineComponent for Map<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<crate::pipeline::ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}
