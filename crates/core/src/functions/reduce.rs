use crate::error::{Error, Result};
use crate::pipeline::{Operator, PipelineComponent, Receiver, Sender};
use std::sync::{Arc, Mutex};
use tracing::debug;

type ReducerFn<I, O> = dyn Fn(&mut O, I) + Send + Sync;

/// Stateful reduction operator that folds incoming items of type `I` into an accumulator `O`.
pub struct Reduce<I, O> {
    reducer: Arc<ReducerFn<I, O>>,
    current_value: Arc<Mutex<O>>,
}

impl<I, O> Clone for Reduce<I, O> {
    fn clone(&self) -> Self {
        Self {
            reducer: Arc::clone(&self.reducer),
            current_value: Arc::clone(&self.current_value),
        }
    }
}

impl<I, O> Reduce<I, O>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
{
    pub fn new<F>(initial: O, reducer: F) -> Self
    where
        F: Fn(&mut O, I) + Send + Sync + 'static,
    {
        Self {
            reducer: Arc::new(reducer),
            current_value: Arc::new(Mutex::new(initial)),
        }
    }

    pub fn get_result(&self) -> O {
        self.current_value
            .lock()
            .expect("Reduce accumulator mutex poisoned")
            .clone()
    }
}

impl<I, O> Operator for Reduce<I, O>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
{
    type Input = I;
    type Output = O;

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("Reduce starting");

        while let Some(item) = input.recv().await {
            debug!("Reduce received item");

            let out_msg = {
                let mut current = self
                    .current_value
                    .lock()
                    .map_err(|_| Error::Custom("Reduce mutex poisoned".to_string()))?;
                item.map(|payload| {
                    (self.reducer)(&mut current, payload);
                    current.clone()
                })
            };

            output.send(out_msg).await?;
            debug!("Reduce sent updated value");
        }

        debug!("Reduce completed");
        Ok(())
    }
}

impl<I, O> PipelineComponent for Reduce<I, O>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
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
