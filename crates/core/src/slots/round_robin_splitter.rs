use crate::error::Result;
use crate::pipeline::{ComponentContext, Operator, PipelineComponent, Receiver, Sender};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::debug;

/// Distributes incoming messages across downstream slots in round-robin order.
///
/// Note: `PipelineTask::with_slots(component, n)` automatically performs round-robin fan-out,
/// so [`RoundRobinSplitter`] is optional and kept for explicit topology definitions.
#[derive(Debug)]
pub struct RoundRobinSplitter<T> {
    current_index: AtomicUsize,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Default for RoundRobinSplitter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> RoundRobinSplitter<T> {
    pub fn new() -> Self {
        Self {
            current_index: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + 'static> Operator for RoundRobinSplitter<T> {
    type Input = T;
    type Output = T;

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("RoundRobinSplitter starting");
        let slot_count = output.slot_count().max(1);
        debug!("Number of downstream slots: {}", slot_count);

        while let Some(item) = input.recv().await {
            let index = self.current_index.fetch_add(1, Ordering::Relaxed) % slot_count;
            debug!("Sending to output slot {}", index);
            if output.slot_count() > 0 {
                output.send_to_slot(index, item).await?;
            }
            debug!("Successfully sent to output slot {}", index);
        }

        debug!("RoundRobinSplitter completed");
        Ok(())
    }
}

impl<T: Send + 'static> PipelineComponent for RoundRobinSplitter<T> {
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
