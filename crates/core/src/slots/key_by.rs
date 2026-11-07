use crate::error::Result;
use crate::pipeline::{ComponentContext, Operator, PipelineComponent, Receiver, Sender};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::debug;

/// Routes messages to downstream slots deterministically by hashing a key extracted from each payload.
///
/// All messages producing the same key `K` are guaranteed to be sent to the same downstream slot index,
/// enabling stateful per-key windowing and reduction across parallel workers.
pub struct KeyBy<T, K, F> {
    key_selector: F,
    _phantom: PhantomData<fn(&T) -> K>,
}

impl<T, K, F: Clone> Clone for KeyBy<T, K, F> {
    fn clone(&self) -> Self {
        Self {
            key_selector: self.key_selector.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, K, F> KeyBy<T, K, F>
where
    T: Send + 'static,
    K: Hash,
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    pub fn new(key_selector: F) -> Self {
        Self {
            key_selector,
            _phantom: PhantomData,
        }
    }

    #[inline]
    fn slot_for(&self, item: &T, slot_count: usize) -> usize {
        let key = (self.key_selector)(item);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % slot_count.max(1)
    }
}

impl<T, K, F> Operator for KeyBy<T, K, F>
where
    T: Send + 'static,
    K: Hash + 'static,
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    type Input = T;
    type Output = T;

    async fn run(
        &self,
        mut input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
    ) -> Result<()> {
        let slot_count = output.slot_count().max(1);
        debug!("KeyBy starting with {} downstream slots", slot_count);

        while let Some(item) = input.recv().await {
            if output.slot_count() > 0 {
                let index = self.slot_for(&item.payload, slot_count);
                output.send_to_slot(index, item).await?;
            }
        }

        debug!("KeyBy completed");
        Ok(())
    }
}

impl<T, K, F> PipelineComponent for KeyBy<T, K, F>
where
    T: Send + 'static,
    K: Hash + 'static,
    F: Fn(&T) -> K + Send + Sync + 'static,
{
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
