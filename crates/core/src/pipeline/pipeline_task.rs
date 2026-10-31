use super::channel::{
    multi_slot_channel, Receiver, Sender, StageMetrics, DEFAULT_CHANNEL_CAPACITY,
};
use super::component_context::ComponentContext;
use super::pipeline_component::{Operator, PipelineComponent, Sink, Source};
use super::pipeline_monitor::{MonitoredTask, PipelineMonitor};
use crate::error::{Error, Result};
use std::ops::BitOr;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

/// Internal trait for lazy pipeline execution nodes.
///
/// Nothing is spawned during `|` composition; tasks and channels are materialized
/// only when [`PipelineTask::run`] is awaited.
trait StageRunner<In, Out>: Send + Sync {
    fn entry_slots(&self) -> usize;
    fn entry_capacity(&self) -> usize;
    fn entry_metrics(&self) -> Arc<StageMetrics>;
    fn spawn(
        &self,
        inputs: Vec<Receiver<In>>,
        output: Sender<Out>,
    ) -> Vec<JoinHandle<Result<()>>>;
}

struct LeafStage<C: PipelineComponent> {
    component: Arc<C>,
    slots: usize,
    capacity: usize,
    metrics: Arc<StageMetrics>,
    combined_sources: Vec<PipelineTask<C::Input, C::Output>>,
}

impl<C: PipelineComponent> StageRunner<C::Input, C::Output> for LeafStage<C> {
    fn entry_slots(&self) -> usize {
        self.slots
    }

    fn entry_capacity(&self) -> usize {
        self.capacity
    }

    fn entry_metrics(&self) -> Arc<StageMetrics> {
        Arc::clone(&self.metrics)
    }

    fn spawn(
        &self,
        inputs: Vec<Receiver<C::Input>>,
        output: Sender<C::Output>,
    ) -> Vec<JoinHandle<Result<()>>> {
        let mut handles = Vec::with_capacity(self.slots + self.combined_sources.len());
        let per_slot_outputs = {
            let v = output.per_slot_senders();
            if v.is_empty() {
                vec![Sender::null()]
            } else {
                v
            }
        };

        let context = Arc::new(ComponentContext {
            output_senders: per_slot_outputs,
            input_receivers: inputs.clone(),
        });

        for (index, input_rx) in inputs.into_iter().enumerate() {
            let component = Arc::clone(&self.component);
            let ctx = Arc::clone(&context);
            let out_tx = output.clone();

            let handle = tokio::spawn(async move {
                debug!("Starting pipeline slot {}", index);
                let res = component.run(input_rx, out_tx, ctx).await;
                debug!("Pipeline slot {} completed", index);
                res
            });
            handles.push(handle);
        }

        for combined in &self.combined_sources {
            let (mut dummy_senders, combined_inputs) = multi_slot_channel(
                combined.runner.entry_slots(),
                combined.runner.entry_capacity(),
                combined.runner.entry_metrics(),
            );
            dummy_senders.clear();
            handles.extend(combined.runner.spawn(combined_inputs, output.clone()));
        }

        handles
    }
}

struct ChainedStage<In, Mid, Out> {
    left: Arc<dyn StageRunner<In, Mid>>,
    right: Arc<dyn StageRunner<Mid, Out>>,
}

impl<In, Mid, Out> StageRunner<In, Out> for ChainedStage<In, Mid, Out>
where
    In: Send + 'static,
    Mid: Send + 'static,
    Out: Send + 'static,
{
    fn entry_slots(&self) -> usize {
        self.left.entry_slots()
    }

    fn entry_capacity(&self) -> usize {
        self.left.entry_capacity()
    }

    fn entry_metrics(&self) -> Arc<StageMetrics> {
        self.left.entry_metrics()
    }

    fn spawn(
        &self,
        inputs: Vec<Receiver<In>>,
        final_output: Sender<Out>,
    ) -> Vec<JoinHandle<Result<()>>> {
        let mid_slots = self.right.entry_slots();
        let mid_capacity = self.right.entry_capacity();
        let mid_metrics = self.right.entry_metrics();

        let (mut mid_senders, mid_receivers) =
            multi_slot_channel::<Mid>(mid_slots, mid_capacity, mid_metrics);
        let mid_sender = mid_senders.swap_remove(0);

        let mut handles = self.left.spawn(inputs, mid_sender);
        handles.extend(self.right.spawn(mid_receivers, final_output));
        handles
    }
}

/// A composable, lazy pipeline stage or connected multi-stage graph taking `In` and producing `Out`.
///
/// Stages are composed declaratively using the `|` operator (`a | b | c`) and only spawn
/// asynchronous tasks when [`PipelineTask::run`] is awaited.
pub struct PipelineTask<In, Out = ()> {
    runner: Arc<dyn StageRunner<In, Out>>,
    metrics: Arc<StageMetrics>,
}

impl<In, Out> Clone for PipelineTask<In, Out> {
    fn clone(&self) -> Self {
        Self {
            runner: Arc::clone(&self.runner),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<In, Out> MonitoredTask for PipelineTask<In, Out> {
    fn get_metrics(&self) -> Vec<(String, usize, usize)> {
        self.metrics.get_metrics()
    }
}

struct SourceAdapter<S>(S);
impl<S: Source> PipelineComponent for SourceAdapter<S> {
    type Input = ();
    type Output = S::Output;

    async fn run(
        &self,
        _input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        self.0.run(output).await
    }
}

struct OperatorAdapter<O>(O);
impl<O: Operator> PipelineComponent for OperatorAdapter<O> {
    type Input = O::Input;
    type Output = O::Output;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        self.0.run(input, output).await
    }
}

struct SinkAdapter<K>(K);
impl<K: Sink> PipelineComponent for SinkAdapter<K> {
    type Input = K::Input;
    type Output = ();

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        _output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        self.0.run(input).await
    }
}

impl<In, Out> PipelineTask<In, Out>
where
    In: Send + 'static,
    Out: Send + 'static,
{
    /// Creates a single-slot [`PipelineTask`] from any [`PipelineComponent`].
    pub fn new<C>(component: C) -> Self
    where
        C: PipelineComponent<Input = In, Output = Out>,
    {
        Self::with_slots_and_capacity(component, 1, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Creates a multi-slot [`PipelineTask`] with `slots` parallel workers.
    pub fn with_slots<C>(component: C, slots: usize) -> Self
    where
        C: PipelineComponent<Input = In, Output = Out>,
    {
        Self::with_slots_and_capacity(component, slots, DEFAULT_CHANNEL_CAPACITY)
    }

    /// Creates a [`PipelineTask`] with explicit `slots` and channel buffer `capacity`.
    pub fn with_slots_and_capacity<C>(component: C, slots: usize, capacity: usize) -> Self
    where
        C: PipelineComponent<Input = In, Output = Out>,
    {
        let slots = slots.max(1);
        let capacity = capacity.max(1);
        let metrics = Arc::new(StageMetrics::new(capacity * slots));
        let leaf = LeafStage {
            component: Arc::new(component),
            slots,
            capacity,
            metrics: Arc::clone(&metrics),
            combined_sources: Vec::new(),
        };
        Self {
            runner: Arc::new(leaf),
            metrics,
        }
    }

    /// Combines multiple sources producing `Out` into a single merged stage.
    pub fn combine(self, sources: impl IntoIterator<Item = PipelineTask<In, Out>>) -> Self {
        struct CombinedRunner<In, Out> {
            primary: Arc<dyn StageRunner<In, Out>>,
            others: Vec<PipelineTask<In, Out>>,
        }

        impl<In, Out> StageRunner<In, Out> for CombinedRunner<In, Out>
        where
            In: Send + 'static,
            Out: Send + 'static,
        {
            fn entry_slots(&self) -> usize {
                self.primary.entry_slots()
            }

            fn entry_capacity(&self) -> usize {
                self.primary.entry_capacity()
            }

            fn entry_metrics(&self) -> Arc<StageMetrics> {
                self.primary.entry_metrics()
            }

            fn spawn(
                &self,
                inputs: Vec<Receiver<In>>,
                output: Sender<Out>,
            ) -> Vec<JoinHandle<Result<()>>> {
                let mut handles = self.primary.spawn(inputs, output.clone());
                for src in &self.others {
                    let (mut dummy_senders, src_inputs) = multi_slot_channel(
                        src.runner.entry_slots(),
                        src.runner.entry_capacity(),
                        src.runner.entry_metrics(),
                    );
                    dummy_senders.clear();
                    handles.extend(src.runner.spawn(src_inputs, output.clone()));
                }
                handles
            }
        }

        let others: Vec<_> = sources.into_iter().collect();
        Self {
            runner: Arc::new(CombinedRunner {
                primary: self.runner,
                others,
            }),
            metrics: self.metrics,
        }
    }

    /// Registers this stage's telemetry metrics with a [`PipelineMonitor`].
    pub fn register_with_monitor(&self, monitor: &mut PipelineMonitor) {
        monitor.register_monitor(Arc::clone(&self.metrics));
    }

    /// Materializes all channels, spawns all stage tasks onto the Tokio runtime,
    /// and awaits pipeline completion.
    pub async fn run(&self) -> Result<()> {
        let (mut root_senders, root_receivers) = multi_slot_channel::<In>(
            self.runner.entry_slots(),
            self.runner.entry_capacity(),
            self.runner.entry_metrics(),
        );
        // Drop root senders immediately so source stages see an closed dummy input channel.
        root_senders.clear();

        let handles = self.runner.spawn(root_receivers, Sender::null());
        let mut first_error: Option<Error> = None;

        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    error!("Pipeline stage error: {}", e);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(join_err) => {
                    error!("Pipeline task panicked or was cancelled: {}", join_err);
                    if first_error.is_none() {
                        first_error = Some(Error::TaskJoin(join_err));
                    }
                }
            }
        }

        match first_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl<Out: Send + 'static> PipelineTask<(), Out> {
    /// Creates a [`PipelineTask`] from a [`Source`] implementation.
    pub fn from_source<S: Source<Output = Out>>(source: S) -> Self {
        Self::new(SourceAdapter(source))
    }
}

impl<In: Send + 'static, Out: Send + 'static> PipelineTask<In, Out> {
    /// Creates a [`PipelineTask`] from an [`Operator`] implementation.
    pub fn from_operator<O: Operator<Input = In, Output = Out>>(operator: O) -> Self {
        Self::new(OperatorAdapter(operator))
    }
}

impl<In: Send + 'static> PipelineTask<In, ()> {
    /// Creates a [`PipelineTask`] from a [`Sink`] implementation.
    pub fn from_sink<K: Sink<Input = In>>(sink: K) -> Self {
        Self::new(SinkAdapter(sink))
    }
}

impl<In, Mid, Out> BitOr<PipelineTask<Mid, Out>> for PipelineTask<In, Mid>
where
    In: Send + 'static,
    Mid: Send + 'static,
    Out: Send + 'static,
{
    type Output = PipelineTask<In, Out>;

    fn bitor(self, rhs: PipelineTask<Mid, Out>) -> Self::Output {
        let chained = ChainedStage {
            left: self.runner,
            right: rhs.runner,
        };
        PipelineTask {
            runner: Arc::new(chained),
            metrics: self.metrics,
        }
    }
}
