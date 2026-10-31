use super::channel::{Receiver, Sender};
use super::component_context::ComponentContext;
use super::pipeline_task::PipelineTask;
use crate::error::Result;
use std::future::Future;
use std::sync::Arc;

/// A pipeline source that produces a stream of `Output` messages.
pub trait Source: Send + Sync + 'static {
    type Output: Send + 'static;

    fn run(&self, output: Sender<Self::Output>) -> impl Future<Output = Result<()>> + Send;
}

/// A pipeline operator/transformer that consumes `Input` messages and emits `Output` messages.
pub trait Operator: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// A terminal pipeline sink that consumes `Input` messages.
pub trait Sink: Send + Sync + 'static {
    type Input: Send + 'static;

    fn run(&self, input: Receiver<Self::Input>) -> impl Future<Output = Result<()>> + Send;
}

/// General pipeline component trait with full [`ComponentContext`] access.
///
/// Components may implement [`Source`], [`Operator`], or [`Sink`] for ergonomic role-specific
/// signatures, or implement [`PipelineComponent`] directly.
pub trait PipelineComponent: Send + Sync + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Extension trait for turning any component into a [`PipelineTask`] directly in a pipe expression.
pub trait IntoTask: PipelineComponent + Sized {
    /// Wraps this component into a single-slot [`PipelineTask`].
    fn into_task(self) -> PipelineTask<Self::Input, Self::Output> {
        PipelineTask::new(self)
    }

    /// Wraps this component into a multi-slot [`PipelineTask`] with `slots` parallel workers.
    fn with_slots(self, slots: usize) -> PipelineTask<Self::Input, Self::Output> {
        PipelineTask::with_slots(self, slots)
    }
}

impl<T: PipelineComponent> IntoTask for T {}

/// Internal macro to bridge a [`Source`] implementation to [`PipelineComponent`].
#[macro_export]
macro_rules! impl_source_component {
    ($ty:ty, $out:ty) => {
        impl $crate::pipeline::PipelineComponent for $ty {
            type Input = ();
            type Output = $out;

            async fn run(
                &self,
                _input: $crate::pipeline::Receiver<Self::Input>,
                output: $crate::pipeline::Sender<Self::Output>,
                _context: ::std::sync::Arc<
                    $crate::pipeline::ComponentContext<Self::Input, Self::Output>,
                >,
            ) -> $crate::Result<()> {
                <$ty as $crate::pipeline::Source>::run(self, output).await
            }
        }
    };
}

/// Internal macro to bridge an [`Operator`] implementation to [`PipelineComponent`].
#[macro_export]
macro_rules! impl_operator_component {
    ($ty:ty, $inp:ty, $out:ty) => {
        impl $crate::pipeline::PipelineComponent for $ty {
            type Input = $inp;
            type Output = $out;

            async fn run(
                &self,
                input: $crate::pipeline::Receiver<Self::Input>,
                output: $crate::pipeline::Sender<Self::Output>,
                _context: ::std::sync::Arc<
                    $crate::pipeline::ComponentContext<Self::Input, Self::Output>,
                >,
            ) -> $crate::Result<()> {
                <$ty as $crate::pipeline::Operator>::run(self, input, output).await
            }
        }
    };
}

/// Internal macro to bridge a [`Sink`] implementation to [`PipelineComponent`].
#[macro_export]
macro_rules! impl_sink_component {
    ($ty:ty, $inp:ty) => {
        impl $crate::pipeline::PipelineComponent for $ty {
            type Input = $inp;
            type Output = ();

            async fn run(
                &self,
                input: $crate::pipeline::Receiver<Self::Input>,
                _output: $crate::pipeline::Sender<Self::Output>,
                _context: ::std::sync::Arc<
                    $crate::pipeline::ComponentContext<Self::Input, Self::Output>,
                >,
            ) -> $crate::Result<()> {
                <$ty as $crate::pipeline::Sink>::run(self, input).await
            }
        }
    };
}
