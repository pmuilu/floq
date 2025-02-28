use super::channel::{Sender, Receiver};
use std::sync::Arc;
use super::component_context::ComponentContext;

pub trait PipelineComponent: Send + Sync + 'static {
    type Input: Send;
    type Output: Send;

    fn new() -> Self;
    fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, context: Arc<ComponentContext<Self::Input, Self::Output>>) 
        -> impl std::future::Future<Output = ()> + Send;
}

