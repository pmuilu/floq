use crossbeam_channel::{Sender, Receiver};
use std::sync::Arc;
use super::component_context::ComponentContext;

pub trait PipelineComponent: Send + Sync + Sized + 'static {
    type Input: Send;
    type Output: Send;

    fn new() -> Self;
    fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, context: Arc<ComponentContext<Self>>) 
        -> impl std::future::Future<Output = ()> + Send;
}
