use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::{Receiver, Sender};
use std::sync::Arc;
use std::marker::PhantomData;
use tracing::{debug, error};

pub struct Map<I, O> 
where 
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    transform: Box<dyn Fn(I) -> O + Send + Sync>,
    _phantom: PhantomData<(I, O)>,
}

impl<I, O> Map<I, O> 
where 
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    pub fn new<F>(transform: F) -> Self 
    where 
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        Map {
            transform: Box::new(transform),
            _phantom: PhantomData,
        }
    }
}

impl<I, O> PipelineComponent for Map<I, O> 
where 
    I: Send + Sync + Clone + 'static,
    O: Send + Sync + Clone + 'static,
{
    type Input = I;
    type Output = O;

    fn new() -> Self {
        panic!("Map requires a transform function. Use Map::new() instead.")
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        debug!("Map starting");
        
        while let Ok(msg) = input.recv() {
            debug!("Map received item");
            let transformed = (self.transform)(msg.payload.clone());
            
            if let Err(e) = output.send(msg.with_new_payload(transformed)) {
                error!("Failed to send transformed item: {:?}", e);
                break;
            }
            debug!("Map sent transformed item");
        }
        
        debug!("Map completed");
    }
}
