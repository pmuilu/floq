use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Receiver, Sender};
use std::marker::PhantomData;
use tracing::{debug, error};
use std::sync::Arc;

pub struct Merger<T: Send + Sync + 'static> {
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + 'static> PipelineComponent for Merger<T> {
    type Input = T;
    type Output = T;

    fn new() -> Self {
        Merger {    
            _phantom: PhantomData,
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, context: Arc<ComponentContext<Self>>) {
        debug!("Merger starting");
        let input_receivers = &context.input_receivers;
        let output_senders = &context.output_senders;

        assert!(input_receivers.len() > 1, "Merger requires more than one input receiver");
        assert!(output_senders.len() == 1, "Merger requires exactly one output sender");
        
        while let Ok(item) = input.recv() {
            debug!("Merger received item"); 
            if let Err(e) = output_senders[0].send(item) {
                error!("Failed to send merged item: {:?}", e);
                break;
            }
            debug!("Merger forwarded item");
        }
    
        debug!("Merger completed");
    }
}
