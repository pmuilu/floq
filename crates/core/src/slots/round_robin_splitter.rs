use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, error};
use std::sync::Arc;
use std::marker::PhantomData;

pub struct RoundRobinSplitter<T: Send + Sync + 'static> {
    current_index: AtomicUsize,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + 'static> PipelineComponent for RoundRobinSplitter<T> {
    type Input = T;
    type Output = T;

    fn new() -> Self {
        RoundRobinSplitter {
            current_index: AtomicUsize::new(0),
            _phantom: PhantomData,
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        debug!("RoundRobinSplitter starting");
        let output_senders = &context.output_senders;
        debug!("Number of output senders: {}", output_senders.len());

        assert!(output_senders.len() > 1, "RoundRobinSplitter requires more than one output sender");
        assert!(context.input_receivers.len() == 1, "RoundRobinSplitter requires exactly one input receiver");
        
        while let Ok(item) = input.recv() {
            let index = self.current_index.fetch_add(1, Ordering::SeqCst) % output_senders.len();
            debug!("Sending to output {}", index);
            
            if let Err(e) = output_senders[index].send(item) {
                error!("Failed to send to output {}: {:?}", index, e);
                break;
            }
            
            debug!("Successfully sent to output {}", index);
        }
        
        debug!("RoundRobinSplitter completed");
    }
}
