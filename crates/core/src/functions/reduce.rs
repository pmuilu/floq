use crate::pipeline::{PipelineComponent, ComponentContext, Receiver, Sender, Message};
use std::sync::{Arc, Mutex};
use std::marker::PhantomData;
use tracing::{debug, error};

pub struct Reduce<I, O> 
where 
    I: Send + Sync + 'static,
    O: Send + Sync + 'static,
{
    reducer: Arc<dyn Fn(&mut O, I) + Send + Sync>,
    current_value: Arc<Mutex<O>>,
    _phantom: PhantomData<I>,
}

impl<I, O> Clone for Reduce<I, O>
where 
    I: Send + Sync + 'static,
    O: Send + Sync + Clone + 'static,
{
    fn clone(&self) -> Self {
        Self {
            reducer: self.reducer.clone(),  // Now this works because Arc implements Clone
            current_value: self.current_value.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O> Reduce<I, O> 
where 
    I: Send + Sync + 'static,
    O: Send + Sync + Clone + 'static,
{
    pub fn new<F>(initial: O, reducer: F) -> Self 
    where 
        F: Fn(&mut O, I) + Send + Sync + 'static,
    {
        Reduce {
            reducer: Arc::new(reducer),  // Wrap in Arc here
            current_value: Arc::new(Mutex::new(initial)),
            _phantom: PhantomData,
        }
    }

    pub fn get_result(&self) -> O {
        self.current_value.lock().unwrap().clone()
    }
}

impl<I, O> PipelineComponent for Reduce<I, O> 
where 
    I: Send + Sync + 'static,
    O: Send + Sync + Clone + 'static,
{
    type Input = I;
    type Output = O;

    fn new() -> Self {
        panic!("Reduce requires initial value and reducer function. Use Reduce::new() instead.")
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        debug!("Reduce starting");
        
        while let Ok(item) = input.recv() {
            debug!("Reduce received item");
            
            // Apply reducer function to current value
            if let Ok(mut current) = self.current_value.lock() {
                (self.reducer)(&mut current, item.payload);
                
                // Send the updated value
                if let Err(e) = output.send(Message::new(current.clone())) {
                    error!("Failed to send reduced value: {:?}", e);
                    break;
                }
                debug!("Reduce sent updated value");
            } else {
                error!("Failed to lock current value");
                break;
            }
        }
        
        debug!("Reduce completed");
    }
}
