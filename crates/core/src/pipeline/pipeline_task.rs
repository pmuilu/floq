use super::channel::{Sender, Receiver};

use std::ops::BitOr;
use tokio::task::JoinHandle;
use tracing::{debug, error};
use std::sync::{Arc, Mutex};
use super::pipeline_component::PipelineComponent;
use super::component_context::ComponentContext;

pub struct PipelineTaskArc<T: PipelineComponent, S: PipelineComponent<Output = T::Output> = T> {
    component: Arc<T>,
    input_receivers: Vec<Receiver<T::Input>>,
    input_senders: Vec<Sender<T::Input>>,
    output_receivers: Vec<Receiver<T::Output>>,
    output_senders: Vec<Sender<T::Output>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    slots: usize,
    combined_sources: Vec<Arc<PipelineTaskArc<S>>>,
}

impl<T: PipelineComponent, S: PipelineComponent<Output = T::Output>> PipelineTaskArc<T, S> {
    fn deploy_to_slots(&self) -> Vec<JoinHandle<()>> {
        let mut new_tasks = Vec::new();

        for index in 0..self.slots {
            let component = Arc::clone(&self.component);
            let context = Arc::new(ComponentContext {
                output_senders: self.output_senders.clone(),
                input_receivers: self.input_receivers.clone(),
            });
            let default_receiver = self.input_receivers[index].clone();
            let default_sender = self.output_senders[index%self.output_senders.len()].clone();
            
            let task = tokio::spawn(async move {
                debug!("Starting pipeline task");
                component.run(default_receiver, default_sender, context).await;
                debug!("Pipeline task completed");
            });
            new_tasks.push(task);
        }

        new_tasks
    }

    pub async fn run(&self) {
        let mut final_tasks = Vec::new();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        let context = Arc::new(ComponentContext {
            output_senders: self.output_senders.clone(),
            input_receivers: self.input_receivers.clone(),
        });

        // Spawn a task for each slot
        for (index, input_receiver) in self.input_receivers.iter().cloned().enumerate() {
            let component = Arc::clone(&self.component);
            let context = Arc::clone(&context);
            
            let task = tokio::spawn(async move {
                let (null_sender, _) = crate::pipeline::channel::channel();
                
                debug!("Running slot component {}", index);
                component.run(input_receiver, null_sender, context).await;
                debug!("Slot component {} completed", index);
            });
            final_tasks.push(task);
        }

        // Wait for previous tasks
        for task in tasks {
            if let Err(e) = task.await {
                error!("Task failed: {:?}", e);
            }
        }

        // Wait for final tasks
        for task in final_tasks {
            if let Err(e) = task.await {
                error!("Slot task failed: {:?}", e);
            }
        }
    }

    fn connect_with<B: PipelineComponent<Input = T::Output>>(mut self, rhs: PipelineTaskArc<B>) -> PipelineTaskArc<B> {
        self.output_senders = Vec::new();
        self.output_receivers = Vec::new();
        
        for _ in 0..rhs.slots {
            let (output_s, output_r) = crate::pipeline::channel::channel();
            self.output_senders.push(output_s);
            self.output_receivers.push(output_r);
        }
        
        let mut new_tasks = self.deploy_to_slots();
        
        if !self.combined_sources.is_empty() {
            for source in &self.combined_sources {
                let component = Arc::clone(&source.component);
                let context = Arc::new(ComponentContext {
                    output_senders: self.output_senders.clone(),
                    input_receivers: source.input_receivers.clone(),
                });
                let default_receiver = source.input_receivers[0].clone();
                let default_sender = self.output_senders[0].clone();
                
                let task = tokio::spawn(async move {
                    debug!("Starting pipeline task");
                    component.run(default_receiver, default_sender, context).await;
                    debug!("Pipeline task completed");
                });
                new_tasks.push(task);
            }
        }

        // Move tasks from self into new_tasks
        if let Ok(mut tasks) = self.tasks.lock() {
            new_tasks.extend(tasks.drain(..));
        }

        PipelineTaskArc {
            component: rhs.component.clone(),
            input_receivers: self.output_receivers,
            input_senders: rhs.input_senders.clone(),
            output_receivers: rhs.output_receivers.clone(),
            output_senders: rhs.output_senders.clone(),
            tasks: Arc::new(Mutex::new(new_tasks)),
            slots: rhs.slots,
            combined_sources: Vec::new(),
        }
    }
}

// Wrapper type that holds Arc<PipelineTaskArc>
#[derive(Clone)]
pub struct PipelineTask<T: PipelineComponent, S: PipelineComponent<Output = T::Output> = T>(Arc<PipelineTaskArc<T, S>>);

impl<T: PipelineComponent> PipelineTask<T> {
    pub fn new(component: T) -> Self {
        let (input_sender, input_receiver) = crate::pipeline::channel::channel();
        let (output_sender, output_receiver) = crate::pipeline::channel::channel();

        PipelineTask(Arc::new(PipelineTaskArc {
            component: Arc::new(component),
            input_receivers: vec![input_receiver],
            input_senders: vec![input_sender],
            output_receivers: vec![output_receiver],
            output_senders: vec![output_sender],
            tasks: Arc::new(Mutex::new(Vec::new())),
            slots: 1,
            combined_sources: Vec::new(),
        }))
    }   

    pub fn with_slots(component: T, slots: usize) -> Self {
        debug!("Creating PipelineTask with {} slots", slots);
        let mut input_senders = Vec::new();
        let mut input_receivers = Vec::new();
        let mut output_senders = Vec::new();
        let mut output_receivers = Vec::new();
        
        // Create connected channel pairs for each slot
        for _ in 0..slots {
            let (input_s, input_r) = crate::pipeline::channel::channel();
            let (output_s, output_r) = crate::pipeline::channel::channel();
            input_senders.push(input_s);
            input_receivers.push(input_r);
            output_senders.push(output_s);
            output_receivers.push(output_r);
        }

        debug!("Input senders created: {}", input_senders.len());
        debug!("Input receivers created: {}", input_receivers.len());
        debug!("Output senders created: {}", output_senders.len());
        debug!("Output receivers created: {}", output_receivers.len());

        PipelineTask(Arc::new(PipelineTaskArc {
            component: Arc::new(component),
            input_receivers,
            input_senders,
            output_receivers,
            output_senders,
            tasks: Arc::new(Mutex::new(Vec::new())),
            slots,
            combined_sources: Vec::new(),
        }))
    }

    pub fn inner(&self) -> Arc<PipelineTaskArc<T>> {
        self.0.clone()
    }

    pub async fn run(&self) {
        self.0.run().await
    }
}

impl<A, S, B> BitOr<PipelineTask<B>> for PipelineTask<A, S>
where
    A: PipelineComponent,
    S: PipelineComponent<Output = A::Output>,
    B: PipelineComponent<Input = A::Output>,
    A::Output: Send,
    B::Input: Send,
{
    type Output = PipelineTask<B>;

    fn bitor(self, rhs: PipelineTask<B>) -> PipelineTask<B> {
        // Get ownership of both inner values
        let self_inner = Arc::try_unwrap(self.0).unwrap_or_else(|arc| {
            panic!("Cannot connect pipeline stages: multiple references to source stage exist")
        });
        let rhs_inner = Arc::try_unwrap(rhs.0).unwrap_or_else(|arc| {
            panic!("Cannot connect pipeline stages: multiple references to target stage exist")
        });

        // Connect the pipeline stages
        let connected = self_inner.connect_with(rhs_inner);
        PipelineTask(Arc::new(connected))
    }
}

