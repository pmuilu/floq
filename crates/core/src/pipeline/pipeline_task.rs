use crossbeam_channel::{Sender, Receiver};
use std::ops::BitOr;
use tokio::task::JoinHandle;
use tracing::{debug, error};
use std::sync::Arc;
use super::pipeline_component::PipelineComponent;
use super::component_context::ComponentContext;

pub struct PipelineTask<T: PipelineComponent, S: PipelineComponent<Output = T::Output> = T> {
    component: Arc<T>,
    input_receivers: Vec<Receiver<T::Input>>,
    input_senders: Vec<Sender<T::Input>>,
    output_receivers: Vec<Receiver<T::Output>>,
    output_senders: Vec<Sender<T::Output>>,
    tasks: Vec<JoinHandle<()>>,
    slots: usize,
    combined_sources: Vec<PipelineTask<S>>,
}

impl<T: PipelineComponent, S: PipelineComponent<Output = T::Output>> PipelineTask<T, S> {
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

    pub async fn run(self) {
        let mut final_tasks = Vec::new();
        let tasks = self.tasks;
        let context = Arc::new(ComponentContext {
            output_senders: self.output_senders.clone(),
            input_receivers: self.input_receivers.clone(),
        });

        // Spawn a task for each slot
        for (index, input_receiver) in self.input_receivers.into_iter().enumerate() {
            let component = Arc::clone(&self.component);
            let context = Arc::clone(&context);
            
            let task = tokio::spawn(async move {
                let (null_sender, _) = crossbeam_channel::unbounded();
                
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
}

impl<T: PipelineComponent> PipelineTask<T> {
    pub fn new(component: T) -> Self {
        let (input_sender, input_receiver) = crossbeam_channel::unbounded();
        let (output_sender, output_receiver) = crossbeam_channel::unbounded();
        
        PipelineTask {
            component: Arc::new(component),
            input_receivers: vec![input_receiver],
            input_senders: vec![input_sender],
            output_receivers: vec![output_receiver],
            output_senders: vec![output_sender],
            tasks: Vec::new(),
            slots: 1,
            combined_sources: Vec::new(),
        }
    }   

    pub fn with_slots(component: T, slots: usize) -> Self {
        debug!("Creating PipelineTask with {} slots", slots);
        let mut input_senders = Vec::new();
        let mut input_receivers = Vec::new();
        let mut output_senders = Vec::new();
        let mut output_receivers = Vec::new();
        
        // Create connected channel pairs for each slot
        for _ in 0..slots {
            let (input_s, input_r) = crossbeam_channel::unbounded();
            let (output_s, output_r) = crossbeam_channel::unbounded();
            input_senders.push(input_s);
            input_receivers.push(input_r);
            output_senders.push(output_s);
            output_receivers.push(output_r);
        }

        debug!("Input senders created: {}", input_senders.len());
        debug!("Input receivers created: {}", input_receivers.len());
        debug!("Output senders created: {}", output_senders.len());
        debug!("Output receivers created: {}", output_receivers.len());

        PipelineTask {
            component: Arc::new(component),
            input_receivers,
            input_senders,
            output_receivers,
            output_senders,
            tasks: Vec::new(),
            slots,
            combined_sources: Vec::new(),
        }
    }

    pub fn combine<U>(self, sources: Vec<PipelineTask<U>>) -> PipelineTask<T, U>
    where 
        U: PipelineComponent<Output = T::Output> + 'static
    {
        PipelineTask {
            component: self.component,
            input_receivers: self.input_receivers,
            input_senders: self.input_senders,
            output_receivers: self.output_receivers,
            output_senders: self.output_senders,
            tasks: self.tasks,
            slots: self.slots,
            combined_sources: sources,
        }
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

    fn bitor(mut self, rhs: PipelineTask<B>) -> PipelineTask<B> {
        // Adjust output senders to match rhs input senders
        self.output_senders = Vec::new();
        self.output_receivers = Vec::new();

        for _ in 0..rhs.slots {
            let (output_s, output_r) = crossbeam_channel::unbounded();
            self.output_senders.push(output_s);
            self.output_receivers.push(output_r);
        }
        
        let mut new_tasks = self.deploy_to_slots();
        
        if !self.combined_sources.is_empty() {
            for source in self.combined_sources {
                
                println!("processing combined source: {:?}", source.output_senders);
                
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

        new_tasks.extend(self.tasks);

        PipelineTask {
            component: rhs.component,
            input_receivers: self.output_receivers,
            input_senders: rhs.input_senders,
            output_receivers: rhs.output_receivers,
            output_senders: rhs.output_senders,
            tasks: new_tasks,
            slots: rhs.slots,
            combined_sources: Vec::new(),
        }
    }

    
}
