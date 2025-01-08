use super::channel::{Sender, Receiver};

use std::ops::BitOr;
use tokio::task::JoinHandle;
use tracing::{debug, error};
use std::sync::{Arc, Mutex};
use super::pipeline_component::PipelineComponent;
use super::component_context::ComponentContext;
use super::pipeline_monitor::{MonitoredTask, PipelineMonitor};

pub struct PipelineTaskArc<T: PipelineComponent, S: PipelineComponent<Output = T::Output> = T> {
    component: Arc<T>,
    input_receivers: Vec<Receiver<T::Input>>,
    input_senders: Vec<Sender<T::Input>>,
    output_receivers: Arc<Mutex<Vec<Receiver<T::Output>>>>,
    output_senders: Arc<Mutex<Vec<Sender<T::Output>>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    slots: usize,
    combined_sources: Vec<Arc<PipelineTaskArc<S>>>,
}

impl<T: PipelineComponent, S: PipelineComponent<Output = T::Output>> MonitoredTask for PipelineTaskArc<T, S> {
    fn get_metrics(&self) -> Vec<(String, usize, usize)> {
        let mut metrics = Vec::new();

        if let Ok(senders) = self.output_senders.lock() {
            for sender in senders.iter() {
                metrics.push(("output_senders".to_string(), sender.len(), sender.capacity().unwrap_or(0)));
            }
        }

        if let Ok(receivers) = self.output_receivers.lock() {
            for receiver in receivers.iter() {
                metrics.push(("output_receivers".to_string(), receiver.len(), receiver.capacity().unwrap_or(0)));
            }
        }
        metrics
    }
}

impl<T: PipelineComponent, S: PipelineComponent<Output = T::Output>> PipelineTaskArc<T, S> {
    fn deploy_to_slots(&self) -> Vec<JoinHandle<()>> {
        let mut new_tasks = Vec::new();

        let output_senders = self.output_senders.lock().unwrap();
        for index in 0..self.slots {
            let component = Arc::clone(&self.component);
            let context = Arc::new(ComponentContext {
                output_senders: output_senders.clone(),
                input_receivers: self.input_receivers.clone(),
            });
            let default_receiver = self.input_receivers[index].clone();
            let default_sender = output_senders[index % output_senders.len()].clone();
            
            let task = tokio::spawn(async move {
                debug!("Starting pipeline task");
                component.run(default_receiver, default_sender, context).await;
                debug!("Pipeline task completed");
            });
            new_tasks.push(task);
        }

        new_tasks
    }

    fn connect_with<B: PipelineComponent<Input = T::Output>>(
        source: Arc<PipelineTaskArc<T, S>>, 
        target: Arc<PipelineTaskArc<B>>
    ) -> PipelineTaskArc<B> {
        // Clear and recreate output channels
        let mut source_senders = source.output_senders.lock().unwrap();
        let mut source_receivers = source.output_receivers.lock().unwrap();

        source_senders.clear();
        source_receivers.clear();
        
        for _ in 0..target.slots {
            let (output_s, output_r) = crate::pipeline::channel::channel();
            source_senders.push(output_s);
            source_receivers.push(output_r);
        }
        drop(source_senders);
        drop(source_receivers);
        
        let mut new_tasks = source.deploy_to_slots();
        
        if !source.combined_sources.is_empty() {
            for src in &source.combined_sources {
                let component = Arc::clone(&src.component);
                let context = Arc::new(ComponentContext {
                    output_senders: source.output_senders.lock().unwrap().clone(),
                    input_receivers: src.input_receivers.clone(),
                });
                let default_receiver = src.input_receivers[0].clone();
                let default_sender = source.output_senders.lock().unwrap()[0].clone();
                
                let task = tokio::spawn(async move {
                    debug!("Starting pipeline task");
                    component.run(default_receiver, default_sender, context).await;
                    debug!("Pipeline task completed");
                });
                new_tasks.push(task);
            }
        }

        // Move tasks from source into new_tasks
        if let Ok(mut tasks) = source.tasks.lock() {
            new_tasks.extend(tasks.drain(..));
        }

        PipelineTaskArc {
            component: target.component.clone(),
            input_receivers: source.output_receivers.lock().unwrap().clone(),
            input_senders: target.input_senders.clone(),
            output_receivers: target.output_receivers.clone(),
            output_senders: target.output_senders.clone(),
            tasks: Arc::new(Mutex::new(new_tasks)),
            slots: target.slots,
            combined_sources: Vec::new(),
        }
    }

    pub async fn run(&self) {
        let mut final_tasks = Vec::new();
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        let context = Arc::new(ComponentContext {
            output_senders: self.output_senders.lock().unwrap().clone(),
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
}

// Wrapper type that holds Arc<PipelineTaskArc>
#[derive(Clone)]
pub struct PipelineTask<T: PipelineComponent, S: PipelineComponent<Output = T::Output> = T>(Arc<PipelineTaskArc<T, S>>);

impl<T: PipelineComponent> PipelineTask<T> {
    pub fn new(component: T) -> Self {
        let (input_sender, input_receiver) = crate::pipeline::channel::channel::<T::Input>();
        let (output_sender, output_receiver) = crate::pipeline::channel::channel::<T::Output>();

        let mut output_senders = Vec::new();
        let mut output_receivers = Vec::new();
        output_senders.push(output_sender);
        output_receivers.push(output_receiver);

        PipelineTask(Arc::new(PipelineTaskArc {
            component: Arc::new(component),
            input_receivers: vec![input_receiver],
            input_senders: vec![input_sender],
            output_receivers: Arc::new(Mutex::new(output_receivers)),
            output_senders: Arc::new(Mutex::new(output_senders)),
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
            output_receivers: Arc::new(Mutex::new(output_receivers)),
            output_senders: Arc::new(Mutex::new(output_senders)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            slots,
            combined_sources: Vec::new(),
        }))
    }

    pub fn combine<U>(self, sources: Vec<PipelineTask<U>>) -> PipelineTask<T, U>
    where 
        U: PipelineComponent<Output = T::Output> + 'static
    {
        PipelineTask(Arc::new(PipelineTaskArc {
            component: self.0.component.clone(),
            input_receivers: self.0.input_receivers.clone(),
            input_senders: self.0.input_senders.clone(),
            output_receivers: self.0.output_receivers.clone(),
            output_senders: self.0.output_senders.clone(),
            tasks: self.0.tasks.clone(),
            slots: self.0.slots,
            combined_sources: sources.into_iter().map(|task| task.0).collect(),
        }))
    }

    pub fn inner(&self) -> Arc<PipelineTaskArc<T>> {
        self.0.clone()
    }

    pub async fn run(&self) {
        self.0.run().await
    }

    pub fn register_with_monitor(&self, monitor: &mut PipelineMonitor) {
        monitor.register_monitor(self.0.clone());
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
        // Connect the pipeline stages using Arc references directly
        let connected = PipelineTaskArc::connect_with(self.0, rhs.0);
        PipelineTask(Arc::new(connected))
    }
}

