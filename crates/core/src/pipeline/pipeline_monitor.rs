use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{info, debug};

pub trait MonitoredTask: Send + Sync {
    fn get_metrics(&self) -> Vec<(String, usize, usize)>;
}

pub struct PipelineMonitor {
    tasks: Vec<Arc<dyn MonitoredTask>>,
    is_monitoring: bool,
}

impl PipelineMonitor {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            is_monitoring: false,
        }
    }

    pub fn register_monitor<T: MonitoredTask + 'static>(&mut self, task: Arc<T>) {
        debug!("Registering new task with monitor");
        self.tasks.push(task as Arc<dyn MonitoredTask>);
        debug!("Total registered tasks: {}", self.tasks.len());
    }

    pub fn start(&mut self) {
        if !self.is_monitoring {
            debug!("Starting monitoring thread");
            self.is_monitoring = true;
            
            let tasks = self.tasks.clone();
            debug!("Initial number of tasks to monitor: {}", tasks.len());
            
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    
                    if !tasks.is_empty() {
                        debug!("Collecting metrics from {} tasks", tasks.len());
                        info!("Pipeline Metrics:");
                        for task in &tasks {
                            for (name, current, total) in task.get_metrics() {
                                info!("{}: {}/{}", name, current, total);
                            }
                        }
                    } else {
                        debug!("No tasks to monitor");
                    }
                }
            });
        } else {
            debug!("Monitoring thread already running");
        }
    }
}