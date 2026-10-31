use super::channel::StageMetrics;
use super::message::now_millis;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};
use tracing::{debug, info};

/// Trait implemented by stages that expose runtime metrics to [`PipelineMonitor`].
pub trait MonitoredTask: Send + Sync {
    fn get_metrics(&self) -> Vec<(String, usize, usize)>;
}

impl MonitoredTask for StageMetrics {
    fn get_metrics(&self) -> Vec<(String, usize, usize)> {
        let mut metrics = Vec::with_capacity(4);
        let current_time = now_millis();
        let buffered = self.buffered_items.load(Ordering::Relaxed);
        let cap = self.capacity.load(Ordering::Relaxed);

        metrics.push(("buffered_items".to_string(), buffered, cap));

        let last_send = self.last_send_time.load(Ordering::Relaxed);
        if last_send > 0 {
            let lag = current_time.saturating_sub(last_send);
            metrics.push(("send_lag_ms".to_string(), lag as usize, 0));
        }

        let last_receive = self.last_receive_time.load(Ordering::Relaxed);
        if last_receive > 0 {
            let lag = current_time.saturating_sub(last_receive);
            metrics.push(("receive_lag_ms".to_string(), lag as usize, 0));
        }

        metrics
    }
}

/// Periodically collects and logs queue depth and latency metrics for registered pipeline stages.
#[derive(Default)]
pub struct PipelineMonitor {
    tasks: Vec<Arc<dyn MonitoredTask>>,
    handle: Option<JoinHandle<()>>,
}

impl PipelineMonitor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_monitor<T: MonitoredTask + 'static>(&mut self, task: Arc<T>) {
        debug!("Registering new task with monitor");
        self.tasks.push(task);
        debug!("Total registered tasks: {}", self.tasks.len());
    }

    pub fn start(&mut self) {
        if self.handle.is_some() {
            debug!("Monitoring task already running");
            return;
        }

        debug!("Starting monitoring task");
        let tasks = self.tasks.clone();
        debug!("Initial number of tasks to monitor: {}", tasks.len());

        let handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;

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

        self.handle = Some(handle);
    }

    /// Stops the background monitoring loop if it is running.
    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for PipelineMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}
