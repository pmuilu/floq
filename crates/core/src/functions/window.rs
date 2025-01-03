use crate::pipeline::{PipelineComponent, ComponentContext};
use crossbeam_channel::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, error};

#[derive(Clone)]
pub enum WindowCondition {
    Count(usize),
    Time(Duration),
    Sliding {
        window_size: Duration,
        slide_interval: Duration,
    },
}

pub struct Window<T> {
    condition: WindowCondition,
    shared_buffer: Arc<Mutex<Vec<(T, Instant)>>>,
    last_trigger: Arc<Mutex<Instant>>,
}

impl<T> Window<T> {
    pub fn with_count(count: usize) -> Self {
        Window {
            condition: WindowCondition::Count(count),
            shared_buffer: Arc::new(Mutex::new(Vec::new())),
            last_trigger: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn with_duration(duration: Duration) -> Self {
        Window {
            condition: WindowCondition::Time(duration),
            shared_buffer: Arc::new(Mutex::new(Vec::new())),
            last_trigger: Arc::new(Mutex::new(Instant::now())),
        }
    }

    pub fn with_sliding_window(window_size: Duration, slide_interval: Duration) -> Self {
        Window {
            condition: WindowCondition::Sliding { window_size, slide_interval },
            shared_buffer: Arc::new(Mutex::new(Vec::new())),
            last_trigger: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn should_trigger(&self, buffer_len: usize, last_trigger: &Instant) -> bool {
        match self.condition {
            WindowCondition::Count(_count) => buffer_len >= _count,
            WindowCondition::Time(_duration) => last_trigger.elapsed() >= _duration,
            WindowCondition::Sliding { slide_interval, .. } => last_trigger.elapsed() >= slide_interval,
        }
    }

    fn get_window_items(&self, now: Instant, buffer: &mut Vec<(T, Instant)>) -> Vec<T> 
    where T: Clone 
    {
        match self.condition {
            WindowCondition::Count(_count) => {
                buffer.drain(0..).map(|(item, _)| item).collect()
            },
            WindowCondition::Time(_duration) => {
                buffer.drain(0..).map(|(item, _)| item).collect()
            },
            WindowCondition::Sliding { window_size, .. } => {
                // Remove items outside the window
                let cutoff = now - window_size;
                buffer.retain(|(_, timestamp)| *timestamp >= cutoff);
                
                // Return clones of all items in the window
                buffer.iter().map(|(item, _)| item.clone()).collect()
            }
        }
    }
}

impl<T: Send + Sync + Clone + 'static> PipelineComponent for Window<T> {
    type Input = T;
    type Output = Vec<T>;

    fn new() -> Self {
        Window::with_count(10)
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("Window starting");
        
        while let Ok(item) = input.recv() {
            let now = Instant::now();
            let mut should_trigger = false;
            let mut items_to_send = Vec::new();
            
            
            let mut buffer = self.shared_buffer.lock().unwrap();
            let mut last_trigger = self.last_trigger.lock().unwrap();
            
            buffer.push((item, now));
            
            if self.should_trigger(buffer.len(), &last_trigger) {
                should_trigger = true;
                items_to_send = self.get_window_items(now, &mut buffer);
                *last_trigger = now;
            }
        
            if should_trigger && !items_to_send.is_empty() {
                if let Err(e) = output.send(items_to_send) {
                    error!("Failed to send windowed items: {:?}", e);
                    break;
                }
            }
        }
        
        // Send any remaining items
        let remaining_items = {
            let mut buffer = self.shared_buffer.lock().unwrap();
            buffer.drain(0..).map(|(item, _)| item).collect::<Vec<_>>()
        };
        
        if !remaining_items.is_empty() {
            if let Err(e) = output.send(remaining_items) {
                error!("Failed to send final windowed items: {:?}", e);
            }
        }
        
        debug!("Window completed");
    }
}
