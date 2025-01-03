use crate::pipeline::{PipelineComponent, ComponentContext};
use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::marker::PhantomData;
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
    _phantom: PhantomData<T>,
}

impl<T> Window<T> {
    pub fn with_count(count: usize) -> Self {
        Window {
            condition: WindowCondition::Count(count),
            _phantom: PhantomData,
        }
    }

    pub fn with_duration(duration: Duration) -> Self {
        Window {
            condition: WindowCondition::Time(duration),
            _phantom: PhantomData,
        }
    }

    pub fn with_sliding_window(window_size: Duration, slide_interval: Duration) -> Self {
        Window {
            condition: WindowCondition::Sliding { window_size, slide_interval },
            _phantom: PhantomData,
        }
    }

    fn should_trigger(&self, buffer_len: usize, last_trigger: &Instant) -> bool {
        match self.condition {
            WindowCondition::Count(count) => buffer_len >= count,
            WindowCondition::Time(duration) => last_trigger.elapsed() >= duration,
            WindowCondition::Sliding { slide_interval, .. } => last_trigger.elapsed() >= slide_interval,
        }
    }

    fn get_window_items(&self, now: Instant, buffer: &mut Vec<(T, Instant)>) -> Vec<T> 
    where T: Clone 
    {
        match self.condition {
            WindowCondition::Count(_) => {
                buffer.drain(0..).map(|(item, _)| item).collect()
            },
            WindowCondition::Time(_) => {
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
        Window::<T>::with_count(10)
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("Window starting");
        
        let mut buffer: Vec<(T, Instant)> = Vec::new();
        let mut last_trigger = Instant::now();
        
        while let Ok(item) = input.recv() {
            let now = Instant::now();
            
            buffer.push((item, now));
            
            if self.should_trigger(buffer.len(), &last_trigger) {
                let items_to_send = self.get_window_items(now, &mut buffer);
                if !items_to_send.is_empty() {
                    if let Err(e) = output.send(items_to_send) {
                        error!("Failed to send windowed items: {:?}", e);
                        break;
                    }
                }
                last_trigger = now;
            }
        }
        
        // Send any remaining items
        if !buffer.is_empty() {
            let remaining_items = buffer.drain(..).map(|(item, _)| item).collect::<Vec<_>>();
            if let Err(e) = output.send(remaining_items) {
                error!("Failed to send final windowed items: {:?}", e);
            }
        }
        
        debug!("Window completed");
    }
}
