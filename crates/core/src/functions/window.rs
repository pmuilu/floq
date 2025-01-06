use crate::pipeline::{PipelineComponent, ComponentContext, Receiver, Sender, Message};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

    fn should_trigger(&self, buffer_len: usize, last_trigger_time: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        match self.condition {
            WindowCondition::Count(count) => buffer_len >= count,
            WindowCondition::Time(duration) => {
                now >= last_trigger_time + duration.as_millis() as u64
            },
            WindowCondition::Sliding { slide_interval, .. } => {
                now >= last_trigger_time + slide_interval.as_millis() as u64
            }
        }
    }

    fn get_window_items(&self, now: u64, buffer: &mut Vec<Message<T>>) -> Message<Vec<T>> 
    where T: Clone 
    {
        match self.condition {
            WindowCondition::Count(_) => {
                let items = buffer.drain(0..).map(|msg| msg.payload.clone()).collect();
                Message::new(items)
            },
            WindowCondition::Time(_) => {
                let items = buffer.drain(0..).map(|msg| msg.payload.clone()).collect();
                Message::new(items)
            },
            WindowCondition::Sliding { window_size, .. } => {
                // Remove items outside the window
                let cutoff = now - window_size.as_millis() as u64;
                buffer.retain(|msg| msg.event_timestamp >= cutoff);
                
                // Return clones of all items in the window
                let items = buffer.iter().map(|msg| msg.payload.clone()).collect();
                Message::new(items)
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
        
        let mut buffer: Vec<Message<T>> = Vec::new();
        let mut last_trigger = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        while let Ok(msg) = input.recv() {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            
            // Use the message's event timestamp
            buffer.push(msg);
            
            if self.should_trigger(buffer.len(), last_trigger) {
                let items_to_send = self.get_window_items(now, &mut buffer);
                if !items_to_send.payload.is_empty() {
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
            let remaining_items = buffer.drain(..).map(|msg| msg.payload.clone()).collect::<Vec<_>>();
            if let Err(e) = output.send(Message::new(remaining_items)) {
                error!("Failed to send final windowed items: {:?}", e);
            }
        }
        
        debug!("Window completed");
    }
}
