use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::Arc;
use tracing::{debug, error};
use regex::Regex;

pub enum FilterCondition {
    Regex(Regex),
    Lambda(Box<dyn Fn(&str) -> bool + Send + Sync>),
}

pub struct Filter {
    condition: FilterCondition,
}

impl Filter {
    pub fn with_pattern(pattern: &str) -> Result<Self, regex::Error> {
        Ok(Filter {
            condition: FilterCondition::Regex(Regex::new(pattern)?),
        })
    }

    pub fn with_lambda<F>(f: F) -> Self 
    where 
        F: Fn(&str) -> bool + Send + Sync + 'static 
    {
        Filter {
            condition: FilterCondition::Lambda(Box::new(f)),
        }
    }
}

impl PipelineComponent for Filter {
    type Input = String;
    type Output = String;

    fn new() -> Self {
        Filter {
            condition: FilterCondition::Regex(Regex::new(".*").expect("Default pattern should always be valid")),
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("Filter starting");
        
        while let Ok(msg) = input.recv() {
            let text = msg.payload.clone();

            debug!("Filter received text: {}", text);

            let matches = match &self.condition {
                FilterCondition::Regex(pattern) => pattern.is_match(&text),
                FilterCondition::Lambda(f) => f(&text),
            };
            
            if matches {
                debug!("Text matches condition, forwarding");
                if let Err(e) = output.send(msg) {
                    error!("Failed to send filtered text: {:?}", e);
                    break;
                }
            } else {
                debug!("Text does not match condition, dropping");
            }
        }
        
        debug!("Filter completed");
    }
}
