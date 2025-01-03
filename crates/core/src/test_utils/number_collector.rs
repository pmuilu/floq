use crate::pipeline::{PipelineComponent, ComponentContext};
use crossbeam_channel::{Receiver, Sender};
use std::sync::{Arc, Mutex};

pub struct NumberCollector {
    pub results: Arc<Mutex<Vec<i32>>>,
}

impl PipelineComponent for NumberCollector {
    type Input = i32;
    type Output = ();

    fn new() -> Self {
        NumberCollector { 
            results: Arc::new(Mutex::new(Vec::new()))
        }
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        eprintln!("NumberCollector starting");
        while let Ok(num) = input.recv() {
            eprintln!("NumberCollector received {}", num);
            if let Ok(mut results) = self.results.lock() {
                results.push(num);
                eprintln!("NumberCollector stored {}", num);
            }
        }
        eprintln!("NumberCollector completed");
    }
} 