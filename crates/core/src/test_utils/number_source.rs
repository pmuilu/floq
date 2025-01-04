use crate::pipeline::{PipelineComponent, ComponentContext};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::Arc;

pub struct NumberSource {
    count: usize,
}

impl PipelineComponent for NumberSource {
    type Input = ();
    type Output = i32;

    fn new() -> Self {
        NumberSource { count: 3 }
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        eprintln!("NumberSource starting");
        for i in 0..self.count {
            eprintln!("NumberSource sending {}", i);
            if output.send(i as i32).is_err() {
                eprintln!("NumberSource failed to send {}", i);
                break;
            }
        }
        eprintln!("NumberSource completed");
    }
} 