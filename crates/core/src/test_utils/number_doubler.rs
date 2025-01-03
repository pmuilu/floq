use crate::pipeline::{PipelineComponent, ComponentContext};
use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;

pub struct NumberDoubler;

impl PipelineComponent for NumberDoubler {
    type Input = i32;
    type Output = i32;

    fn new() -> Self {
        NumberDoubler
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        eprintln!("NumberDoubler starting");
        while let Ok(num) = input.recv() {
            eprintln!("NumberDoubler received {}", num);
            if output.send(num * 2).is_err() {
                eprintln!("NumberDoubler failed to send {}", num * 2);
                break;
            }
            eprintln!("NumberDoubler sent {}", num * 2);
        }
        eprintln!("NumberDoubler completed");
    }
} 