use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::Arc;

pub struct NumberDoubler;

impl PipelineComponent for NumberDoubler {
    type Input = i32;
    type Output = i32;

    fn new() -> Self {
        NumberDoubler
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        eprintln!("NumberDoubler starting");
        while let Ok(msg) = input.recv() {
            let num = msg.payload;
            eprintln!("NumberDoubler received {}", num);
            if output.send(Message::with_new_payload(msg, num * 2)).is_err() {
                eprintln!("NumberDoubler failed to send {}", num * 2);
                break;
            }
            eprintln!("NumberDoubler sent {}", num * 2);
        }
        eprintln!("NumberDoubler completed");
    }
} 