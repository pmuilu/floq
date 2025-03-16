use super::channel::{Sender, Receiver};

pub struct ComponentContext<Input, Output> {
    pub output_senders: Vec<Sender<Output>>,
    pub input_receivers: Vec<Receiver<Input>>,
}

impl<Input, Output> ComponentContext<Input, Output> {
    pub fn get_output_senders(&self) -> &Vec<Sender<Output>> {
        &self.output_senders
    }

    pub fn get_input_receivers(&self) -> &Vec<Receiver<Input>> {
        &self.input_receivers
    }
}
