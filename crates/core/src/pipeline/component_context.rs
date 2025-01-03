use crossbeam_channel::{Sender, Receiver};
use super::pipeline_component::PipelineComponent;

pub struct ComponentContext<T: PipelineComponent> {
    pub output_senders: Vec<Sender<T::Output>>,
    pub input_receivers: Vec<Receiver<T::Input>>,
}

impl<T: PipelineComponent> ComponentContext<T> {
    pub fn get_output_senders(&self) -> &Vec<Sender<T::Output>> {
        &self.output_senders
    }

    pub fn get_input_receivers(&self) -> &Vec<Receiver<T::Input>> {
        &self.input_receivers
    }
}
