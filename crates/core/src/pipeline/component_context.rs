use super::channel::{Receiver, Sender};

/// Execution context passed to components that inspect slot topology directly.
#[derive(Debug, Clone)]
pub struct ComponentContext<Input, Output> {
    pub output_senders: Vec<Sender<Output>>,
    pub input_receivers: Vec<Receiver<Input>>,
}

impl<Input, Output> ComponentContext<Input, Output> {
    /// Returns a slice of per-slot output senders.
    pub fn get_output_senders(&self) -> &[Sender<Output>] {
        &self.output_senders
    }

    /// Returns a slice of per-slot input receivers.
    pub fn get_input_receivers(&self) -> &[Receiver<Input>] {
        &self.input_receivers
    }
}
