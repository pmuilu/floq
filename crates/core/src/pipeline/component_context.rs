use super::channel::Sender;
use std::marker::PhantomData;

/// Execution context passed to components that inspect slot topology directly.
#[derive(Debug, Clone)]
pub struct ComponentContext<Input, Output> {
    pub output_senders: Vec<Sender<Output>>,
    pub input_slot_count: usize,
    _phantom: PhantomData<fn(Input)>,
}

impl<Input, Output> ComponentContext<Input, Output> {
    pub fn new(output_senders: Vec<Sender<Output>>, input_slot_count: usize) -> Self {
        Self {
            output_senders,
            input_slot_count,
            _phantom: PhantomData,
        }
    }

    /// Returns a slice of per-slot output senders.
    pub fn get_output_senders(&self) -> &[Sender<Output>] {
        &self.output_senders
    }

    /// Returns the number of parallel input slots configured for this stage.
    pub fn input_slot_count(&self) -> usize {
        self.input_slot_count
    }
}
