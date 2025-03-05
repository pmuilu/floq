use pyo3::prelude::*;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::{PipelineComponent, PipelineTask, ComponentContext, Sender, Receiver, Message};

#[derive(Clone)]
pub struct RustPipelineComponent {
    callback: PyObject,
}

impl PipelineComponent for RustPipelineComponent {
    type Input = String;
    type Output = String;

    fn new() -> Self {
        panic!("RustPipelineComponent::new() should not be called directly")
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        Python::with_gil(|py| {
            while let Ok(msg) = input.recv() {
                match self.callback.call1(py, (msg.payload,)) {
                    Ok(result) => {
                        if let Ok(result_str) = result.extract::<String>(py) {
                            let _ = output.send(Message::new(result_str));
                        }
                    },
                    Err(e) => e.print(py),
                }
            }
        });
    }
}

/// Python wrapper for a simple pipeline component
#[pyclass]
#[derive(Clone)]
pub struct PyPipelineComponent {
    callback: PyObject,
    component: RustPipelineComponent,
    task: Arc<PipelineTask<RustPipelineComponent>>,
}

impl PyPipelineWrapper<String, String> for PyPipelineComponent {
    type Component = RustPipelineComponent;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            callback: component.callback.clone(),
            component,
            task: Arc::new(task),
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.component
    }
}

#[pymethods]
impl PyPipelineComponent {
    #[new]
    fn new(callback: PyObject) -> Self {
        let component = RustPipelineComponent { callback: callback.clone() };
        PyPipelineComponent {
            callback: callback.clone(),
            component: component.clone(),
            task: Arc::new(PipelineTask::new(component)),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            self.compose_string(py, &other)
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, String>>::run_impl(self, py)
    }
} 