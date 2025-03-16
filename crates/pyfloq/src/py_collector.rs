use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::{PipelineComponent, PipelineTask, ComponentContext, Sender, Receiver};

#[derive(Clone)]
pub struct CollectorComponent {
    callback: PyObject,
}

impl PipelineComponent for CollectorComponent {
    type Input = PyObject;
    type Output = ();

    fn new() -> Self {
        panic!("CollectorComponent::new() should not be called directly")
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        while let Ok(msg) = input.recv() {
            Python::with_gil(|py| {
                if let Err(e) = self.callback.call1(py, (msg.payload,)) {
                    e.print(py);
                }
            });
        }
    }
}

/// Python wrapper for Collector that stores items in a Python list
#[pyclass(name = "Collector")]
#[derive(Clone)]
pub struct PyCollector {
    #[allow(dead_code)]
    callback: PyObject,
    component: CollectorComponent,
    task: Arc<PipelineTask<CollectorComponent>>,
}

impl PyPipelineWrapper<PyObject, ()> for PyCollector {
    type Component = CollectorComponent;
    
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
impl PyCollector {
    #[new]
    fn new(callback: PyObject) -> Self {
        let component = CollectorComponent { callback: callback.clone() };
        PyCollector {
            callback: callback.clone(),
            component: component.clone(),
            task: Arc::new(PipelineTask::new(component)),
        }
    }

    fn __or__(&self, _other: PyObject) -> PyResult<PyObject> {
        // Collector is a sink, it cannot be composed with other components
        Err(PyRuntimeError::new_err("Cannot connect: Collector is a terminal component"))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<PyObject, ()>>::run_impl(self, py)
    }
} 