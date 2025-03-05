use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::{PipelineComponent, PipelineTask};
use floq::functions::Window;

/// Python wrapper for Window
#[pyclass]
#[derive(Clone)]
pub struct PyWindow {
    window: Window<String>,
    task: Arc<PipelineTask<Window<String>>>,
}

impl PyPipelineWrapper<String, Vec<String>> for PyWindow {
    type Component = Window<String>;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            window: component,
            task: Arc::new(task),
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.window
    }
}

#[pymethods]
impl PyWindow {
    #[new]
    fn new(duration_ms: u64) -> Self {
        let window = Window::with_duration(Duration::from_millis(duration_ms));

        PyWindow {
            window: window.clone(),
            task: Arc::new(PipelineTask::new(window)),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            self.compose_vec_string(py, &other)
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, Vec<String>>>::run_impl(self, py)
    }
} 