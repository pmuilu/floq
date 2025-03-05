use pyo3::prelude::*;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::{PipelineComponent, PipelineTask};
use floq::sources::BlueskyFirehoseSource;

/// Python wrapper for BlueskyFirehoseSource
#[pyclass]
#[derive(Clone)]
pub struct PyBlueskyFirehoseSource {
    source: BlueskyFirehoseSource,
    task: Arc<PipelineTask<BlueskyFirehoseSource>>,
}

impl PyPipelineWrapper<(), String> for PyBlueskyFirehoseSource {
    type Component = BlueskyFirehoseSource;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            source: component,
            task: Arc::new(task),
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.source
    }
}

#[pymethods]
impl PyBlueskyFirehoseSource {
    #[new]
    fn new() -> Self {
        let source = BlueskyFirehoseSource::new();
        PyBlueskyFirehoseSource {
            source: source.clone(),
            task: Arc::new(PipelineTask::new(source)),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            self.compose_string(py, &other)
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<(), String>>::run_impl(self, py)
    }
} 