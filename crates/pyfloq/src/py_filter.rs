use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::PipelineTask;
use floq::functions::Filter;

/// Python wrapper for Filter
#[pyclass(name = "Filter")]
#[derive(Clone)]
pub struct PyFilter {
    filter: Filter,
    task: Arc<PipelineTask<Filter>>,
}

impl PyPipelineWrapper<String, String> for PyFilter {
    type Component = Filter;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            filter: component,
            task: Arc::new(task),
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.filter
    }
}

#[pymethods]
impl PyFilter {
    #[new]
    fn new(pattern: &str) -> PyResult<Self> {
        match Filter::with_pattern(pattern) {
            Ok(filter) => Ok(PyFilter {
                filter: filter.clone(),
                task: Arc::new(PipelineTask::new(filter)),
            }),
            Err(e) => Err(PyRuntimeError::new_err(format!("Invalid regex pattern: {}", e)))
        }
    }

    #[staticmethod]
    fn with_lambda(callback: PyObject) -> Self {
        let filter = Filter::with_lambda(move |text: &str| {
            Python::with_gil(|py| {
                match callback.call1(py, (text,)) {
                    Ok(result) => result.extract::<bool>(py).unwrap_or(false),
                    Err(_) => false,
                }
            })
        });

        PyFilter {
            filter: filter.clone(),
            task: Arc::new(PipelineTask::new(filter)),
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