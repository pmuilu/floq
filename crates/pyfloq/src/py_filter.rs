use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::functions::Filter;
use floq::pipeline::PipelineTask;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Python wrapper for [`Filter`].
#[pyclass(name = "Filter")]
#[derive(Clone)]
pub struct PyFilter {
    filter: Filter,
    stage: PipelineTask<String, String>,
    rooted: Option<PipelineTask<(), String>>,
}

impl PyPipelineWrapper<String, String> for PyFilter {
    type Component = Filter;

    fn get_stage(&self) -> &PipelineTask<String, String> {
        &self.stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), String>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, String>,
        rooted: Option<PipelineTask<(), String>>,
    ) -> Self {
        Self {
            filter: component,
            stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.filter
    }
}

#[pymethods]
impl PyFilter {
    #[new]
    #[pyo3(signature = (pattern, *, json_path = None))]
    fn new(pattern: &str, json_path: Option<&str>) -> PyResult<Self> {
        let filter = match json_path {
            Some(pointer) => Filter::with_json_pattern(pointer, pattern),
            None => Filter::with_pattern(pattern),
        }
        .map_err(|e| PyRuntimeError::new_err(format!("Invalid regex pattern: {}", e)))?;

        Ok(Self {
            filter: filter.clone(),
            stage: PipelineTask::new(filter),
            rooted: None,
        })
    }

    #[staticmethod]
    fn with_json_pattern(json_path: &str, pattern: &str) -> PyResult<Self> {
        Self::new(pattern, Some(json_path))
    }

    #[staticmethod]
    fn with_lambda(callback: PyObject) -> Self {
        let filter = Filter::with_lambda(move |text: &str| {
            Python::with_gil(|py| {
                callback
                    .call1(py, (text,))
                    .and_then(|res| res.extract::<bool>(py))
                    .unwrap_or(false)
            })
        });

        Self {
            filter: filter.clone(),
            stage: PipelineTask::new(filter),
            rooted: None,
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| self.compose_string(py, &other))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, String>>::run_impl(self, py)
    }
}
