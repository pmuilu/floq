use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::functions::Window;
use floq::pipeline::PipelineTask;
use pyo3::prelude::*;
use std::time::Duration;

/// Python wrapper for [`Window`].
#[pyclass(name = "Window")]
#[derive(Clone)]
pub struct PyWindow {
    window: Window<String>,
    stage: PipelineTask<String, Vec<String>>,
    rooted: Option<PipelineTask<(), Vec<String>>>,
}

impl PyPipelineWrapper<String, Vec<String>> for PyWindow {
    type Component = Window<String>;

    fn get_stage(&self) -> &PipelineTask<String, Vec<String>> {
        &self.stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), Vec<String>>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, Vec<String>>,
        rooted: Option<PipelineTask<(), Vec<String>>>,
    ) -> Self {
        Self {
            window: component,
            stage,
            rooted,
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
        Self {
            window: window.clone(),
            stage: PipelineTask::new(window),
            rooted: None,
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| self.compose_vec_string(py, &other))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, Vec<String>>>::run_impl(self, py)
    }
}
