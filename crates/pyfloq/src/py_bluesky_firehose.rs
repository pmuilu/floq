use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::PipelineTask;
use floq::sources::BlueskyFirehoseSource;
use pyo3::prelude::*;

/// Python wrapper for [`BlueskyFirehoseSource`].
#[pyclass(name = "BlueskyFirehoseSource")]
#[derive(Clone)]
pub struct PyBlueskyFirehoseSource {
    source: BlueskyFirehoseSource,
    stage: PipelineTask<(), String>,
    rooted: Option<PipelineTask<(), String>>,
}

impl PyPipelineWrapper<(), String> for PyBlueskyFirehoseSource {
    type Component = BlueskyFirehoseSource;

    fn get_stage(&self) -> &PipelineTask<(), String> {
        &self.stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), String>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<(), String>,
        rooted: Option<PipelineTask<(), String>>,
    ) -> Self {
        Self {
            source: component,
            stage: stage.clone(),
            rooted: Some(rooted.unwrap_or(stage)),
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
        let stage = PipelineTask::new(source.clone());
        Self {
            source,
            stage: stage.clone(),
            rooted: Some(stage),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| self.compose_string(py, &other))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<(), String>>::run_impl(self, py)
    }
}
