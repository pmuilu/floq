use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::checkpoint::SqliteCheckpointStore;
use floq::pipeline::PipelineTask;
use floq::sources::BlueskyFirehoseSource;
use pyo3::exceptions::PyRuntimeError;
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
    #[pyo3(signature = (*, json_output = false, checkpoint_db = None, checkpoint_key = None))]
    fn new(
        json_output: bool,
        checkpoint_db: Option<&str>,
        checkpoint_key: Option<&str>,
    ) -> PyResult<Self> {
        let mut source = BlueskyFirehoseSource::new();
        if json_output {
            source = source.with_json_output();
        }
        if let Some(db_path) = checkpoint_db {
            let store = SqliteCheckpointStore::open(db_path)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            source = match checkpoint_key {
                Some(key) => source.with_checkpoint(store, key),
                None => source.with_default_checkpoint(store),
            };
        }
        let stage = PipelineTask::new(source.clone());
        Ok(Self {
            source,
            stage: stage.clone(),
            rooted: Some(stage),
        })
    }

    fn with_json_output(&self) -> Self {
        let source = self.source.clone().with_json_output();
        let stage = PipelineTask::new(source.clone());
        Self {
            source,
            stage: stage.clone(),
            rooted: Some(stage),
        }
    }

    #[pyo3(signature = (db_path, key = None))]
    fn with_checkpoint(&self, db_path: &str, key: Option<&str>) -> PyResult<Self> {
        let store = SqliteCheckpointStore::open(db_path)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let source = match key {
            Some(k) => self.source.clone().with_checkpoint(store, k),
            None => self.source.clone().with_default_checkpoint(store),
        };
        let stage = PipelineTask::new(source.clone());
        Ok(Self {
            source,
            stage: stage.clone(),
            rooted: Some(stage),
        })
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| self.compose_string(py, &other))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<(), String>>::run_impl(self, py)
    }
}
