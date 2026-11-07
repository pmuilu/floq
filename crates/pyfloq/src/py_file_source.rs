use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::checkpoint::SqliteCheckpointStore;
use floq::pipeline::PipelineTask;
use floq::sources::FileSource;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::time::Duration;

/// Python wrapper for [`FileSource`] supporting both batch reading and live file tailing (`tail -F`).
#[pyclass(name = "FileSource")]
#[derive(Clone)]
pub struct PyFileSource {
    source: FileSource,
    stage: PipelineTask<(), String>,
    rooted: Option<PipelineTask<(), String>>,
}

impl PyPipelineWrapper<(), String> for PyFileSource {
    type Component = FileSource;

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
impl PyFileSource {
    #[new]
    #[pyo3(signature = (path, *, follow = false, from_end = false, poll_ms = 200, checkpoint_db = None, checkpoint_key = None))]
    fn new(
        path: &str,
        follow: bool,
        from_end: bool,
        poll_ms: u64,
        checkpoint_db: Option<&str>,
        checkpoint_key: Option<&str>,
    ) -> PyResult<Self> {
        let mut source = FileSource::new(path)
            .with_follow(follow)
            .with_poll_interval(Duration::from_millis(poll_ms));
        if from_end {
            source = source.from_end();
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

    #[staticmethod]
    #[pyo3(signature = (path, *, from_end = false, poll_ms = 200, checkpoint_db = None, checkpoint_key = None))]
    fn tail(
        path: &str,
        from_end: bool,
        poll_ms: u64,
        checkpoint_db: Option<&str>,
        checkpoint_key: Option<&str>,
    ) -> PyResult<Self> {
        Self::new(path, true, from_end, poll_ms, checkpoint_db, checkpoint_key)
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
