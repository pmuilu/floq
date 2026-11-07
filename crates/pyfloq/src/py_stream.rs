use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::impl_sink_component;
use floq::pipeline::{PipelineTask, Receiver, Sink};
use floq::{Error, Result};
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::{mpsc, Mutex as AsyncMutex};

/// Shared channel state between [`StreamSink`] and [`PyStream`].
pub struct StreamState {
    pub tx: StdMutex<Option<mpsc::Sender<PyObject>>>,
    pub rx: AsyncMutex<mpsc::Receiver<PyObject>>,
    pub started: AtomicBool,
}

impl StreamState {
    pub fn new(max_buffer_size: usize) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(max_buffer_size.max(1));
        Arc::new(Self {
            tx: StdMutex::new(Some(tx)),
            rx: AsyncMutex::new(rx),
            started: AtomicBool::new(false),
        })
    }
}

/// Generic sink that forwards pipeline items into an async channel for Python's `async for`.
#[derive(Clone)]
pub struct StreamSink<T> {
    state: Arc<StreamState>,
    _marker: PhantomData<T>,
}

impl<T> StreamSink<T> {
    pub fn new(state: Arc<StreamState>) -> Self {
        Self {
            state,
            _marker: PhantomData,
        }
    }
}

impl<T> Sink for StreamSink<T>
where
    T: IntoPy<PyObject> + Clone + Send + Sync + 'static,
{
    type Input = T;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        let sender = {
            let mut guard = self
                .state
                .tx
                .lock()
                .map_err(|_| Error::Custom("stream sender mutex poisoned".to_string()))?;
            guard.take()
        };

        let Some(sender) = sender else {
            return Err(Error::ChannelClosed);
        };

        while let Some(msg) = input.recv().await {
            let py_obj = Python::with_gil(|py| msg.payload.into_py(py));
            if sender.send(py_obj).await.is_err() {
                break;
            }
        }

        Ok(())
    }
}

impl_sink_component!(StreamSink<String>, String);
impl_sink_component!(StreamSink<Vec<String>>, Vec<String>);
impl_sink_component!(StreamSink<PyObject>, PyObject);

/// Async iterator sink (`async for item in stream:`) bridging Rust Tokio pipelines to Python `asyncio`.
#[pyclass(name = "Stream")]
#[derive(Clone)]
pub struct PyStream {
    state: Arc<StreamState>,
    string_component: StreamSink<String>,
    string_stage: PipelineTask<String, ()>,
    vec_component: StreamSink<Vec<String>>,
    vec_stage: PipelineTask<Vec<String>, ()>,
    py_component: StreamSink<PyObject>,
    py_stage: PipelineTask<PyObject, ()>,
    rooted: Option<PipelineTask<(), ()>>,
}

impl PyPipelineWrapper<String, ()> for PyStream {
    type Component = StreamSink<String>;

    fn get_stage(&self) -> &PipelineTask<String, ()> {
        &self.string_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let state = Arc::clone(&component.state);
        let vec_component = StreamSink::<Vec<String>>::new(Arc::clone(&state));
        let py_component = StreamSink::<PyObject>::new(Arc::clone(&state));
        Self {
            state,
            string_component: component,
            string_stage: stage,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.string_component
    }
}

impl PyPipelineWrapper<Vec<String>, ()> for PyStream {
    type Component = StreamSink<Vec<String>>;

    fn get_stage(&self) -> &PipelineTask<Vec<String>, ()> {
        &self.vec_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<Vec<String>, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let state = Arc::clone(&component.state);
        let string_component = StreamSink::<String>::new(Arc::clone(&state));
        let py_component = StreamSink::<PyObject>::new(Arc::clone(&state));
        Self {
            state,
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_component: component,
            vec_stage: stage,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.vec_component
    }
}

impl PyPipelineWrapper<PyObject, ()> for PyStream {
    type Component = StreamSink<PyObject>;

    fn get_stage(&self) -> &PipelineTask<PyObject, ()> {
        &self.py_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<PyObject, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let state = Arc::clone(&component.state);
        let string_component = StreamSink::<String>::new(Arc::clone(&state));
        let vec_component = StreamSink::<Vec<String>>::new(Arc::clone(&state));
        Self {
            state,
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_component: component,
            py_stage: stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.py_component
    }
}

#[pymethods]
impl PyStream {
    #[new]
    #[pyo3(signature = (max_buffer_size = 1024))]
    fn new(max_buffer_size: usize) -> Self {
        let state = StreamState::new(max_buffer_size);
        let string_component = StreamSink::<String>::new(Arc::clone(&state));
        let vec_component = StreamSink::<Vec<String>>::new(Arc::clone(&state));
        let py_component = StreamSink::<PyObject>::new(Arc::clone(&state));
        Self {
            state,
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted: None,
        }
    }

    fn __or__(&self, _other: PyObject) -> PyResult<PyObject> {
        Err(PyRuntimeError::new_err(
            "Cannot connect: Stream is a terminal component",
        ))
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<Option<&'p PyAny>> {
        let rooted = self.rooted.clone().ok_or_else(|| {
            PyRuntimeError::new_err("Stream is not connected to a source pipeline")
        })?;
        let state = Arc::clone(&self.state);

        let fut = pyo3_asyncio::tokio::future_into_py(py, async move {
            if state
                .started
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let cleanup_state = Arc::clone(&state);
                tokio::spawn(async move {
                    let res = rooted.run().await;
                    if let Ok(mut guard) = cleanup_state.tx.lock() {
                        let _ = guard.take();
                    }
                    res
                });
            }

            let mut receiver = state.rx.lock().await;
            match receiver.recv().await {
                Some(item) => Ok(item),
                None => Err(PyStopAsyncIteration::new_err("stream exhausted")),
            }
        })?;

        Ok(Some(fut))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<PyObject, ()>>::run_impl(self, py)
    }
}
