use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;

use floq::pipeline::{PipelineComponent, PipelineTask, ComponentContext, Sender, Receiver, Message};
use floq::sources::BlueskyFirehoseSource;
use floq::transformers::PrinterSink;

// A Python module implemented in Rust.
#[pymodule]
fn pyfloq(_py: Python, m: &PyModule) -> PyResult<()> {
    pyo3_asyncio::tokio::get_runtime();
    
    m.add_class::<PyPipelineComponent>()?;
    m.add_class::<PyBlueskyFirehoseSource>()?;
    m.add_class::<PyPrinterSink>()?;
    Ok(())
}

/// Python wrapper for a simple pipeline component
#[pyclass]
#[derive(Clone)]
struct PyPipelineComponent {
    callback: PyObject,
    task: Arc<PipelineTask<RustPipelineComponent>>,
}

#[pymethods]
impl PyPipelineComponent {
    #[new]
    fn new(callback: PyObject) -> Self {
        let component = RustPipelineComponent { callback: callback.clone() };
        PyPipelineComponent {
            callback,
            task: Arc::new(PipelineTask::new(component)),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let rt = pyo3_asyncio::tokio::get_runtime();
            rt.block_on(async {
                if let Ok(sink) = other.extract::<PyPrinterSink>(py) {
                    Ok(PyPrinterSink::from_task(sink.sink.clone(), (*self.task).clone() | (*sink.task).clone()).into_py(py))
                } else if let Ok(component) = other.extract::<PyPipelineComponent>(py) {
                    Ok(PyPipelineComponent::from_task(component.callback.clone(), (*self.task).clone() | (*component.task).clone()).into_py(py))
                } else {
                    Err(PyRuntimeError::new_err("Unsupported pipeline composition"))
                }
            })
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let task = self.task.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            task.run().await;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}

// Regular impl block for Rust-only methods
impl PyPipelineComponent {
    fn from_task(callback: PyObject, task: PipelineTask<RustPipelineComponent>) -> Self {
        PyPipelineComponent {
            callback,
            task: Arc::new(task),
        }
    }
}

/// Python wrapper for BlueskyFirehoseSource
#[pyclass]
#[derive(Clone)]
struct PyBlueskyFirehoseSource {
    source: BlueskyFirehoseSource,
    task: Arc<PipelineTask<BlueskyFirehoseSource>>,
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
            let rt = pyo3_asyncio::tokio::get_runtime();
            rt.block_on(async {
                if let Ok(sink) = other.extract::<PyPrinterSink>(py) {
                    Ok(PyPrinterSink::from_task(sink.sink.clone(), (*self.task).clone() | (*sink.task).clone()).into_py(py))
                } else if let Ok(component) = other.extract::<PyPipelineComponent>(py) {
                    Ok(PyPipelineComponent::from_task(component.callback.clone(), (*self.task).clone() | (*component.task).clone()).into_py(py))
                } else {
                    Err(PyRuntimeError::new_err("Unsupported pipeline composition"))
                }
            })
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let task = self.task.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            task.run().await;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}

/// Python wrapper for PrinterSink
#[pyclass]
#[derive(Clone)]
struct PyPrinterSink {
    sink: PrinterSink,
    task: Arc<PipelineTask<PrinterSink>>,
}

#[pymethods]
impl PyPrinterSink {
    #[new]
    fn new() -> Self {
        let sink = PrinterSink::new("[] - ");
        PyPrinterSink {
            sink: sink.clone(),
            task: Arc::new(PipelineTask::new(sink)),
        }
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let task = self.task.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            task.run().await;
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}

// Regular impl block for Rust-only methods
impl PyPrinterSink {
    fn from_task(sink: PrinterSink, task: PipelineTask<PrinterSink>) -> Self {
        PyPrinterSink {
            sink,
            task: Arc::new(task),
        }
    }
}

// Internal implementation of PipelineComponent for PyPipelineComponent
#[derive(Clone)]
struct RustPipelineComponent {
    callback: PyObject,
}

impl PipelineComponent for RustPipelineComponent {
    type Input = String;
    type Output = String;

    fn new() -> Self {
        panic!("RustPipelineComponent::new() should not be called directly")
    }

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self::Input, Self::Output>>) {
        Python::with_gil(|py| {
            while let Ok(msg) = input.recv() {
                match self.callback.call1(py, (msg.payload,)) {
                    Ok(result) => {
                        if let Ok(result_str) = result.extract::<String>(py) {
                            let _ = output.send(Message::new(result_str));
                        }
                    },
                    Err(e) => e.print(py),
                }
            }
        });
    }
} 