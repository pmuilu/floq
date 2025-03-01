use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;

use floq::pipeline::{PipelineComponent, PipelineTask, ComponentContext, Sender, Receiver, Message};
use floq::sources::BlueskyFirehoseSource;
use floq::transformers::PrinterSink;
use tokio::runtime::Handle;

// A Python module implemented in Rust.
#[pymodule]
fn pyfloq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyPipelineComponent>()?;
    m.add_class::<PyBlueskyFirehoseSource>()?;
    m.add_class::<PyPrinterSink>()?;
    Ok(())
}

/// Trait for common pipeline wrapper functionality
trait PyPipelineWrapper<In: Send + 'static, Out: Send + 'static>: Clone + IntoPy<PyObject> {
    type Component: PipelineComponent<Input = In, Output = Out> + Clone + Send;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>>;
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self;

    fn run_impl<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let task = self.get_task().clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            task.run().await;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    fn get_component(&self) -> &Self::Component;

    // Composition method that works with any component that accepts String input
    fn compose(&self, py: Python<'_>, other: &PyObject) -> PyResult<PyObject> 
    where
        Out: 'static,
        Self::Component: PipelineComponent<Output = String>,
    {
        if let Ok(printer_sink) = other.extract::<PyPrinterSink>(py) {
            let rt = pyo3_asyncio::tokio::get_runtime();
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*printer_sink.get_task()).clone()
            });
            Ok(PyPrinterSink::from_task_and_component(printer_sink.get_component().clone(), task).into_py(py))
        } else if let Ok(pipeline_component) = other.extract::<PyPipelineComponent>(py) {
            let rt = pyo3_asyncio::tokio::get_runtime();
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*pipeline_component.get_task()).clone()
            });
            Ok(PyPipelineComponent::from_task_and_component(pipeline_component.get_component().clone(), task).into_py(py))
        } else {
            Err(PyRuntimeError::new_err("Cannot connect: component must accept String input"))
        }
    }
}

/// Python wrapper for a simple pipeline component
#[pyclass]
#[derive(Clone)]
struct PyPipelineComponent {
    callback: PyObject,
    component: RustPipelineComponent,
    task: Arc<PipelineTask<RustPipelineComponent>>,
}

impl PyPipelineWrapper<String, String> for PyPipelineComponent {
    type Component = RustPipelineComponent;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            callback: component.callback.clone(),
            component,
            task: Arc::new(task),
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.component
    }
}

#[pymethods]
impl PyPipelineComponent {
    #[new]
    fn new(callback: PyObject) -> Self {
        let component = RustPipelineComponent { callback: callback.clone() };
        PyPipelineComponent {
            callback: callback.clone(),
            component: component.clone(),
            task: Arc::new(PipelineTask::new(component)),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            self.compose(py, &other)
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, String>>::run_impl(self, py)
    }
}

/// Python wrapper for BlueskyFirehoseSource
#[pyclass]
#[derive(Clone)]
struct PyBlueskyFirehoseSource {
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
            self.compose(py, &other)
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<(), String>>::run_impl(self, py)
    }
}

/// Python wrapper for PrinterSink
#[pyclass]
#[derive(Clone)]
struct PyPrinterSink {
    sink: PrinterSink,
    task: Arc<PipelineTask<PrinterSink>>,
}

impl PyPipelineWrapper<String, ()> for PyPrinterSink {
    type Component = PrinterSink;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            sink: component,
            task: Arc::new(task),
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.sink
    }
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

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        // Since this is a sink, it should not compose with anything
        Err(PyRuntimeError::new_err("Cannot connect: PrinterSink is a terminal component"))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, ()>>::run_impl(self, py)
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