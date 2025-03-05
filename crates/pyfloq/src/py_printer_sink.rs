use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::{PipelineComponent, PipelineTask};
use floq::transformers::PrinterSink;

/// Python wrapper for PrinterSink
#[pyclass]
#[derive(Clone)]
pub struct PyPrinterSink {
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

    fn __or__(&self, _other: PyObject) -> PyResult<PyObject> {
        // Since this is a sink, it should not compose with anything
        Err(PyRuntimeError::new_err("Cannot connect: PrinterSink is a terminal component"))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, ()>>::run_impl(self, py)
    }
} 