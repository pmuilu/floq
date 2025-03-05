use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use floq::pipeline::{PipelineComponent, PipelineTask};
use crate::py_filter::PyFilter;
use crate::py_printer_sink::PyPrinterSink;
use crate::py_window::PyWindow;
use crate::py_reduce::PyReduce;
use crate::py_pipeline_component::PyPipelineComponent;

/// Trait for common pipeline wrapper functionality
pub trait PyPipelineWrapper<In: Send + 'static, Out: Send + 'static>: Clone + IntoPy<PyObject> {
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

    /**
    *   Compose pipeline components that take String as an Input.
    */
    fn compose_string(&self, py: Python<'_>, other: &PyObject) -> PyResult<PyObject> 
    where
        Out: 'static,
        Self::Component: PipelineComponent<Output = String>,
    {
        let rt = pyo3_asyncio::tokio::get_runtime();
        if let Ok(printer_sink) = other.extract::<PyPrinterSink>(py) {
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*printer_sink.get_task()).clone()
            });
            Ok(PyPrinterSink::from_task_and_component(printer_sink.get_component().clone(), task).into_py(py))
        } else if let Ok(pipeline_component) = other.extract::<PyPipelineComponent>(py) {
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*pipeline_component.get_task()).clone()
            });
            Ok(PyPipelineComponent::from_task_and_component(pipeline_component.get_component().clone(), task).into_py(py))
        } else if let Ok(window) = other.extract::<PyWindow>(py) {
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*window.get_task()).clone()
            });
            Ok(PyWindow::from_task_and_component(window.get_component().clone(), task).into_py(py))
        } else if let Ok(filter) = other.extract::<PyFilter>(py) {
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*filter.get_task()).clone()
            });
            Ok(PyFilter::from_task_and_component(filter.get_component().clone(), task).into_py(py))
        } else {
            Err(PyRuntimeError::new_err("Cannot connect: component must accept String input"))
        }
    }

    /**
    *   Compose pipeline components that take Vec<String> as an Input.
    */
    fn compose_vec_string(&self, py: Python<'_>, other: &PyObject) -> PyResult<PyObject> 
    where
        Out: 'static,
        Self::Component: PipelineComponent<Output = Vec<String>>,
    {
        let rt = pyo3_asyncio::tokio::get_runtime();
        if let Ok(reduce) = other.extract::<PyReduce>(py) {
            let task = rt.block_on(async {
                (*self.get_task()).clone() | (*reduce.get_task()).clone()
            });
            Ok(PyReduce::from_task_and_component(reduce.get_component().clone(), task).into_py(py))
        } else {
            Err(PyRuntimeError::new_err("Cannot connect: component must accept Vec<String> input"))
        }
    }
} 