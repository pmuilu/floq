use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use floq::pipeline::{PipelineComponent, PipelineTask};
use crate::py_filter::PyFilter;
use crate::py_window::PyWindow;
use crate::py_reduce::PyReduce;
use crate::py_map::PyMap;
use crate::py_pipeline_component::PyPipelineComponent;

/// Macro for composing pipeline components
macro_rules! try_compose {
    ($self:expr, $py:expr, $rt:expr, $other:expr, $component_type:ty) => {
        if let Ok(component) = $other.extract::<$component_type>($py) {
            let task = $rt.block_on(async {
                (*$self.get_task()).clone() | (*component.get_task()).clone()
            });
            return Ok(<$component_type>::from_task_and_component(
                component.get_component().clone(), 
                task
            ).into_py($py));
        }
    };
}

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
        
        try_compose!(self, py, rt, other, PyPipelineComponent);
        try_compose!(self, py, rt, other, PyWindow);
        try_compose!(self, py, rt, other, PyFilter);
        
        Err(PyRuntimeError::new_err("Cannot connect: component must accept String input"))
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
        
        try_compose!(self, py, rt, other, PyReduce);
        try_compose!(self, py, rt, other, PyMap);
        
        Err(PyRuntimeError::new_err("Cannot connect: component must accept Vec<String> input"))
    }
} 