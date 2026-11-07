use crate::py_adk::{PyAdkAgent, PyAdkLiveSink};
use crate::py_collector::PyCollector;
use crate::py_filter::PyFilter;
use crate::py_map::PyMap;
use crate::py_pipeline_component::PyPipelineComponent;
use crate::py_reduce::PyReduce;
use crate::py_stream::PyStream;
use crate::py_window::PyWindow;
use floq::pipeline::{PipelineComponent, PipelineTask};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// Macro for composing Python pipeline components without requiring an active Tokio runtime during `|`.
macro_rules! try_compose {
    ($self:expr, $py:expr, $other:expr, $out_type:ty, $next_out_type:ty, $component_type:ty) => {
        if let Ok(component) = $other.extract::<$component_type>($py) {
            let composed = <Self as PyPipelineWrapper<In, $out_type>>::connect_to::<
                $next_out_type,
                $component_type,
            >($self, &component);
            return Ok(composed.into_py($py));
        }
    };
}

/// Trait for common Python pipeline wrapper functionality.
pub trait PyPipelineWrapper<In: Send + 'static, Out: Send + 'static>:
    Clone + IntoPy<PyObject>
{
    type Component: PipelineComponent<Input = In, Output = Out> + Clone + Send;

    fn get_stage(&self) -> &PipelineTask<In, Out>;

    fn get_rooted(&self) -> Option<&PipelineTask<(), Out>>;

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<In, Out>,
        rooted: Option<PipelineTask<(), Out>>,
    ) -> Self;

    fn get_component(&self) -> &Self::Component;

    fn connect_to<NextOut, Next>(&self, next: &Next) -> Next
    where
        NextOut: Send + 'static,
        Next: PyPipelineWrapper<Out, NextOut>,
    {
        let next_stage = next.get_stage().clone();
        let rooted = self
            .get_rooted()
            .map(|left_root| left_root.clone() | next_stage.clone());
        Next::from_parts(next.get_component().clone(), next_stage, rooted)
    }

    fn run_impl<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        if let Some(rooted) = self.get_rooted().cloned() {
            pyo3_asyncio::tokio::future_into_py(py, async move {
                rooted
                    .run()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(Python::with_gil(|py| py.None()))
            })
        } else {
            let stage = self.get_stage().clone();
            pyo3_asyncio::tokio::future_into_py(py, async move {
                stage
                    .run()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                Ok(Python::with_gil(|py| py.None()))
            })
        }
    }

    /// Compose pipeline components that take `String` as input.
    fn compose_string(&self, py: Python<'_>, other: &PyObject) -> PyResult<PyObject>
    where
        Self: PyPipelineWrapper<In, String>,
    {
        try_compose!(self, py, other, String, String, PyPipelineComponent);
        try_compose!(self, py, other, String, Vec<String>, PyWindow);
        try_compose!(self, py, other, String, String, PyFilter);
        try_compose!(self, py, other, String, String, PyAdkAgent);
        try_compose!(self, py, other, String, (), PyAdkLiveSink);
        try_compose!(self, py, other, String, (), PyStream);
        try_compose!(self, py, other, String, (), PyCollector);

        Err(PyRuntimeError::new_err(
            "Cannot connect: component must accept String input",
        ))
    }

    /// Compose pipeline components that take `Vec<String>` as input.
    fn compose_vec_string(&self, py: Python<'_>, other: &PyObject) -> PyResult<PyObject>
    where
        Self: PyPipelineWrapper<In, Vec<String>>,
    {
        try_compose!(self, py, other, Vec<String>, PyObject, PyReduce);
        try_compose!(self, py, other, Vec<String>, PyObject, PyMap);
        try_compose!(self, py, other, Vec<String>, String, PyAdkAgent);
        try_compose!(self, py, other, Vec<String>, (), PyAdkLiveSink);
        try_compose!(self, py, other, Vec<String>, (), PyStream);
        try_compose!(self, py, other, Vec<String>, (), PyCollector);

        Err(PyRuntimeError::new_err(
            "Cannot connect: component must accept Vec<String> input",
        ))
    }

    /// Compose pipeline components that take `PyObject` as input.
    fn compose_pyobject(&self, py: Python<'_>, other: &PyObject) -> PyResult<PyObject>
    where
        Self: PyPipelineWrapper<In, PyObject>,
    {
        try_compose!(self, py, other, PyObject, (), PyCollector);
        try_compose!(self, py, other, PyObject, (), PyStream);
        try_compose!(self, py, other, PyObject, (), PyAdkLiveSink);

        Err(PyRuntimeError::new_err(
            "Cannot connect: component must accept PyObject input",
        ))
    }
}
