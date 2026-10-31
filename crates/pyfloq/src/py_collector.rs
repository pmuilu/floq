use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::impl_sink_component;
use floq::pipeline::{PipelineTask, Receiver, Sink};
use floq::Result;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[derive(Clone)]
pub struct CollectorComponent {
    callback: PyObject,
}

impl Sink for CollectorComponent {
    type Input = PyObject;

    async fn run(&self, input: Receiver<Self::Input>) -> Result<()> {
        while let Some(msg) = input.recv().await {
            Python::with_gil(|py| {
                if let Err(e) = self.callback.call1(py, (msg.payload,)) {
                    e.print(py);
                }
            });
        }
        Ok(())
    }
}

impl_sink_component!(CollectorComponent, PyObject);

/// Python wrapper for a terminal collector sink that invokes a Python callback per item.
#[pyclass(name = "Collector")]
#[derive(Clone)]
pub struct PyCollector {
    component: CollectorComponent,
    stage: PipelineTask<PyObject, ()>,
    rooted: Option<PipelineTask<(), ()>>,
}

impl PyPipelineWrapper<PyObject, ()> for PyCollector {
    type Component = CollectorComponent;

    fn get_stage(&self) -> &PipelineTask<PyObject, ()> {
        &self.stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<PyObject, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        Self {
            component,
            stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.component
    }
}

#[pymethods]
impl PyCollector {
    #[new]
    fn new(callback: PyObject) -> Self {
        let component = CollectorComponent { callback };
        Self {
            component: component.clone(),
            stage: PipelineTask::new(component),
            rooted: None,
        }
    }

    fn __or__(&self, _other: PyObject) -> PyResult<PyObject> {
        Err(PyRuntimeError::new_err(
            "Cannot connect: Collector is a terminal component",
        ))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<PyObject, ()>>::run_impl(self, py)
    }
}
