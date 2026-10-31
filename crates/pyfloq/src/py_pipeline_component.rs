use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::impl_operator_component;
use floq::pipeline::{Operator, PipelineTask, Receiver, Sender};
use floq::Result;
use pyo3::prelude::*;

#[derive(Clone)]
pub struct RustPipelineComponent {
    callback: PyObject,
}

impl Operator for RustPipelineComponent {
    type Input = String;
    type Output = String;

    async fn run(&self, input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        while let Some(msg) = input.recv().await {
            let transformed = Python::with_gil(|py| {
                match self.callback.call1(py, (msg.payload.as_str(),)) {
                    Ok(result) => result.extract::<String>(py).ok(),
                    Err(e) => {
                        e.print(py);
                        None
                    }
                }
            });

            if let Some(result_str) = transformed {
                output.send(msg.with_new_payload(result_str)).await?;
            }
        }
        Ok(())
    }
}

impl_operator_component!(RustPipelineComponent, String, String);

/// Python wrapper for a custom string-to-string pipeline operator.
#[pyclass(name = "PipelineComponent")]
#[derive(Clone)]
pub struct PyPipelineComponent {
    component: RustPipelineComponent,
    stage: PipelineTask<String, String>,
    rooted: Option<PipelineTask<(), String>>,
}

impl PyPipelineWrapper<String, String> for PyPipelineComponent {
    type Component = RustPipelineComponent;

    fn get_stage(&self) -> &PipelineTask<String, String> {
        &self.stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), String>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, String>,
        rooted: Option<PipelineTask<(), String>>,
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
impl PyPipelineComponent {
    #[new]
    fn new(callback: PyObject) -> Self {
        let component = RustPipelineComponent { callback };
        Self {
            component: component.clone(),
            stage: PipelineTask::new(component),
            rooted: None,
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| self.compose_string(py, &other))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, String>>::run_impl(self, py)
    }
}
