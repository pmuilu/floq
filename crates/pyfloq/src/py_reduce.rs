use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::functions::Reduce;
use floq::pipeline::PipelineTask;
use pyo3::prelude::*;

/// Python wrapper for [`Reduce`].
#[pyclass(name = "Reduce")]
#[derive(Clone)]
pub struct PyReduce {
    reduce: Reduce<Vec<String>, PyObject>,
    stage: PipelineTask<Vec<String>, PyObject>,
    rooted: Option<PipelineTask<(), PyObject>>,
}

impl PyPipelineWrapper<Vec<String>, PyObject> for PyReduce {
    type Component = Reduce<Vec<String>, PyObject>;

    fn get_stage(&self) -> &PipelineTask<Vec<String>, PyObject> {
        &self.stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), PyObject>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<Vec<String>, PyObject>,
        rooted: Option<PipelineTask<(), PyObject>>,
    ) -> Self {
        Self {
            reduce: component,
            stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.reduce
    }
}

#[pymethods]
impl PyReduce {
    #[new]
    fn new(initial: PyObject, callback: PyObject) -> Self {
        Python::with_gil(|py| {
            let reducer = move |acc: &mut PyObject, items: Vec<String>| {
                Python::with_gil(|py| {
                    if let Ok(result) = callback.call1(py, (acc.clone_ref(py), items)) {
                        *acc = result;
                    }
                });
            };

            let reduce = Reduce::new_boxed(initial.clone_ref(py), reducer);
            Self {
                reduce: reduce.clone(),
                stage: PipelineTask::new(reduce),
                rooted: None,
            }
        })
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| self.compose_pyobject(py, &other))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<Vec<String>, PyObject>>::run_impl(self, py)
    }
}
