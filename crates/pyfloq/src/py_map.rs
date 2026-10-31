use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::functions::Map;
use floq::pipeline::PipelineTask;
use pyo3::prelude::*;

/// Python wrapper for [`Map`].
#[pyclass(name = "Map")]
#[derive(Clone)]
pub struct PyMap {
    map: Map<Vec<String>, PyObject>,
    stage: PipelineTask<Vec<String>, PyObject>,
    rooted: Option<PipelineTask<(), PyObject>>,
}

impl PyPipelineWrapper<Vec<String>, PyObject> for PyMap {
    type Component = Map<Vec<String>, PyObject>;

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
            map: component,
            stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.map
    }
}

#[pymethods]
impl PyMap {
    #[new]
    fn new(callback: PyObject) -> Self {
        let map = Map::new_boxed(move |items: Vec<String>| {
            Python::with_gil(|py| match callback.call1(py, (items,)) {
                Ok(result) => result,
                Err(e) => {
                    e.print(py);
                    Vec::<String>::new().into_py(py)
                }
            })
        });

        Self {
            map: map.clone(),
            stage: PipelineTask::new(map),
            rooted: None,
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            if let Ok(collector) = other.extract::<crate::py_collector::PyCollector>(py) {
                Ok(self.connect_to(&collector).into_py(py))
            } else if let Ok(reduce) = other.extract::<crate::py_reduce::PyReduce>(py) {
                let converter = Map::new(move |py_obj: PyObject| {
                    Python::with_gil(|py| py_obj.extract::<Vec<String>>(py).unwrap_or_default())
                });
                let converter_task = PipelineTask::new(converter);
                let reduce_stage = reduce.get_stage().clone();
                let rooted = self
                    .get_rooted()
                    .map(|left_root| left_root.clone() | converter_task | reduce_stage.clone());
                Ok(crate::py_reduce::PyReduce::from_parts(
                    reduce.get_component().clone(),
                    reduce_stage,
                    rooted,
                )
                .into_py(py))
            } else {
                Ok(py.None())
            }
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<Vec<String>, PyObject>>::run_impl(self, py)
    }
}
