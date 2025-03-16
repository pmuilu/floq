use pyo3::prelude::*;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::PipelineTask;
use floq::functions::Reduce;
use crate::py_collector::PyCollector;

/// Python wrapper for Reduce
#[pyclass(name = "Reduce")]
#[derive(Clone)]
pub struct PyReduce {
    #[allow(dead_code)]
    callback: PyObject,
    #[allow(dead_code)]
    initial: PyObject,
    reduce: Reduce<Vec<String>, PyObject>,
    task: Arc<PipelineTask<Reduce<Vec<String>, PyObject>>>,
}

impl PyPipelineWrapper<Vec<String>, PyObject> for PyReduce {
    type Component = Reduce<Vec<String>, PyObject>;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            callback: Python::with_gil(|py| py.None()),  // These fields are only used for new instances
            initial: Python::with_gil(|py| py.None()),   // The task contains the actual reducer and state
            reduce: component,
            task: Arc::new(task),
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
            let callback_clone = callback.clone();
            let reducer = move |acc: &mut PyObject, items: Vec<String>| {
                Python::with_gil(|py| {
                    if let Ok(result) = callback_clone.call1(py, (acc.clone_ref(py), items,)) {
                        *acc = result;
                    }
                });
            };

            let reduce = Reduce::new(initial.clone_ref(py), reducer);
            
            PyReduce {
                callback: callback.clone(),
                initial: initial.clone(),
                reduce: reduce.clone(),
                task: Arc::new(PipelineTask::new(reduce)),
            }
        })
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            if let Ok(collector) = other.extract::<PyCollector>(py) {
                let rt = pyo3_asyncio::tokio::get_runtime();
                let task = rt.block_on(async {
                    (*self.get_task()).clone() | (*collector.get_task()).clone()
                });
                Ok(PyCollector::from_task_and_component(collector.get_component().clone(), task).into_py(py))
            } else {
                Ok(py.None())
            }
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<Vec<String>, PyObject>>::run_impl(self, py)
    }
} 