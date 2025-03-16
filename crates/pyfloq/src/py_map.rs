use pyo3::prelude::*;
use std::sync::Arc;
use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::PipelineTask;
use floq::functions::Map;

/// Python wrapper for Map
#[pyclass]
#[derive(Clone)]
pub struct PyMap {
    map: Map<Vec<String>, PyObject>,
    task: Arc<PipelineTask<Map<Vec<String>, PyObject>>>,
}

impl PyPipelineWrapper<Vec<String>, PyObject> for PyMap {
    type Component = Map<Vec<String>, PyObject>;
    
    fn get_task(&self) -> Arc<PipelineTask<Self::Component>> {
        self.task.clone()
    }
    
    fn from_task_and_component(component: Self::Component, task: PipelineTask<Self::Component>) -> Self {
        Self {
            map: component,
            task: Arc::new(task),
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
        let map = Map::new(move |items: Vec<String>| {
            Python::with_gil(|py| {
                match callback.call1(py, (items,)) {
                    Ok(result) => result,
                    Err(e) => {
                        e.print(py);
                        // Convert empty Vec to Python list on error
                        Vec::<String>::new().into_py(py)
                    }
                }
            })
        });

        PyMap {
            map: map.clone(),
            task: Arc::new(PipelineTask::new(map)),
        }
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            if let Ok(collector) = other.extract::<crate::py_collector::PyCollector>(py) {
                let rt = pyo3_asyncio::tokio::get_runtime();
                let task = rt.block_on(async {
                    (*self.get_task()).clone() | (*collector.get_task()).clone()
                });
                Ok(crate::py_collector::PyCollector::from_task_and_component(collector.get_component().clone(), task).into_py(py))
            } else if let Ok(reduce) = other.extract::<crate::py_reduce::PyReduce>(py) {
                // Create a conversion Map that turns PyObject back into Vec<String>. But not sure do we want to do this :D
                // TODO: I need ot work with the type system anyway, so I guess this will be fixed when that is fixed. 
                let converter = Map::new(move |py_obj: PyObject| {
                    Python::with_gil(|py| {
                        match py_obj.extract::<Vec<String>>(py) {
                            Ok(vec) => vec,
                            Err(_) => Vec::new()
                        }
                    })
                });
                let converter_task = PipelineTask::new(converter);
                
                let rt = pyo3_asyncio::tokio::get_runtime();
                let task = rt.block_on(async {
                    (*self.get_task()).clone() | converter_task | (*reduce.get_task()).clone()
                });
                Ok(crate::py_reduce::PyReduce::from_task_and_component(reduce.get_component().clone(), task).into_py(py))
            } else {
                Ok(py.None())
            }
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<Vec<String>, PyObject>>::run_impl(self, py)
    }
} 