use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::impl_sink_component;
use floq::pipeline::{PipelineTask, Receiver, Sink};
use floq::Result;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct CollectorComponent<T> {
    callback: PyObject,
    _marker: PhantomData<T>,
}

impl<T> CollectorComponent<T> {
    pub fn new(callback: PyObject) -> Self {
        Self {
            callback,
            _marker: PhantomData,
        }
    }
}

impl<T> Sink for CollectorComponent<T>
where
    T: IntoPy<PyObject> + Clone + Send + Sync + 'static,
{
    type Input = T;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        while let Some(msg) = input.recv().await {
            Python::with_gil(|py| {
                if let Err(e) = self.callback.call1(py, (msg.payload.into_py(py),)) {
                    e.print(py);
                }
            });
        }
        Ok(())
    }
}

impl_sink_component!(CollectorComponent<String>, String);
impl_sink_component!(CollectorComponent<Vec<String>>, Vec<String>);
impl_sink_component!(CollectorComponent<PyObject>, PyObject);

/// Python wrapper for a terminal collector sink that invokes a Python callback per item.
#[pyclass(name = "Collector")]
#[derive(Clone)]
pub struct PyCollector {
    string_component: CollectorComponent<String>,
    string_stage: PipelineTask<String, ()>,
    vec_component: CollectorComponent<Vec<String>>,
    vec_stage: PipelineTask<Vec<String>, ()>,
    py_component: CollectorComponent<PyObject>,
    py_stage: PipelineTask<PyObject, ()>,
    rooted: Option<PipelineTask<(), ()>>,
}

impl PyPipelineWrapper<String, ()> for PyCollector {
    type Component = CollectorComponent<String>;

    fn get_stage(&self) -> &PipelineTask<String, ()> {
        &self.string_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let cb = Python::with_gil(|py| component.callback.clone_ref(py));
        let vec_component = CollectorComponent::<Vec<String>>::new(Python::with_gil(|py| cb.clone_ref(py)));
        let py_component = CollectorComponent::<PyObject>::new(cb);
        Self {
            string_component: component,
            string_stage: stage,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.string_component
    }
}

impl PyPipelineWrapper<Vec<String>, ()> for PyCollector {
    type Component = CollectorComponent<Vec<String>>;

    fn get_stage(&self) -> &PipelineTask<Vec<String>, ()> {
        &self.vec_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<Vec<String>, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let cb = Python::with_gil(|py| component.callback.clone_ref(py));
        let string_component = CollectorComponent::<String>::new(Python::with_gil(|py| cb.clone_ref(py)));
        let py_component = CollectorComponent::<PyObject>::new(cb);
        Self {
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_component: component,
            vec_stage: stage,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.vec_component
    }
}

impl PyPipelineWrapper<PyObject, ()> for PyCollector {
    type Component = CollectorComponent<PyObject>;

    fn get_stage(&self) -> &PipelineTask<PyObject, ()> {
        &self.py_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<PyObject, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let cb = Python::with_gil(|py| component.callback.clone_ref(py));
        let string_component = CollectorComponent::<String>::new(Python::with_gil(|py| cb.clone_ref(py)));
        let vec_component = CollectorComponent::<Vec<String>>::new(cb);
        Self {
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_component: component,
            py_stage: stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.py_component
    }
}

#[pymethods]
impl PyCollector {
    #[new]
    fn new(callback: PyObject) -> Self {
        let (cb1, cb2, cb3) = Python::with_gil(|py| {
            (
                callback.clone_ref(py),
                callback.clone_ref(py),
                callback,
            )
        });
        let string_component = CollectorComponent::<String>::new(cb1);
        let vec_component = CollectorComponent::<Vec<String>>::new(cb2);
        let py_component = CollectorComponent::<PyObject>::new(cb3);
        Self {
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
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
