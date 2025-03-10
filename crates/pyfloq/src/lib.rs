use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use std::time::Duration;

use floq::pipeline::{PipelineComponent, PipelineTask, ComponentContext, Sender, Receiver, Message};
use floq::sources::BlueskyFirehoseSource;
use floq::functions::{Window, Reduce, Filter};
use tokio::runtime::Handle;

mod py_pipeline_wrapper;
mod py_filter;
mod py_window;
mod py_reduce;
mod py_collector;
mod py_pipeline_component;
mod py_bluesky_firehose;
mod py_map;

use py_filter::PyFilter;
use py_window::PyWindow;
use py_reduce::PyReduce;
use py_collector::PyCollector;
use py_pipeline_component::PyPipelineComponent;
use py_bluesky_firehose::PyBlueskyFirehoseSource;
use py_map::PyMap;

// A Python module implemented in Rust.
#[pymodule]
fn pyfloq(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyPipelineComponent>()?;
    m.add_class::<PyBlueskyFirehoseSource>()?;
    m.add_class::<PyWindow>()?;
    m.add_class::<PyReduce>()?;
    m.add_class::<PyCollector>()?;
    m.add_class::<PyFilter>()?;
    m.add_class::<PyMap>()?;
    Ok(())
}
