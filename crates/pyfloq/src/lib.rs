#![allow(non_local_definitions)]

use pyo3::prelude::*;

mod py_adk;
mod py_bluesky_firehose;
mod py_collector;
mod py_file_source;
mod py_filter;
mod py_map;
mod py_pipeline_component;
mod py_pipeline_wrapper;
mod py_reduce;
mod py_stream;
mod py_window;

use py_adk::{PyAdkAgent, PyAdkLiveSink};
use py_bluesky_firehose::PyBlueskyFirehoseSource;
use py_collector::PyCollector;
use py_file_source::PyFileSource;
use py_filter::PyFilter;
use py_map::PyMap;
use py_pipeline_component::PyPipelineComponent;
use py_reduce::PyReduce;
use py_stream::PyStream;
use py_window::PyWindow;

// A Python module implemented in Rust.
#[pymodule]
fn pyfloq(_py: Python, m: &PyModule) -> PyResult<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    m.add_class::<PyPipelineComponent>()?;
    m.add_class::<PyBlueskyFirehoseSource>()?;
    m.add_class::<PyFileSource>()?;
    m.add_class::<PyWindow>()?;
    m.add_class::<PyReduce>()?;
    m.add_class::<PyCollector>()?;
    m.add_class::<PyFilter>()?;
    m.add_class::<PyMap>()?;
    m.add_class::<PyStream>()?;
    m.add_class::<PyAdkLiveSink>()?;
    m.add_class::<PyAdkAgent>()?;
    Ok(())
}
