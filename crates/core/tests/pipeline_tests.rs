use semasense::engine::{PipelineTask, PipelineComponent, ComponentContext};
use semasense::test_utils::{
    NumberSource, NumberDoubler, NumberCollector, 
    StringSource, StringCollector, DelayedStringSource
};
use semasense::slots::round_robin_splitter::RoundRobinSplitter;
use semasense::functions::filter::Filter;
use semasense::functions::map::Map;
use semasense::functions::reduce::Reduce;
use semasense::functions::window::Window;
use tracing::debug;
use std::time::Duration;
use tokio::time::sleep;
use crossbeam_channel::{Sender, Receiver};
use std::sync::Arc;

#[tokio::test]
async fn test_simple_pipeline() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let pipeline = PipelineTask::new(NumberSource::new()) 
        | PipelineTask::new(NumberDoubler::new()) 
        | PipelineTask::new(collector);

    pipeline.run().await;

    assert_eq!(*collector_results.lock().unwrap(), vec![0, 2, 4]);
}

#[tokio::test]
async fn test_source_only() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let pipeline = PipelineTask::new(NumberSource::new()) 
        | PipelineTask::new(collector);

    pipeline.run().await;

    assert_eq!(*collector_results.lock().unwrap(), vec![0, 1, 2]);
}

#[tokio::test]
async fn test_multiple_transformers() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let pipeline = PipelineTask::new(NumberSource::new())
        | PipelineTask::new(NumberDoubler::new())
        | PipelineTask::new(NumberDoubler::new())
        | PipelineTask::new(collector);

    pipeline.run().await;

    assert_eq!(*collector_results.lock().unwrap(), vec![0, 4, 8]);
}

#[tokio::test]
async fn test_round_robin_splitter() {
    /*tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();*/

    // Create two collectors to verify split output
    let collector = NumberCollector::new();
    let results = collector.results.clone();
    let collector_task = PipelineTask::with_slots(collector, 2);

    let mut splitter = RoundRobinSplitter::new();
    let splitter_task = PipelineTask::new(splitter);
    
    // Create a pipeline with a splitter and two parallel collectors
    let pipeline = PipelineTask::new(NumberSource::new())
        | splitter_task
        | collector_task;

    pipeline.run().await;

    // Get the results from both collectors
    let results = results.lock().unwrap();
    
    // Verify that numbers were distributed between collectors
    // Source generates [0,1,2], so we expect:
    // Collector1: [0, 2]
    // Collector2: [1]
    assert_eq!(*results, vec![0, 2, 1]);
    
    // Verify total number of items processed
    assert_eq!(results.len(), 3);
} 

#[tokio::test]
async fn test_filter() {
   
    // Create a collector for strings
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    // Create a filter that only allows strings containing "2"
    let filter = Filter::with_pattern(r"2").unwrap();
    let filter_task = PipelineTask::new(filter);

    // Create a pipeline with a source, filter, and collector
    let pipeline = PipelineTask::new(StringSource::new()) 
        | filter_task
        | collector_task;

    pipeline.run().await;

    // Get the results from the collector
    let results = collector_results.lock().unwrap();
    
    // Verify that only strings containing "2" were collected
    assert_eq!(*results, vec!["2"]);
    
    // Verify total number of items processed
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_filter_lambda() {
    // Create a collector for strings
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    // Create a filter that only allows strings that parse as even numbers
    let filter = Filter::with_lambda(|s| {
        if let Ok(num) = s.parse::<i32>() {
            num % 2 == 0
        } else {
            false
        }
    });
    let filter_task = PipelineTask::new(filter);

    // Create a pipeline with a source, filter, and collector
    let pipeline = PipelineTask::new(StringSource::new()) 
        | filter_task
        | collector_task;

    pipeline.run().await;

    // Get the results from the collector
    let results = collector_results.lock().unwrap();
    
    // Verify that only even numbers were collected
    assert_eq!(*results, vec!["0", "2"]);
    
    // Verify total number of items processed
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_map() {
    // Create a collector for strings
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    // Create a map that converts numbers to strings with "Number: " prefix
    let map = Map::new(|num: i32| format!("Number: {}", num));
    let map_task = PipelineTask::new(map);

    // Create a pipeline with a source, map, and collector
    let pipeline = PipelineTask::new(NumberSource::new()) 
        | map_task
        | collector_task;

    pipeline.run().await;

    // Get the results from the collector
    let results = collector_results.lock().unwrap();
    
    // Verify the transformation
    assert_eq!(*results, vec![
        "Number: 0",
        "Number: 1",
        "Number: 2"
    ]);
    
    // Verify total number of items processed
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn test_reduce() {
    // Create a collector for numbers
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    // Create a reducer that sums numbers
    let reduce = Reduce::new(0, |acc: &mut i32, x: i32| {
        *acc += x;
    });
    let reduce_task = PipelineTask::new(reduce);

    // Create a pipeline with a source, reducer, and collector
    let pipeline = PipelineTask::new(NumberSource::new()) 
        | reduce_task
        | collector_task;

    pipeline.run().await;

    // Get the results from the collector
    let results = collector_results.lock().unwrap();
    
    // Verify the reduction
    // Source generates [0,1,2]
    // Running sum should be [0,1,3]
    assert_eq!(*results, vec![0, 1, 3]);
    
    // Verify total number of items processed
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn test_count_based_window() {
    // Create a collector for vectors of strings
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    // Create a window that batches every 2 items
    let window = Window::with_count(2);
    let window_task = PipelineTask::new(window);

    // Create a pipeline with a source, window, and collector
    let source = PipelineTask::new(StringSource::new());
    let map_task = PipelineTask::new(Map::new(|batch: Vec<String>| {
        // Join the batch into a single string for easier verification
        batch.join(",")
    }));

    let pipeline = source 
        | window_task
        | map_task
        | collector_task;

    pipeline.run().await;

    // Get the results from the collector
    let results = collector_results.lock().unwrap();
    
    // Source generates ["0", "1", "2"]
    // Window size 2 should produce ["0,1", "2"]
    assert_eq!(*results, vec!["0,1", "2"]);
}

