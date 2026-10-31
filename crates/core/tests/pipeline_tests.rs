use floq::functions::{Filter, Map, Reduce, Window};
use floq::pipeline::{IntoTask, PipelineMonitor, PipelineTask};
use floq::slots::{Merger, RoundRobinSplitter};
use floq::test_utils::{
    DelayedStringSource, NumberCollector, NumberDoubler, NumberSource, StringCollector,
    StringSource,
};
use std::time::Duration;

#[test]
fn test_lazy_pipeline_construction_without_tokio_runtime() {
    // Verify that `|` composition is purely declarative and does NOT require an active Tokio runtime.
    let collector = NumberCollector::new();
    let _pipeline =
        PipelineTask::new(NumberSource::new()) | PipelineTask::new(NumberDoubler::new()) | PipelineTask::new(collector);
}

#[tokio::test]
async fn test_simple_pipeline() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let pipeline = PipelineTask::new(NumberSource::new())
        | PipelineTask::new(NumberDoubler::new())
        | PipelineTask::new(collector);

    pipeline.run().await.unwrap();

    assert_eq!(*collector_results.lock().unwrap(), vec![0, 2, 4]);
}

#[tokio::test]
async fn test_into_task_fluent_syntax() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();

    let pipeline = NumberSource::new().into_task()
        | NumberDoubler::new().into_task()
        | collector.into_task();

    pipeline.run().await.unwrap();

    assert_eq!(*collector_results.lock().unwrap(), vec![0, 2, 4]);
}

#[tokio::test]
async fn test_source_only() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let pipeline = PipelineTask::new(NumberSource::new()) | PipelineTask::new(collector);

    pipeline.run().await.unwrap();

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

    pipeline.run().await.unwrap();

    assert_eq!(*collector_results.lock().unwrap(), vec![0, 4, 8]);
}

#[tokio::test]
async fn test_round_robin_splitter() {
    let collector = NumberCollector::new();
    let results = collector.results.clone();
    let collector_task = PipelineTask::with_slots(collector, 2);

    let splitter = RoundRobinSplitter::new();
    let splitter_task = PipelineTask::new(splitter);

    let pipeline = PipelineTask::new(NumberSource::new()) | splitter_task | collector_task;

    pipeline.run().await.unwrap();

    let results = results.lock().unwrap();
    assert_eq!(*results, vec![0, 2, 1]);
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn test_implicit_multi_slot_fanout_and_fanin() {
    let collector = NumberCollector::new();
    let results = collector.results.clone();

    // 1-slot source -> 2-slot doubler -> 1-slot collector (no explicit Splitter or Merger needed)
    let pipeline = NumberSource::with_count(4).into_task()
        | NumberDoubler::new().with_slots(2)
        | Merger::new().with_slots(2)
        | collector.into_task();

    pipeline.run().await.unwrap();

    let mut collected = results.lock().unwrap().clone();
    collected.sort_unstable();
    assert_eq!(collected, vec![0, 2, 4, 6]);
}

#[tokio::test]
async fn test_combine_heterogeneous_sources() {
    let collector = StringCollector::new();
    let results = collector.results.clone();

    let source_a = PipelineTask::new(StringSource::with_items(vec!["a".to_string()]));
    let source_b = PipelineTask::new(DelayedStringSource::new(vec![(
        "b".to_string(),
        Duration::from_millis(5),
    )]));

    let combined = source_a.combine(vec![source_b]);
    let pipeline = combined | PipelineTask::new(collector);

    pipeline.run().await.unwrap();

    let mut collected = results.lock().unwrap().clone();
    collected.sort();
    assert_eq!(collected, vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn test_filter() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    let filter = Filter::with_pattern(r"2").unwrap();
    let filter_task = PipelineTask::new(filter);

    let pipeline = PipelineTask::new(StringSource::new()) | filter_task | collector_task;

    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["2"]);
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn test_filter_lambda() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    let filter = Filter::with_lambda(|s| s.parse::<i32>().is_ok_and(|num| num % 2 == 0));
    let filter_task = PipelineTask::new(filter);

    let pipeline = PipelineTask::new(StringSource::new()) | filter_task | collector_task;

    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["0", "2"]);
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_map() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    let map = Map::new(|num: i32| format!("Number: {}", num));
    let map_task = PipelineTask::new(map);

    let pipeline = PipelineTask::new(NumberSource::new()) | map_task | collector_task;

    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["Number: 0", "Number: 1", "Number: 2"]);
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn test_reduce() {
    let collector = NumberCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    let reduce = Reduce::new(0, |acc: &mut i32, x: i32| {
        *acc += x;
    });
    let reduce_task = PipelineTask::new(reduce);

    let pipeline = PipelineTask::new(NumberSource::new()) | reduce_task | collector_task;

    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec![0, 1, 3]);
    assert_eq!(results.len(), 3);
}

#[tokio::test]
async fn test_count_based_window() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();
    let collector_task = PipelineTask::new(collector);

    let window = Window::with_count(2);
    let window_task = PipelineTask::new(window);

    let source = PipelineTask::new(StringSource::new());
    let map_task = PipelineTask::new(Map::new(|batch: Vec<String>| batch.join(",")));

    let pipeline = source | window_task | map_task | collector_task;

    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["0,1", "2"]);
}

#[tokio::test]
async fn test_time_based_window_flushes_on_timer() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();

    let source = DelayedStringSource::new(vec![
        ("a".to_string(), Duration::from_millis(5)),
        ("b".to_string(), Duration::from_millis(5)),
        ("c".to_string(), Duration::from_millis(60)),
    ]);

    let mut monitor = PipelineMonitor::new();
    let source_task = PipelineTask::new(source);
    let window_task = PipelineTask::new(Window::with_duration(Duration::from_millis(30)));
    let map_task = PipelineTask::new(Map::new(|batch: Vec<String>| batch.join(",")));
    let sink_task = PipelineTask::new(collector);

    source_task.register_with_monitor(&mut monitor);
    window_task.register_with_monitor(&mut monitor);

    let pipeline = source_task | window_task | map_task | sink_task;
    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["a,b", "c"]);
}
