use floq::functions::{Filter, Map, Reduce, Window};
use floq::pipeline::{IntoTask, PipelineMonitor, PipelineTask};
use floq::slots::{KeyBy, Merger, RoundRobinSplitter};
use floq::test_utils::{
    DelayedStringSource, NumberCollector, NumberDoubler, NumberSource, StringCollector,
    StringSource,
};
use floq::transformers::ChannelSink;
use std::time::Duration;

#[test]
fn test_lazy_pipeline_construction_without_tokio_runtime() {
    // Verify that `|` composition is purely declarative and does NOT require an active Tokio runtime.
    let collector = NumberCollector::new();
    let _pipeline = PipelineTask::new(NumberSource::new())
        | PipelineTask::new(NumberDoubler::new())
        | PipelineTask::new(collector);
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

#[tokio::test]
async fn test_hybrid_count_or_duration_window() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();

    // First 2 items arrive quickly and trigger on max_count=2; 3rd item triggers on timeout.
    let source = DelayedStringSource::new(vec![
        ("a".to_string(), Duration::from_millis(2)),
        ("b".to_string(), Duration::from_millis(2)),
        ("c".to_string(), Duration::from_millis(5)),
    ]);

    let pipeline = source.into_task()
        | Window::with_count_or_duration(2, Duration::from_millis(30)).into_task()
        | Map::new(|batch: Vec<String>| batch.join(",")).into_task()
        | collector.into_task();

    pipeline.run().await.unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["a,b", "c"]);
}

#[tokio::test]
async fn test_key_by_deterministic_partitioning() {
    let (sink, mut rx) = ChannelSink::new(16);

    let items = vec![
        "user1:hello".to_string(),
        "user2:world".to_string(),
        "user1:again".to_string(),
        "user2:bye".to_string(),
    ];

    // Partition by user prefix across 2 window slots (each batching 2 items per key).
    let pipeline = StringSource::with_items(items).into_task()
        | KeyBy::new(|s: &String| s.split(':').next().unwrap_or_default().to_string()).into_task()
        | Window::with_count(2).with_slots(2)
        | Map::new(|batch: Vec<String>| batch.join("|")).into_task()
        | sink.into_task();

    pipeline.run().await.unwrap();

    let mut batches = Vec::new();
    while let Some(msg) = rx.recv().await {
        batches.push(msg.payload);
    }
    assert_eq!(batches.len(), 2);
}

#[tokio::test]
async fn test_graceful_shutdown_drains_inflight_window() {
    let collector = StringCollector::new();
    let collector_results = collector.results.clone();

    // Source has 3 items: 2 arrive before shutdown (at 35ms), 3rd arrives at 500ms (after shutdown).
    let source = DelayedStringSource::new(vec![
        ("item1".to_string(), Duration::from_millis(5)),
        ("item2".to_string(), Duration::from_millis(5)),
        ("never_sent".to_string(), Duration::from_millis(500)),
    ]);

    // Window has a long 10-second duration, but when shutdown fires at 35ms,
    // the source stops and drops its sender, causing the window to flush ["item1", "item2"] to the sink!
    let pipeline = source.into_task()
        | Window::with_duration(Duration::from_secs(10)).into_task()
        | Map::new(|batch: Vec<String>| batch.join(",")).into_task()
        | collector.into_task();

    pipeline
        .run_with_shutdown(async {
            tokio::time::sleep(Duration::from_millis(35)).await;
        })
        .await
        .unwrap();

    let results = collector_results.lock().unwrap();
    assert_eq!(*results, vec!["item1,item2"]);
}

#[test]
fn test_adk_agent_polymorphic_pipeline_composition() {
    use floq::transformers::AdkAgent;

    // 1. Compose AdkAgent directly after a String stage (Filter -> AdkAgent<String> -> Sink)
    let string_agent = AdkAgent::<String>::new("http://localhost:8000", "triage_agent")
        .with_prompt_prefix("Classify event:\n")
        .with_source_id_session("default");
    let _string_pipeline = StringSource::new().into_task()
        | Filter::with_pattern(".*").unwrap().into_task()
        | string_agent.into_task()
        | StringCollector::new().into_task();

    // 2. Compose AdkAgent directly after a Window stage (Window -> AdkAgent<Vec<String>> -> Sink)
    let batch_agent = AdkAgent::<Vec<String>>::new("http://localhost:8000", "summary_agent")
        .with_prompt_prefix("Summarize batch:\n")
        .with_separator("\n---\n")
        .with_ephemeral_sessions();
    let _batch_pipeline = StringSource::new().into_task()
        | Window::with_count(5).into_task()
        | batch_agent.into_task()
        | StringCollector::new().into_task();
}
