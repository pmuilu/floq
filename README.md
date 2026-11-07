# Floq — Lightweight Real-Time Data Streaming for Rust & AI Agents

Floq is a fast, single-node, data-parallel streaming framework for Rust (with Python bindings in [`pyfloq`](crates/pyfloq)). It is designed for AI agents, real-time social/IoT firehoses, and micro-batching workloads that need the ergonomics of declarative stream processing without the operational overhead of distributed clusters like Apache Flink or Spark Streaming.

## Key Features

- **Declarative Pipe Syntax (`|`)**: Compose sources, operators, and sinks lazily (`source | filter | window | map | sink`). Channels and Tokio tasks are only materialized when `.run().await` is called.
- **Bounded Async Backpressure & Lock-Free Receivers**: Built on bounded `tokio::sync::mpsc` channels with exclusive per-slot receivers and zero-copy payload movement.
- **Data-Parallel Slots & Keyed Partitioning**: Scale any stage across parallel workers with `.with_slots(n)` (automatic round-robin fan-out and fan-in) or route stateful streams deterministically with `KeyBy`.
- **Flexible Windowing**: Count windows, tumbling time windows, sliding windows, and hybrid **count-or-duration** windows (`Window::with_count_or_duration`) ideal for batch-limited LLM and embedding APIs.
- **Auto-Reconnecting Sources**: Exponential-backoff WebSocket reconnection (with sequence cursor resumption on Bluesky's ATProto firehose).
- **Batteries-Included Connectors (Feature-Gated)**:
  - **Sources**: `BlueskyFirehoseSource`, `MastodonFirehoseSource`, `WebSocketSource`, `WebSocketMqttSource`, `FileSource`
  - **Transformers & Sinks**: `AdkAgent` (Google ADK), `GeminiEmbeddings` (`models/gemini-embedding-001`), `HuggingfaceEmbeddings`, `ChannelSink`, `WebhookSink`, `PrinterSink`

---

## Quick Start

```rust
use floq::{
    functions::{Filter, Map, Window},
    pipeline::{IntoTask, PipelineTask},
    sources::BlueskyFirehoseSource,
    transformers::{GeminiEmbeddings, PrinterSink},
    Result,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Stream live posts from the Bluesky firehose (auto-reconnects with cursor resumption)
    let source = BlueskyFirehoseSource::new().into_task();

    // 2. Filter posts matching a regex pattern
    let filter = Filter::with_pattern(r"(?i)\b(rust|tokio|ai)\b")?.into_task();

    // 3. Batch up to 50 posts or flush every 3 seconds (whichever comes first) across 4 slots
    let window = Window::with_count_or_duration(50, Duration::from_secs(3)).with_slots(4);

    // 4. Compute embeddings in parallel across 4 workers
    let embed = GeminiEmbeddings::try_new()?.with_slots(4);

    // 5. Summarize each batch and print to stdout
    let summarize = Map::new(|vectors: Vec<Vec<f32>>| {
        format!("Embedded batch of {} posts", vectors.len())
    })
    .into_task();
    let sink = PrinterSink::new("[embeddings] ").into_task();

    // Wire the pipeline declaratively and run
    let pipeline: PipelineTask<(), ()> = source | filter | window | embed | summarize | sink;
    pipeline.run().await
}
```

---

## Core Concepts

### 1. Defining Custom Components (`Source`, `Operator`, `Sink`)

Implement the role-specific trait (`Source`, `Operator`, or `Sink`) and bridge it with `impl_source_component!`, `impl_operator_component!`, or `impl_sink_component!`:

```rust
use floq::{
    impl_sink_component,
    pipeline::{IntoTask, Receiver, Sink},
    Result,
};

struct CustomSink;

impl Sink for CustomSink {
    type Input = String;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        while let Some(msg) = input.recv().await {
            println!("Event ({} ms): {}", msg.event_timestamp, msg.payload);
        }
        Ok(())
    }
}

impl_sink_component!(CustomSink, String);
```

### 2. Consuming Pipeline Output in Host Applications (`ChannelSink`)

To feed a pipeline's output directly into an AI agent or application loop as a standard Tokio channel:

```rust
use floq::{
    pipeline::IntoTask,
    sources::FileSource,
    transformers::ChannelSink,
};

let (sink, mut rx) = ChannelSink::new(256);
let pipeline = FileSource::new("events.log").into_task() | sink.into_task();

tokio::spawn(async move {
    let _ = pipeline.run().await;
});

while let Some(msg) = rx.recv().await {
    println!("Received: {}", msg.payload);
}
```

### 3. Keyed Partitioning (`KeyBy`)

When running stateful operators across multiple slots, insert `KeyBy` so events with the same key are guaranteed to land on the same worker slot:

```rust
use floq::{
    functions::Window,
    pipeline::IntoTask,
    slots::KeyBy,
};

let stage = KeyBy::new(|event: &String| {
    event.split(':').next().unwrap_or_default().to_owned()
})
.into_task()
    | Window::with_count(10).with_slots(4);
```

### 4. Combining Multiple Sources

Merge heterogeneous sources producing the same output type into a single stream:

```rust
let combined_source = mastodon_source.combine(vec![bluesky_source]);
let pipeline = combined_source | window | reducer | sink;
```

### 5. Google ADK Agent Integration (`AdkAgent`)

Pipe either individual `String` events or windowed `Vec<String>` batches directly into a Google ADK (`google-adk`) agent without an intermediate `Map` stage, and emit the agent's text response downstream:

```rust
use floq::{
    functions::{Filter, Window},
    pipeline::IntoTask,
    sources::BlueskyFirehoseSource,
    transformers::{AdkAgent, PrinterSink},
};
use std::time::Duration;

let source = BlueskyFirehoseSource::new().into_task();
let filter = Filter::with_pattern(r"(?i)\b(rust|ai|agent)\b")?.into_task();
let window = Window::with_count_or_duration(25, Duration::from_secs(5)).into_task();

// Polymorphic: accepts Vec<String> (or String) and emits String responses downstream.
// Supports shared, keyed (per-user/source), or ephemeral per-batch sessions.
let adk_agent = AdkAgent::new("http://localhost:8000", "trend_analyzer")
    .with_prompt_prefix("Summarize key technical signals in this batch:\n")
    .with_separator("\n---\n")
    .with_shared_session("live_firehose_session")
    .into_task();

let sink = PrinterSink::new("[ADK] ").into_task();

let pipeline = source | filter | window | adk_agent | sink;
pipeline.run().await?;
```

---

## Cargo Feature Flags

By default, `floq` enables the `full` feature set. For minimal builds, disable default features and select only the connectors you need:

```toml
[dependencies]
floq = { version = "0.1", default-features = false, features = ["bluesky", "gemini"] }
```

| Feature | Description |
| :--- | :--- |
| `full` *(default)* | Enables all built-in sources, embedding transformers, and sinks |
| `websocket` | Enables `WebSocketSource` (`tokio-tungstenite`) |
| `bluesky` | Enables `BlueskyFirehoseSource` (`websocket`, `ciborium`, `rs-car`, `chrono`) |
| `mastodon` | Enables `MastodonFirehoseSource` (`websocket`, `chrono`) |
| `mqtt` | Enables `WebSocketMqttSource` (`rumqttc`) |
| `gemini` | Enables `GeminiEmbeddings` (`reqwest`) |
| `adk` | Enables `AdkAgent` Google ADK connector (`reqwest`) |
| `huggingface` | Enables `HuggingfaceEmbeddings` (`reqwest`) |
| `webhook` | Enables `WebhookSink` (`reqwest`) |

---

## Examples

- **[`count_words`](examples/count_words/src/main.rs)**: Combines live Mastodon and Bluesky firehoses into a single stream and counts word frequencies in 10-second tumbling windows.
- **[`embeddings`](examples/embeddings/src/main.rs)**: Streams Bluesky posts, strips HTML, batches across 4 parallel slots using hybrid count-or-duration windows, computes embeddings via `GeminiEmbeddings`, and monitors stage metrics with `PipelineMonitor`.

Run an example with:

```bash
GEMINI_API_KEY="your-key" cargo run --package embeddings
```
