# Floq

Single-node, data-parallel stream processing engine in Rust with zero-GIL Python bindings (`pyfloq`) and native Google ADK (`google-adk`) integration.

## Architecture & Execution Model

- **Lazy DAG Materialization (`BitOr` Composition)**: The `|` operator (`std::ops::BitOr` in Rust, `__or__` in `pyfloq.Stream`) constructs a typed pipeline graph without spawning tasks or allocating queues. Bounded `tokio::sync::mpsc` channels (`Message<T>`) and per-slot worker futures are materialized only when `.run().await` is invoked, running inside a single `tokio::task::JoinSet` that aborts all sibling stages immediately on the first unhandled stage error.
- **Exclusive Per-Slot SPSC Receivers & Fan-Out Topologies**: Each parallel slot (`with_slots(n)`) owns an exclusive `mpsc::Receiver<Message<I>>` without shared `Arc<Mutex<Receiver>>` contention across workers. Upstream stages dispatch via:
  - **Round-Robin (`AtomicUsize` cursor)**: Lock-free distribution across downstream slot channels.
  - **Deterministic Keyed Routing (`KeyBy`)**: Computes `DefaultHasher` over the extracted routing key (`hash % num_slots`) so stateful operators (`Window`, `Reduce`, keyed `AdkAgent` sessions) preserve per-key ordering and state locality across parallel slots.
  - **Broadcast**: Clones payloads across independent downstream branches.
- **Polymorphic Stage Bridging (`AdkInput`)**: Operators like `AdkAgent<I>` implement `Operator` generically over `I: AdkInput` (`String` and `Vec<String>`), allowing direct composition after either unwindowed filters (`Filter -> AdkAgent<String>`) or windowed micro-batches (`Window -> AdkAgent<Vec<String>>`) without intermediate adapter stages.
- **Zero-GIL Python Execution (`pyfloq`)**: In Python, `pyfloq` compiles a type-erased `StreamNode` chain (`StreamOutput::Strings` | `StreamOutput::Batches`) into a native Rust `PipelineTask<(), ()>` executed on a multi-threaded Tokio runtime via `pyo3_async_runtimes`. Ingestion, regex filtering, CBOR/CAR decoding, and windowing execute entirely on Rust worker threads without holding the Python GIL; the GIL is acquired only at the sink/agent boundary via `loop.call_soon_threadsafe` or `into_future_with_locals`.

---

## Python (`pyfloq`) + Google ADK Integration

`pyfloq` provides two execution topologies for connecting Rust streaming pipelines to [`google-adk`](https://pypi.org/project/google-adk/) agents:

1. **`pyfloq.AdkLiveSink` (Push to `LiveRequestQueue`)**: Terminal sink that formats incoming `String` or `Vec<String>` micro-batches in Rust and dispatches `google.genai.types.Content` into an ADK `LiveRequestQueue` on the Python `asyncio` event loop via `call_soon_threadsafe`.
2. **`pyfloq.AdkAgent` (In-Pipeline Operator)**: Mid-pipeline transformer (`Source | Filter | Window | AdkAgent | Sink`) that invokes `Runner.run_async` per item/batch on the Python asyncio loop (`shared`, `ephemeral`, or `from_source_id` session routing) and feeds the agent's synthesized text response back into the Rust pipeline.

### Example: Windowed Bluesky Firehose -> `AdkLiveSink` (`crates/pyfloq/examples/adk_live_example.py`)

```python
import asyncio
import pyfloq
from google.adk.agents import Agent, LiveRequestQueue
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService


async def main():
    agent = Agent(
        name="firehose_analyst",
        model="gemini-3.5-flash-lite",
        instruction="Summarize emerging technical trends from incoming Bluesky post batches in 2-3 bullet points.",
    )
    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name="floq_app", user_id="floq", session_id="live_stream"
    )
    runner = Runner(agent=agent, app_name="floq_app", session_service=session_service)
    live_queue = LiveRequestQueue()

    # Ingest ATProto CBOR/CAR frames, regex-filter, and batch in Rust (GIL-free),
    # then push formatted types.Content batches into ADK's LiveRequestQueue.
    pipeline = (
        pyfloq.BlueskyFirehoseSource()
        | pyfloq.Filter(r"(?i)\b(rust|python|ai|llm|agent)\b")
        | pyfloq.Window.with_count_or_duration(15, 8000)
        | pyfloq.AdkLiveSink(
            live_queue,
            prompt_prefix="Summarize the following batch of live social posts:\n",
            separator="\n---\n",
        )
    )

    pipeline_future = asyncio.ensure_future(pipeline.run())

    while not live_queue.closed:
        req = await live_queue.get()
        if req.close or req.content is None:
            break

        async for event in runner.run_async(
            user_id="floq",
            session_id="live_stream",
            new_message=req.content,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        print(part.text)

    await pipeline_future


if __name__ == "__main__":
    asyncio.run(main())
```

### Example: In-Pipeline `pyfloq.AdkAgent` Operator

```python
collector = pyfloq.Collector()

pipeline = (
    pyfloq.BlueskyFirehoseSource()
    | pyfloq.Filter(r"(?i)\b(rust|tokio|pyo3)\b")
    | pyfloq.Window.with_count_or_duration(20, 5000)
    | pyfloq.AdkAgent(
        runner,
        session_mode="ephemeral",  # or "shared" / "from_source_id"
        prompt_prefix="Extract key technical topics:\n",
    )
    | collector
)
```

---

## Rust Pipeline API

### Declarative Pipeline & Parallel Slots

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
    let source = BlueskyFirehoseSource::new().into_task();
    let filter = Filter::with_pattern(r"(?i)\b(rust|tokio|ai)\b")?.into_task();

    // Hybrid count-or-duration window partitioned across 4 parallel worker slots
    let window = Window::with_count_or_duration(50, Duration::from_secs(3)).with_slots(4);
    let embed = GeminiEmbeddings::try_new()?.with_slots(4);

    let summarize = Map::new(|vectors: Vec<Vec<f32>>| {
        format!("Embedded batch of {} posts", vectors.len())
    })
    .into_task();
    let sink = PrinterSink::new("[embeddings] ").into_task();

    let pipeline: PipelineTask<(), ()> = source | filter | window | embed | summarize | sink;
    pipeline.run().await
}
```

### Rust `AdkAgent` Operator (HTTP Sidecar / ADK API Server)

In pure Rust pipelines, `AdkAgent<I>` communicates with an `adk api_server` endpoint over HTTP (`/apps/{app}/users/{user}/sessions/{session}` and `/run`) with automatic `404` session initialization and configurable session scoping (`AdkSessionMode::Shared`, `Keyed`, `FromSourceId`, or `Ephemeral`):

```rust
use floq::{
    functions::{Filter, Window},
    pipeline::IntoTask,
    sources::BlueskyFirehoseSource,
    transformers::{AdkAgent, PrinterSink},
};
use std::time::Duration;

let pipeline = BlueskyFirehoseSource::new().into_task()
    | Filter::with_pattern(r"(?i)\b(rust|ai|agent)\b")?.into_task()
    | Window::with_count_or_duration(25, Duration::from_secs(5)).into_task()
    | AdkAgent::new("http://localhost:8000", "trend_analyzer")
        .with_prompt_prefix("Summarize key technical signals in this batch:\n")
        .with_separator("\n---\n")
        .with_shared_session("live_firehose_session")
        .into_task()
    | PrinterSink::new("[ADK] ").into_task();

pipeline.run().await?;
```

### Custom Components (`Source`, `Operator`, `Sink`)

Implement the role trait and bridge it to `PipelineComponent` with `impl_source_component!`, `impl_operator_component!`, or `impl_sink_component!`:

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

### Keyed Partitioning (`KeyBy`) & Host Channel Bridge (`ChannelSink`)

```rust
use floq::{
    functions::Window,
    pipeline::IntoTask,
    slots::KeyBy,
    transformers::ChannelSink,
};

// Deterministic hash routing across 4 stateful window slots
let keyed_window = KeyBy::new(|event: &String| {
    event.split(':').next().unwrap_or_default().to_owned()
})
.into_task()
    | Window::with_count(10).with_slots(4);

// Bridge pipeline output into an external Tokio mpsc::Receiver
let (sink, mut rx) = ChannelSink::new(256);
```

---

## Cargo Feature Flags

```toml
[dependencies]
floq = { version = "0.1", default-features = false, features = ["bluesky", "adk"] }
```

| Feature | Crate / Protocol Dependencies |
| :--- | :--- |
| `full` *(default)* | All sources, operators, and sinks |
| `websocket` | `WebSocketSource` (`tokio-tungstenite`) |
| `bluesky` | `BlueskyFirehoseSource` (`websocket`, `ciborium` DAG-CBOR, `rs-car` ATProto CAR blocks) |
| `mastodon` | `MastodonFirehoseSource` (`websocket`, `chrono`) |
| `mqtt` | `WebSocketMqttSource` (`rumqttc`) |
| `gemini` | `GeminiEmbeddings` (`reqwest`) |
| `adk` | `AdkAgent` (`reqwest`, `uuid`) |
| `huggingface` | `HuggingfaceEmbeddings` (`reqwest`) |
| `webhook` | `WebhookSink` (`reqwest`) |
