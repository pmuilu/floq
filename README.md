# Floq

Single-node, data-parallel stream processing engine in Rust with zero-GIL Python bindings (`pyfloq`) and native Google ADK (`google-adk`) integration.

## Architecture & Execution Model

- **Lazy DAG Materialization (`BitOr` Composition)**: The `|` operator (`std::ops::BitOr` in Rust, `__or__` in `pyfloq`) constructs a typed pipeline graph without spawning tasks or allocating queues. Bounded `tokio::sync::mpsc` channels (`Message<T>`) and per-slot worker futures are materialized only when `.run().await` is invoked, running inside a single `tokio::task::JoinSet` that aborts all sibling stages immediately on the first unhandled stage error.
- **Exclusive Per-Slot SPSC Receivers & Fan-Out Topologies**: Each parallel slot (`with_slots(n)`) owns an exclusive `mpsc::Receiver<Message<I>>` without shared `Arc<Mutex<Receiver>>` contention across workers. Upstream stages dispatch via:
  - **Round-Robin (`AtomicUsize` cursor)**: Lock-free distribution across downstream slot channels.
  - **Deterministic Keyed Routing (`KeyBy`)**: Computes `DefaultHasher` over an extracted routing key or RFC 6901 JSON Pointer (`KeyBy::with_json_field("/did")`, `hash % num_slots`) so stateful operators (`Window`, `Reduce`, keyed `AdkAgent` sessions) preserve per-key ordering and state locality across parallel slots.
  - **Broadcast**: Clones payloads across independent downstream branches.
- **Structured JSON Wire Mode & JSON-Pointer Filtering**: Built-in sources (`BlueskyFirehoseSource`, `MastodonFirehoseSource`) support `OutputFormat::Json` (`.with_json_output()` / `json_output=True`), emitting canonical JSON records (`uri`, `cid`, `did`, `text`, `langs`, `reply_to`, `created_at`, `seq`) and tagging `Message::source_id`. `Filter::with_json_pattern("/text", pattern)` (`pyfloq.Filter(pattern, json_path="/text")`) evaluates regexes against a specific RFC 6901 JSON Pointer while forwarding the full structured JSON payload downstream to `Window` and ADK agents. For typed Rust stages, `JsonParse<T>` deserializes JSON strings into `T: DeserializeOwned` mid-pipeline.
- **WAL-Mode SQLite Checkpointing (`SqliteCheckpointStore`)**: Persists source cursors (`BlueskyFirehoseSource` ATProto sequence numbers) and byte offsets (`FileSource`) in a local SQLite database (`PRAGMA journal_mode=WAL`) for exact resumption across restarts.
- **Zero-GIL Python Execution (`pyfloq`)**: In Python, `pyfloq` compiles a pipeline into a native Rust `PipelineTask<(), ()>` executed on a multi-threaded Tokio runtime. Ingestion, CBOR/CAR decoding, JSON-pointer regex filtering, and windowing execute entirely on Rust worker threads without holding the Python GIL; the GIL is acquired only at the sink/agent boundary.

---

## Python (`pyfloq`) + Google ADK Integration

`pyfloq` provides two execution topologies for connecting Rust streaming pipelines to [`google-adk`](https://pypi.org/project/google-adk/) agents:

1. **`pyfloq.AdkLiveSink` (Push to `LiveRequestQueue`)**: Terminal sink that formats incoming `String` or `Vec<String>` micro-batches in Rust and dispatches `google.genai.types.Content` into an ADK `LiveRequestQueue` on the Python `asyncio` event loop via `call_soon_threadsafe`.
2. **`pyfloq.AdkAgent` (In-Pipeline Operator)**: Mid-pipeline transformer (`Source | Filter | Window | AdkAgent | Sink`) that invokes `Runner.run_async` per item/batch on the Python asyncio loop (`shared`, `ephemeral`, or `from_source_id` session routing) and feeds the agent's synthesized text response back into the Rust pipeline.

### Example: Structured Bluesky Firehose + SQLite Checkpointing -> `AdkLiveSink` (`crates/pyfloq/examples/adk_live_example.py`)

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

    # Ingest ATProto CBOR/CAR frames as structured JSON, persist the sequence cursor
    # to SQLite, filter specifically on `/text` in Rust (GIL-free), and push batches
    # into ADK's LiveRequestQueue.
    pipeline = (
        pyfloq.BlueskyFirehoseSource(
            json_output=True,
            checkpoint_db="floq_checkpoints.sqlite",
        )
        | pyfloq.Filter(r"(?i)\b(rust|python|ai|llm|agent)\b", json_path="/text")
        | pyfloq.Window.with_count_or_duration(15, 8000)
        | pyfloq.AdkLiveSink(
            live_queue,
            prompt_prefix="Summarize the following batch of live social posts (JSON records):\n",
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

### Example: Tailing Local JSONL Logs (`FileSource.tail`) -> `AdkAgent`

`pyfloq.FileSource.tail` implements `tail -F` semantics with inode/truncation rotation detection and SQLite byte-offset checkpointing:

```python
collector = pyfloq.Collector()

pipeline = (
    pyfloq.FileSource.tail(
        "/var/log/service.jsonl",
        checkpoint_db="floq_checkpoints.sqlite",
    )
    | pyfloq.Filter(r"(?i)\b(error|panic|timeout)\b", json_path="/message")
    | pyfloq.Window.with_count_or_duration(20, 5000)
    | pyfloq.AdkAgent(
        runner,
        session_mode="ephemeral",  # or "shared" / "from_source_id"
        prompt_prefix="Diagnose the root cause from these error log records:\n",
    )
    | collector
)
```

---

## Rust Pipeline API

### Structured JSON, SQLite Checkpoints & Parallel Slots

```rust
use floq::{
    checkpoint::SqliteCheckpointStore,
    functions::{Filter, Map, Window},
    pipeline::{IntoTask, PipelineTask},
    sources::BlueskyFirehoseSource,
    transformers::{GeminiEmbeddings, PrinterSink},
    Result,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let store = SqliteCheckpointStore::open("floq_checkpoints.sqlite")?;

    let source = BlueskyFirehoseSource::new()
        .with_json_output()
        .with_default_checkpoint(store)
        .into_task();

    // Match regex against `/text` while forwarding the full JSON record
    let filter = Filter::with_json_pattern("/text", r"(?i)\b(rust|tokio|ai)\b")?.into_task();

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

### File Tailing with Rotation Detection & Checkpointing (`FileSource::tail`)

```rust
use floq::{
    checkpoint::SqliteCheckpointStore,
    functions::Filter,
    pipeline::IntoTask,
    sources::FileSource,
    transformers::PrinterSink,
};

let store = SqliteCheckpointStore::open("checkpoints.sqlite")?;

let pipeline = FileSource::tail("/var/log/app.jsonl")
    .with_default_checkpoint(store)
    .into_task()
    | Filter::with_json_pattern("/level", "^ERROR$")?.into_task()
    | PrinterSink::new("[alert] ").into_task();
```

### Keyed JSON Partitioning (`KeyBy::with_json_field`) & Typed Parsing (`JsonParse<T>`)

```rust
use floq::{
    functions::{JsonParse, Window},
    pipeline::IntoTask,
    slots::KeyBy,
};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
struct BlueskyPost {
    uri: String,
    did: String,
    text: String,
}

// Route JSON strings deterministically by `/did` across 4 parallel window slots
let keyed_window = KeyBy::with_json_field("/did").into_task()
    | Window::with_count(10).with_slots(4);

// Or deserialize JSON strings into typed Rust structs mid-pipeline
let typed_stage = JsonParse::<BlueskyPost>::new().into_task();
```

---

## Cargo Feature Flags

```toml
[dependencies]
floq = { version = "0.1", default-features = false, features = ["bluesky", "adk", "sqlite"] }
```

| Feature | Crate / Protocol Dependencies |
| :--- | :--- |
| `full` *(default)* | All sources, operators, sinks, and SQLite checkpointing |
| `sqlite` | `SqliteCheckpointStore` (links system `libsqlite3` in WAL mode) |
| `websocket` | `WebSocketSource` (`tokio-tungstenite`) |
| `bluesky` | `BlueskyFirehoseSource` (`websocket`, `ciborium` DAG-CBOR, `rs-car` ATProto CAR blocks) |
| `mastodon` | `MastodonFirehoseSource` (`websocket`, `chrono`) |
| `mqtt` | `WebSocketMqttSource` (`rumqttc`) |
| `gemini` | `GeminiEmbeddings` (`reqwest`) |
| `adk` | `AdkAgent` (`reqwest`) |
| `huggingface` | `HuggingfaceEmbeddings` (`reqwest`) |
| `webhook` | `WebhookSink` (`reqwest`) |
