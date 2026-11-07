# `pyfloq` — Python Bindings for Floq

`pyfloq` brings Floq's Rust streaming engine to Python `asyncio`. High-throughput socket ingestion, ATProto CBOR/CAR decoding, regex filtering, and windowing run in native Rust worker tasks without holding the Python GIL, while your Python callbacks process windowed batches or individual items.

## Building & Installing Locally

Ensure you have a Python virtual environment and [`maturin`](https://www.maturin.rs/) installed:

```bash
cd crates/pyfloq
maturin develop --release
```

## Quick Start

```python
import asyncio
import pyfloq

async def main():
    # 1. Stream live posts from the Bluesky firehose in Rust
    source = pyfloq.BlueskyFirehoseSource()

    # 2. Filter posts using a compiled Rust regex (or Filter.with_lambda(fn))
    filter_stage = pyfloq.Filter(r"(?i)\b(rust|python|ai)\b")

    # 3. Group matching posts into 5-second windows (or Window.with_count_or_duration(100, 5000))
    window = pyfloq.Window(5000)

    # 4. Fold each batch into a running word-count dictionary
    def count_words(acc, messages):
        if acc is None:
            acc = {}
        for message in messages:
            for word in message.split():
                acc[word] = acc.get(word, 0) + 1
        return acc

    reducer = pyfloq.Reduce(None, count_words)

    # 5. Terminal collector callback
    def print_counts(counts):
        if counts:
            top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
            print("Top words:", top)

    collector = pyfloq.Collector(print_counts)

    # Compose the pipeline with `|` and await execution
    pipeline = source | filter_stage | window | reducer | collector
    await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
```

## Available Classes

| Class | Input → Output | Constructors |
| :--- | :--- | :--- |
| `pyfloq.BlueskyFirehoseSource` | `() -> str` | `BlueskyFirehoseSource()` |
| `pyfloq.Filter` | `str -> str` | `Filter(regex_str)` or `Filter.with_lambda(callable)` |
| `pyfloq.PipelineComponent` | `str -> str` | `PipelineComponent(callable)` |
| `pyfloq.Window` | `str -> list[str]` | `Window(duration_ms)`, `Window.with_count(count)`, `Window.with_count_or_duration(max_count, duration_ms)` |
| `pyfloq.Map` | `list[str] -> Any` | `Map(callable)` |
| `pyfloq.Reduce` | `list[str] -> Any` | `Reduce(initial_value, callable)` |
| `pyfloq.AdkAgent` | `str \| list[str] -> str` | `AdkAgent(*, base_url=..., app_name=..., handler=..., ...)` |
| `pyfloq.AdkLiveSink` | `str \| list[str] \| Any -> ()` | `AdkLiveSink(live_request_queue, prompt_prefix=None, ...)` |
| `pyfloq.Stream` | `str \| list[str] \| Any -> AsyncIterator` | `Stream(max_buffer_size=1024)` |
| `pyfloq.Collector` | `str \| list[str] \| Any -> ()` | `Collector(callable)` |

## Google ADK Integration (`google-adk`)

`pyfloq` provides three ways to connect high-throughput Rust streams to Google ADK agents:

### 1. Bidirectional Streaming via `AdkLiveSink` + `LiveRequestQueue`

```python
import asyncio
import pyfloq
from google.adk.agents import Agent, LiveRequestQueue
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService

async def main():
    agent = Agent(
        name="stream_analyst",
        model="gemini-3.8-flash",
        instruction="Summarize emerging trends from incoming Bluesky post batches.",
    )
    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name="floq_app", user_id="floq", session_id="live_stream"
    )
    runner = Runner(agent=agent, app_name="floq_app", session_service=session_service)
    live_queue = LiveRequestQueue()

    pipeline = (
        pyfloq.BlueskyFirehoseSource()
        | pyfloq.Filter(r"(?i)\b(rust|python|ai|agent)\b")
        | pyfloq.Window.with_count_or_duration(15, 5000)
        | pyfloq.AdkLiveSink(
            live_queue,
            prompt_prefix="Summarize this batch of live posts:\n",
        )
    )

    pipeline_future = asyncio.ensure_future(pipeline.run())
    while not live_queue.closed:
        req = await live_queue.get()
        if req.close or req.content is None:
            break
        async for event in runner.run_async(
            user_id="floq", session_id="live_stream", new_message=req.content
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        print("[ADK]", part.text)
    await pipeline_future
```

### 2. Native `async for` Iteration via `pyfloq.Stream`

```python
stream = (
    pyfloq.BlueskyFirehoseSource()
    | pyfloq.Filter(r"(?i)\b(rust|ai)\b")
    | pyfloq.Window.with_count_or_duration(20, 5000)
    | pyfloq.Stream()
)

async for batch in stream:
    print("Received batch of", len(batch), "posts")
```

### 3. Mid-Pipeline Operator via `pyfloq.AdkAgent`

`AdkAgent` accepts either `str` or `list[str]` and emits the agent's `str` response downstream, supporting shared, keyed (`session_key_fn` / `use_source_id=True`), and `ephemeral=True` session modes:

```python
agent_stage = pyfloq.AdkAgent(
    base_url="http://localhost:8000",
    app_name="trend_analyzer",
    prompt_prefix="Analyze these posts:\n",
    ephemeral=True,
)

pipeline = (
    pyfloq.BlueskyFirehoseSource()
    | pyfloq.Window.with_count(10)
    | agent_stage
    | pyfloq.Collector(lambda reply: print("[ADK Reply]", reply))
)
await pipeline.run()
```
