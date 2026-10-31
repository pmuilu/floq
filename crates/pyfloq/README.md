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
| `pyfloq.Collector` | `Any -> ()` | `Collector(callable)` |
