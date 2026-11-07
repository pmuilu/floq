"""
Example: Connecting a real-time `pyfloq` streaming pipeline to a Google ADK Agent.

Demonstrates pushing windowed Bluesky firehose batches from Rust into an ADK
`LiveRequestQueue` via `pyfloq.AdkLiveSink` and processing each batch with
`runner.run_async(...)` using `gemini-3.5-flash-lite`.
"""

import asyncio
import os
import pyfloq
from google.adk.agents import Agent, LiveRequestQueue
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService


async def run_live_sink_example():
    # 1. Define a Google ADK agent and Runner
    model_name = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
    agent = Agent(
        name="firehose_analyst",
        model=model_name,
        instruction="Summarize emerging technical trends from incoming Bluesky post batches in 2-3 bullet points.",
    )
    session_service = InMemorySessionService()
    await session_service.create_session(
        app_name="floq_app", user_id="floq", session_id="live_stream"
    )
    runner = Runner(
        agent=agent, app_name="floq_app", session_service=session_service
    )
    live_queue = LiveRequestQueue()

    # 2. Build a Rust-powered `pyfloq` pipeline that streams structured JSON posts
    #    (with SQLite cursor checkpointing), filters specifically on `/text`, and pushes
    #    formatted `types.Content` batches into `live_queue` every 15 posts or 8 seconds.
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

    # 3. Start the Rust pipeline in the background
    print("Listening to live Bluesky firehose (waiting for first batch)...")
    pipeline_future = asyncio.ensure_future(pipeline.run())

    def _on_pipeline_done(fut: asyncio.Future):
        if fut.cancelled():
            live_queue.close()
        elif fut.exception() is not None:
            print(f"[floq] Pipeline exited with error: {fut.exception()}")
            live_queue.close()

    pipeline_future.add_done_callback(_on_pipeline_done)

    # 4. Consume `LiveRequest` items pushed by `AdkLiveSink` and run the ADK agent
    while not live_queue.closed:
        req = await live_queue.get()
        if req.close or req.content is None:
            break

        text_preview = req.content.parts[0].text if req.content.parts else ""
        preview_snippet = text_preview[:240].replace("\n", " ")
        print(
            f"\n[floq] Flushed batch ({len(text_preview)} chars): {preview_snippet}...\n"
            f"[floq] -> querying ADK agent ({model_name})..."
        )

        try:
            async for event in runner.run_async(
                user_id="floq",
                session_id="live_stream",
                new_message=req.content,
            ):
                if event.content and event.content.parts:
                    for part in event.content.parts:
                        if part.text:
                            print(f"\n[ADK Agent]\n{part.text}")
        except Exception as exc:
            print(f"[ADK Agent] Request failed (will retry on next batch): {exc}")

    await pipeline_future


if __name__ == "__main__":
    asyncio.run(run_live_sink_example())
