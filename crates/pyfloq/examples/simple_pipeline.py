#!/usr/bin/env python3
import asyncio
import pyfloq

async def main():
    # Create a simple component that transforms messages
    def process_message(message):
        print(f"Received: {message}")
        return f"Processed: {message.upper()}"
    
    # Create a PyPipelineComponent with our callback

    # Create a pipeline task with the component
    source = pyfloq.PyBlueskyFirehoseSource()
    sink = pyfloq.PyPrinterSink()
    
    # Chain the tasks using the | operator
    task = source | sink
    
    # Run the pipeline (this would normally be started in the background)
    await task.run()

if __name__ == "__main__":
    asyncio.run(main()) 