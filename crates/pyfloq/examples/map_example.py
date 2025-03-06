#!/usr/bin/env python3
import asyncio
import pyfloq

async def main():
    # Create a window to collect messages over time
    window = pyfloq.PyWindow(5000)  # 5 second window
    
    # Create a map function that processes batches of messages
    def process_batch(messages):
        # messages is Vec<String> containing messages in the current window
        # Process messages and return a new Vec<String> with statistics
        total_words = sum(len(message.split()) for message in messages)
        total_chars = sum(len(message) for message in messages)
        stats = [
            f"Messages in batch: {len(messages)}",
            f"Total words: {total_words}",
            f"Total characters: {total_chars}"
        ]
        if messages:
            stats.extend([
                f"Average words per message: {total_words / len(messages):.2f}",
                f"Average chars per message: {total_chars / len(messages):.2f}"
            ])
        return stats
    
    # Create the map component with our processing function
    mapper = pyfloq.PyMap(process_batch)
    
    # Create source and collector
    source = pyfloq.PyBlueskyFirehoseSource()
    
    # Collector callback that prints statistics
    def print_stats(stats):
        if stats:
            print("\nBatch statistics:")
            for stat in stats:
                print(stat)
        else:
            print("\nNo messages in this batch")
    
    collector = pyfloq.PyCollector(print_stats)
    
    # Chain the tasks using the | operator:
    # source -> window -> mapper -> collector
    task = source | window | mapper | collector
    
    # Run the pipeline
    try:
        await task.run()
    except KeyboardInterrupt:
        pass  # Allow graceful exit with Ctrl+C

if __name__ == "__main__":
    asyncio.run(main()) 