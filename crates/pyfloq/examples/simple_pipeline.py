#!/usr/bin/env python3
import asyncio
import pyfloq

async def main():
    # Create a window to collect messages over time
    window = pyfloq.Window(5000)  # 5 second window
    
    # Create a reducer that counts words in the window
    def count_words(acc, messages):
        # acc is our accumulator (word count dictionary)
        # messages is Vec<String> containing messages in the current window
        if acc is None:
            acc = {}
            
        for message in messages:
            for word in message.split():
                acc[word] = acc.get(word, 0) + 1

        return acc
    
    # Create the reduce component with initial value None and our reducer function
    reducer = pyfloq.Reduce(None, count_words)
    
    # Create source and collector
    source = pyfloq.BlueskyFirehoseSource()
    
    # Collector callback that prints word counts
    def print_counts(counts):
        if counts:
            print("\nWord counts in the last window:")
            for word, count in sorted(counts.items()):
                print(f"{word}: {count}")
        else:
            print("\nNo counts in this window")
    
    collector = pyfloq.Collector(print_counts)
    
    filter = pyfloq.Filter(r"(Elon|Musk)")

    # Chain the tasks using the | operator:
    # source -> window -> reducer -> collector
    task = source | filter | window | reducer | collector
    
    # Run the pipeline
    try:
        await task.run()
    except KeyboardInterrupt:
        pass  # Allow graceful exit with Ctrl+C

if __name__ == "__main__":
    asyncio.run(main()) 