#!/usr/bin/env python3
import asyncio
import pyfloq

async def main():
    # Create a window to collect messages over time
    window = pyfloq.PyWindow(5000)  # 5 second window
    
    # Create a reducer that counts words in the window
    def count_words(acc, messages):
        # acc is our accumulator (word count dictionary)
        # messages is Vec<String> containing messages in the current window
        if acc is None:
            acc = {}
            
        for message in messages:
            for word in message.split():
                acc[word] = acc.get(word, 0) + 1
        
        print(acc)
        
        return acc
    
    # Create the reduce component with initial value None and our reducer function
    reducer = pyfloq.PyReduce(None, count_words)
    
    # Create source and sink
    source = pyfloq.PyBlueskyFirehoseSource()
    sink = pyfloq.PyPrinterSink()
    
    # Chain the tasks using the | operator:
    # source -> window -> reducer -> sink
    task = source | window | reducer #| sink
    
    # Run the pipeline
    await task.run()

if __name__ == "__main__":
    asyncio.run(main()) 