#!/usr/bin/env python3
import asyncio
import pyfloq

async def main():
    # Create a series of components that transform messages
    def first_component(message):
        print(f"First component received: {message}")
        return f"{message} - processed by first component"
    
    def second_component(message):
        print(f"Second component received: {message}")
        return f"{message} - processed by second component"
    
    def final_component(message):
        print(f"Final component received: {message}")
        return f"{message} - final result"
    
    # Create PyPipelineComponents
    component1 = pyfloq.PyPipelineComponent(first_component)
    component2 = pyfloq.PyPipelineComponent(second_component)
    component3 = pyfloq.PyPipelineComponent(final_component)
    
    # Create pipeline tasks
    task1 = pyfloq.PyPipelineTask(component1)
    task2 = pyfloq.PyPipelineTask(component2)
    task3 = pyfloq.PyPipelineTask(component3)
    
    # Chain the tasks using the | operator
    # This creates a pipeline where output from task1 flows to task2, and output from task2 flows to task3
    final_task = task1 | task2 | task3
    
    print("Starting the chained pipeline...")
    
    # Run the chained pipeline task
    await final_task.run()
    
    print("Pipeline execution completed")

if __name__ == "__main__":
    asyncio.run(main()) 