use floq::{
    pipeline::{PipelineTask, PipelineComponent, ComponentContext},

    sources::{
        BlueskyFirehoseSource,
        MastodonFirehoseSource,
    },
    slots::{
        RoundRobinSplitter,
        Merger,
    },
    functions::{
        Window,
        Reduce,
    },  
};
use std::{collections::HashMap, time::Duration};
use crossbeam_channel::{Sender, Receiver};
use std::sync::Arc;
use tracing::{info, debug};

struct HashMapPrinterSink {
}


/**
 * This is a simple sink that prints the word counts to the console.
 */
impl PipelineComponent for HashMapPrinterSink {
    type Input = HashMap<String, usize>;
    type Output = ();

    fn new() -> Self {
        HashMapPrinterSink {}
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, _task: Arc<ComponentContext<Self>>) {
        debug!("HashMapPrinterSink starting");
        while let Ok(word_counts) = input.recv() {
            info!("Word counts in last window:");
            for (word, count) in word_counts.iter() {
                if *count > 20 {  // Only show words that appear more than 20 times
                    info!("  '{}': {} times", word, count);
                }
            }
        }
        debug!("HashMapPrinterSink completed");
    }
}


#[tokio::main(flavor = "multi_thread", worker_threads = 20)]   
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let access_token = std::env::var("MASTODON_ACCESS_TOKEN").expect("MASTODON_ACCESS_TOKEN not set");
    
    // We process this pipeline with 4 slots (each slot corresponds to a thread in a task i.e. 3 tasks using 4 slots is 3*4 threads in total)
    let slots = 4;
    
    // Let's create Mastodon and Bluesky sources
    let mastodon_source = PipelineTask::new(MastodonFirehoseSource::with_token("https://mastodon.social".to_string(), access_token));
    let bluesky_source = PipelineTask::new(BlueskyFirehoseSource::new());

    // We combine these sources into a single source, so we receive messages from both networks into a same channel
    let source = mastodon_source.combine(vec![bluesky_source]);

    // Splitter distributes messages to multiple workers
    let splitter = PipelineTask::new(RoundRobinSplitter::new());

    // Window function groups messages into windows of 10 seconds, i.e. we  print out the word counts every 10 seconds
    let window = PipelineTask::with_slots(Window::with_duration(Duration::from_secs(10)), slots);

    // Merger combines messages from multiple workers into a single channel
    let merger = PipelineTask::with_slots(Merger::new(), slots);

    // Create a reducer that counts word frequencies
    let reducer = PipelineTask::with_slots(Reduce::new(
        HashMap::new(),
        |acc: &mut HashMap<String, usize>, texts: Vec<String>| {
            // Clear the previous counts
            acc.clear();
            
            // Count words in this window
            for text in texts {
                // Split text into words and count them
                for word in text.split_whitespace() {
                    let count = acc.entry(word.to_string()).or_insert(0);
                    *count += 1;
                }
            }
        }
    ), slots);

    // Create a sink that prints the word counts to the console
    let sink = PipelineTask::new(HashMapPrinterSink::new());

    info!("Starting pipeline...");

    // Define the pipeline using the pipeline builder syntax
    let pipeline = source 
                    | splitter 
                    | window
                    | reducer
                    | merger
                    | sink;
    
    // Run the pipeline and wait for it to complete, obviously this is streaming pipeline and will run forever unless
    // we stop it manually or error occurs.
    pipeline.run().await;

    info!("Pipeline completed");
}


