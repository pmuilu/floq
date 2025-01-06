use floq::{
    pipeline::{PipelineTask, PipelineComponent, ComponentContext},
    sources::{MastodonFirehoseSource},
    slots::{RoundRobinSplitter, Merger},
    functions::{
        Window,
        Map,
    },  
    transformers::{GeminiEmbeddings},
    pipeline::channel::{Sender, Receiver},
};
use regex::Regex;
use once_cell::sync::Lazy;
use std::{time::Duration};
use std::sync::Arc;
use tracing::{info, debug};

// Use Lazy static for the regex to compile it only once
static HTML_TAG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"<[^>]*>").unwrap()
});

struct VectorDebugPrinter {
}

impl PipelineComponent for VectorDebugPrinter {
    type Input = Vec<Vec<f32>>;
    type Output = ();

    fn new() -> Self {
        VectorDebugPrinter {}
    }

    async fn run(&self, input: Receiver<Self::Input>, _output: Sender<Self::Output>, _task: Arc<ComponentContext<Self>>) {
        debug!("VectorDebugPrinter starting");
        while let Ok(msg) = input.recv() {
            let vectors = msg.payload;
            info!("Received {} embeddings:", vectors.len());
            for (i, vector) in vectors.iter().enumerate() {
                info!("  Embedding {}: {} dimensions. First 5 values: {:?}", 
                    i + 1,
                    vector.len(), 
                    &vector.iter().take(5).collect::<Vec<_>>()
                );
            }
        }
        debug!("VectorDebugPrinter completed");
    }
}

fn strip_html(text: String) -> String {
    // Remove HTML tags
    let stripped = HTML_TAG_REGEX.replace_all(&text, " ");
    
    // Clean up whitespace
    stripped
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let slots = 4;

    // Let's use Mastodon as a source
    let access_token = std::env::var("MASTODON_ACCESS_TOKEN").expect("MASTODON_ACCESS_TOKEN not set");
    let source = PipelineTask::new(MastodonFirehoseSource::with_token("https://mastodon.social".to_string(), access_token));
    
     // Crude HTML stripper as Mastodon messages are HTML
     let html_stripper = PipelineTask::new(Map::new(strip_html));

    // Splitter will split the incoming data into the slots of next component
    let splitter = PipelineTask::new(RoundRobinSplitter::new());

    // Window is used to group the text into windows as Gemini transformer handles posts in batches most efficiently
    let window = PipelineTask::with_slots(Window::with_duration(Duration::from_secs(10)), slots);

    // Gemini is used to get embeddings for the text
    let gemini_embeddings = PipelineTask::with_slots(GeminiEmbeddings::new(), slots);
   
    let merger = PipelineTask::with_slots(Merger::new(), slots);

    let sink = PipelineTask::new(VectorDebugPrinter::new());

    info!("Starting pipeline...");
    let pipeline = source 
                    | html_stripper
                    | splitter
                    | window
                    | gemini_embeddings
                    | merger
                    | sink;
                   
    
    // Run the pipeline and wait for it to complete
    pipeline.run().await;
    info!("Pipeline completed");
}
