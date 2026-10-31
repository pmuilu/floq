use floq::{
    functions::{Map, Window},
    impl_sink_component,
    pipeline::{PipelineMonitor, PipelineTask, Receiver, Sink},
    slots::{Merger, RoundRobinSplitter},
    sources::BlueskyFirehoseSource,
    transformers::GeminiEmbeddings,
    Result,
};
use regex::Regex;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::{debug, info};

static HTML_TAG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<[^>]*>").expect("valid HTML tag regex"));

#[derive(Default)]
struct VectorDebugPrinter;

impl VectorDebugPrinter {
    fn new() -> Self {
        Self
    }
}

impl Sink for VectorDebugPrinter {
    type Input = Vec<Vec<f32>>;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        debug!("VectorDebugPrinter starting");
        while let Some(msg) = input.recv().await {
            let vectors = msg.payload;
            info!("Received {} embeddings:", vectors.len());
            for (i, vector) in vectors.iter().enumerate() {
                info!(
                    "  Embedding {}: {} dimensions. First 5 values: {:?}",
                    i + 1,
                    vector.len(),
                    &vector[..vector.len().min(5)]
                );
            }
        }
        debug!("VectorDebugPrinter completed");
        Ok(())
    }
}

impl_sink_component!(VectorDebugPrinter, Vec<Vec<f32>>);

fn strip_html(text: String) -> String {
    let stripped = HTML_TAG_REGEX.replace_all(&text, " ");
    stripped.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let slots = 4;
    let mut monitor = PipelineMonitor::new();

    let source = PipelineTask::new(BlueskyFirehoseSource::new());
    let html_stripper = PipelineTask::new(Map::new(strip_html));
    let splitter = PipelineTask::new(RoundRobinSplitter::new());
    let window = PipelineTask::with_slots(Window::with_duration(Duration::from_secs(3)), slots);
    let gemini_embeddings = PipelineTask::with_slots(GeminiEmbeddings::new(), slots);
    let merger = PipelineTask::with_slots(Merger::new(), slots);
    let sink = PipelineTask::new(VectorDebugPrinter::new());

    source.register_with_monitor(&mut monitor);
    html_stripper.register_with_monitor(&mut monitor);
    splitter.register_with_monitor(&mut monitor);
    window.register_with_monitor(&mut monitor);
    gemini_embeddings.register_with_monitor(&mut monitor);
    merger.register_with_monitor(&mut monitor);
    sink.register_with_monitor(&mut monitor);

    info!("Starting pipeline...");
    let pipeline =
        source | html_stripper | splitter | window | gemini_embeddings | merger | sink;

    monitor.start();
    pipeline.run().await?;
    info!("Pipeline completed");
    Ok(())
}
