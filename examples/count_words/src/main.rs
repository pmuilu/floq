use floq::{
    functions::{Reduce, Window},
    impl_sink_component,
    pipeline::{PipelineTask, Receiver, Sink},
    sources::{BlueskyFirehoseSource, MastodonFirehoseSource},
    Result,
};
use std::{collections::HashMap, time::Duration};
use tracing::{debug, info};

#[derive(Default)]
struct HashMapPrinterSink;

impl HashMapPrinterSink {
    fn new() -> Self {
        Self
    }
}

/// Simple sink that logs words appearing more than 20 times in the window.
impl Sink for HashMapPrinterSink {
    type Input = HashMap<String, usize>;

    async fn run(&self, input: Receiver<Self::Input>) -> Result<()> {
        debug!("HashMapPrinterSink starting");
        while let Some(msg) = input.recv().await {
            info!("Word counts in last window:");
            for (word, count) in &msg.payload {
                if *count > 20 {
                    info!("  '{}': {} times", word, count);
                }
            }
        }
        debug!("HashMapPrinterSink completed");
        Ok(())
    }
}

impl_sink_component!(HashMapPrinterSink, HashMap<String, usize>);

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let access_token =
        std::env::var("MASTODON_ACCESS_TOKEN").expect("MASTODON_ACCESS_TOKEN not set");

    let mastodon_source = PipelineTask::new(MastodonFirehoseSource::with_token(
        "https://mastodon.social",
        access_token,
    ));
    let bluesky_source = PipelineTask::new(BlueskyFirehoseSource::new());

    // Combine heterogeneous sources into a single stream.
    let source = mastodon_source.combine(vec![bluesky_source]);

    // Group messages into tumbling windows of 10 seconds.
    let window = PipelineTask::new(Window::with_duration(Duration::from_secs(10)));

    // Count word frequencies in each window.
    let reducer = PipelineTask::new(Reduce::new(
        HashMap::new(),
        |acc: &mut HashMap<String, usize>, texts: Vec<String>| {
            acc.clear();
            for text in texts {
                for word in text.split_whitespace() {
                    *acc.entry(word.to_string()).or_insert(0) += 1;
                }
            }
        },
    ));

    let sink = PipelineTask::new(HashMapPrinterSink::new());

    info!("Starting pipeline...");

    let pipeline = source | window | reducer | sink;

    pipeline.run().await?;

    info!("Pipeline completed");
    Ok(())
}
