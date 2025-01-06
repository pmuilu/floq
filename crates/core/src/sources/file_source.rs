use crate::pipeline::{PipelineComponent, ComponentContext, Message};
use crate::pipeline::channel::{Receiver, Sender};
use std::sync::Arc;
use tracing::{debug, error};
use tokio::fs::File;
use tokio::io::{BufReader, AsyncBufReadExt};
use std::path::PathBuf;

pub struct FileSource {
    path: PathBuf,
}

impl FileSource {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        FileSource {
            path: path.into(),
        }
    }
}

impl PipelineComponent for FileSource {
    type Input = ();
    type Output = String;

    fn new() -> Self {
        panic!("FileSource requires a file path. Use FileSource::new() instead.")
    }

    async fn run(&self, _input: Receiver<Self::Input>, output: Sender<Self::Output>, _context: Arc<ComponentContext<Self>>) {
        debug!("FileSource starting");

        let file = match File::open(&self.path).await {
            Ok(file) => file,
            Err(e) => {
                error!("Failed to open file {}: {}", self.path.display(), e);
                return;
            }
        };

        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Ok(Some(line)) = lines.next_line().await {
            debug!("FileSource read line");
            if let Err(e) = output.send(Message::new(line)) {
                error!("Failed to send line: {}", e);
                break;
            }
            debug!("FileSource sent line");
        }

        debug!("FileSource completed");
    }
} 