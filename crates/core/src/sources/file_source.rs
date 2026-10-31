use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::debug;

/// A pipeline source that streams a file line-by-line.
#[derive(Clone, Debug)]
pub struct FileSource {
    path: PathBuf,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl Source for FileSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!("FileSource starting: {}", self.path.display());

        let file = File::open(&self.path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            debug!("FileSource read line");
            if output.send(Message::new(line)).await.is_err() {
                break;
            }
        }

        debug!("FileSource completed");
        Ok(())
    }
}

impl_source_component!(FileSource, String);
