#[cfg(feature = "sqlite")]
use crate::checkpoint::SqliteCheckpointStore;
use crate::error::Result;
use crate::impl_source_component;
use crate::pipeline::{Message, Sender, Source};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tracing::{debug, info, warn};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// A pipeline source that streams a file line-by-line, supporting both one-shot batch
/// reading and continuous live tailing (`tail -F` semantics with truncation/rotation handling
/// and optional SQLite byte-offset checkpointing).
#[derive(Clone, Debug)]
pub struct FileSource {
    path: PathBuf,
    follow: bool,
    start_from_end: bool,
    poll_interval: Duration,
    #[cfg(feature = "sqlite")]
    checkpoint: Option<(SqliteCheckpointStore, String)>,
}

impl FileSource {
    /// Creates a `FileSource` that reads `path` from the beginning to EOF and exits.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            follow: false,
            start_from_end: false,
            poll_interval: DEFAULT_POLL_INTERVAL,
            #[cfg(feature = "sqlite")]
            checkpoint: None,
        }
    }

    /// Creates a `FileSource` in continuous tail mode (`follow = true`), watching for appended lines.
    pub fn tail(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            follow: true,
            start_from_end: false,
            poll_interval: DEFAULT_POLL_INTERVAL,
            #[cfg(feature = "sqlite")]
            checkpoint: None,
        }
    }

    /// Enables or disables continuous tail mode (`tail -F`).
    pub fn with_follow(mut self, follow: bool) -> Self {
        self.follow = follow;
        self
    }

    /// When starting without an existing checkpoint, seek to the end of the file and only emit
    /// newly appended lines.
    pub fn from_end(mut self) -> Self {
        self.start_from_end = true;
        self
    }

    /// When starting without an existing checkpoint, read from byte offset `0` (default).
    pub fn from_beginning(mut self) -> Self {
        self.start_from_end = false;
        self
    }

    /// Sets the polling interval used when waiting for newly appended lines in `follow` mode.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval.max(Duration::from_millis(10));
        self
    }

    /// Attaches a SQLite checkpoint store to persist and resume the file byte offset across restarts.
    #[cfg(feature = "sqlite")]
    pub fn with_checkpoint(
        mut self,
        store: SqliteCheckpointStore,
        key: impl Into<String>,
    ) -> Self {
        self.checkpoint = Some((store, key.into()));
        self
    }

    /// Attaches a SQLite checkpoint store using `format!("file:{}", self.path.display())` as the key.
    #[cfg(feature = "sqlite")]
    pub fn with_default_checkpoint(self, store: SqliteCheckpointStore) -> Self {
        let key = format!("file:{}", self.path.display());
        self.with_checkpoint(store, key)
    }

    #[cfg(feature = "sqlite")]
    fn load_checkpoint(&self) -> Result<Option<u64>> {
        if let Some((store, key)) = &self.checkpoint {
            store.get_u64(key)
        } else {
            Ok(None)
        }
    }

    #[cfg(not(feature = "sqlite"))]
    fn load_checkpoint(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    #[cfg(feature = "sqlite")]
    fn save_checkpoint(&self, offset: u64) {
        if let Some((store, key)) = &self.checkpoint {
            if let Err(e) = store.set_u64(key, offset) {
                warn!("Failed to persist FileSource offset checkpoint: {}", e);
            }
        }
    }

    #[cfg(not(feature = "sqlite"))]
    fn save_checkpoint(&self, _offset: u64) {}

    #[cfg(unix)]
    fn file_identity(meta: &std::fs::Metadata) -> (u64, u64) {
        use std::os::unix::fs::MetadataExt;
        (meta.dev(), meta.ino())
    }

    #[cfg(not(unix))]
    fn file_identity(_meta: &std::fs::Metadata) -> (u64, u64) {
        (0, 0)
    }
}

impl Source for FileSource {
    type Output = String;

    async fn run(&self, output: Sender<Self::Output>) -> Result<()> {
        debug!(
            "FileSource starting: {} (follow={})",
            self.path.display(),
            self.follow
        );

        let source_id: Arc<str> = Arc::from(self.path.to_string_lossy().as_ref());
        let mut file = File::open(&self.path).await?;
        let initial_meta = file.metadata().await?;
        let mut current_identity = Self::file_identity(&initial_meta);
        let file_len = initial_meta.len();

        let mut offset: u64 = match self.load_checkpoint()? {
            Some(saved_offset) => {
                if saved_offset <= file_len {
                    saved_offset
                } else {
                    info!(
                        "FileSource {} truncated since checkpoint ({} < {}), resetting to 0",
                        self.path.display(),
                        file_len,
                        saved_offset
                    );
                    0
                }
            }
            None if self.start_from_end => file_len,
            None => 0,
        };

        file.seek(SeekFrom::Start(offset)).await?;
        self.save_checkpoint(offset);

        let mut reader = BufReader::new(file);
        let mut line_buf = String::new();

        loop {
            if output.is_closed() {
                self.save_checkpoint(offset);
                break;
            }

            line_buf.clear();
            let bytes_read = reader.read_line(&mut line_buf).await?;

            if bytes_read > 0 {
                // In follow mode, wait until a full newline-terminated line is written
                // before emitting so half-flushed lines are never split.
                if self.follow && !line_buf.ends_with('\n') {
                    reader.seek(SeekFrom::Start(offset)).await?;
                    tokio::time::sleep(self.poll_interval).await;
                    continue;
                }

                offset += bytes_read as u64;
                if line_buf.ends_with('\n') {
                    line_buf.pop();
                    if line_buf.ends_with('\r') {
                        line_buf.pop();
                    }
                }

                let msg = Message::new(line_buf.clone()).with_source(Arc::clone(&source_id));
                if output.send(msg).await.is_err() {
                    self.save_checkpoint(offset);
                    break;
                }
                self.save_checkpoint(offset);
                continue;
            }

            // Reached EOF
            if !self.follow {
                break;
            }

            tokio::time::sleep(self.poll_interval).await;

            // Check for truncation or log rotation at `self.path`
            if let Ok(path_meta) = tokio::fs::metadata(&self.path).await {
                let new_identity = Self::file_identity(&path_meta);
                let new_len = path_meta.len();

                if new_identity != current_identity || new_len < offset {
                    info!(
                        "FileSource detected rotation/truncation on {} (old_offset={}, new_len={}), reopening from 0",
                        self.path.display(),
                        offset,
                        new_len
                    );
                    if let Ok(new_file) = File::open(&self.path).await {
                        current_identity = new_identity;
                        offset = 0;
                        reader = BufReader::new(new_file);
                        self.save_checkpoint(0);
                    }
                }
            }
        }

        debug!("FileSource completed: {}", self.path.display());
        Ok(())
    }
}

impl_source_component!(FileSource, String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::channel::bounded;
    use crate::pipeline::now_millis;
    use std::io::Write;

    #[tokio::test]
    async fn test_file_source_tail_and_truncation() {
        let path = std::env::temp_dir().join(format!(
            "floq_tail_test_{}_{}.log",
            std::process::id(),
            now_millis()
        ));

        {
            let mut f = std::fs::File::create(&path).unwrap();
            writeln!(f, "line-1").unwrap();
            f.flush().unwrap();
        }

        let source =
            FileSource::tail(&path).with_poll_interval(Duration::from_millis(20));
        let (sender, mut rx) = bounded(16);

        let source_handle = tokio::spawn(async move { Source::run(&source, sender).await });

        // Receive initial line
        let m1 = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m1.payload, "line-1");

        // Append a second line dynamically
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "line-2").unwrap();
            f.flush().unwrap();
        }

        let m2 = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m2.payload, "line-2");

        // Truncate the file and write a shorter line to simulate rotation in place
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            writeln!(f, "rotated-1").unwrap();
            f.flush().unwrap();
        }

        let m3 = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(m3.payload, "rotated-1");

        drop(rx);
        let _ = tokio::time::timeout(Duration::from_secs(1), source_handle).await;
        let _ = std::fs::remove_file(&path);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_file_source_sqlite_checkpoint_resume() {
        let dir = std::env::temp_dir().join(format!(
            "floq_file_ckpt_{}_{}",
            std::process::id(),
            now_millis()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let log_path = dir.join("events.log");
        let db_path = dir.join("ckpt.sqlite");

        {
            let mut f = std::fs::File::create(&log_path).unwrap();
            writeln!(f, "first").unwrap();
            writeln!(f, "second").unwrap();
            f.flush().unwrap();
        }

        let store = SqliteCheckpointStore::open(&db_path).unwrap();

        // First run reads "first" and "second" and checkpoints the byte offset
        {
            let source = FileSource::new(&log_path).with_default_checkpoint(store.clone());
            let (sender, mut rx) = bounded(16);
            Source::run(&source, sender).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().payload, "first");
            assert_eq!(rx.recv().await.unwrap().payload, "second");
            assert!(rx.recv().await.is_none());
        }

        // Append "third" while stopped
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&log_path)
                .unwrap();
            writeln!(f, "third").unwrap();
            f.flush().unwrap();
        }

        // Second run with the same SQLite checkpoint store should only emit "third"
        {
            let reopened_store = SqliteCheckpointStore::open(&db_path).unwrap();
            let source = FileSource::new(&log_path).with_default_checkpoint(reopened_store);
            let (sender, mut rx) = bounded(16);
            Source::run(&source, sender).await.unwrap();
            assert_eq!(rx.recv().await.unwrap().payload, "third");
            assert!(rx.recv().await.is_none());
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
