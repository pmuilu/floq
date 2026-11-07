use crate::error::{Error, Result};
use crate::pipeline::{ComponentContext, Message, PipelineComponent, Receiver, Sender, Sink};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::debug;

/// A terminal sink that forwards each [`Message<T>`] into an external Tokio `mpsc` channel,
/// allowing host applications or AI agents to consume pipeline outputs as an async stream.
#[derive(Clone, Debug)]
pub struct ChannelSink<T> {
    tx: Arc<Mutex<Option<mpsc::Sender<Message<T>>>>>,
}

impl<T> ChannelSink<T> {
    /// Creates a new [`ChannelSink`] and its paired `mpsc::Receiver<Message<T>>` with `capacity`.
    pub fn new(capacity: usize) -> (Self, mpsc::Receiver<Message<T>>) {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        (Self::from_sender(tx), rx)
    }

    /// Wraps an existing `tokio::sync::mpsc::Sender<Message<T>>`.
    pub fn from_sender(tx: mpsc::Sender<Message<T>>) -> Self {
        Self {
            tx: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

impl<T: Send + 'static> Sink for ChannelSink<T> {
    type Input = T;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        debug!("ChannelSink starting");

        let sender = self
            .tx
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
            .ok_or(Error::ChannelClosed)?;

        while let Some(msg) = input.recv().await {
            sender.send(msg).await.map_err(|_| Error::ChannelClosed)?;
        }

        // Drop the stored sender when the sink finishes so the downstream receiver sees EOF (`None`).
        if let Ok(mut guard) = self.tx.lock() {
            guard.take();
        }

        debug!("ChannelSink completed");
        Ok(())
    }
}

impl<T: Send + 'static> PipelineComponent for ChannelSink<T> {
    type Input = T;
    type Output = ();

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        _output: Sender<Self::Output>,
        _context: Arc<ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Sink>::run(self, input).await
    }
}
