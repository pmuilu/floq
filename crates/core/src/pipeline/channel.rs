use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use super::message::Message;
use std::sync::Arc;
use std::fmt;

pub struct Sender<T> {
    inner: CrossbeamSender<Message<T>>,
    source_id: Arc<Option<String>>,
}

pub struct Receiver<T> {
    inner: CrossbeamReceiver<Message<T>>,
}

// Manual Debug implementations that don't require T: Debug
impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("source_id", &self.source_id)
            .finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}

// Manual Clone implementations that don't require T: Clone
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
            source_id: self.source_id.clone(),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, value: Message<T>) -> Result<(), crossbeam_channel::SendError<Message<T>>> {
        let mut msg = value;
        if let Some(source_id) = self.source_id.as_ref() {
            msg = msg.with_source(source_id.clone());
        }
        self.inner.send(msg)
    }

    pub fn send_with_time(&self, value: T, event_time: u64) -> Result<(), crossbeam_channel::SendError<Message<T>>> {
        let mut msg = Message::with_event_time(value, event_time);
        if let Some(source_id) = self.source_id.as_ref() {
            msg = msg.with_source(source_id.clone());
        }
        self.inner.send(msg)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<Message<T>, crossbeam_channel::RecvError> {
        let msg = self.inner.recv()?;
        Ok(msg)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (s, r) = crossbeam_channel::unbounded();
    (
        Sender { 
            inner: s, 
            source_id: Arc::new(None),
        },
        Receiver { inner: r },
    )
}

pub fn with_source<T>(source_id: impl Into<String>) -> (Sender<T>, Receiver<T>) {
    let (s, r) = crossbeam_channel::unbounded();
    (
        Sender { 
            inner: s, 
            source_id: Arc::new(Some(source_id.into())),
        },
        Receiver { inner: r },
    )
} 