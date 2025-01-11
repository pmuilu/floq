use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use super::message::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::fmt;

pub struct Sender<T> {
    inner: CrossbeamSender<Message<T>>,
    source_id: Arc<Option<String>>,
    last_send_time: Arc<AtomicU64>,
}

pub struct Receiver<T> {
    inner: CrossbeamReceiver<Message<T>>,
    last_receive_time: Arc<AtomicU64>,
}

// Manual Debug implementations that don't require T: Debug
impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("source_id", &self.source_id)
            .field("last_send_time", &self.last_send_time.load(Ordering::Relaxed))
            .finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("last_receive_time", &self.last_receive_time.load(Ordering::Relaxed))
            .finish()
    }
}

// Manual Clone implementations that don't require T: Clone
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            inner: self.inner.clone(),
            source_id: self.source_id.clone(),
            last_send_time: self.last_send_time.clone(),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Receiver {
            inner: self.inner.clone(),
            last_receive_time: self.last_receive_time.clone(),
        }
    }
}

impl<T> Sender<T> {
    pub fn send(&self, value: Message<T>) -> Result<(), crossbeam_channel::SendError<Message<T>>> {
        let mut msg = value;
        if let Some(source_id) = self.source_id.as_ref() {
            msg = msg.with_source(source_id.clone());
        }
        let result = self.inner.send(msg);
        if result.is_ok() {
            self.last_send_time.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                Ordering::Relaxed
            );
        }
        result
    }

    pub fn send_with_time(&self, value: T, event_time: u64) -> Result<(), crossbeam_channel::SendError<Message<T>>> {
        let mut msg = Message::with_event_time(value, event_time);
        if let Some(source_id) = self.source_id.as_ref() {
            msg = msg.with_source(source_id.clone());
        }
        let result = self.inner.send(msg);
        if result.is_ok() {
            self.last_send_time.store(
                event_time,
                Ordering::Relaxed
            );
        }
        result
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    pub fn last_send_time(&self) -> u64 {
        self.last_send_time.load(Ordering::Relaxed)
    }
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<Message<T>, crossbeam_channel::RecvError> {
        let msg = self.inner.recv()?;
        self.last_receive_time.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed
        );
        Ok(msg)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.inner.capacity()
    }

    pub fn last_receive_time(&self) -> u64 {
        self.last_receive_time.load(Ordering::Relaxed)
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let (s, r) = crossbeam_channel::unbounded();
    (
        Sender { 
            inner: s, 
            source_id: Arc::new(None),
            last_send_time: Arc::new(AtomicU64::new(0)),
        },
        Receiver { 
            inner: r,
            last_receive_time: Arc::new(AtomicU64::new(0)),
        },
    )
}

pub fn with_source<T>(source_id: impl Into<String>) -> (Sender<T>, Receiver<T>) {
    let (s, r) = crossbeam_channel::unbounded();
    (
        Sender { 
            inner: s, 
            source_id: Arc::new(Some(source_id.into())),
            last_send_time: Arc::new(AtomicU64::new(0)),
        },
        Receiver { 
            inner: r,
            last_receive_time: Arc::new(AtomicU64::new(0)),
        },
    )
} 