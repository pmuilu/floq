use super::message::{now_millis, Message};
use crate::error::{Error, Result};
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// Default bounded capacity per slot channel.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Shared atomic telemetry counters for a pipeline stage.
#[derive(Debug, Default)]
pub struct StageMetrics {
    pub sent_count: AtomicU64,
    pub received_count: AtomicU64,
    pub buffered_items: AtomicUsize,
    pub capacity: AtomicUsize,
    pub last_send_time: AtomicU64,
    pub last_receive_time: AtomicU64,
}

impl StageMetrics {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: AtomicUsize::new(capacity),
            ..Default::default()
        }
    }
}

/// Asynchronous multi-slot-capable sender with bounded backpressure and telemetry.
pub struct Sender<T> {
    slots: Arc<[mpsc::Sender<Message<T>>]>,
    next_slot: Arc<AtomicUsize>,
    source_id: Option<Arc<str>>,
    metrics: Arc<StageMetrics>,
}

/// Asynchronous receiver with telemetry tracking.
pub struct Receiver<T> {
    inner: Arc<Mutex<mpsc::Receiver<Message<T>>>>,
    capacity: usize,
    metrics: Arc<StageMetrics>,
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("slots", &self.slots.len())
            .field("source_id", &self.source_id)
            .field(
                "last_send_time",
                &self.metrics.last_send_time.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("capacity", &self.capacity)
            .field(
                "last_receive_time",
                &self.metrics.last_receive_time.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            slots: Arc::clone(&self.slots),
            next_slot: Arc::clone(&self.next_slot),
            source_id: self.source_id.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            capacity: self.capacity,
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<T> Sender<T> {
    /// Creates a no-op sender with zero downstream slots (used for terminal sinks).
    pub fn null() -> Self {
        Self {
            slots: Arc::from([]),
            next_slot: Arc::new(AtomicUsize::new(0)),
            source_id: None,
            metrics: Arc::new(StageMetrics::new(0)),
        }
    }

    /// Sends a message to the downstream stage, automatically round-robining when downstream has multiple slots.
    pub async fn send(&self, mut msg: Message<T>) -> Result<()> {
        if self.slots.is_empty() {
            return Ok(());
        }
        if let Some(source_id) = &self.source_id {
            if msg.source_id.is_none() {
                msg.source_id = Some(Arc::clone(source_id));
            }
        }
        let slot_idx = if self.slots.len() == 1 {
            0
        } else {
            self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len()
        };
        self.slots[slot_idx]
            .send(msg)
            .await
            .map_err(|_| Error::ChannelClosed)?;

        self.metrics.sent_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.buffered_items.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_send_time
            .store(now_millis(), Ordering::Relaxed);
        Ok(())
    }

    /// Sends a message to an explicit downstream `slot` index.
    pub async fn send_to_slot(&self, slot: usize, mut msg: Message<T>) -> Result<()> {
        let sender = self.slots.get(slot).ok_or(Error::InvalidSlot {
            index: slot,
            slots: self.slots.len(),
        })?;
        if let Some(source_id) = &self.source_id {
            if msg.source_id.is_none() {
                msg.source_id = Some(Arc::clone(source_id));
            }
        }
        sender.send(msg).await.map_err(|_| Error::ChannelClosed)?;
        self.metrics.sent_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.buffered_items.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .last_send_time
            .store(now_millis(), Ordering::Relaxed);
        Ok(())
    }

    /// Sends a payload stamped with an explicit `event_time`.
    pub async fn send_with_time(&self, value: T, event_time: u64) -> Result<()> {
        let msg = Message::with_event_time(value, event_time);
        self.send(msg).await?;
        self.metrics
            .last_send_time
            .store(event_time, Ordering::Relaxed);
        Ok(())
    }

    /// Returns a vector of single-slot [`Sender`] handles, one per downstream slot.
    pub fn per_slot_senders(&self) -> Vec<Sender<T>> {
        self.slots
            .iter()
            .map(|s| Sender {
                slots: Arc::from([s.clone()]),
                next_slot: Arc::new(AtomicUsize::new(0)),
                source_id: self.source_id.clone(),
                metrics: Arc::clone(&self.metrics),
            })
            .collect()
    }

    /// Returns the number of downstream slots connected to this sender.
    #[inline]
    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// Returns the current number of buffered messages across all downstream slot queues.
    pub fn len(&self) -> usize {
        self.slots
            .iter()
            .map(|s| s.max_capacity().saturating_sub(s.capacity()))
            .sum()
    }

    /// Returns `true` if no messages are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns total channel capacity across all slots.
    pub fn capacity(&self) -> Option<usize> {
        Some(self.slots.iter().map(|s| s.max_capacity()).sum())
    }

    /// Returns the Unix timestamp in milliseconds of the last successful send.
    pub fn last_send_time(&self) -> u64 {
        self.metrics.last_send_time.load(Ordering::Relaxed)
    }

    /// Attaches a `source_id` tag to all messages sent through this handle.
    pub fn with_source_id(mut self, source_id: impl Into<Arc<str>>) -> Self {
        self.source_id = Some(source_id.into());
        self
    }
}

impl<T> Receiver<T> {
    /// Creates an immediately-closed receiver (used for sources that take no input).
    pub fn null() -> Self {
        let (_tx, rx) = mpsc::channel(1);
        Self {
            inner: Arc::new(Mutex::new(rx)),
            capacity: 0,
            metrics: Arc::new(StageMetrics::new(0)),
        }
    }

    /// Receives the next message from the channel, returning `None` when all upstream senders have closed.
    pub async fn recv(&self) -> Option<Message<T>> {
        let msg = self.inner.lock().await.recv().await?;
        self.metrics.received_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.metrics.buffered_items.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |n| Some(n.saturating_sub(1)),
        );
        self.metrics
            .last_receive_time
            .store(now_millis(), Ordering::Relaxed);
        Some(msg)
    }

    /// Returns the approximate number of buffered items waiting to be received.
    pub fn len(&self) -> usize {
        self.metrics.buffered_items.load(Ordering::Relaxed)
    }

    /// Returns `true` if no items are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the bounded capacity of this receiver's channel.
    pub fn capacity(&self) -> Option<usize> {
        Some(self.capacity)
    }

    /// Returns the Unix timestamp in milliseconds of the last received message.
    pub fn last_receive_time(&self) -> u64 {
        self.metrics.last_receive_time.load(Ordering::Relaxed)
    }
}

/// Creates a bounded 1-slot channel pair with [`DEFAULT_CHANNEL_CAPACITY`].
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    bounded(DEFAULT_CHANNEL_CAPACITY)
}

/// Creates a bounded 1-slot channel pair with an explicit `capacity`.
pub fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let cap = capacity.max(1);
    let metrics = Arc::new(StageMetrics::new(cap));
    let (mut s, mut r) = multi_slot_channel(1, cap, Arc::clone(&metrics));
    (s. swap_remove(0), r.swap_remove(0))
}

/// Creates a 1-slot channel pair that tags all sent messages with `source_id`.
pub fn with_source<T>(source_id: impl Into<Arc<str>>) -> (Sender<T>, Receiver<T>) {
    let (s, r) = channel();
    (s.with_source_id(source_id), r)
}

/// Creates a multi-slot channel group with a unified fan-out [`Sender`] and one [`Receiver`] per slot.
pub fn multi_slot_channel<T>(
    slots: usize,
    capacity: usize,
    metrics: Arc<StageMetrics>,
) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
    let slot_count = slots.max(1);
    let cap = capacity.max(1);
    metrics.capacity.store(cap * slot_count, Ordering::Relaxed);

    let mut raw_senders = Vec::with_capacity(slot_count);
    let mut receivers = Vec::with_capacity(slot_count);

    for _ in 0..slot_count {
        let (tx, rx) = mpsc::channel(cap);
        raw_senders.push(tx);
        receivers.push(Receiver {
            inner: Arc::new(Mutex::new(rx)),
            capacity: cap,
            metrics: Arc::clone(&metrics),
        });
    }

    let shared_slots: Arc<[mpsc::Sender<Message<T>>]> = Arc::from(raw_senders);
    let next_slot = Arc::new(AtomicUsize::new(0));
    let sender = Sender {
        slots: shared_slots,
        next_slot,
        source_id: None,
        metrics,
    };

    (vec![sender], receivers)
}
