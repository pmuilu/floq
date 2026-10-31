use super::message::{now_millis, Message};
use crate::error::{Error, Result};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Default bounded capacity per slot channel.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Shared atomic telemetry counters for a pipeline stage.
///
/// Telemetry updates and clock queries only run on the hot path when [`StageMetrics::enabled`]
/// is set (e.g., via [`crate::pipeline::PipelineTask::register_with_monitor`]).
#[derive(Debug, Default)]
pub struct StageMetrics {
    pub enabled: AtomicBool,
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

    #[inline]
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    #[inline]
    fn record_send(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            self.sent_count.fetch_add(1, Ordering::Relaxed);
            self.buffered_items.fetch_add(1, Ordering::Relaxed);
            self.last_send_time.store(now_millis(), Ordering::Relaxed);
        }
    }

    #[inline]
    fn record_recv(&self) {
        if self.enabled.load(Ordering::Relaxed) {
            self.received_count.fetch_add(1, Ordering::Relaxed);
            let _ = self.buffered_items.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |n| Some(n.saturating_sub(1)),
            );
            self.last_receive_time
                .store(now_millis(), Ordering::Relaxed);
        }
    }
}

/// Asynchronous multi-slot-capable sender with bounded backpressure and optional telemetry.
pub struct Sender<T> {
    slots: Arc<[mpsc::Sender<Message<T>>]>,
    next_slot: Arc<AtomicUsize>,
    source_id: Option<Arc<str>>,
    metrics: Arc<StageMetrics>,
}

/// Exclusive, lock-free asynchronous receiver for a pipeline slot.
pub struct Receiver<T> {
    inner: mpsc::Receiver<Message<T>>,
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
    #[inline]
    pub async fn send(&self, mut msg: Message<T>) -> Result<()> {
        let len = self.slots.len();
        if len == 0 {
            return Ok(());
        }
        if let Some(source_id) = &self.source_id {
            if msg.source_id.is_none() {
                msg.source_id = Some(Arc::clone(source_id));
            }
        }
        let slot_idx = if len == 1 {
            0
        } else {
            self.next_slot.fetch_add(1, Ordering::Relaxed) % len
        };
        self.slots[slot_idx]
            .send(msg)
            .await
            .map_err(|_| Error::ChannelClosed)?;

        self.metrics.record_send();
        Ok(())
    }

    /// Sends a message to an explicit downstream `slot` index.
    #[inline]
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
        self.metrics.record_send();
        Ok(())
    }

    /// Sends a payload stamped with an explicit `event_time`.
    #[inline]
    pub async fn send_with_time(&self, value: T, event_time: u64) -> Result<()> {
        let msg = Message::with_event_time(value, event_time);
        self.send(msg).await?;
        if self.metrics.enabled.load(Ordering::Relaxed) {
            self.metrics
                .last_send_time
                .store(event_time, Ordering::Relaxed);
        }
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
            inner: rx,
            capacity: 0,
            metrics: Arc::new(StageMetrics::new(0)),
        }
    }

    /// Receives the next message from the channel without any mutex overhead.
    /// Returns `None` when all upstream senders have closed.
    #[inline]
    pub async fn recv(&mut self) -> Option<Message<T>> {
        let msg = self.inner.recv().await?;
        self.metrics.record_recv();
        Some(msg)
    }

    /// Returns the number of buffered items currently queued in this slot's channel.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if no items are currently buffered.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
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
    (s.swap_remove(0), r.swap_remove(0))
}

/// Creates a 1-slot channel pair that tags all sent messages with `source_id`.
pub fn with_source<T>(source_id: impl Into<Arc<str>>) -> (Sender<T>, Receiver<T>) {
    let (s, r) = channel();
    (s.with_source_id(source_id), r)
}

/// Creates a multi-slot channel group with a unified fan-out [`Sender`] and one exclusive [`Receiver`] per slot.
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
            inner: rx,
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
