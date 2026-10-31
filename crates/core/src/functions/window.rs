use crate::error::Result;
use crate::pipeline::{now_millis, Message, Operator, PipelineComponent, Receiver, Sender};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval_at, Instant};
use tracing::debug;

#[derive(Clone, Debug)]
pub enum WindowCondition {
    Count(usize),
    Time(Duration),
    Sliding {
        window_size: Duration,
        slide_interval: Duration,
    },
}

/// Windowing operator supporting count-based, tumbling time-based, and sliding time windows.
#[derive(Clone, Debug)]
pub struct Window<T> {
    condition: WindowCondition,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Default for Window<T> {
    fn default() -> Self {
        Self::with_count(10)
    }
}

impl<T> Window<T> {
    pub fn with_count(count: usize) -> Self {
        Self {
            condition: WindowCondition::Count(count.max(1)),
            _phantom: PhantomData,
        }
    }

    pub fn with_duration(duration: Duration) -> Self {
        Self {
            condition: WindowCondition::Time(duration),
            _phantom: PhantomData,
        }
    }

    pub fn with_sliding_window(window_size: Duration, slide_interval: Duration) -> Self {
        Self {
            condition: WindowCondition::Sliding {
                window_size,
                slide_interval,
            },
            _phantom: PhantomData,
        }
    }
}

impl<T: Send + Sync + Clone + 'static> Operator for Window<T> {
    type Input = T;
    type Output = Vec<T>;

    async fn run(&self, mut input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("Window starting");
        let mut buffer: Vec<Message<T>> = Vec::new();

        match self.condition {
            WindowCondition::Count(count) => {
                while let Some(msg) = input.recv().await {
                    buffer.push(msg);
                    if buffer.len() >= count {
                        let items: Vec<T> = buffer.drain(..).map(|m| m.payload).collect();
                        output.send(Message::new(items)).await?;
                    }
                }
            }
            WindowCondition::Time(duration) => {
                let period = if duration.is_zero() {
                    Duration::from_millis(1)
                } else {
                    duration
                };
                let mut ticker = interval_at(Instant::now() + period, period);
                loop {
                    tokio::select! {
                        biased;
                        maybe_msg = input.recv() => {
                            match maybe_msg {
                                Some(msg) => buffer.push(msg),
                                None => break,
                            }
                        }
                        _ = ticker.tick() => {
                            if !buffer.is_empty() {
                                let items: Vec<T> = buffer.drain(..).map(|m| m.payload).collect();
                                output.send(Message::new(items)).await?;
                            }
                        }
                    }
                }
            }
            WindowCondition::Sliding {
                window_size,
                slide_interval,
            } => {
                let period = if slide_interval.is_zero() {
                    Duration::from_millis(1)
                } else {
                    slide_interval
                };
                let window_ms = window_size.as_millis() as u64;
                let mut ticker = interval_at(Instant::now() + period, period);
                loop {
                    tokio::select! {
                        biased;
                        maybe_msg = input.recv() => {
                            match maybe_msg {
                                Some(msg) => buffer.push(msg),
                                None => break,
                            }
                        }
                        _ = ticker.tick() => {
                            let cutoff = now_millis().saturating_sub(window_ms);
                            buffer.retain(|msg| msg.event_timestamp >= cutoff);
                            if !buffer.is_empty() {
                                let items: Vec<T> = buffer.iter().map(|m| m.payload.clone()).collect();
                                output.send(Message::new(items)).await?;
                            }
                        }
                    }
                }
            }
        }

        if !buffer.is_empty() {
            let remaining: Vec<T> = buffer.drain(..).map(|m| m.payload).collect();
            output.send(Message::new(remaining)).await?;
        }

        debug!("Window completed");
        Ok(())
    }
}

impl<T: Send + Sync + Clone + 'static> PipelineComponent for Window<T> {
    type Input = T;
    type Output = Vec<T>;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<crate::pipeline::ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}
