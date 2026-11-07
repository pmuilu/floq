use std::time::Duration;

/// Exponential backoff reconnection policy for network streaming sources.
#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    pub enabled: bool,
    pub max_retries: Option<usize>,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: None,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl ReconnectPolicy {
    /// Creates a policy with automatic reconnection disabled.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Creates a policy with a bounded number of reconnection attempts.
    pub fn with_max_retries(max_retries: usize) -> Self {
        Self {
            enabled: true,
            max_retries: Some(max_retries),
            ..Self::default()
        }
    }

    /// Returns `true` if another reconnection attempt is permitted for `attempt` (0-indexed).
    #[inline]
    pub fn should_retry(&self, attempt: usize) -> bool {
        if !self.enabled {
            return false;
        }
        match self.max_retries {
            Some(max) => attempt < max,
            None => true,
        }
    }

    /// Computes exponential backoff duration for `attempt` (0-indexed), capped at `max_backoff`.
    #[inline]
    pub fn backoff_for_attempt(&self, attempt: usize) -> Duration {
        let shift = attempt.min(10) as u32;
        let multiplier = 1u32 << shift;
        self.initial_backoff
            .saturating_mul(multiplier)
            .min(self.max_backoff)
    }
}
