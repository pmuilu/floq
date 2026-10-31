use crate::error::Result;
use crate::pipeline::{Operator, PipelineComponent, Receiver, Sender};
use regex::Regex;
use std::sync::Arc;
use tracing::debug;

type PredicateFn<T> = dyn Fn(&T) -> bool + Send + Sync;

enum FilterCondition<T> {
    Regex(Regex),
    Predicate(Arc<PredicateFn<T>>),
}

impl<T> Clone for FilterCondition<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Regex(r) => Self::Regex(r.clone()),
            Self::Predicate(p) => Self::Predicate(Arc::clone(p)),
        }
    }
}

/// Filtering operator that forwards messages matching a regex pattern or predicate closure.
pub struct Filter<T = String> {
    condition: FilterCondition<T>,
}

impl<T> Clone for Filter<T> {
    fn clone(&self) -> Self {
        Self {
            condition: self.condition.clone(),
        }
    }
}

impl Filter<String> {
    /// Creates a string filter from a regular expression `pattern`.
    pub fn with_pattern(pattern: &str) -> std::result::Result<Self, regex::Error> {
        Ok(Self {
            condition: FilterCondition::Regex(Regex::new(pattern)?),
        })
    }

    /// Creates a string filter from a closure taking `&str`.
    pub fn with_lambda<F>(f: F) -> Self
    where
        F: Fn(&str) -> bool + Send + Sync + 'static,
    {
        Self {
            condition: FilterCondition::Predicate(Arc::new(move |s: &String| f(s.as_str()))),
        }
    }
}

impl<T> Filter<T>
where
    T: Send + 'static,
{
    /// Creates a generic typed filter from a predicate closure `Fn(&T) -> bool`.
    pub fn with_predicate<F>(f: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self {
            condition: FilterCondition::Predicate(Arc::new(f)),
        }
    }
}

impl Default for Filter<String> {
    fn default() -> Self {
        Self::with_lambda(|_| true)
    }
}

impl<T> Operator for Filter<T>
where
    T: AsRef<str> + Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn run(&self, mut input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        debug!("Filter starting");

        while let Some(msg) = input.recv().await {
            let matches = match &self.condition {
                FilterCondition::Regex(pattern) => pattern.is_match(msg.payload.as_ref()),
                FilterCondition::Predicate(f) => f(&msg.payload),
            };

            if matches {
                debug!("Message matches filter condition, forwarding");
                output.send(msg).await?;
            } else {
                debug!("Message does not match filter condition, dropping");
            }
        }

        debug!("Filter completed");
        Ok(())
    }
}

impl<T> PipelineComponent for Filter<T>
where
    T: AsRef<str> + Send + 'static,
{
    type Input = T;
    type Output = T;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<crate::pipeline::ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}
