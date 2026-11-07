use crate::error::Result;
use crate::pipeline::{Operator, PipelineComponent, Receiver, Sender};
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::{debug, warn};

/// Trait for stateless transformation functions, implemented for all `Fn(I) -> O + Send + Sync + 'static`
/// closures as well as [`BoxedTransform`].
pub trait TransformFn<I, O>: Send + Sync + 'static {
    fn call(&self, input: I) -> O;
}

impl<I, O, F> TransformFn<I, O> for F
where
    F: Fn(I) -> O + Send + Sync + 'static,
{
    #[inline]
    fn call(&self, input: I) -> O {
        (self)(input)
    }
}

type DynTransform<I, O> = dyn Fn(I) -> O + Send + Sync;

/// Ref-counted, dynamically dispatched transform function used when storing [`Map`] in non-generic containers.
pub struct BoxedTransform<I, O>(Arc<DynTransform<I, O>>);

impl<I, O> Clone for BoxedTransform<I, O> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<I: 'static, O: 'static> TransformFn<I, O> for BoxedTransform<I, O> {
    #[inline]
    fn call(&self, input: I) -> O {
        (self.0)(input)
    }
}

/// Stateless transformation operator that maps each `Message<I>` to `Message<O>` by value.
///
/// Parameterized over `F` so closures are monomorphized and inlined on the hot path.
pub struct Map<I, O, F = BoxedTransform<I, O>> {
    transform: F,
    _phantom: PhantomData<fn(I) -> O>,
}

impl<I, O, F: Clone> Clone for Map<I, O, F> {
    fn clone(&self) -> Self {
        Self {
            transform: self.transform.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, F> Map<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: TransformFn<I, O>,
{
    /// Creates a new monomorphized [`Map`] operator that is inlined by the compiler.
    pub fn new(transform: F) -> Self {
        Self {
            transform,
            _phantom: PhantomData,
        }
    }
}

impl<I, O> Map<I, O, BoxedTransform<I, O>>
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Creates a heap-allocated, dynamically dispatched [`Map`] for FFI/dynamic containers.
    pub fn new_boxed<F>(transform: F) -> Self
    where
        F: Fn(I) -> O + Send + Sync + 'static,
    {
        Self {
            transform: BoxedTransform(Arc::new(transform)),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, F> Operator for Map<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: TransformFn<I, O>,
{
    type Input = I;
    type Output = O;

    async fn run(
        &self,
        mut input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
    ) -> Result<()> {
        debug!("Map starting");

        while let Some(msg) = input.recv().await {
            let mapped = msg.map(|payload| self.transform.call(payload));
            output.send(mapped).await?;
        }

        debug!("Map completed");
        Ok(())
    }
}

impl<I, O, F> PipelineComponent for Map<I, O, F>
where
    I: Send + 'static,
    O: Send + 'static,
    F: TransformFn<I, O>,
{
    type Input = I;
    type Output = O;

    async fn run(
        &self,
        input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
        _context: Arc<crate::pipeline::ComponentContext<Self::Input, Self::Output>>,
    ) -> Result<()> {
        <Self as Operator>::run(self, input, output).await
    }
}

/// Deserializes incoming JSON `String` messages into strongly-typed Rust structs `T: DeserializeOwned`.
///
/// Malformed JSON payloads are logged and skipped without aborting the pipeline.
pub struct JsonParse<T> {
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Clone for JsonParse<T> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Default for JsonParse<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JsonParse<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for JsonParse<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Input = String;
    type Output = T;

    async fn run(
        &self,
        mut input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
    ) -> Result<()> {
        while let Some(msg) = input.recv().await {
            match serde_json::from_str::<T>(&msg.payload) {
                Ok(parsed) => {
                    output.send(msg.with_new_payload(parsed)).await?;
                }
                Err(e) => {
                    warn!("JsonParse skipped malformed JSON payload: {}", e);
                }
            }
        }
        Ok(())
    }
}

impl<T> PipelineComponent for JsonParse<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    type Input = String;
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
