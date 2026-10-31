use crate::error::{Error, Result};
use crate::pipeline::{Operator, PipelineComponent, Receiver, Sender};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Trait for reduction functions, implemented for all `Fn(&mut O, I) + Send + Sync + 'static`
/// closures as well as [`BoxedReducer`].
pub trait ReducerFn<I, O>: Send + Sync + 'static {
    fn call(&self, acc: &mut O, input: I);
}

impl<I, O, F> ReducerFn<I, O> for F
where
    F: Fn(&mut O, I) + Send + Sync + 'static,
{
    #[inline]
    fn call(&self, acc: &mut O, input: I) {
        (self)(acc, input);
    }
}

type DynReducer<I, O> = dyn Fn(&mut O, I) + Send + Sync;

/// Ref-counted, dynamically dispatched reducer function used when storing [`Reduce`] in non-generic containers.
pub struct BoxedReducer<I, O>(Arc<DynReducer<I, O>>);

impl<I, O> Clone for BoxedReducer<I, O> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<I: 'static, O: 'static> ReducerFn<I, O> for BoxedReducer<I, O> {
    #[inline]
    fn call(&self, acc: &mut O, input: I) {
        (self.0)(acc, input);
    }
}

/// Stateful reduction operator that folds incoming items of type `I` into an accumulator `O`.
///
/// Parameterized over `F` so reduction closures are monomorphized and inlined on the hot path.
pub struct Reduce<I, O, F = BoxedReducer<I, O>> {
    reducer: F,
    current_value: Arc<Mutex<O>>,
    _phantom: PhantomData<fn(I)>,
}

impl<I, O, F: Clone> Clone for Reduce<I, O, F> {
    fn clone(&self) -> Self {
        Self {
            reducer: self.reducer.clone(),
            current_value: Arc::clone(&self.current_value),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, F> Reduce<I, O, F>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: ReducerFn<I, O>,
{
    /// Creates a new monomorphized [`Reduce`] operator.
    pub fn new(initial: O, reducer: F) -> Self {
        Self {
            reducer,
            current_value: Arc::new(Mutex::new(initial)),
            _phantom: PhantomData,
        }
    }

    pub fn get_result(&self) -> O {
        self.current_value
            .lock()
            .expect("Reduce accumulator mutex poisoned")
            .clone()
    }
}

impl<I, O> Reduce<I, O, BoxedReducer<I, O>>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
{
    /// Creates a heap-allocated, dynamically dispatched [`Reduce`] for FFI/dynamic containers.
    pub fn new_boxed<F>(initial: O, reducer: F) -> Self
    where
        F: Fn(&mut O, I) + Send + Sync + 'static,
    {
        Self {
            reducer: BoxedReducer(Arc::new(reducer)),
            current_value: Arc::new(Mutex::new(initial)),
            _phantom: PhantomData,
        }
    }
}

impl<I, O, F> Operator for Reduce<I, O, F>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: ReducerFn<I, O>,
{
    type Input = I;
    type Output = O;

    async fn run(
        &self,
        mut input: Receiver<Self::Input>,
        output: Sender<Self::Output>,
    ) -> Result<()> {
        debug!("Reduce starting");

        while let Some(item) = input.recv().await {
            let out_msg = {
                let mut current = self
                    .current_value
                    .lock()
                    .map_err(|_| Error::Custom("Reduce mutex poisoned".to_string()))?;
                item.map(|payload| {
                    self.reducer.call(&mut current, payload);
                    current.clone()
                })
            };

            output.send(out_msg).await?;
        }

        debug!("Reduce completed");
        Ok(())
    }
}

impl<I, O, F> PipelineComponent for Reduce<I, O, F>
where
    I: Send + 'static,
    O: Send + Clone + 'static,
    F: ReducerFn<I, O>,
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
