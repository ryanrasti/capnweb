//! Rust boundary for the JavaScript Promise behavior required by Cap'n Web.
//!
//! This API is intentionally small and provisional. Its implementation needs more inspection to
//! determine the simplest eager-poll model that preserves e-order: operations registered against
//! one unresolved promise must start in registration order, without waiting for earlier operations
//! to complete. Independent promises do not require a shared scheduler or microtask queue.

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::core::Result;

/// An eagerly-started, shared promise result with ordered operation starts.
///
/// `RUST-DEFERRED(JS built-in: Promise/e-order scheduler)`: the interface is used by the mechanical
/// runtime translation, but its scheduler is deliberately left unimplemented until the e-order
/// constraints have been inspected and tested independently.
pub(crate) struct EagerPromise<T> {
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for EagerPromise<T> {
    fn clone(&self) -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<T: Clone + Send + 'static> EagerPromise<T> {
    /// Starts `future`, including one synchronous initial poll.
    pub(crate) fn new(_future: impl Future<Output = Result<T>> + Send + 'static) -> Self {
        unimplemented!("eager promise scheduling needs e-order inspection")
    }

    /// Starts `operation` in this promise's registration order after successful resolution.
    pub(crate) fn then<U: Clone + Send + 'static>(
        &self,
        _operation: impl FnOnce(T) -> Result<U> + Send + 'static,
    ) -> EagerPromise<U> {
        unimplemented!("ordered promise operation starts need e-order inspection")
    }

    /// Asynchronously resolves an operation after starting it in registration order.
    pub(crate) fn then_async<U: Clone + Send + 'static>(
        &self,
        _operation: impl FnOnce(T) -> Pin<Box<dyn Future<Output = Result<U>> + Send + 'static>>
        + Send
        + 'static,
    ) -> EagerPromise<U> {
        unimplemented!("ordered asynchronous promise starts need e-order inspection")
    }

    /// Returns the cached settlement when already resolved.
    pub(crate) fn peek(&self) -> Option<Result<T>> {
        unimplemented!("settled promise inspection needs e-order inspection")
    }

    /// Registers a settlement observer without changing the result.
    pub(crate) fn observe(&self, _observer: impl FnOnce(Result<T>) + Send + 'static) {
        unimplemented!("promise rejection observation needs e-order inspection")
    }

    /// Consumes one handle and awaits its owned result.
    pub(crate) fn into_future(self) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>> {
        unimplemented!("promise awaiting needs e-order inspection")
    }
}
