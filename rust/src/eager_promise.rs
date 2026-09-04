//! Rust boundary for the JavaScript Promise behavior required by Cap'n Web.
//!
//! TODO(e-order): This is an initial implementation and needs focused inspection. In particular,
//! verify that eager first polling, per-promise FIFO operation starts, rejection propagation,
//! dropped child handles, panic handling, and executor shutdown match every Cap'n Web hot spot. The
//! intended constraint is narrow: operations registered on one promise start in registration order;
//! they do not wait for earlier operations to complete, and independent promises share no queue.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::core::{Error, Result};

type Reaction<T> = Box<dyn FnOnce(Result<T>) + Send + 'static>;

struct State<T> {
    settlement: Option<Result<T>>,
    reactions: VecDeque<Reaction<T>>,
    drain_scheduled: bool,
}

struct Inner<T> {
    state: Mutex<State<T>>,
}

/// An eagerly-started, shared promise result with per-promise ordered reaction starts.
pub(crate) struct EagerPromise<T> {
    inner: Arc<Inner<T>>,
}

/// Rust equivalent of JavaScript's `Promise.withResolvers<T>()` result.
pub(crate) struct PromiseWithResolvers<T> {
    promise: EagerPromise<T>,
}

impl<T> Clone for PromiseWithResolvers<T> {
    fn clone(&self) -> Self {
        Self {
            promise: self.promise.clone(),
        }
    }
}

impl<T> Clone for EagerPromise<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Clone + Send + 'static> PromiseWithResolvers<T> {
    /// Creates one pending, shared promise and its settlement handle.
    pub(crate) fn new() -> Self {
        Self {
            promise: EagerPromise::pending(),
        }
    }

    /// Returns another handle to the same pending or settled promise.
    pub(crate) fn promise(&self) -> EagerPromise<T> {
        self.promise.clone()
    }

    /// Fulfills the promise exactly once.
    pub(crate) fn resolve(&self, value: T) {
        self.promise.settle(Ok(value));
    }

    /// Rejects the promise exactly once.
    pub(crate) fn reject(&self, error: Error) {
        self.promise.settle(Err(error));
    }
}

impl<T: Clone + Send + 'static> Default for PromiseWithResolvers<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + 'static> EagerPromise<T> {
    fn pending() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    settlement: None,
                    reactions: VecDeque::new(),
                    drain_scheduled: false,
                }),
            }),
        }
    }

    /// Starts `future`, including one synchronous initial poll.
    pub(crate) fn new(future: impl Future<Output = Result<T>> + Send + 'static) -> Self {
        let promise = Self::pending();
        promise.start(future);
        promise
    }

    fn start(&self, future: impl Future<Output = Result<T>> + Send + 'static) {
        let mut future: Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>> =
            Box::pin(future);
        let first_poll = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
            future.as_mut().poll(&mut context)
        }));

        match first_poll {
            Ok(std::task::Poll::Ready(settlement)) => self.settle(settlement),
            Ok(std::task::Poll::Pending) => {
                let promise = self.clone();
                match tokio::runtime::Handle::try_current() {
                    Ok(runtime) => {
                        runtime.spawn(async move {
                            use futures::FutureExt as _;
                            let settlement = std::panic::AssertUnwindSafe(future)
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|_| {
                                    Err(Error::Message(
                                        "eager promise future panicked while pending".to_owned(),
                                    ))
                                });
                            promise.settle(settlement);
                        });
                    }
                    Err(_) => self.settle(Err(Error::Message(
                        "pending eager promise requires an active Tokio runtime".to_owned(),
                    ))),
                }
            }
            Err(_) => self.settle(Err(Error::Message(
                "eager promise future panicked during its first poll".to_owned(),
            ))),
        }
    }

    fn settle(&self, settlement: Result<T>) {
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("eager promise mutex poisoned");
            if state.settlement.is_some() {
                return;
            }
            state.settlement = Some(settlement);
        }
        self.schedule_drain();
    }

    fn enqueue(&self, reaction: Reaction<T>) {
        self.inner
            .state
            .lock()
            .expect("eager promise mutex poisoned")
            .reactions
            .push_back(reaction);
        self.schedule_drain();
    }

    fn schedule_drain(&self) {
        let should_schedule = {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("eager promise mutex poisoned");
            if state.settlement.is_none() || state.reactions.is_empty() || state.drain_scheduled {
                false
            } else {
                state.drain_scheduled = true;
                true
            }
        };
        if !should_schedule {
            return;
        }

        let promise = self.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move { promise.drain() });
        } else {
            std::thread::spawn(move || promise.drain());
        }
    }

    fn drain(&self) {
        loop {
            let next = {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .expect("eager promise mutex poisoned");
                match state.reactions.pop_front() {
                    Some(reaction) => {
                        let settlement = state
                            .settlement
                            .clone()
                            .expect("promise drain requires settlement");
                        Some((reaction, settlement))
                    }
                    None => {
                        state.drain_scheduled = false;
                        None
                    }
                }
            };
            match next {
                Some((reaction, settlement)) => reaction(settlement),
                None => return,
            }
        }
    }

    /// Registers a settlement reaction and returns its child promise.
    ///
    /// The reaction receives Rust's `Result` equivalent of JavaScript's two
    /// `.then(onFulfilled, onRejected)` callbacks. It remains registered if the child is dropped.
    pub(crate) fn then<U: Clone + Send + 'static>(
        &self,
        reaction: impl FnOnce(Result<T>) -> Result<U> + Send + 'static,
    ) -> EagerPromise<U> {
        let child = EagerPromise::pending();
        let child_for_reaction = child.clone();
        self.enqueue(Box::new(move |settlement| {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reaction(settlement)))
                    .unwrap_or_else(|_| {
                        Err(Error::Message("eager promise reaction panicked".to_owned()))
                    });
            child_for_reaction.settle(result);
        }));
        child
    }

    /// Registers an asynchronous reaction, polling it once before the next reaction starts.
    pub(crate) fn then_async<U: Clone + Send + 'static>(
        &self,
        reaction: impl FnOnce(Result<T>) -> Pin<Box<dyn Future<Output = Result<U>> + Send + 'static>>
        + Send
        + 'static,
    ) -> EagerPromise<U> {
        let child = EagerPromise::pending();
        let child_for_reaction = child.clone();
        self.enqueue(Box::new(move |settlement| {
            let future =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reaction(settlement)));
            match future {
                Ok(future) => child_for_reaction.start(future),
                Err(_) => child_for_reaction.settle(Err(Error::Message(
                    "eager promise reaction panicked".to_owned(),
                ))),
            }
        }));
        child
    }

    /// Returns the cached fulfillment when this no longer behaves as an unresolved promise.
    ///
    /// Rejections intentionally return `None`, matching JavaScript's separate successful-resolution
    /// cache in `PromiseStubHook`; rejection still flows through registered reactions.
    pub(crate) fn peek(&self) -> Option<T> {
        self.inner
            .state
            .lock()
            .expect("eager promise mutex poisoned")
            .settlement
            .clone()
            .and_then(Result::ok)
    }

    /// Consumes one handle and awaits its owned result.
    pub(crate) fn into_future(self) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.enqueue(Box::new(move |settlement| {
            let _ = sender.send(settlement);
        }));
        Box::pin(async move {
            receiver.await.map_err(|_| {
                Error::Message("eager promise result delivery was canceled".to_owned())
            })?
        })
    }
}
