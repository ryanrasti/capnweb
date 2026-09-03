//! Structural home for the translation of `src/map.ts`.
//!
//! Parity status: the public behavior currently uses a pull-then-map fallback.
//! `MapBuilder`, `MapVariableHook`, `MapApplicator`, capture serialization, and
//! the wire-level `map` expression remain to be translated in source order.

use std::sync::Arc;

use crate::Result;
use crate::core::{
    CapnwebValue, IntoCapnwebValue, PayloadStubHook, PromiseStubHook, RpcPayload, RpcPromise,
};

/// Temporary semantic fallback used until the map instruction protocol is translated.
///
/// Difference from JS: `src/map.ts` records calls synchronously and sends map
/// instructions without pulling the subject. This implementation first pulls
/// the subject, but retains synchronous callback execution and recursive RPC
/// promise delivery.
pub(crate) fn map_after_pull<F, R>(promise: RpcPromise, callback: F) -> RpcPromise
where
    F: Fn(CapnwebValue) -> Result<R> + Send + Sync + 'static,
    R: IntoCapnwebValue,
{
    let callback = Arc::new(callback);
    let hook = PromiseStubHook::create(async move {
        let value = promise.pull().await?.deliver().await?;
        let mapped = match value {
            CapnwebValue::Null => CapnwebValue::Null,
            CapnwebValue::Array(values) => {
                let mut mapped = Vec::with_capacity(values.len());
                for value in values {
                    mapped.push(callback(value)?.into_capnweb_value()?);
                }
                CapnwebValue::Array(mapped)
            }
            value => callback(value)?.into_capnweb_value()?,
        };
        Ok(PayloadStubHook::create(RpcPayload::from_app_return(mapped)))
    });
    RpcPromise::new(hook, Vec::new())
}
