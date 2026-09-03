//! Direct structural translation of `src/rpc.ts`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde_json::{Value as Json, json};
use tokio::sync::{Notify, mpsc};

use crate::core::{
    ErrorStubHook, MaybePromise, PayloadStubHook, PropertyPath, RpcPayload, RpcStub, RpcTarget,
    StubHook, StubHookInner, TargetStubHook, as_import_hook,
};
use crate::serialize::{Devaluator, Evaluator, ExportId, Exporter, ImportId, Importer};
use crate::transport::Transport;
use crate::{Error, Result};

struct ExportTableEntry {
    hook: StubHook,
    refcount: u64,
    pull_started: bool,
}

/// Direct translation of JS `ImportTableEntry`.
pub(crate) struct ImportTableEntry {
    session: Weak<RpcSessionInner>,
    import_id: ImportId,
    resolution: Mutex<Option<StubHook>>,
    leases: Mutex<Vec<Weak<ImportLease>>>,
    ready: Notify,
    pulling: AtomicBool,
}

impl ImportTableEntry {
    fn new(session: &Arc<RpcSessionInner>, import_id: ImportId, pulling: bool) -> Self {
        Self {
            session: Arc::downgrade(session),
            import_id,
            resolution: Mutex::new(None),
            leases: Mutex::new(Vec::new()),
            ready: Notify::new(),
            pulling: AtomicBool::new(pulling),
        }
    }

    /// Equivalent of `ImportTableEntry.resolve()`.
    fn resolve(&self, hook: StubHook) {
        *self.resolution.lock().expect("import mutex poisoned") = Some(hook);
        for lease in self
            .leases
            .lock()
            .expect("import lease mutex poisoned")
            .iter()
            .filter_map(Weak::upgrade)
        {
            lease.release();
        }
        self.ready.notify_waiters();
    }

    /// Equivalent of `ImportTableEntry.awaitResolution()`.
    async fn await_resolution(&self) -> Result<StubHook> {
        if !self.pulling.swap(true, Ordering::AcqRel) {
            self.session
                .upgrade()
                .ok_or(Error::Disconnected)?
                .send(json!(["pull", self.import_id]))?;
        }
        loop {
            let notified = self.ready.notified();
            if let Some(hook) = self
                .resolution
                .lock()
                .expect("import mutex poisoned")
                .clone()
            {
                return Ok(hook);
            }
            notified.await;
        }
    }

    fn current_resolution(&self) -> Option<StubHook> {
        self.resolution
            .lock()
            .expect("import mutex poisoned")
            .clone()
    }
}

struct ImportLease {
    entry: Arc<ImportTableEntry>,
    released: AtomicBool,
}

impl ImportLease {
    fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel)
            && let Some(session) = self.entry.session.upgrade()
        {
            let _ = session.send(json!(["release", self.entry.import_id, 1]));
        }
    }
}

impl Drop for ImportLease {
    fn drop(&mut self) {
        self.release();
    }
}

/// Direct translation of JS `RpcImportHook`.
pub(crate) struct RpcImportHook {
    entry: Arc<ImportTableEntry>,
    lease: Arc<ImportLease>,
    is_promise: bool,
}

impl RpcImportHook {
    fn create(entry: Arc<ImportTableEntry>, is_promise: bool) -> StubHook {
        let lease = Arc::new(ImportLease {
            entry: entry.clone(),
            released: AtomicBool::new(false),
        });
        entry
            .leases
            .lock()
            .expect("import lease mutex poisoned")
            .push(Arc::downgrade(&lease));
        Arc::new(Self {
            entry,
            lease,
            is_promise,
        })
    }

    pub(crate) fn belongs_to(&self, session: &RpcSessionInner) -> bool {
        self.entry
            .session
            .upgrade()
            .is_some_and(|owner| std::ptr::eq(Arc::as_ptr(&owner), session))
    }

    pub(crate) fn import_id(&self) -> ImportId {
        self.entry.import_id
    }
}

impl StubHookInner for RpcImportHook {
    /// Equivalent of `RpcImportHook.call()`.
    fn call(&self, path: PropertyPath, args: RpcPayload) -> StubHook {
        if let Some(resolution) = self.entry.current_resolution() {
            resolution.call(path, args)
        } else if let Some(session) = self.entry.session.upgrade() {
            session.send_call(self.entry.import_id, path, Some(args))
        } else {
            ErrorStubHook::create(Error::Disconnected)
        }
    }

    /// Equivalent of `RpcImportHook.get()`.
    fn get(&self, path: PropertyPath) -> StubHook {
        if let Some(resolution) = self.entry.current_resolution() {
            resolution.get(path)
        } else if let Some(session) = self.entry.session.upgrade() {
            session.send_call(self.entry.import_id, path, None)
        } else {
            ErrorStubHook::create(Error::Disconnected)
        }
    }

    /// Equivalent of `RpcImportHook.pull()`.
    fn pull(&self) -> MaybePromise<RpcPayload> {
        if !self.is_promise {
            return MaybePromise::Pending(Box::pin(async {
                Err(Error::Type("cannot pull a non-promise import".into()))
            }));
        }
        if let Some(resolution) = self.entry.current_resolution() {
            return resolution.pull();
        }
        let entry = self.entry.clone();
        MaybePromise::Pending(Box::pin(async move {
            let hook = entry.await_resolution().await?;
            match hook.pull() {
                MaybePromise::Immediate(payload) => Ok(payload),
                MaybePromise::Pending(pending) => pending.await,
            }
        }))
    }

    /// Equivalent of `RpcImportHook.dup()`.
    ///
    /// Difference from JS: clones share the Arc-backed introduction lease; the
    /// lease emits one release when its final local owner drops.
    fn dup(&self) -> StubHook {
        Arc::new(Self {
            entry: self.entry.clone(),
            lease: self.lease.clone(),
            is_promise: false,
        })
    }
}

struct RpcSessionState {
    exports: HashMap<ExportId, ExportTableEntry>,
    reverse_exports: HashMap<usize, ExportId>,
    imports: HashMap<ImportId, Weak<ImportTableEntry>>,
    next_export_id: ExportId,
    next_incoming_push_id: ExportId,
}

/// Direct translation of JS `RpcSessionImpl`.
pub(crate) struct RpcSessionInner {
    self_weak: Weak<RpcSessionInner>,
    state: Mutex<RpcSessionState>,
    next_import_id: AtomicI64,
    outbound: mpsc::UnboundedSender<Json>,
}

impl RpcSessionInner {
    fn send(&self, message: Json) -> Result<()> {
        self.outbound.send(message).map_err(|_| Error::Disconnected)
    }

    /// Equivalent of `RpcSessionImpl.sendCall()`.
    fn send_call(
        self: &Arc<Self>,
        id: ImportId,
        path: PropertyPath,
        args: Option<RpcPayload>,
    ) -> StubHook {
        let expression = if let Some(args) = args.as_ref() {
            let encoded = match Devaluator::new(self.as_ref()).devaluate(&args.value) {
                Ok(Json::Array(mut outer)) if outer.len() == 1 => outer.remove(0),
                Ok(_) => {
                    return ErrorStubHook::create(Error::Protocol(
                        "encoded arguments were not an array".into(),
                    ));
                }
                Err(error) => return ErrorStubHook::create(error),
            };
            json!(["pipeline", id, path, encoded])
        } else {
            json!(["pipeline", id, path])
        };

        let import_id = self.next_import_id.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(ImportTableEntry::new(self, import_id, false));
        self.state
            .lock()
            .expect("session mutex poisoned")
            .imports
            .insert(import_id, Arc::downgrade(&entry));
        if let Err(error) = self.send(json!(["push", expression])) {
            return ErrorStubHook::create(error);
        }
        RpcImportHook::create(entry, true)
    }

    /// Equivalent of `RpcSessionImpl.exportStub()`.
    fn export_stub_impl(&self, hook: StubHook) -> Result<ExportId> {
        let identity = Arc::as_ptr(&hook) as *const () as usize;
        let mut state = self.state.lock().expect("session mutex poisoned");
        if let Some(id) = state.reverse_exports.get(&identity).copied() {
            state
                .exports
                .get_mut(&id)
                .expect("reverse export missing")
                .refcount += 1;
            return Ok(id);
        }
        let id = state.next_export_id;
        state.next_export_id -= 1;
        state.exports.insert(
            id,
            ExportTableEntry {
                hook,
                refcount: 1,
                pull_started: false,
            },
        );
        state.reverse_exports.insert(identity, id);
        Ok(id)
    }

    /// Equivalent of `RpcSessionImpl.exportPromise()`.
    fn export_promise_impl(self: &Arc<Self>, hook: StubHook) -> Result<ExportId> {
        let id = {
            let mut state = self.state.lock().expect("session mutex poisoned");
            let id = state.next_export_id;
            state.next_export_id -= 1;
            state.exports.insert(
                id,
                ExportTableEntry {
                    hook,
                    refcount: 1,
                    pull_started: false,
                },
            );
            id
        };
        self.ensure_resolving_export(id)?;
        Ok(id)
    }

    /// Equivalent of `RpcSessionImpl.ensureResolvingExport()`.
    fn ensure_resolving_export(self: &Arc<Self>, id: ExportId) -> Result<()> {
        let hook = {
            let mut state = self.state.lock().expect("session mutex poisoned");
            let entry = state
                .exports
                .get_mut(&id)
                .ok_or_else(|| Error::Protocol(format!("no such export: {id}")))?;
            if entry.pull_started {
                return Ok(());
            }
            entry.pull_started = true;
            entry.hook.clone()
        };
        let session = self.clone();
        tokio::spawn(async move {
            let payload = pull_export_payload(&session, hook).await;
            let message = match payload {
                Ok(payload) => match Devaluator::new(session.as_ref()).devaluate(&payload.value) {
                    Ok(value) => json!(["resolve", id, value]),
                    Err(error) => reject_message(id, &error),
                },
                Err(error) => reject_message(id, &error),
            };
            let _ = session.send(message);
        });
        Ok(())
    }

    fn import_entry(self: &Arc<Self>, id: ImportId, pulling: bool) -> Arc<ImportTableEntry> {
        let mut state = self.state.lock().expect("session mutex poisoned");
        if let Some(entry) = state.imports.get(&id).and_then(Weak::upgrade) {
            return entry;
        }
        let entry = Arc::new(ImportTableEntry::new(self, id, pulling));
        state.imports.insert(id, Arc::downgrade(&entry));
        entry
    }

    async fn process_message(self: &Arc<Self>, message: Json) -> Result<()> {
        let message = message
            .as_array()
            .ok_or_else(|| Error::Protocol("message is not an array".into()))?;
        let tag = message.first().and_then(Json::as_str).unwrap_or_default();
        match tag {
            "push" => {
                let expression = message
                    .get(1)
                    .cloned()
                    .ok_or_else(|| Error::Protocol("push is missing its expression".into()))?;
                let payload = Evaluator::new(self.as_ref()).evaluate(expression)?;
                let hook = PayloadStubHook::create(payload);
                let mut state = self.state.lock().expect("session mutex poisoned");
                let id = state.next_incoming_push_id;
                state.next_incoming_push_id += 1;
                state.exports.insert(
                    id,
                    ExportTableEntry {
                        hook,
                        refcount: 1,
                        pull_started: false,
                    },
                );
            }
            "pull" => {
                let id = message_id(message, 1)?;
                self.ensure_resolving_export(id)?;
            }
            "resolve" | "reject" => {
                let id = message_id(message, 1)?;
                let entry = self
                    .state
                    .lock()
                    .expect("session mutex poisoned")
                    .imports
                    .get(&id)
                    .and_then(Weak::upgrade);
                if let Some(entry) = entry {
                    let hook = if tag == "resolve" {
                        let payload = Evaluator::new(self.as_ref())
                            .evaluate(message.get(2).cloned().unwrap_or(Json::Null))?;
                        PayloadStubHook::create(payload)
                    } else {
                        ErrorStubHook::create(Error::Remote(decode_error(message.get(2))))
                    };
                    // This matches JS `ImportTableEntry.resolve()`: resolving
                    // releases each wire introduction while local hooks forward
                    // through the stored resolution.
                    entry.resolve(hook);
                }
            }
            "release" => {
                let id = message_id(message, 1)?;
                let count = message
                    .get(2)
                    .and_then(Json::as_u64)
                    .ok_or_else(|| Error::Protocol("invalid release count".into()))?;
                let mut state = self.state.lock().expect("session mutex poisoned");
                let remove = {
                    let entry = state
                        .exports
                        .get_mut(&id)
                        .ok_or_else(|| Error::Protocol(format!("no such export: {id}")))?;
                    if entry.refcount < count {
                        return Err(Error::Protocol("export refcount underflow".into()));
                    }
                    entry.refcount -= count;
                    entry.refcount == 0 && id != 0
                };
                if remove && let Some(entry) = state.exports.remove(&id) {
                    let identity = Arc::as_ptr(&entry.hook) as *const () as usize;
                    state.reverse_exports.remove(&identity);
                }
            }
            "abort" => return Err(Error::Remote(decode_error(message.get(1)))),
            other => return Err(Error::Protocol(format!("unsupported message: {other}"))),
        }
        Ok(())
    }
}

impl Exporter for RpcSessionInner {
    fn export_stub(&self, hook: StubHook) -> Result<ExportId> {
        self.export_stub_impl(hook)
    }

    fn export_promise(&self, hook: StubHook) -> Result<ExportId> {
        self.self_weak
            .upgrade()
            .ok_or(Error::Disconnected)?
            .export_promise_impl(hook)
    }

    fn get_import(&self, hook: &StubHook) -> Option<ImportId> {
        as_import_hook(hook)
            .filter(|hook| hook.belongs_to(self))
            .map(RpcImportHook::import_id)
    }
}

impl Importer for RpcSessionInner {
    fn import_stub(&self, id: ImportId) -> Result<StubHook> {
        let session = self.self_weak.upgrade().ok_or(Error::Disconnected)?;
        Ok(RpcImportHook::create(
            session.import_entry(id, false),
            false,
        ))
    }

    fn import_promise(&self, id: ImportId) -> Result<StubHook> {
        let session = self.self_weak.upgrade().ok_or(Error::Disconnected)?;
        Ok(RpcImportHook::create(session.import_entry(id, true), true))
    }

    fn get_export(&self, id: ExportId) -> Option<StubHook> {
        self.state
            .lock()
            .expect("session mutex poisoned")
            .exports
            .get(&id)
            .map(|entry| entry.hook.clone())
    }
}

/// Public session handle corresponding to JS `RpcSession`.
pub struct RpcSession {
    inner: Arc<RpcSessionInner>,
}

impl RpcSession {
    /// Constructs a session and starts its read/write loops.
    ///
    /// Difference from JS: transport I/O is run in Tokio tasks rather than an
    /// async method started by the constructor.
    pub fn new<T>(transport: T, local_main: RpcTarget) -> Self
    where
        T: Transport,
    {
        let (mut writer, mut reader) = transport.split();
        let (outbound, mut outgoing) = mpsc::unbounded_channel::<Json>();
        let main_hook = TargetStubHook::create(local_main);
        let mut exports = HashMap::new();
        exports.insert(
            0,
            ExportTableEntry {
                hook: main_hook,
                refcount: 1,
                pull_started: false,
            },
        );
        let inner = Arc::new_cyclic(|self_weak| RpcSessionInner {
            self_weak: self_weak.clone(),
            state: Mutex::new(RpcSessionState {
                exports,
                reverse_exports: HashMap::new(),
                imports: HashMap::new(),
                next_export_id: -1,
                next_incoming_push_id: 1,
            }),
            next_import_id: AtomicI64::new(1),
            outbound,
        });

        tokio::spawn(async move {
            while let Some(message) = outgoing.recv().await {
                if writer.send(message.to_string()).await.is_err() {
                    break;
                }
            }
        });

        let read_session = inner.clone();
        tokio::spawn(async move {
            loop {
                let raw = match reader.receive().await {
                    Ok(Some(raw)) => raw,
                    Ok(None) | Err(_) => break,
                };
                let message = match serde_json::from_str(&raw) {
                    Ok(message) => message,
                    Err(error) => {
                        let _ = read_session.send(json!(["abort", error.to_string()]));
                        break;
                    }
                };

                if let Err(error) = read_session.process_message(message).await {
                    let _ = read_session.send(json!(["abort", error.to_string()]));
                    break;
                }
            }
        });

        Self { inner }
    }

    /// Equivalent of `RpcSession.getRemoteMain()`.
    pub fn get_remote_main(&self) -> RpcStub {
        let entry = self.inner.import_entry(0, false);
        RpcStub::from_hook(RpcImportHook::create(entry, false))
    }
}

async fn pull_export_payload(
    session: &Arc<RpcSessionInner>,
    mut hook: StubHook,
) -> Result<RpcPayload> {
    loop {
        let payload = match hook.pull() {
            MaybePromise::Immediate(payload) => payload,
            MaybePromise::Pending(pending) => pending.await?,
        };

        // This is the promise-chain elision loop in JS
        // `ensureResolvingExport()`. A promise pointing back to this peer must
        // remain a pipeline expression; any other root promise can be followed
        // locally without introducing an intermediate promise export.
        let crate::CapnwebValue::Promise(promise) = &payload.value else {
            return Ok(payload);
        };
        let inner = promise.hook();
        if as_import_hook(&inner).is_some_and(|hook| hook.belongs_to(session)) {
            return Ok(payload);
        }
        hook = inner;
    }
}

fn message_id(message: &[Json], index: usize) -> Result<i64> {
    message
        .get(index)
        .and_then(Json::as_i64)
        .ok_or_else(|| Error::Protocol("invalid message ID".into()))
}

fn reject_message(id: ExportId, error: &Error) -> Json {
    json!(["reject", id, ["error", "Error", error.to_string()]])
}

fn decode_error(value: Option<&Json>) -> String {
    value
        .and_then(Json::as_array)
        .and_then(|value| value.get(2))
        .and_then(Json::as_str)
        .or_else(|| value.and_then(Json::as_str))
        .unwrap_or("remote error")
        .to_owned()
}
