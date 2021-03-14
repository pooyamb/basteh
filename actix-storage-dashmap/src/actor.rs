use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, Instant};

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use actix_storage::dev::actor::{
    ExpiryRequest, ExpiryResponse, ExpiryStoreRequest, ExpiryStoreResponse, StoreRequest,
    StoreResponse,
};
use dashmap::DashMap;
use delay_queue::{Delay, DelayQueue};

/// The value representation that is stored in DashMap. Includes metadata for expiration logic.
struct Value {
    bytes: Arc<[u8]>,
    timeout: Option<Instant>,
    persist: bool,
    // nonce increases whenever a new value is set or expiration time changes
    nonce: usize,
}

impl Value {
    pub fn new(bytes: Arc<[u8]>, nonce: usize) -> Self {
        Value {
            bytes,
            timeout: None,
            persist: true,
            nonce,
        }
    }

    pub fn new_expiring(bytes: Arc<[u8]>, nonce: usize, expires_in: Duration) -> Self {
        Value {
            bytes,
            timeout: Some(Instant::now() + expires_in),
            persist: false,
            nonce,
        }
    }

    pub fn expires_in(&self) -> Option<Duration> {
        if self.persist == true {
            None
        } else {
            self.timeout
                .and_then(|timeout| timeout.checked_duration_since(Instant::now()))
        }
    }

    pub fn set_expires_in(&mut self, expires_in: Duration) -> Instant {
        let timeout = Instant::now() + expires_in;
        self.persist = false;
        self.timeout = Some(timeout);
        self.increase_nonce();
        timeout
    }

    pub fn extend_expires_in(&mut self, expires_in: Duration) -> Instant {
        if let Some(timeout) = self.timeout {
            let new_timeout = timeout + expires_in;
            self.persist = false;
            self.timeout = Some(new_timeout);
            self.increase_nonce();
            new_timeout
        } else {
            self.set_expires_in(expires_in)
        }
    }

    fn increase_nonce(&mut self) {
        self.nonce = self.nonce.checked_add(1).unwrap_or(0);
    }

    pub fn persist(&mut self) {
        self.persist = true;
    }
}

type ScopeMap = DashMap<Arc<[u8]>, Value>;
type InternalMap = DashMap<Arc<[u8]>, ScopeMap>;
/// (Scope, Key, Nonce)
type ExpiringKey = (Arc<[u8]>, Arc<[u8]>, usize);

/// An implementation of [`ExpiryStore`](actix_storage::dev::ExpiryStore) based on sync
/// actix actors and HashMap
///
/// It relies on delay_queue crate to provide expiration.
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_dashmap::DashMapActor;
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     const THREADS_NUMBER: usize = 4;
///     let store = DashMapActor::start_default(THREADS_NUMBER);
///     // OR
///     let store = DashMapActor::with_capacity(100).start(THREADS_NUMBER);
///     
///     let storage = Storage::build().expiry_store(store).finish();
///     let server = HttpServer::new(move || {
///         App::new()
///             .data(storage.clone())
///     });
///     server.bind("localhost:5000")?.run().await
/// }
/// ```
///
/// requires ["actor"] feature
#[derive(Clone, Default)]
pub struct DashMapActor {
    map: Arc<InternalMap>,
    queue: DelayQueue<Delay<ExpiringKey>>,

    #[doc(hidden)]
    stopped: Arc<AtomicBool>,
}

impl DashMapActor {
    /// Makes a new DashMapActor without starting it
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes a new DashMapActor with specified DashMap capacity without starting it
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: DashMap::with_capacity(capacity).into(),
            queue: DelayQueue::default(),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create default actor and start the actor in an actix sync arbiter with specified
    /// number of threads
    pub fn start_default(threads_num: usize) -> Addr<Self> {
        let storage = Self::default();
        SyncArbiter::start(threads_num, move || storage.clone())
    }

    /// Start the actor in an actix sync arbiter with specified number of threads
    pub fn start(self, threads_num: usize) -> Addr<Self> {
        SyncArbiter::start(threads_num, move || self.clone())
    }
}

impl Actor for DashMapActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        let map = self.map.clone();
        let mut queue = self.queue.clone();

        let stopped = self.stopped.clone();

        std::thread::spawn(move || loop {
            if let Some(item) = queue.try_pop_for(Duration::from_secs(1)) {
                let mut should_delete = false;
                let scope = &item.value.0;
                let key = &item.value.1;
                let nonce = item.value.2;
                if let Some(scope_map) = map.get_mut(scope) {
                    if let Some(value) = scope_map.get(key) {
                        if value.nonce != nonce {
                            continue;
                        }

                        if !value.persist {
                            should_delete = true;
                        }
                    }
                };
                if should_delete {
                    map.get_mut(scope)
                        .and_then(|scope_map| scope_map.remove(key));
                }
            } else if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
        });
    }
}

impl Handler<StoreRequest> for DashMapActor {
    type Result = StoreResponse;

    fn handle(&mut self, msg: StoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            StoreRequest::Set(scope, key, value) => {
                self.map
                    .entry(scope)
                    .or_default()
                    .entry(key)
                    .and_modify(|val| {
                        val.nonce += 1;
                        val.bytes = value.clone();
                    })
                    .or_insert_with(|| Value::new(value, 0));
                StoreResponse::Set(Ok(()))
            }
            StoreRequest::Get(scope, key) => {
                let value = if let Some(scope_map) = self.map.get(&scope) {
                    scope_map.get(&key).map(|val| val.bytes.clone())
                } else {
                    None
                };
                StoreResponse::Get(Ok(value))
            }
            StoreRequest::Delete(scope, key) => {
                self.map
                    .get_mut(&scope)
                    .and_then(|scope_map| scope_map.remove(&key));
                StoreResponse::Delete(Ok(()))
            }
            StoreRequest::Contains(scope, key) => {
                let contains = self
                    .map
                    .get(&scope)
                    .map(|scope_map| scope_map.contains_key(&key))
                    .unwrap_or(false);
                StoreResponse::Contains(Ok(contains))
            }
        }
    }
}

impl Handler<ExpiryRequest> for DashMapActor {
    type Result = ExpiryResponse;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(scope, key, expires_in) => {
                if let Some(scope_map) = self.map.get_mut(&scope) {
                    if let Some(mut val) = scope_map.get_mut(&key) {
                        let timeout = val.set_expires_in(expires_in);
                        self.queue
                            .push(Delay::until_instant((scope, key, val.nonce), timeout));
                    }
                }
                ExpiryResponse::Set(Ok(()))
            }
            ExpiryRequest::Persist(scope, key) => {
                if let Some(scope_map) = self.map.get_mut(&scope) {
                    if let Some(mut val) = scope_map.get_mut(&key) {
                        val.persist();
                    }
                }
                ExpiryResponse::Persist(Ok(()))
            }
            ExpiryRequest::Get(scope, key) => {
                let item = if let Some(scope_map) = self.map.get(&scope) {
                    scope_map.get(&key).and_then(|val| val.expires_in())
                } else {
                    None
                };
                ExpiryResponse::Get(Ok(item))
            }
            ExpiryRequest::Extend(scope, key, duration) => {
                if let Some(scope_map) = self.map.get_mut(&scope) {
                    if let Some(mut val) = scope_map.get_mut(&key) {
                        let new_timeout = val.extend_expires_in(duration);
                        self.queue
                            .push(Delay::until_instant((scope, key, val.nonce), new_timeout));
                    }
                }
                ExpiryResponse::Extend(Ok(()))
            }
        }
    }
}

impl Handler<ExpiryStoreRequest> for DashMapActor {
    type Result = ExpiryStoreResponse;

    fn handle(&mut self, msg: ExpiryStoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryStoreRequest::SetExpiring(scope, key, value, expires_in) => {
                let scope_map = self.map.entry(scope.clone()).or_default();
                let val = scope_map
                    .entry(key.clone())
                    .and_modify(|val| {
                        val.nonce += 1;
                        val.bytes = value.clone();
                        val.set_expires_in(expires_in);
                    })
                    .or_insert_with(|| Value::new_expiring(value, 0, expires_in));
                self.queue
                    .push(Delay::for_duration((scope, key, val.nonce), expires_in));
                ExpiryStoreResponse::SetExpiring(Ok(()))
            }
            ExpiryStoreRequest::GetExpiring(scope, key) => {
                let values = if let Some(scope_map) = self.map.get(&scope) {
                    scope_map
                        .get(&key)
                        .map(|val| (val.bytes.clone(), val.expires_in()))
                } else {
                    None
                };

                ExpiryStoreResponse::GetExpiring(Ok(values))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[test]
    fn test_dashmap_store() {
        test_store(Box::pin(async { DashMapActor::default().start(1) }));
    }

    #[test]
    fn test_dashmap_expiry() {
        test_expiry(
            Box::pin(async {
                let store = DashMapActor::default().start(1);
                (store.clone(), store)
            }),
            2,
        );
    }

    #[test]
    fn test_dashmap_expiry_store() {
        test_expiry_store(
            Box::pin(async {
                let store = DashMapActor::default().start(1);
                store
            }),
            2,
        );
    }

    #[test]
    fn test_dashmap_formats() {
        test_all_formats(Box::pin(async {
            let store = DashMapActor::default().start(1);
            store
        }));
    }
}
