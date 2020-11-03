use std::sync::Arc;
use std::time::Instant;

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use actix_storage::dev::actor::{
    ExpiryRequest, ExpiryResponse, ExpiryStoreRequest, ExpiryStoreResponse, StoreRequest,
    StoreResponse,
};
use dashmap::DashMap;
use delay_queue::{Delay, DelayQueue};

struct Value {
    bytes: Arc<[u8]>,
    timeout: Option<Instant>,
    persist: bool,
    ref_count: u32,
    nonce: u32,
}

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
    map: Arc<DashMap<Arc<[u8]>, Value>>,
    queue: DelayQueue<Delay<(Arc<[u8]>, u32)>>,
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

        std::thread::spawn(move || loop {
            let item = queue.pop();
            let mut should_delete = false;
            if let Some(mut value) = map.get_mut(&item.value.0) {
                if value.nonce != item.value.1 {
                    continue;
                }

                value.ref_count -= 1;

                if value.ref_count == 0 && !value.persist {
                    should_delete = true;
                }
            }
            if should_delete {
                map.remove(&item.value.0);
            }
        });
    }
}

impl Handler<StoreRequest> for DashMapActor {
    type Result = StoreResponse;

    fn handle(&mut self, msg: StoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            StoreRequest::Set(key, value) => {
                if let Some((_, val)) = self.map.remove(&key) {
                    let value = Value {
                        bytes: value,
                        timeout: None,
                        persist: true,
                        ref_count: 0,
                        nonce: val.nonce + 1,
                    };
                    self.map.insert(key, value);
                } else {
                    let value = Value {
                        bytes: value,
                        timeout: None,
                        persist: true,
                        ref_count: 0,
                        nonce: 0,
                    };
                    self.map.insert(key, value);
                }
                StoreResponse::Set(Ok(()))
            }
            StoreRequest::Get(key) => {
                StoreResponse::Get(Ok(self.map.get(&key).map(|v| v.bytes.clone())))
            }
            StoreRequest::Delete(key) => {
                self.map.remove(&key);
                StoreResponse::Delete(Ok(()))
            }
            StoreRequest::Contains(key) => StoreResponse::Contains(Ok(self.map.contains_key(&key))),
        }
    }
}

impl Handler<ExpiryRequest> for DashMapActor {
    type Result = ExpiryResponse;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(key, expires_in) => {
                if let Some(mut val) = self.map.get_mut(key.as_ref()) {
                    val.persist = false;
                    val.timeout = Some(Instant::now() + expires_in);
                    val.ref_count += 1;
                    self.queue
                        .push(Delay::for_duration((key, val.nonce), expires_in));
                }
                ExpiryResponse::Set(Ok(()))
            }
            ExpiryRequest::Persist(key) => {
                if let Some(mut val) = self.map.get_mut(key.as_ref()) {
                    val.persist = true;
                }
                ExpiryResponse::Persist(Ok(()))
            }
            ExpiryRequest::Get(key) => {
                let item = self
                    .map
                    .get(&key)
                    .map(|val| val.timeout.map(|timeout| timeout - Instant::now()))
                    .flatten();
                ExpiryResponse::Get(Ok(item))
            }
            ExpiryRequest::Extend(key, duration) => {
                if let Some(mut val) = self.map.get_mut(key.as_ref()) {
                    if let Some(ref timeout) = val.timeout {
                        let new_timeout = *timeout + duration;
                        self.queue
                            .push(Delay::until_instant((key, val.nonce), new_timeout));
                        val.persist = false;
                        val.ref_count += 1;
                        val.timeout = Some(new_timeout);
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
            ExpiryStoreRequest::SetExpiring(key, value, expires_in) => {
                let val = if let Some((_, val)) = self.map.remove(&key) {
                    Value {
                        bytes: value,
                        timeout: Some(Instant::now() + expires_in),
                        persist: false,
                        ref_count: 1,
                        nonce: val.nonce + 1,
                    }
                } else {
                    Value {
                        bytes: value,
                        timeout: Some(Instant::now() + expires_in),
                        persist: false,
                        ref_count: 1,
                        nonce: 0,
                    }
                };
                self.queue
                    .push(Delay::for_duration((key.clone(), val.nonce), expires_in));
                self.map.insert(key, val);
                ExpiryStoreResponse::SetExpiring(Ok(()))
            }
            ExpiryStoreRequest::GetExpiring(key) => {
                let item = self.map.get(&key);
                if let Some(value) = item {
                    ExpiryStoreResponse::GetExpiring(Ok(Some((
                        value.bytes.clone(),
                        value
                            .timeout
                            .and_then(|timeout| timeout.checked_duration_since(Instant::now())),
                    ))))
                } else {
                    ExpiryStoreResponse::GetExpiring(Ok(None))
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[actix_rt::test]
    async fn test_dashmap_store() {
        let store = DashMapActor::default().start(1);
        test_store(store).await;
    }

    #[actix_rt::test]
    async fn test_dashmap_expiry() {
        let store = DashMapActor::default().start(1);
        test_expiry(store.clone(), store).await;
    }

    #[actix_rt::test]
    async fn test_dashmap_expiry_store() {
        let store = DashMapActor::default().start(1);
        test_expiry_store(store).await;
    }

    #[actix_rt::test]
    async fn test_dashmap_formats() {
        let store = DashMapActor::default().start(1);
        test_all_formats(store).await;
    }
}
