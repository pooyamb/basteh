use std::sync::{atomic::AtomicBool, Arc};
use std::time::Duration;

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use actix_storage::dev::actor::{
    ExpiryRequest, ExpiryResponse, ExpiryStoreRequest, ExpiryStoreResponse, StoreRequest,
    StoreResponse,
};

use crate::SledConfig;

mod delay;
mod flags;
mod inner;
mod utils;

#[cfg(test)]
mod tests;

pub use self::flags::ExpiryFlags;
use self::inner::SledActorInner;
pub use utils::{decode, decode_mut, encode};

/// An implementation of [`ExpiryStore`](actix_storage::dev::ExpiryStore) based on sync
/// actix actors and sled, using delay_queue crate to provide expiration
///
/// It stores expiration data as the value's suffix in sled, using byteorder, so to share data this actor
/// creates with other programs outside of its scope, you need to remove the suffix of it exported as
/// [`ExpiryFlags`](struct.ExpiryFlags.html), or directly use encode/decode methods provided.
///
/// To construct the actor you can either use the [`ToActorExt::to_actor`](trait.ToActorExt.html#tymethod.to_actor)
/// on a normal sled Config, or feed the sled db to this actor using [`from_db`](#method.from_db).
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_sled::{SledConfig, actor::{SledActor, ToActorExt}};
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     const THREADS_NUMBER: usize = 4;
///     let store = SledConfig::default().to_actor()?.start(THREADS_NUMBER);
///     // OR
///     let db = SledConfig::default().open()?;
///     let store = SledActor::from_db(db).start(THREADS_NUMBER);
///     
///     let storage = Storage::build().store(store).finish();
///     let server = HttpServer::new(move || {
///         App::new()
///             .app_data(storage.clone())
///     });
///     server.bind("localhost:5000")?.run().await
/// }
/// ```
///
/// requires ["actor"] feature
#[derive(Clone)]
pub struct SledActor {
    inner: SledActorInner,
    perform_deletion: bool,
    scan_db_on_start: bool,

    #[doc(hidden)]
    stopped: Arc<AtomicBool>,
}

impl SledActor {
    /// If set to true, actor will perform real deletion when an item expires instead of soft deleting it,
    /// it requires a seprate thread for expiration notification.
    #[must_use = "Actor should be started by calling start method"]
    pub fn perform_deletion(mut self, to: bool) -> Self {
        self.perform_deletion = to;
        self
    }

    /// If set to true, actor will scan the database on start to mark expired items.
    #[must_use = "Actor should be started by calling start method"]
    pub fn scan_db_on_start(mut self, to: bool) -> Self {
        self.scan_db_on_start = to;
        self
    }

    #[must_use = "Actor should be started by calling start method"]
    pub fn from_db(db: sled::Db) -> Self {
        Self {
            inner: SledActorInner::from_db(db),
            perform_deletion: false,
            scan_db_on_start: false,
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the actor in an actix's sync arbiter
    pub fn start(self, threads_num: usize) -> Addr<Self> {
        SyncArbiter::start(threads_num, move || self.clone())
    }

    fn spawn_expiry_thread(&self) {
        let mut inner = self.inner.clone();
        let stopped = self.stopped.clone();
        std::thread::spawn(move || loop {
            inner.try_delete_expired_item_for(Duration::from_secs(1));
            if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
        });
    }
}

impl Actor for SledActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        if self.scan_db_on_start && self.perform_deletion {
            self.inner.scan_db();
        }

        if self.perform_deletion {
            self.spawn_expiry_thread();
        }
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        loop {
            if self
                .stopped
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::Acquire,
                    std::sync::atomic::Ordering::Acquire,
                )
                .is_ok()
            {
                break;
            }
        }
    }
}

impl Handler<StoreRequest> for SledActor {
    type Result = StoreResponse;

    fn handle(&mut self, msg: StoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            StoreRequest::Set(scope, key, value) => {
                StoreResponse::Set(self.inner.set(scope, key, value))
            }
            StoreRequest::SetNumber(scope, key, value) => {
                StoreResponse::SetNumber(self.inner.set_number(scope, key, value))
            }
            StoreRequest::Get(scope, key) => StoreResponse::Get(self.inner.get(scope, key)),
            StoreRequest::GetNumber(scope, key) => {
                StoreResponse::GetNumber(self.inner.get_number(scope, key))
            }
            StoreRequest::Delete(scope, key) => {
                StoreResponse::Delete(self.inner.delete(scope, key))
            }
            StoreRequest::Contains(scope, key) => {
                StoreResponse::Contains(self.inner.contains(scope, key))
            }
            StoreRequest::MutateNumber(scope, key, mutations) => {
                StoreResponse::Mutate(self.inner.mutate(scope, key, mutations))
            }
            _ => StoreResponse::MethodNotSupported,
        }
    }
}

impl Handler<ExpiryRequest> for SledActor {
    type Result = ExpiryResponse;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(scope, key, expires_in) => {
                ExpiryResponse::Set(self.inner.set_expiry(scope, key, expires_in))
            }
            ExpiryRequest::Persist(scope, key) => {
                ExpiryResponse::Persist(self.inner.persist(scope, key))
            }
            ExpiryRequest::Get(scope, key) => {
                ExpiryResponse::Get(self.inner.get_expiry(scope, key))
            }
            ExpiryRequest::Extend(scope, key, duration) => {
                ExpiryResponse::Extend(self.inner.extend_expiry(scope, key, duration))
            }
        }
    }
}

impl Handler<ExpiryStoreRequest> for SledActor {
    type Result = ExpiryStoreResponse;

    fn handle(&mut self, msg: ExpiryStoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryStoreRequest::SetExpiring(scope, key, value, duration) => {
                ExpiryStoreResponse::SetExpiring(
                    self.inner.set_expiring(scope, key, value, duration),
                )
            }
            ExpiryStoreRequest::GetExpiring(scope, key) => {
                ExpiryStoreResponse::GetExpiring(self.inner.get_expiring(scope, key))
            }
        }
    }
}

/// An extension actor for sled::Config to convert it to a [`SledActor`](struct.SledActor.html)
pub trait ToActorExt {
    #[must_use = "Actor should be started by calling start method"]
    fn to_actor(&self) -> Result<SledActor, sled::Error>;
}

impl ToActorExt for SledConfig {
    fn to_actor(&self) -> Result<SledActor, sled::Error> {
        let db = self.open()?;
        Ok(SledActor::from_db(db))
    }
}
