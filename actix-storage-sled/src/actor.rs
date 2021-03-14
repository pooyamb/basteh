use std::time::{Duration, SystemTime};
use std::{
    ops::Deref,
    sync::{atomic::AtomicBool, Arc},
};

use actix::{Actor, Addr, Handler, SyncArbiter, SyncContext};
use actix_storage::{
    dev::actor::{
        ExpiryRequest, ExpiryResponse, ExpiryStoreRequest, ExpiryStoreResponse, StoreRequest,
        StoreResponse,
    },
    StorageError,
};
use byteorder::LittleEndian;
use delay_queue::{Delay, DelayQueue};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned, U16, U64};

use crate::SledConfig;

fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Represents expiry data and is stored as suffix to the value.
///
/// Nonce is used to ignore expiration requests after the value has changed as we don't have direct access to delay-queue
/// for removing notifications from it.
#[derive(Debug, Default, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
pub struct ExpiryFlags {
    nonce: U64<LittleEndian>,
    expires_at: U64<LittleEndian>,
    persist: U16<LittleEndian>,
}

impl ExpiryFlags {
    /// Make a new flags struct with persist flag set to true. Provide 0 for nonce if it's a new key.
    pub fn new_persist(nonce: u64) -> Self {
        Self {
            nonce: U64::new(nonce),
            expires_at: U64::new(0),
            persist: U16::new(1),
        }
    }

    /// Make a new flags struct with persist flag set to false. Provide 0 for nonce if it's a new key.
    pub fn new_expiring(nonce: u64, expires_in: Duration) -> Self {
        let expires_at = get_current_timestamp() + expires_in.as_secs();
        Self {
            nonce: U64::new(nonce),
            expires_at: U64::new(expires_at),
            persist: U16::new(0),
        }
    }

    /// Increase the nonce in place
    pub fn increase_nonce(&mut self) {
        self.nonce = U64::new(self.next_nonce());
    }

    /// Get the next nonce without mutating the current value
    pub fn next_nonce(&self) -> u64 {
        if self.nonce == U64::MAX_VALUE {
            0
        } else {
            self.nonce.get() + 1
        }
    }

    /// Change the expiration time
    pub fn expire_in(&mut self, duration: Duration) {
        self.expires_at
            .set(get_current_timestamp() + duration.as_secs())
    }

    /// Get the expiration time, returns None if persist flag is true.
    pub fn expires_in(&self) -> Option<Duration> {
        if self.persist.get() == 1 {
            return None;
        }
        let expires_at = self.expires_at.get();
        let now = get_current_timestamp();
        if expires_at <= now {
            Some(Duration::default())
        } else {
            Some(Duration::from_secs(expires_at - now))
        }
    }

    /// Check if the key is expired
    pub fn expired(&self) -> bool {
        let expires_at = self.expires_at.get();
        self.persist.get() == 0 && expires_at <= get_current_timestamp()
    }
}

struct DelayedIem {
    scope: Arc<[u8]>,
    key: Arc<[u8]>,
    nonce: u64,
}

impl DelayedIem {
    fn new(scope: Arc<[u8]>, key: Arc<[u8]>, nonce: u64) -> Self {
        Self { scope, key, nonce }
    }
}

/// Takes an IVec and returns value bytes with its expiry flags as mutable
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode_mut(bytes: &mut [u8]) -> Option<(&mut [u8], &mut ExpiryFlags)> {
    let (val, exp): (&mut [u8], LayoutVerified<&mut [u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_mut())?;
    Some((val, exp.into_mut()))
}

/// Takes an IVec and returns value bytes with its expiry flags
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode(bytes: &[u8]) -> Option<(&[u8], &ExpiryFlags)> {
    let (val, exp): (&[u8], LayoutVerified<&[u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_ref())?;
    Some((val, exp.into_ref()))
}

/// Takes a value as bytes and an ExpiryFlags and turns them into bytes
#[allow(clippy::type_complexity)]
#[inline]
pub fn encode(value: &[u8], exp: ExpiryFlags) -> Vec<u8> {
    let mut buff = vec![];
    buff.extend_from_slice(value);
    buff.extend_from_slice(exp.as_bytes());
    buff
}

#[cfg(not(feature = "v01-compat"))]
#[inline]
fn open_tree(db: &sled::Db, scope: &[u8]) -> Result<sled::Tree, sled::Error> {
    db.open_tree(scope)
}

#[cfg(feature = "v01-compat")]
#[inline]
fn open_tree(db: &sled::Db, scope: &[u8]) -> Result<sled::Tree, sled::Error> {
    if scope.as_ref() == &actix_storage::GLOBAL_SCOPE {
        Ok(db.deref().clone())
    } else {
        db.open_tree(scope)
    }
}

/// Used in expiration thread to remove delayed items from map
#[inline]
fn remove_expired_item(db: &sled::Db, item: DelayedIem) -> Result<(), sled::Error> {
    let tree = open_tree(&db, &item.scope)?;
    let val = tree.get(&item.key)?;
    if let Some(mut bytes) = val {
        if let Some((_, exp)) = decode_mut(&mut bytes) {
            if exp.nonce.get() == item.nonce && exp.persist.get() == 0 {
                tree.remove(&item.key)?;
            }
        }
    }

    Ok(())
}

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
#[derive(Clone)]
pub struct SledActor {
    db: sled::Db,
    queue: DelayQueue<Delay<DelayedIem>>,
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
            db,
            queue: DelayQueue::default(),
            perform_deletion: false,
            scan_db_on_start: false,
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Start the actor in an actix's sync arbiter
    pub fn start(self, threads_num: usize) -> Addr<Self> {
        SyncArbiter::start(threads_num, move || self.clone())
    }

    fn scan_expired_items(&mut self) {
        for tree_name in self.db.tree_names() {
            if let Ok(tree) = open_tree(&self.db, &tree_name) {
                let mut deleted_keys = vec![];
                for kv in tree.iter() {
                    if let Ok((key, value)) = kv {
                        let mut bytes = value.clone();
                        if let Some((_, exp)) = decode_mut(&mut bytes) {
                            if exp.expired() {
                                deleted_keys.push(key);
                            } else if let Some(dur) = exp.expires_in() {
                                self.queue.push(Delay::for_duration(
                                    DelayedIem::new(
                                        tree_name.to_vec().into(),
                                        key.to_vec().into(),
                                        exp.nonce.get(),
                                    ),
                                    dur,
                                ));
                            }
                        }
                    }
                }
                for key in deleted_keys {
                    tree.remove(&key).unwrap();
                }
            }
        }
    }
}

impl Actor for SledActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        if self.scan_db_on_start && self.perform_deletion {
            self.scan_expired_items();
        }

        if self.perform_deletion {
            let db = self.db.clone();
            let mut queue = self.queue.clone();
            let stopped = self.stopped.clone();
            std::thread::spawn(move || loop {
                if let Some(item) = queue.try_pop_for(Duration::from_secs(1)) {
                    if let Err(err) = remove_expired_item(&db, item.value) {
                        log::error!("actix-storage-sled: {}", err);
                    }
                } else if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            });
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
                let res = open_tree(&self.db, &scope)
                    .and_then(|tree| {
                        tree.update_and_fetch(&key, |bytes| {
                            let nonce = if let Some(bytes) = bytes {
                                decode(&bytes)
                                    .map(|(_, exp)| exp.next_nonce())
                                    .unwrap_or_default()
                            } else {
                                0
                            };

                            let exp = ExpiryFlags::new_persist(nonce);
                            let val = encode(&value, exp);

                            Some(val)
                        })
                    })
                    .map(|_| ())
                    .map_err(StorageError::custom);
                StoreResponse::Set(res)
            }
            StoreRequest::Get(scope, key) => {
                let value = open_tree(&self.db, &scope)
                    .and_then(|tree| {
                        tree.get(&key).map(|val| {
                            val.and_then(|bytes| {
                                let (val, exp) = decode(&bytes)?;
                                if !exp.expired() {
                                    Some(val.into())
                                } else {
                                    None
                                }
                            })
                        })
                    })
                    .map_err(StorageError::custom);
                StoreResponse::Get(value)
            }
            StoreRequest::Delete(scope, key) => {
                let res = open_tree(&self.db, &scope)
                    .and_then(|tree| tree.remove(&key).map(|_| ()))
                    .map_err(StorageError::custom);
                StoreResponse::Delete(res)
            }
            StoreRequest::Contains(scope, key) => {
                let res = open_tree(&self.db, &scope)
                    .and_then(|tree| tree.contains_key(&key))
                    .map_err(StorageError::custom);
                StoreResponse::Contains(res)
            }
        }
    }
}

impl Handler<ExpiryRequest> for SledActor {
    type Result = ExpiryResponse;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(scope, key, expires_in) => {
                let mut nonce = 0;
                let mut total_duration = None;
                let val = open_tree(&self.db, &scope).and_then(|tree| {
                    tree.update_and_fetch(&key, |existing| {
                        let mut bytes = sled::IVec::from(existing?);

                        // If we can't decode the bytes, leave them as they are
                        if let Some((_, exp)) = decode_mut(&mut bytes) {
                            exp.increase_nonce();
                            exp.expire_in(expires_in);
                            exp.persist.set(0);

                            // Sending values to outer scope
                            nonce = exp.nonce.get();
                            total_duration = exp.expires_in();
                        }
                        Some(bytes)
                    })
                });
                // We can't add item to queue in update_and_fetch as it may run multiple times
                // before taking into effect.
                match val {
                    Err(err) => ExpiryResponse::Set(Err(StorageError::custom(err))),
                    Ok(_) => {
                        if let Some(total_duration) = total_duration {
                            self.queue.push(Delay::for_duration(
                                DelayedIem::new(scope, key, nonce),
                                total_duration,
                            ));
                        }
                        ExpiryResponse::Set(Ok(()))
                    }
                }
            }
            ExpiryRequest::Persist(scope, key) => {
                let val = open_tree(&self.db, &scope).and_then(|tree| {
                    tree.update_and_fetch(&key, |existing| {
                        let mut bytes = sled::IVec::from(existing?);
                        if let Some((_, exp)) = decode_mut(&mut bytes) {
                            exp.persist.set(1);
                        }
                        Some(bytes)
                    })
                });
                if let Err(err) = val {
                    ExpiryResponse::Persist(Err(StorageError::custom(err)))
                } else {
                    ExpiryResponse::Persist(Ok(()))
                }
            }
            ExpiryRequest::Get(scope, key) => {
                let item = open_tree(&self.db, &scope)
                    .and_then(|tree| {
                        tree.get(&key).map(|val| {
                            val.and_then(|bytes| {
                                let (_, exp) = decode(&bytes)?;
                                exp.expires_in()
                            })
                        })
                    })
                    .map_err(StorageError::custom);
                ExpiryResponse::Get(item)
            }
            ExpiryRequest::Extend(scope, key, duration) => {
                let mut nonce = 0;
                let mut total_duration = None;
                let val = open_tree(&self.db, &scope).and_then(|tree| {
                    tree.update_and_fetch(&key, |existing| {
                        let mut bytes = sled::IVec::from(existing?);

                        // If we can't decode the bytes, leave them as they are
                        if let Some((_, exp)) = decode_mut(&mut bytes) {
                            exp.increase_nonce();
                            if let Some(expiry) = exp.expires_in() {
                                exp.expire_in(expiry + duration);
                            } else {
                                exp.expire_in(duration);
                            }
                            exp.persist.set(0);

                            // Sending values to outer scope to prevent decoding again
                            nonce = exp.nonce.get();
                            total_duration = exp.expires_in();
                        }
                        Some(bytes)
                    })
                });
                match val {
                    Err(err) => ExpiryResponse::Extend(Err(StorageError::custom(err))),
                    Ok(_) => {
                        if let Some(total_duration) = total_duration {
                            self.queue.push(Delay::for_duration(
                                DelayedIem::new(scope, key, nonce),
                                total_duration,
                            ));
                        }
                        ExpiryResponse::Extend(Ok(()))
                    }
                }
            }
        }
    }
}

impl Handler<ExpiryStoreRequest> for SledActor {
    type Result = ExpiryStoreResponse;

    fn handle(&mut self, msg: ExpiryStoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryStoreRequest::SetExpiring(scope, key, value, exp) => {
                let res = open_tree(&self.db, &scope)
                    .and_then(|tree| {
                        tree.update_and_fetch(key.as_ref(), |bytes| {
                            let nonce = if let Some(bytes) = bytes {
                                decode(&bytes)
                                    .map(|(_, exp)| exp.next_nonce())
                                    .unwrap_or_default()
                            } else {
                                0
                            };

                            let exp = ExpiryFlags::new_expiring(nonce, exp);
                            let val = encode(&value, exp);

                            Some(val)
                        })
                    })
                    .map(|_| ())
                    .map_err(StorageError::custom);
                ExpiryStoreResponse::SetExpiring(res)
            }
            ExpiryStoreRequest::GetExpiring(scope, key) => {
                let value = open_tree(&self.db, &scope)
                    .and_then(|tree| tree.get(&key))
                    .map_err(StorageError::custom)
                    .map(|val| {
                        val.and_then(|bytes| {
                            let (val, exp) = decode(&bytes)?;
                            if !exp.expired() {
                                Some((val.into(), exp.expires_in()))
                            } else {
                                None
                            }
                        })
                    });
                ExpiryStoreResponse::GetExpiring(value)
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

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    async fn open_database() -> sled::Db {
        let mut tries = 0;
        loop {
            tries += 1;
            if tries > 5 {
                break;
            };
            let db = SledConfig::default().temporary(true).open();
            if let Ok(db) = db {
                return db;
            } else {
                // Wait for sometime and try again.
                actix::clock::delay_for(Duration::from_millis(500)).await;
            }
        }
        panic!("Sled can not open the database files")
    }

    #[test]
    fn test_sled_store() {
        test_store(Box::pin(async {
            SledActor::from_db(open_database().await).start(1)
        }));
    }

    #[test]
    fn test_sled_expiry() {
        test_expiry(
            Box::pin(async {
                let store = SledActor::from_db(open_database().await).start(1);
                (store.clone(), store)
            }),
            4,
        );
    }

    #[test]
    fn test_sled_expiry_store() {
        test_expiry_store(
            Box::pin(async { SledActor::from_db(open_database().await).start(1) }),
            4,
        );
    }

    #[test]
    fn test_sled_formats() {
        test_all_formats(Box::pin(async {
            SledActor::from_db(open_database().await).start(1)
        }));
    }

    #[actix_rt::test]
    async fn test_sled_perform_deletion() {
        let scope: Arc<[u8]> = "prefix".as_bytes().into();
        let key: Arc<[u8]> = "key".as_bytes().into();
        let value = "val".as_bytes().into();
        let db = open_database().await;
        let dur = Duration::from_secs(1);
        let store = SledActor::from_db(db.clone())
            .perform_deletion(true)
            .start(1);
        store
            .send(StoreRequest::Set(scope.clone(), key.clone(), value))
            .await
            .unwrap();
        store
            .send(ExpiryRequest::Set(scope.clone(), key.clone(), dur))
            .await
            .unwrap();
        assert!(open_tree(&db, &scope)
            .unwrap()
            .contains_key(key.clone())
            .unwrap());
        actix::clock::delay_for(dur * 2).await;
        assert!(!open_tree(&db, &scope).unwrap().contains_key(key).unwrap());
    }

    #[actix_rt::test]
    async fn test_sled_scan_on_start() {
        let db = open_database().await;

        let dur = Duration::from_secs(2);
        let value = encode("value".as_bytes(), ExpiryFlags::new_expiring(1, dur));
        let value2 = encode(
            "value2".as_bytes(),
            ExpiryFlags {
                persist: U16::ZERO,
                nonce: U64::new(1),
                expires_at: U64::new(get_current_timestamp() - 1),
            },
        );

        db.insert("key", value).unwrap();
        db.insert("key2", value2).unwrap();
        let actor = SledActor::from_db(db.clone())
            .scan_db_on_start(true)
            .perform_deletion(true)
            .start(1);

        // Waiting for the actor to start up, there should be a better way
        actix::clock::delay_for(Duration::from_millis(500)).await;
        assert!(db.contains_key("key").unwrap());
        assert!(!db.contains_key("key2").unwrap());
        actix::clock::delay_for(Duration::from_millis(2000)).await;
        assert!(!db.contains_key("key").unwrap());

        // Making sure actor stays alive
        drop(actor)
    }
}
