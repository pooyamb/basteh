use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, SystemTime};

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

/// Represents expiry data and is stored as suffix to the value
#[derive(Debug, Default, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
pub struct ExpiryFlags {
    nonce: U64<LittleEndian>,
    expires_at: U64<LittleEndian>,
    persist: U16<LittleEndian>,
}

impl ExpiryFlags {
    fn new_persist(nonce: u64) -> Self {
        Self {
            nonce: U64::new(nonce),
            expires_at: U64::new(0),
            persist: U16::new(1),
        }
    }

    fn new_expiring(nonce: u64, expires_in: Duration) -> Self {
        let expires_at = get_current_timestamp() + expires_in.as_secs();
        Self {
            nonce: U64::new(nonce),
            expires_at: U64::new(expires_at),
            persist: U16::new(0),
        }
    }

    fn increase_nonce(&mut self) {
        self.nonce = U64::new(self.next_nonce());
    }

    fn next_nonce(&self) -> u64 {
        if self.nonce == U64::MAX_VALUE {
            0
        } else {
            self.nonce.get() + 1
        }
    }

    fn expire_in(&mut self, duration: Duration) {
        self.expires_at
            .set(get_current_timestamp() + duration.as_secs())
    }

    // Returns None if it is persistant
    fn expires_in(&self) -> Option<Duration> {
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

    fn expired(&self) -> bool {
        let expires_at = self.expires_at.get();
        self.persist.get() == 0 && expires_at <= get_current_timestamp()
    }
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
    queue: DelayQueue<Delay<(Arc<[u8]>, u64)>>,
    perform_deletion: bool,
    scan_db_on_start: bool,

    #[doc(hidden)]
    stopped: Arc<AtomicBool>,
}

/// Takes an IVec and returns value bytes with its expiry flags as mutable
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode_mut(bytes: &mut sled::IVec) -> Option<(&mut [u8], &mut ExpiryFlags)> {
    let (val, exp): (&mut [u8], LayoutVerified<&mut [u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_mut())?;
    Some((val, exp.into_mut()))
}

/// Takes an IVec and returns value bytes with its expiry flags
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode(bytes: &sled::IVec) -> Option<(&[u8], &ExpiryFlags)> {
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

impl SledActor {
    #[must_use = "Actor should be started by calling start method"]
    pub fn perform_deletion(mut self, to: bool) -> Self {
        self.perform_deletion = to;
        self
    }

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

    pub fn start(self, threads_num: usize) -> Addr<Self> {
        SyncArbiter::start(threads_num, move || self.clone())
    }

    fn scan_expired_items(&mut self) {
        let mut deleted_keys = vec![];
        for kv in self.db.iter() {
            if let Ok((key, value)) = kv {
                let mut bytes = value.clone();
                if let Some((_, exp)) = decode_mut(&mut bytes) {
                    if exp.expired() {
                        deleted_keys.push(key);
                    } else if let Some(dur) = exp.expires_in() {
                        self.queue.push(Delay::for_duration(
                            (key.to_vec().into(), exp.nonce.get()),
                            dur,
                        ));
                    }
                }
            }
        }
        for key in deleted_keys {
            self.db.remove(&key).unwrap();
        }
    }
}

impl Actor for SledActor {
    type Context = SyncContext<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        let map = self.db.clone();
        let mut queue = self.queue.clone();

        if self.scan_db_on_start && self.perform_deletion {
            self.scan_expired_items();
        }

        let stopped = self.stopped.clone();

        if self.perform_deletion {
            std::thread::spawn(move || loop {
                if let Some(item) = queue.try_pop_for(Duration::from_secs(1)) {
                    let val = map.get(&item.value.0).ok().flatten();
                    if let Some(mut bytes) = val {
                        if let Some((_, exp)) = decode_mut(&mut bytes) {
                            if exp.nonce.get() == item.value.1 && exp.persist.get() == 0 {
                                map.remove(&item.value.0).ok();
                            }
                        }
                    }
                } else if stopped.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            });
        }
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        self.stopped
            .compare_and_swap(false, true, std::sync::atomic::Ordering::Acquire);
    }
}

impl Handler<StoreRequest> for SledActor {
    type Result = StoreResponse;

    fn handle(&mut self, msg: StoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            StoreRequest::Set(key, value) => {
                let res = self
                    .db
                    .remove(key.as_ref())
                    .and_then(|bytes| {
                        let nonce = if let Some(bytes) = bytes {
                            decode(&bytes)
                                .map(|(_, exp)| exp.next_nonce())
                                .unwrap_or_default()
                        } else {
                            0
                        };

                        let exp = ExpiryFlags::new_persist(nonce);
                        let val = encode(value.as_bytes(), exp);

                        self.db.insert(key.as_ref(), val).map(|_| ())
                    })
                    .map_err(StorageError::custom);
                StoreResponse::Set(res)
            }
            StoreRequest::Get(key) => {
                let value = self.db.get(&key).map_err(StorageError::custom).map(|val| {
                    val.and_then(|bytes| {
                        let (val, exp) = decode(&bytes)?;
                        if !exp.expired() {
                            Some(val.into())
                        } else {
                            None
                        }
                    })
                });
                StoreResponse::Get(value)
            }
            StoreRequest::Delete(key) => {
                let res = self
                    .db
                    .remove(&key)
                    .map(|_| ())
                    .map_err(StorageError::custom);
                StoreResponse::Delete(res)
            }
            StoreRequest::Contains(key) => {
                let res = self.db.contains_key(&key).map_err(StorageError::custom);
                StoreResponse::Contains(res)
            }
        }
    }
}

impl Handler<ExpiryRequest> for SledActor {
    type Result = ExpiryResponse;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(key, expires_in) => {
                let mut nonce = 0;
                let mut total_duration = None;
                let val = self.db.update_and_fetch(&key, |existing| {
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
                });
                // We can't add item to queue in update_and_fetch as it may run multiple times
                // before taking into effect.
                match val {
                    Err(err) => ExpiryResponse::Set(Err(StorageError::custom(err))),
                    Ok(_) => {
                        if let Some(total_duration) = total_duration {
                            self.queue
                                .push(Delay::for_duration((key, nonce), total_duration));
                        }
                        ExpiryResponse::Set(Ok(()))
                    }
                }
            }
            ExpiryRequest::Persist(key) => {
                let val = self.db.update_and_fetch(&key, |existing| {
                    let mut bytes = sled::IVec::from(existing?);
                    if let Some((_, exp)) = decode_mut(&mut bytes) {
                        exp.persist.set(1);
                    }
                    Some(bytes)
                });
                if let Err(err) = val {
                    ExpiryResponse::Persist(Err(StorageError::custom(err)))
                } else {
                    ExpiryResponse::Persist(Ok(()))
                }
            }
            ExpiryRequest::Get(key) => {
                let item = self
                    .db
                    .get(&key)
                    .map(|val| {
                        val.and_then(|bytes| {
                            let (_, exp) = decode(&bytes)?;
                            exp.expires_in()
                        })
                    })
                    .map_err(StorageError::custom);
                ExpiryResponse::Get(item)
            }
            ExpiryRequest::Extend(key, duration) => {
                let mut nonce = 0;
                let mut total_duration = None;
                let val = self.db.update_and_fetch(&key, |existing| {
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
                });
                match val {
                    Err(err) => ExpiryResponse::Extend(Err(StorageError::custom(err))),
                    Ok(_) => {
                        if let Some(total_duration) = total_duration {
                            self.queue
                                .push(Delay::for_duration((key, nonce), total_duration));
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
            ExpiryStoreRequest::SetExpiring(key, value, exp) => {
                let res = self
                    .db
                    .remove(key.as_ref())
                    .and_then(|bytes| {
                        let nonce = if let Some(bytes) = bytes {
                            decode(&bytes)
                                .map(|(_, exp)| exp.next_nonce())
                                .unwrap_or_default()
                        } else {
                            0
                        };

                        let exp = ExpiryFlags::new_expiring(nonce, exp);
                        let val = encode(value.as_bytes(), exp);

                        self.db.insert(key.as_ref(), val).map(|_| ())
                    })
                    .map_err(StorageError::custom);
                ExpiryStoreResponse::SetExpiring(res)
            }
            ExpiryStoreRequest::GetExpiring(key) => {
                let value = self.db.get(&key).map_err(StorageError::custom).map(|val| {
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
            let db = SledConfig::default().path("../target/sled-actor").open();
            if let Ok(db) = db {
                return db;
            } else {
                // Wait for sometime and try again.
                actix::clock::delay_for(Duration::from_millis(500)).await;
            }
        }
        panic!("Sled can not open the database files")
    }

    #[actix_rt::test]
    async fn test_sled_store() {
        let store = SledActor::from_db(open_database().await).start(1);
        test_store(store.clone()).await;
    }

    #[actix_rt::test]
    async fn test_sled_expiry() {
        let store = SledActor::from_db(open_database().await).start(1);
        test_expiry(store.clone(), store.clone()).await;
    }

    #[actix_rt::test]
    async fn test_sled_expiry_store() {
        let store = SledActor::from_db(open_database().await).start(1);
        test_expiry_store(store).await;
    }

    #[actix_rt::test]
    async fn test_sled_formats() {
        let store = SledActor::from_db(open_database().await).start(1);
        test_all_formats(store).await;
    }

    #[actix_rt::test]
    async fn test_sled_perform_deletion() {
        let key: Arc<[u8]> = "key".as_bytes().into();
        let value = "val".as_bytes().into();
        let db = open_database().await;
        let dur = Duration::from_secs(1);
        let store = SledActor::from_db(db.clone())
            .perform_deletion(true)
            .start(1);
        store
            .send(StoreRequest::Set(key.clone(), value))
            .await
            .unwrap();
        store
            .send(ExpiryRequest::Set(key.clone(), dur))
            .await
            .unwrap();
        assert!(db.contains_key(key.clone()).unwrap());
        actix::clock::delay_for(dur).await;
        assert!(!db.contains_key(key).unwrap());
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
