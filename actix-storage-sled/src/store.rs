use std::sync::Arc;
use std::time::Duration;

use actix_storage::dev::{Expiry, ExpiryStore, Store};
use actix_storage::{Result, StorageError};

use crate::inner::SledInner;
use crate::message::{Message, Request, Response};

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
/// use actix_storage_sled::{SledConfig, SledBackend};
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     const THREADS_NUMBER: usize = 4;
///
///     let db = SledConfig::default().open()?;
///     let store = SledBackend::from_db(db).start(THREADS_NUMBER);
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
pub struct SledBackend {
    db: Option<sled::Db>,

    tx: Option<crossbeam_channel::Sender<Message>>,

    perform_deletion: bool,
    scan_db_on_start: bool,
}

impl SledBackend {
    /// If set to true, it will perform real deletion when an item expires instead of soft deleting it,
    /// it requires a seprate thread(in tokio threadpool) for expiration notification.
    #[must_use = "Should be started by calling start method"]
    pub fn perform_deletion(mut self, to: bool) -> Self {
        self.perform_deletion = to;
        self
    }

    /// If set to true, actor will scan the database on start to mark expired items.
    #[must_use = "Should be started by calling start method"]
    pub fn scan_db_on_start(mut self, to: bool) -> Self {
        self.scan_db_on_start = to;
        self
    }

    #[must_use = "Should be started by calling start method"]
    pub fn from_db(db: sled::Db) -> Self {
        Self {
            db: Some(db),
            tx: None,
            perform_deletion: false,
            scan_db_on_start: false,
        }
    }

    pub fn start(mut self, thread_num: usize) -> Self {
        let mut inner = SledInner::from_db(self.db.take().unwrap());
        let (tx, rx) = crossbeam_channel::bounded(4096);

        self.tx = Some(tx);

        if self.scan_db_on_start && self.perform_deletion {
            inner.scan_db();
        }

        if self.perform_deletion {
            inner.spawn_expiry_thread();
        }

        for _ in 0..thread_num {
            let mut inner = inner.clone();
            let rx = rx.clone();
            tokio::task::spawn_blocking(move || {
                inner.listen(rx);
            });
        }

        self
    }

    async fn msg(&self, req: Request) -> Result<Response> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.tx
            .as_ref()
            .map(|tx| tx.clone())
            .unwrap()
            .try_send(Message { req, tx })
            .map_err(StorageError::custom)?;
        rx.await.map_err(StorageError::custom)?
    }
}

#[async_trait::async_trait]
impl Store for SledBackend {
    async fn set(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    ) -> actix_storage::Result<()> {
        match self.msg(Request::Set(scope, key, value)).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn set_number(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        value: i64,
    ) -> actix_storage::Result<()> {
        match self.msg(Request::SetNumber(scope, key, value)).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn get(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
    ) -> actix_storage::Result<Option<Arc<[u8]>>> {
        match self.msg(Request::Get(scope, key)).await? {
            Response::Value(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn get_number(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
    ) -> actix_storage::Result<Option<i64>> {
        match self.msg(Request::GetNumber(scope, key)).await? {
            Response::Number(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn mutate(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        mutations: actix_storage::dev::Mutation,
    ) -> actix_storage::Result<()> {
        match self
            .msg(Request::MutateNumber(scope, key, mutations))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> actix_storage::Result<()> {
        match self.msg(Request::Delete(scope, key)).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> actix_storage::Result<bool> {
        match self.msg(Request::Contains(scope, key)).await? {
            Response::Bool(r) => Ok(r),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Expiry for SledBackend {
    async fn persist(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> actix_storage::Result<()> {
        match self.msg(Request::Persist(scope, key)).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn expire(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        expire_in: Duration,
    ) -> actix_storage::Result<()> {
        match self.msg(Request::Expire(scope, key, expire_in)).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn expiry(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
    ) -> actix_storage::Result<Option<Duration>> {
        match self.msg(Request::Expiry(scope, key)).await? {
            Response::Duration(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn extend(&self, scope: Arc<[u8]>, key: Arc<[u8]>, duration: Duration) -> Result<()> {
        match self.msg(Request::Extend(scope, key, duration)).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl ExpiryStore for SledBackend {
    async fn set_expiring(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        expire_in: Duration,
    ) -> actix_storage::Result<()> {
        match self
            .msg(Request::SetExpiring(scope, key, value, expire_in))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn get_expiring(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
    ) -> actix_storage::Result<Option<(Arc<[u8]>, Option<Duration>)>> {
        match self.msg(Request::GetExpiring(scope, key)).await? {
            Response::ValueDuration(r) => Ok(r),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use actix_storage::test_utils::*;
    use zerocopy::{U16, U64};

    use super::SledBackend;
    use crate::inner::open_tree;
    use crate::message::Request;
    use crate::utils::{encode, get_current_timestamp};
    use crate::{ExpiryFlags, SledConfig};

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
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
        panic!("Sled can not open the database files")
    }

    #[test]
    fn test_sled_store() {
        test_store(Box::pin(async {
            SledBackend::from_db(open_database().await).start(1)
        }));
    }

    #[test]
    fn test_sled_store_numbers() {
        test_store_numbers(Box::pin(async {
            SledBackend::from_db(open_database().await).start(1)
        }));
    }

    #[test]
    fn test_sled_mutate_numbers() {
        test_mutate_numbers(Box::pin(async {
            SledBackend::from_db(open_database().await).start(1)
        }));
    }

    #[test]
    fn test_sled_expiry() {
        test_expiry(
            Box::pin(async {
                let store = SledBackend::from_db(open_database().await).start(1);
                (store.clone(), store)
            }),
            4,
        );
    }

    #[test]
    fn test_sled_expiry_store() {
        test_expiry_store(
            Box::pin(async { SledBackend::from_db(open_database().await).start(1) }),
            4,
        );
    }

    #[tokio::test]
    async fn test_sled_perform_deletion() {
        let scope: Arc<[u8]> = "prefix".as_bytes().into();
        let key: Arc<[u8]> = "key".as_bytes().into();
        let value = "val".as_bytes().into();
        let db = open_database().await;
        let dur = Duration::from_secs(1);
        let store = SledBackend::from_db(db.clone())
            .perform_deletion(true)
            .start(1);
        store
            .msg(Request::Set(scope.clone(), key.clone(), value))
            .await
            .unwrap();
        store
            .msg(Request::Expire(scope.clone(), key.clone(), dur))
            .await
            .unwrap();
        assert!(open_tree(&db, &scope)
            .unwrap()
            .contains_key(key.clone())
            .unwrap());
        tokio::time::sleep(dur * 2).await;
        assert!(!open_tree(&db, &scope).unwrap().contains_key(key).unwrap());
    }

    #[tokio::test]
    async fn test_sled_scan_on_start() {
        let db = open_database().await;

        let dur = Duration::from_secs(2);
        let value = encode("value".as_bytes(), &ExpiryFlags::new_expiring(1, dur));
        let value2 = encode(
            "value2".as_bytes(),
            &ExpiryFlags {
                persist: U16::ZERO,
                nonce: U64::new(1),
                expires_at: U64::new(get_current_timestamp() - 1),
            },
        );

        db.insert("key", value).unwrap();
        db.insert("key2", value2).unwrap();
        let actor = SledBackend::from_db(db.clone())
            .scan_db_on_start(true)
            .perform_deletion(true)
            .start(1);

        // Waiting for the actor to start up, there should be a better way
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(db.contains_key("key").unwrap());
        assert!(!db.contains_key("key2").unwrap());
        tokio::time::sleep(Duration::from_millis(2000)).await;
        assert!(!db.contains_key("key").unwrap());

        // Making sure actor stays alive
        drop(actor)
    }
}