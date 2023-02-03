use std::time::Duration;

use basteh::{
    dev::{Expiry, ExpiryStore, Store},
    StorageError,
};
use inner::RedbInner;
use message::{Message, Request, Response};

mod delayqueue;
mod flags;
mod inner;
mod message;

/// Reexport of redb Database, to make sure we're using the same version
pub use redb::Database;

/// An implementation of [`ExpiryStore`](basteh::dev::ExpiryStore) using sled with tokio's blocking
/// tasksZ
///
/// It stores expiration data as the value's suffix in sled, using byteorder, so to share data this actor
/// creates with other programs outside of its scope, you need to remove the suffix of it exported as
/// [`ExpiryFlags`](struct.ExpiryFlags.html), or directly use encode/decode methods provided.
///
/// ## Example
/// ```no_run
/// use basteh::Storage;
/// use basteh_redb::{RedbBackend, Database};
///
/// const THREADS_NUMBER: usize = 4;
///
/// # async fn your_main() {
/// let db = Database::open("/tmp/test.db").expect("Couldn't open sled database");
/// let store = RedbBackend::from_db(db).start(THREADS_NUMBER);
/// let storage = Storage::build().store(store).finish();
/// # }
/// ```
///
#[derive(Clone)]
pub struct RedbBackend<T = ()> {
    inner: T,

    perform_deletion: bool,
    scan_db_on_start: bool,
}

impl RedbBackend<()> {
    #[must_use = "Should be started by calling start method"]
    pub fn from_db(db: redb::Database) -> RedbBackend<redb::Database> {
        RedbBackend {
            inner: db,
            perform_deletion: false,
            scan_db_on_start: false,
        }
    }
}

impl<T> RedbBackend<T> {
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
}

impl RedbBackend<redb::Database> {
    pub fn start(self, thread_num: usize) -> RedbBackend<crossbeam_channel::Sender<Message>> {
        let mut inner = RedbInner::from_db(self.inner);
        let (tx, rx) = crossbeam_channel::bounded(4096);

        if self.scan_db_on_start && self.perform_deletion {
            inner.scan_db().ok();
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

        RedbBackend {
            inner: tx,
            perform_deletion: false,
            scan_db_on_start: false,
        }
    }
}

impl RedbBackend<crossbeam_channel::Sender<Message>> {
    async fn msg(&self, req: Request) -> basteh::Result<Response> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.inner
            .try_send(Message { req, tx })
            .map_err(StorageError::custom)?;
        rx.await.map_err(StorageError::custom)?
    }
}

#[async_trait::async_trait]
impl Store for RedbBackend<crossbeam_channel::Sender<Message>> {
    async fn keys(&self, scope: &str) -> basteh::Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        match self.msg(Request::Keys(scope.into())).await? {
            Response::Iterator(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn set(&self, scope: &str, key: &[u8], value: &[u8]) -> basteh::Result<()> {
        match self
            .msg(Request::Set(scope.into(), key.into(), value.into()))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn set_number(&self, scope: &str, key: &[u8], value: i64) -> basteh::Result<()> {
        match self
            .msg(Request::SetNumber(scope.into(), key.into(), value.into()))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn get(&self, scope: &str, key: &[u8]) -> basteh::Result<Option<Vec<u8>>> {
        match self.msg(Request::Get(scope.into(), key.into())).await? {
            Response::Value(r) => Ok(r.map(|v| v.to_vec())),
            _ => unreachable!(),
        }
    }

    async fn get_number(&self, scope: &str, key: &[u8]) -> basteh::Result<Option<i64>> {
        match self
            .msg(Request::GetNumber(scope.into(), key.into()))
            .await?
        {
            Response::Number(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn mutate(
        &self,
        scope: &str,
        key: &[u8],
        mutations: basteh::dev::Mutation,
    ) -> basteh::Result<()> {
        match self
            .msg(Request::MutateNumber(scope.into(), key.into(), mutations))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> basteh::Result<()> {
        match self.msg(Request::Delete(scope.into(), key.into())).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn contains_key(&self, scope: &str, key: &[u8]) -> basteh::Result<bool> {
        match self
            .msg(Request::Contains(scope.into(), key.into()))
            .await?
        {
            Response::Bool(r) => Ok(r),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Expiry for RedbBackend<crossbeam_channel::Sender<Message>> {
    async fn persist(&self, scope: &str, key: &[u8]) -> basteh::Result<()> {
        match self.msg(Request::Persist(scope.into(), key.into())).await? {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn expire(&self, scope: &str, key: &[u8], expire_in: Duration) -> basteh::Result<()> {
        match self
            .msg(Request::Expire(scope.into(), key.into(), expire_in))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn expiry(&self, scope: &str, key: &[u8]) -> basteh::Result<Option<Duration>> {
        match self.msg(Request::Expiry(scope.into(), key.into())).await? {
            Response::Duration(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn extend(&self, scope: &str, key: &[u8], duration: Duration) -> basteh::Result<()> {
        match self
            .msg(Request::Extend(scope.into(), key.into(), duration))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl ExpiryStore for RedbBackend<crossbeam_channel::Sender<Message>> {
    async fn set_expiring(
        &self,
        scope: &str,
        key: &[u8],
        value: &[u8],
        expire_in: Duration,
    ) -> basteh::Result<()> {
        match self
            .msg(Request::SetExpiring(
                scope.into(),
                key.into(),
                value.into(),
                expire_in,
            ))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn get_expiring(
        &self,
        scope: &str,
        key: &[u8],
    ) -> basteh::Result<Option<(Vec<u8>, Option<Duration>)>> {
        match self
            .msg(Request::GetExpiring(scope.into(), key.into()))
            .await?
        {
            Response::ValueDuration(r) => Ok(r.map(|(v, d)| (v.to_vec(), d))),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use basteh::test_utils::*;

    use crate::RedbBackend;

    type ReDb = RedbBackend<redb::Database>;

    fn open_database(path: &str) -> ReDb {
        let p = Path::new(path);
        if p.exists() {
            std::fs::remove_file(p).ok();
        }

        RedbBackend::from_db(redb::Database::create(path).unwrap())
    }

    #[tokio::test]
    async fn test_redb_store() {
        test_store(open_database("/tmp/redb.store.db").start(1)).await;
    }

    #[tokio::test]
    async fn test_redb_store_numbers() {
        test_store_numbers(open_database("/tmp/redb.store_numbers.db").start(1)).await;
    }

    #[tokio::test]
    async fn test_redb_mutate_numbers() {
        test_mutate_numbers(open_database("/tmp/redb.mutate.db").start(1)).await;
    }

    #[tokio::test]
    async fn test_redb_expiry() {
        let store = open_database("/tmp/redb.expiry.db").start(1);
        test_expiry(store.clone(), store, 2).await;
    }

    #[tokio::test]
    async fn test_redb_expiry_store() {
        test_expiry_store(open_database("/tmp/redb.exp_store.db").start(1), 2).await;
    }

    // #[tokio::test]
    // async fn test_redb_scan_on_start() {
    //     let db = open_database().await;

    //     let dur = Duration::from_secs(2);
    //     let value = encode("value".as_bytes(), &ExpiryFlags::new_expiring(1, dur));
    //     let value2 = encode(
    //         "value2".as_bytes(),
    //         &ExpiryFlags {
    //             persist: U16::ZERO,
    //             nonce: U64::new(1),
    //             expires_at: U64::new(get_current_timestamp() - 1),
    //         },
    //     );

    //     db.insert("key", value).unwrap();
    //     db.insert("key2", value2).unwrap();
    //     let store = SledBackend::from_db(db.clone())
    //         .scan_db_on_start(true)
    //         .perform_deletion(true)
    //         .start(1);

    //     // Waiting for the actor to start up, there should be a better way
    //     tokio::time::sleep(Duration::from_millis(500)).await;
    //     assert!(db.contains_key("key").unwrap());
    //     assert!(!db.contains_key("key2").unwrap());
    //     tokio::time::sleep(Duration::from_millis(2000)).await;
    //     assert!(!db.contains_key("key").unwrap());

    //     // Making sure store stays alive
    //     drop(store)
    // }
}
