use std::time::Duration;

use basteh::dev::{OwnedValue, Provider, Value};
use basteh::{BastehError, Result};

use crate::inner::SledInner;
use crate::message::{Message, Request, Response};

/// An implementation of [`ExpiryStore`](basteh::dev::ExpiryStore) using sled with tokio's blocking
/// tasksZ
///
/// It stores expiration data as the value's suffix in sled, using byteorder, so to share data this actor
/// creates with other programs outside of its scope, you need to remove the suffix of it exported as
/// [`ExpiryFlags`](struct.ExpiryFlags.html), or directly use encode/decode methods provided.
///
/// ## Example
/// ```no_run
/// use basteh::Basteh;
/// use basteh_sled::{SledBackend, SledConfig};
///
/// const THREADS_NUMBER: usize = 4;
///
/// # async fn your_main() {
/// let db = SledConfig::default().open().expect("Couldn't open sled database");
/// let provider = SledBackend::from_db(db).start(THREADS_NUMBER);
/// let storage = Basteh::build().provider(provider).finish();
/// # }
/// ```
///
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
            .map_err(BastehError::custom)?;
        rx.await.map_err(BastehError::custom)?
    }
}

#[async_trait::async_trait]
impl Provider for SledBackend {
    async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        match self.msg(Request::Keys(scope.into())).await? {
            Response::Iterator(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> basteh::Result<()> {
        match self
            .msg(Request::Set(scope.into(), key.into(), value.into_owned()))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn get(&self, scope: &str, key: &[u8]) -> basteh::Result<Option<OwnedValue>> {
        match self.msg(Request::Get(scope.into(), key.into())).await? {
            Response::Value(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn mutate(
        &self,
        scope: &str,
        key: &[u8],
        mutations: basteh::dev::Mutation,
    ) -> basteh::Result<i64> {
        match self
            .msg(Request::MutateNumber(scope.into(), key.into(), mutations))
            .await?
        {
            Response::Number(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn remove(&self, scope: &str, key: &[u8]) -> basteh::Result<Option<OwnedValue>> {
        match self.msg(Request::Remove(scope.into(), key.into())).await? {
            Response::Value(r) => Ok(r),
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

    async fn extend(&self, scope: &str, key: &[u8], duration: Duration) -> Result<()> {
        match self
            .msg(Request::Extend(scope.into(), key.into(), duration))
            .await?
        {
            Response::Empty(r) => Ok(r),
            _ => unreachable!(),
        }
    }

    async fn set_expiring(
        &self,
        scope: &str,
        key: &[u8],
        value: Value<'_>,
        expire_in: Duration,
    ) -> basteh::Result<()> {
        match self
            .msg(Request::SetExpiring(
                scope.into(),
                key.into(),
                value.into_owned(),
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
    ) -> basteh::Result<Option<(OwnedValue, Option<Duration>)>> {
        match self
            .msg(Request::GetExpiring(scope.into(), key.into()))
            .await?
        {
            Response::ValueDuration(r) => Ok(r),
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use basteh::dev::{OwnedValue, Value};
    use basteh::test_utils::*;
    use sled::IVec;
    use zerocopy::{AsBytes, U16, U64};

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

    #[tokio::test]
    async fn test_sled_store() {
        test_store(SledBackend::from_db(open_database().await).start(1)).await;
    }

    #[tokio::test]
    async fn test_sled_mutate_numbers() {
        test_mutations(SledBackend::from_db(open_database().await).start(1)).await;
    }

    #[tokio::test]
    async fn test_sled_expiry() {
        test_expiry(SledBackend::from_db(open_database().await).start(1), 4).await;
    }

    #[tokio::test]
    async fn test_sled_expiry_store() {
        test_expiry_store(SledBackend::from_db(open_database().await).start(1), 4).await;
    }

    #[tokio::test]
    async fn test_sled_perform_deletion() {
        let scope: IVec = "prefix".as_bytes().into();
        let key: IVec = "key".as_bytes().into();
        let value = OwnedValue::String(String::from("val"));
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
        let value = encode(
            Value::String("value".into()),
            &ExpiryFlags::new_expiring(1, dur),
        );
        let value2 = encode(
            Value::Bytes(b"value2".as_bytes().into()),
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
