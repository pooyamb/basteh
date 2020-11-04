use std::sync::Arc;
use std::time::Duration;

use actix_storage::{
    dev::{Expiry, ExpiryStore, Store},
    Result, StorageError,
};
use redis::{aio::ConnectionManager, AsyncCommands, RedisResult};

pub use redis::{ConnectionAddr, ConnectionInfo, RedisError};

/// An implementation of [`ExpiryStore`](actix_storage::dev::ExpiryStore) based on redis
/// using redis-rs async runtime
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_redis::{RedisBackend, ConnectionInfo, ConnectionAddr};
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     const THREADS_NUMBER: usize = 4;
///     let store = RedisBackend::connect_default();
///     // OR
///     let connection_info = ConnectionInfo {
///         addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234).into(),
///         db: 0,
///         username: Some("god".to_string()),
///         passwd: Some("bless".to_string()),
///     };
///     let store = RedisBackend::connect(connection_info).await.expect("Redis connection failed");
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
pub struct RedisBackend {
    con: ConnectionManager,
}

impl RedisBackend {
    /// Connect using the provided connection info
    pub async fn connect(connection_info: ConnectionInfo) -> RedisResult<Self> {
        let client = redis::Client::open(connection_info)?;
        let con = client.get_tokio_connection_manager().await?;
        Ok(Self { con })
    }

    /// Connect using the default redis port on local machine
    pub async fn connect_default() -> RedisResult<Self> {
        Self::connect("redis://127.0.0.1/".parse().unwrap()).await
    }

    #[cfg(test)]
    pub async fn connect_test() -> RedisResult<Self> {
        use redis::aio::ConnectionLike;
        let r = Self::connect_default().await?;
        r.con
            .clone()
            .req_packed_command(&redis::cmd("FLUSHDB"))
            .await?;
        Ok(r)
    }
}

#[async_trait::async_trait]
impl Store for RedisBackend {
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
        self.con
            .clone()
            .set(key.as_ref(), value.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn get(&self, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
        let res: Option<Vec<u8>> = self
            .con
            .clone()
            .get(key.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(res.map(|val| val.into()))
    }

    async fn delete(&self, key: Arc<[u8]>) -> Result<()> {
        self.con
            .clone()
            .del(key.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn contains_key(&self, key: Arc<[u8]>) -> Result<bool> {
        let res: u8 = self
            .con
            .clone()
            .exists(key.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(res > 0)
    }
}

#[async_trait::async_trait]
impl Expiry for RedisBackend {
    async fn persist(&self, key: Arc<[u8]>) -> Result<()> {
        self.con
            .clone()
            .persist(key.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn expiry(&self, key: Arc<[u8]>) -> Result<Option<Duration>> {
        let res: i32 = self
            .con
            .clone()
            .ttl(key.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(if res >= 0 {
            Some(Duration::from_secs(res as u64))
        } else {
            None
        })
    }

    async fn expire(&self, key: Arc<[u8]>, expire_in: Duration) -> Result<()> {
        self.con
            .clone()
            .expire(key.as_ref(), expire_in.as_secs() as usize)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExpiryStore for RedisBackend {
    async fn set_expiring(
        &self,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        expire_in: Duration,
    ) -> Result<()> {
        self.con
            .clone()
            .set_ex(key.as_ref(), value.as_ref(), expire_in.as_secs() as usize)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[actix_rt::test]
    async fn test_redis_store() {
        let store = RedisBackend::connect_test().await.unwrap();
        test_store(store).await;
    }

    #[actix_rt::test]
    async fn test_redis_expiry() {
        let store = RedisBackend::connect_test().await.unwrap();
        test_expiry(store.clone(), store, 5).await;
    }

    #[actix_rt::test]
    async fn test_redis_expiry_store() {
        let store = RedisBackend::connect_test().await.unwrap();
        test_expiry_store(store, 5).await;
    }

    #[actix_rt::test]
    async fn test_redis_formats() {
        let store = RedisBackend::connect_test().await.unwrap();
        test_all_formats(store).await;
    }
}
