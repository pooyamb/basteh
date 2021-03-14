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
        Self::connect("redis://127.0.0.1/".parse()?).await
    }
}

#[async_trait::async_trait]
impl Store for RedisBackend {
    async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        self.con
            .clone()
            .set(full_key, value.as_ref())
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        let res: Option<Vec<u8>> = self
            .con
            .clone()
            .get(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(res.map(|val| val.into()))
    }

    async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        self.con
            .clone()
            .del(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        let res: u8 = self
            .con
            .clone()
            .exists(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(res > 0)
    }
}

#[async_trait::async_trait]
impl Expiry for RedisBackend {
    async fn persist(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        self.con
            .clone()
            .persist(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn expiry(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Duration>> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        let res: i32 = self
            .con
            .clone()
            .ttl(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(if res >= 0 {
            Some(Duration::from_secs(res as u64))
        } else {
            None
        })
    }

    async fn expire(&self, scope: Arc<[u8]>, key: Arc<[u8]>, expire_in: Duration) -> Result<()> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        self.con
            .clone()
            .expire(full_key, expire_in.as_secs() as usize)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ExpiryStore for RedisBackend {
    async fn set_expiring(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        expire_in: Duration,
    ) -> Result<()> {
        let full_key = [scope.as_ref(), b":", key.as_ref()].concat();
        self.con
            .clone()
            .set_ex(full_key, value.as_ref(), expire_in.as_secs() as usize)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;
    use std::sync::Once;

    static INIT: Once = Once::new();

    async fn get_connection() -> RedisBackend {
        let con = RedisBackend::connect_default().await;
        match con {
            Ok(con) => {
                INIT.call_once(|| {
                    let mut client = redis::Client::open("redis://localhost").unwrap();
                    let _: () = redis::cmd("FLUSHDB").query(&mut client).unwrap();
                });
                con
            }
            Err(err) => panic!("{:?}", err),
        }
    }

    #[actix_rt::test]
    async fn test_redis_store() {
        let store = get_connection().await;
        test_store(store).await;
    }

    #[test]
    fn test_redis_expiry() {
        test_expiry(
            Box::pin(async {
                let store = get_connection().await;
                (store.clone(), store)
            }),
            5,
        );
    }

    #[actix_rt::test]
    async fn test_redis_expiry_store() {
        let store = get_connection().await;
        test_expiry_store(store, 5).await;
    }

    #[actix_rt::test]
    async fn test_redis_formats() {
        let store = get_connection().await;
        test_all_formats(store).await;
    }
}
