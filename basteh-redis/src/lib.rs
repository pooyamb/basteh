#![doc = include_str!("../README.md")]

use std::time::Duration;

use basteh::{
    dev::{Action, Expiry, ExpiryStore, Mutation, Store},
    Result, StorageError,
};
use redis::{aio::ConnectionManager, AsyncCommands, RedisResult};

pub use redis::{ConnectionAddr, ConnectionInfo, RedisConnectionInfo, RedisError};
use utils::run_mutations;

mod utils;

#[inline]
fn get_full_key(scope: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Vec<u8> {
    [scope.as_ref(), b":", key.as_ref()].concat()
}

/// An implementation of [`ExpiryStore`](basteh::dev::ExpiryStore) based on redis
/// using redis-rs async runtime
///
/// ## Example
/// ```no_run
/// use basteh::Storage;
/// use basteh_redis::{RedisBackend, ConnectionInfo, RedisConnectionInfo, ConnectionAddr};
///
/// # async fn your_main() {
/// let store = RedisBackend::connect_default();
/// // OR
/// let connection_info = ConnectionInfo {
///     addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234).into(),
///     redis: RedisConnectionInfo{
///         db: 0,
///         username: Some("god".to_string()),
///         password: Some("bless".to_string()),
///     }
/// };
/// let store = RedisBackend::connect(connection_info).await.expect("Redis connection failed");
/// let storage = Storage::build().store(store).finish();
/// # }
/// ```
///
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
    async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        let keys = self
            .con
            .clone()
            .keys::<_, Vec<Vec<u8>>>([scope, ":*"].concat())
            .await
            .map_err(StorageError::custom)?
            .into_iter()
            .map(move |k| {
                let ignored = scope.len() + 1;
                k[ignored..].to_vec()
            })
            .collect::<Vec<_>>();
        Ok(Box::new(keys.into_iter()))
    }

    async fn set(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .set(full_key, value)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn set_number(&self, scope: &str, key: &[u8], value: i64) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .set(full_key, value)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn get(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .get(full_key)
            .await
            .map_err(StorageError::custom)
    }

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn get_number(&self, scope: &str, key: &[u8]) -> Result<Option<i64>> {
        self.get(scope, key)
            .await?
            .map(|val| {
                String::from_utf8_lossy(&val)
                    .parse()
                    .map_err(|_| StorageError::InvalidNumber)
            })
            .transpose()
    }

    async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<()> {
        if mutations.len() == 0 {
            return Ok(());
        }

        let full_key = get_full_key(scope, key);
        if mutations.len() == 1 {
            match mutations.into_iter().next().unwrap() {
                Action::Incr(delta) => self
                    .con
                    .clone()
                    .incr(full_key, delta)
                    .await
                    .map_err(StorageError::custom)?,
                Action::Decr(delta) => self
                    .con
                    .clone()
                    .decr(full_key, delta)
                    .await
                    .map_err(StorageError::custom)?,
                Action::Set(value) => self
                    .con
                    .clone()
                    .set(full_key, value)
                    .await
                    .map_err(StorageError::custom)?,
                action => run_mutations(self.con.clone(), full_key, [action])
                    .await
                    .map_err(|e| StorageError::Custom(Box::new(e)))?,
            }
        } else {
            run_mutations(self.con.clone(), full_key, mutations.into_iter())
                .await
                .map_err(|e| StorageError::Custom(Box::new(e)))?
        }
        Ok(())
    }

    async fn delete(&self, scope: &str, key: &[u8]) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .del(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool> {
        let full_key = get_full_key(scope, key);
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
    async fn persist(&self, scope: &str, key: &[u8]) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .persist(full_key)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }

    async fn expiry(&self, scope: &str, key: &[u8]) -> Result<Option<Duration>> {
        let full_key = get_full_key(scope, key);
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

    async fn expire(&self, scope: &str, key: &[u8], expire_in: Duration) -> Result<()> {
        let full_key = get_full_key(scope, key);
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
        scope: &str,
        key: &[u8],
        value: &[u8],
        expire_in: Duration,
    ) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .set_ex(full_key, value, expire_in.as_secs() as usize)
            .await
            .map_err(StorageError::custom)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use basteh::test_utils::*;
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

    #[tokio::test]
    async fn test_redis_store() {
        test_store(get_connection().await).await;
    }

    #[tokio::test]
    async fn test_redis_store_numbers() {
        test_store_numbers(get_connection().await).await;
    }

    #[tokio::test]
    async fn test_redis_mutate_numbers() {
        test_mutate_numbers(get_connection().await).await;
    }

    #[tokio::test]
    async fn test_redis_expiry() {
        let store = get_connection().await;
        test_expiry(store.clone(), store, 5).await;
    }

    #[tokio::test]
    async fn test_redis_expiry_store() {
        test_expiry_store(get_connection().await, 5).await;
    }
}
