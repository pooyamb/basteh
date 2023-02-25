#![doc = include_str!("../README.md")]

use std::time::Duration;

use basteh::{
    dev::{Action, Mutation, OwnedValue, Provider, Value},
    BastehError, Result,
};
use redis::{aio::ConnectionManager, AsyncCommands, FromRedisValue, RedisResult, ToRedisArgs};

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
/// use basteh::Basteh;
/// use basteh_redis::{RedisBackend, ConnectionInfo, RedisConnectionInfo, ConnectionAddr};
///
/// # async fn your_main() {
/// let provider = RedisBackend::connect_default();
/// // OR
/// let connection_info = ConnectionInfo {
///     addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234).into(),
///     redis: RedisConnectionInfo{
///         db: 0,
///         username: Some("god".to_string()),
///         password: Some("bless".to_string()),
///     }
/// };
/// let provider = RedisBackend::connect(connection_info).await.expect("Redis connection failed");
/// let basteh = Basteh::build().provider(provider).finish();
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
impl Provider for RedisBackend {
    async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        let keys = self
            .con
            .clone()
            .keys::<_, Vec<Vec<u8>>>([scope, ":*"].concat())
            .await
            .map_err(BastehError::custom)?
            .into_iter()
            .map(move |k| {
                let ignored = scope.len() + 1;
                k[ignored..].to_vec()
            })
            .collect::<Vec<_>>();
        Ok(Box::new(keys.into_iter()))
    }

    async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()> {
        let full_key = get_full_key(scope, key);
        match value {
            Value::List(l) => {
                redis::pipe()
                    .del(&full_key)
                    .rpush(
                        full_key,
                        l.into_iter().map(ValueWrapper).collect::<Vec<_>>(),
                    )
                    .query_async(&mut self.con.clone())
                    .await
                    .map_err(BastehError::custom)?;
            }
            _ => {
                self.con
                    .clone()
                    .set(full_key, ValueWrapper(value))
                    .await
                    .map_err(BastehError::custom)?;
            }
        }
        Ok(())
    }

    async fn get(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .get::<_, OwnedValueWrapper>(full_key)
            .await
            .map(|v| v.0)
            .map_err(BastehError::custom)
    }

    async fn get_range(
        &self,
        scope: &str,
        key: &[u8],
        start: i64,
        end: i64,
    ) -> Result<Vec<OwnedValue>> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .lrange::<_, OwnedValueWrapper>(full_key, start as isize, end as isize)
            .await
            .map(|v| v.0)
            .map_err(BastehError::custom)
            .and_then(|v| match v {
                Some(OwnedValue::List(l)) => Ok(l),
                Some(OwnedValue::Bytes(b)) => Ok(b
                    .into_iter()
                    .map(Into::<Value>::into)
                    .map(|v| v.into_owned())
                    .collect::<Vec<_>>()),
                _ => Err(BastehError::TypeConversion),
            })
    }

    async fn push(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .rpush(full_key, ValueWrapper(value))
            .await
            .map_err(BastehError::custom)?;
        Ok(())
    }

    async fn push_multiple(&self, scope: &str, key: &[u8], value: Vec<Value<'_>>) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .rpush(
                full_key,
                value.into_iter().map(ValueWrapper).collect::<Vec<_>>(),
            )
            .await
            .map_err(BastehError::custom)?;
        Ok(())
    }

    async fn pop(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .rpop::<_, OwnedValueWrapper>(full_key, None)
            .await
            .map(|v| v.0)
            .map_err(BastehError::custom)
    }

    async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<i64> {
        let full_key = get_full_key(scope, key);

        if mutations.len() == 0 {
            let mut con = self.con.clone();

            // Get the value or set to 0 and return
            let res = con
                .get::<_, Option<i64>>(&full_key)
                .await
                .map_err(BastehError::custom)?;

            if let Some(res) = res {
                Ok(res)
            } else {
                con.set(full_key, 0__i64)
                    .await
                    .map_err(BastehError::custom)?;
                Ok(0)
            }
        } else if mutations.len() == 1 {
            match mutations.into_iter().next().unwrap() {
                Action::Incr(delta) => self
                    .con
                    .clone()
                    .incr(full_key, delta)
                    .await
                    .map_err(BastehError::custom),
                Action::Decr(delta) => self
                    .con
                    .clone()
                    .decr(full_key, delta)
                    .await
                    .map_err(BastehError::custom),
                Action::Set(value) => {
                    self.con
                        .clone()
                        .set(full_key, value)
                        .await
                        .map_err(BastehError::custom)?;
                    return Ok(value);
                }
                action => run_mutations(self.con.clone(), full_key, [action])
                    .await
                    .map_err(|e| BastehError::Custom(Box::new(e))),
            }
        } else {
            run_mutations(self.con.clone(), full_key, mutations.into_iter())
                .await
                .map_err(|e| BastehError::Custom(Box::new(e)))
        }
    }

    async fn remove(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
        let full_key = get_full_key(scope, key);
        Ok(redis::pipe()
            .get(&full_key)
            .del(full_key)
            .ignore()
            .query_async::<_, Vec<OwnedValueWrapper>>(&mut self.con.clone())
            .await
            .map_err(BastehError::custom)?
            .into_iter()
            .next()
            .and_then(|v| v.0))
    }

    async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool> {
        let full_key = get_full_key(scope, key);
        let res: u8 = self
            .con
            .clone()
            .exists(full_key)
            .await
            .map_err(BastehError::custom)?;
        Ok(res > 0)
    }

    async fn persist(&self, scope: &str, key: &[u8]) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .persist(full_key)
            .await
            .map_err(BastehError::custom)?;
        Ok(())
    }

    async fn expiry(&self, scope: &str, key: &[u8]) -> Result<Option<Duration>> {
        let full_key = get_full_key(scope, key);
        let res: i32 = self
            .con
            .clone()
            .ttl(full_key)
            .await
            .map_err(BastehError::custom)?;
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
            .map_err(BastehError::custom)?;
        Ok(())
    }

    async fn set_expiring(
        &self,
        scope: &str,
        key: &[u8],
        value: Value<'_>,
        expire_in: Duration,
    ) -> Result<()> {
        let full_key = get_full_key(scope, key);
        self.con
            .clone()
            .set_ex(full_key, ValueWrapper(value), expire_in.as_secs() as usize)
            .await
            .map_err(BastehError::custom)?;
        Ok(())
    }
}

struct ValueWrapper<'a>(Value<'a>);

impl<'a> ToRedisArgs for ValueWrapper<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match &self.0 {
            Value::Number(n) => <i64 as ToRedisArgs>::write_redis_args(&n, out),
            Value::Bytes(b) => <&[u8] as ToRedisArgs>::write_redis_args(&b.as_ref(), out),
            Value::String(s) => <&str as ToRedisArgs>::write_redis_args(&s.as_ref(), out),
            Value::List(l) => {
                for item in l {
                    ValueWrapper(item.clone()).write_redis_args(out);
                }
            }
        }
    }
}
struct OwnedValueWrapper(Option<OwnedValue>);

impl<'a> FromRedisValue for OwnedValueWrapper {
    fn from_redis_value(v: &redis::Value) -> RedisResult<OwnedValueWrapper> {
        Ok(OwnedValueWrapper(match v {
            // If it's Nil then return None
            redis::Value::Nil => None,
            // Otherwise try to decode as Number, String or Bytes in order
            _ => Some(
                <i64 as FromRedisValue>::from_redis_value(v)
                    .map(OwnedValue::Number)
                    .or_else(|_| {
                        <String as FromRedisValue>::from_redis_value(v).map(OwnedValue::String)
                    })
                    .or_else(|_| {
                        <Vec<u8> as FromRedisValue>::from_redis_value(v).map(OwnedValue::Bytes)
                    })
                    .or_else(|_| {
                        <Vec<OwnedValueWrapper> as FromRedisValue>::from_redis_value(v)
                            .map(|v| v.into_iter().filter_map(|v| v.0).collect())
                            .map(OwnedValue::List)
                    })?,
            ),
        }))
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
    async fn test_redis_mutations() {
        test_mutations(get_connection().await).await;
    }

    #[tokio::test]
    async fn test_redis_expiry() {
        test_expiry(get_connection().await, 5).await;
    }

    #[tokio::test]
    async fn test_redis_expiry_store() {
        test_expiry_store(get_connection().await, 5).await;
    }
}
