use std::time::Duration;

use crate::{dev::OwnedValue, error::Result, mutation::Mutation, value::Value};

/// It is usefull for when store and expiry are implemented for the same struct,
/// and should be implemented in those cases even if there can't be any optimization,
/// as it will prevent some runtime checks for expiry validity.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>>;

    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn get(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>>;

    /// Mutate and get a value for specified key, it should set the value to 0 if it doesn't exist
    async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<i64>;

    /// Delete the key from storage, if the key doesn't exist, it shouldn't return an error
    async fn remove(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>>;

    /// Check if key exist in storage
    async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool>;

    /// Remove all expiry requests from a key and make it persistent,
    /// the persistenty can be overwriten by calling expire on the key.
    async fn persist(&self, scope: &str, key: &[u8]) -> Result<()>;

    /// Sets an expiry for a key, the key may or may not be removed based on
    /// implementation, but it should be guaranteed that it won't appear in
    /// get based methods or contains checks after the period specified.
    async fn expire(&self, scope: &str, key: &[u8], expire_in: Duration) -> Result<()>;

    /// Gets expiry for a key, returning None means it doesn't have an expiry,
    /// if the provider can't return an expiry, it should return an error instead.
    /// The result of this function can have some error, but it should be documented.
    async fn expiry(&self, scope: &str, key: &[u8]) -> Result<Option<Duration>>;

    /// Extend expiry for a key for another duration of time.
    /// If the key doesn't have an expiry, it should be equivalent of calling expire.
    async fn extend(&self, scope: &str, key: &[u8], expire_in: Duration) -> Result<()> {
        let expiry = self.expiry(scope.clone(), key.clone()).await?;
        self.expire(scope, key, expiry.unwrap_or_default() + expire_in)
            .await
    }

    /// Set a key-value for a duration of time, if the key already exists, it should overwrite
    /// both the value and the expiry for that key.
    async fn set_expiring(
        &self,
        scope: &str,
        key: &[u8],
        value: Value<'_>,
        expire_in: Duration,
    ) -> Result<()> {
        self.set(scope.clone(), key.clone(), value).await?;
        self.expire(scope, key, expire_in).await
    }

    /// Get the value and expiry for a key, it is possible to return None if the key doesn't exist,
    /// or return None for the expiry if the key is persistent.
    async fn get_expiring(
        &self,
        scope: &str,
        key: &[u8],
    ) -> Result<Option<(OwnedValue, Option<Duration>)>> {
        let val = self.get(scope.clone(), key.clone()).await?;
        match val {
            Some(val) => {
                let expiry = self.expiry(scope, key).await?;
                Ok(Some((val, expiry)))
            }
            None => Ok(None),
        }
    }
}
