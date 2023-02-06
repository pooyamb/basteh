use crate::{dev::OwnedValue, error::Result, value::Value};

use super::Mutation;

/// Set of method for basic storage providers to implement.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>>;

    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn get(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<()>;

    /// Delete the key from storage, if the key doesn't exist, it shouldn't return an error
    async fn delete(&self, scope: &str, key: &[u8]) -> Result<()>;

    /// Check if key exist in storage
    async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool>;
}
