use crate::error::Result;

use super::Mutation;

/// Set of method for basic storage providers to implement.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn keys(&self, scope: &[u8]) -> Result<Box<dyn Iterator<Item = Vec<u8>>>>;

    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn set(&self, scope: &[u8], key: &[u8], value: &[u8]) -> Result<()>;

    /// Set a key-value pair with a numeric value, if the key already exist, value should be overwritten.
    async fn set_number(&self, scope: &[u8], key: &[u8], value: i64) -> Result<()>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn get(&self, scope: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Get a numeric value for specified key, it should result in None if the value does not exist
    async fn get_number(&self, scope: &[u8], key: &[u8]) -> Result<Option<i64>>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn mutate(&self, scope: &[u8], key: &[u8], mutations: Mutation) -> Result<()>;

    /// Delete the key from storage, if the key doesn't exist, it shouldn't return an error
    async fn delete(&self, scope: &[u8], key: &[u8]) -> Result<()>;

    /// Check if key exist in storage
    async fn contains_key(&self, scope: &[u8], key: &[u8]) -> Result<bool>;
}
