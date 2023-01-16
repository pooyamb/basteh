use std::sync::Arc;

use crate::error::Result;

/// Set of method for basic storage providers to implement.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()>;

    /// Set a key-value pair with a numeric value, if the key already exist, value should be overwritten.
    async fn set_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: i64) -> Result<()>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>>;

    /// Get a numeric value for specified key, it should result in None if the value does not exist
    async fn get_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<i64>>;

    /// Delete the key from storage, if the key doesn't exist, it shouldn't return an error
    async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()>;

    /// Check if key exist in storage
    async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool>;
}
