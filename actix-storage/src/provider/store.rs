use std::sync::Arc;

use crate::error::Result;

/// Set of method for basic storage providers to implement.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// Set a key-value pair, if the key already exist, value should be overwritten
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()>;

    /// Get a value for specified key, it should result in None if the value does not exist
    async fn get(&self, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>>;

    /// Delete the key from storage, if the key doesn't exist, it shouldn't return an error
    async fn delete(&self, key: Arc<[u8]>) -> Result<()>;

    /// Check if key exist in storage
    async fn contains_key(&self, key: Arc<[u8]>) -> Result<bool>;
}
