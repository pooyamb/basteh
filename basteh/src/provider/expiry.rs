use std::time::Duration;

use crate::Result;

/// Set of methods for expiry providers to implement.
///
/// The expiry itself should guarantee that it's working on the correct storage
/// and provide a hint that it's persistant or temprorary.
#[async_trait::async_trait]
pub trait Expiry: Send + Sync {
    /// Remove all expiry requests from a key and make it persistent,
    /// the persistenty can be overwriten by calling expire on the key.
    async fn persist(&self, scope: &[u8], key: &[u8]) -> Result<()>;

    /// Sets an expiry for a key, the key may or may not be removed based on
    /// implementation, but it should be guaranteed that it won't appear in
    /// get based methods or contains checks after the period specified.
    async fn expire(&self, scope: &[u8], key: &[u8], expire_in: Duration) -> Result<()>;

    /// Gets expiry for a key, returning None means it doesn't have an expiry,
    /// if the provider can't return an expiry, it should return an error instead.
    /// The result of this function can have some error, but it should be documented.
    async fn expiry(&self, scope: &[u8], key: &[u8]) -> Result<Option<Duration>>;

    /// Extend expiry for a key for another duration of time.
    /// If the key doesn't have an expiry, it should be equivalent of calling expire.
    async fn extend(&self, scope: &[u8], key: &[u8], expire_in: Duration) -> Result<()> {
        let expiry = self.expiry(scope.clone(), key.clone()).await?;
        self.expire(scope, key, expiry.unwrap_or_default() + expire_in)
            .await
    }

    #[allow(unused_variables)]
    /// A notification that should be implemented if expiry is a different entity that
    /// store itself to remove expiry when set is called for a key.
    async fn set_called(&self, key: &[u8]) {}
}
