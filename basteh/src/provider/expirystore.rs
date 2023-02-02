use std::time::Duration;

use super::{Expiry, Store};
use crate::error::Result;

/// It is usefull for when store and expiry are implemented for the same struct,
/// and should be implemented in those cases even if there can't be any optimization,
/// as it will prevent some runtime checks for expiry validity.
#[async_trait::async_trait]
pub trait ExpiryStore: Store + Expiry + Send + Sync {
    /// Set a key-value for a duration of time, if the key already exists, it should overwrite
    /// both the value and the expiry for that key.
    async fn set_expiring(
        &self,
        scope: &str,
        key: &[u8],
        value: &[u8],
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
    ) -> Result<Option<(Vec<u8>, Option<Duration>)>> {
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
