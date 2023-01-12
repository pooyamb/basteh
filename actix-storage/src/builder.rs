use std::sync::Arc;

use crate::{
    dev::{Expiry, ExpiryStore, Store},
    Storage,
};

pub const GLOBAL_SCOPE: [u8; 20] = *b"STORAGE_GLOBAL_SCOPE";

/// Builder struct for [`Storage`](../struct.Storage.html)
///
/// A provider can either implement [`ExpiryStore`](trait.ExpiryStore.html) directly,
/// or implement [`Store`](trait.Store.html) and rely on another provider to provide
/// expiration capablities. The builder will polyfill a [`ExpiryStore`](trait.ExpiryStore.html)
/// by combining an [`Expiry`](trait.Expiry.html) and a [`Store`](trait.Store.html) itself.
///
/// If there is no [`Expiry`](trait.Expiry.html) set in either of the ways, it will result in runtime
/// errors when calling methods which require that functionality.
#[derive(Default)]
pub struct StorageBuilder {
    store: Option<Arc<dyn Store>>,
    expiry: Option<Arc<dyn Expiry>>,
    expiry_store: Option<Arc<dyn ExpiryStore>>,
}

impl StorageBuilder {
    #[must_use = "Builder must be used by calling finish"]
    /// This method can be used to set a [`Store`](trait.Store.html), the second call to this
    /// method will overwrite the store.
    pub fn store(mut self, store: impl Store + 'static) -> Self {
        self.store = Some(Arc::new(store));
        self
    }

    #[must_use = "Builder must be used by calling finish"]
    /// This method can be used to set a [`Expiry`](trait.Expiry.html), the second call to this
    /// method will overwrite the expiry.
    ///
    /// The expiry should work on the same storage as the provided store.
    pub fn expiry(mut self, expiry: impl Expiry + 'static) -> Self {
        self.expiry = Some(Arc::new(expiry));
        self
    }

    #[must_use = "Builder must be used by calling finish"]
    /// This method can be used to set an [`ExpiryStore`](trait.ExpiryStore.html) directly,
    /// Its error to call [`expiry`](#method.expiry) or [`store`](#method.store) after calling this method.
    pub fn expiry_store<T>(mut self, expiry_store: T) -> Self
    where
        T: 'static + Store + Expiry + ExpiryStore,
    {
        self.expiry_store = Some(Arc::new(expiry_store));
        self
    }

    /// This method should be used after configuring the storage.
    ///
    /// ## Panics
    /// If there is no store provided either by calling [`expiry_store`](#method.expiry_store)
    /// or [`store`](#method.store) it will panic.
    pub fn finish(self) -> Storage {
        let expiry_store = if let Some(expiry_store) = self.expiry_store {
            expiry_store
        } else if let Some(store) = self.store {
            Arc::new(self::private::ExpiryStoreGlue(store, self.expiry))
        } else {
            // It is a configuration error, so we just panic.
            panic!("Storage builder needs at least a store");
        };

        Storage {
            scope: Arc::new(GLOBAL_SCOPE),
            store: expiry_store,
        }
    }
}

mod private {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::{
        error::{Result, StorageError},
        provider::{Expiry, ExpiryStore, Store},
    };

    pub(crate) struct ExpiryStoreGlue(pub Arc<dyn Store>, pub Option<Arc<dyn Expiry>>);

    #[async_trait::async_trait]
    impl Expiry for ExpiryStoreGlue {
        async fn expire(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
            expire_in: Duration,
        ) -> Result<()> {
            if let Some(expiry) = self.1.clone() {
                expiry.expire(scope, key, expire_in).await
            } else {
                Err(StorageError::MethodNotSupported)
            }
        }

        async fn expiry(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Duration>> {
            if let Some(ref expiry) = self.1 {
                expiry.expiry(scope, key).await
            } else {
                Err(StorageError::MethodNotSupported)
            }
        }

        async fn extend(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
            expire_in: Duration,
        ) -> Result<()> {
            if let Some(ref expiry) = self.1 {
                expiry.extend(scope, key, expire_in).await
            } else {
                Err(StorageError::MethodNotSupported)
            }
        }

        async fn persist(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
            if let Some(ref expiry) = self.1 {
                expiry.persist(scope, key).await
            } else {
                Err(StorageError::MethodNotSupported)
            }
        }
    }

    #[async_trait::async_trait]
    impl Store for ExpiryStoreGlue {
        async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
            self.0.set(scope, key.clone(), value).await?;
            if let Some(ref expiry) = self.1 {
                expiry.set_called(key).await;
            };
            Ok(())
        }

        async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
            self.0.get(scope, key).await
        }

        async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
            self.0.delete(scope, key).await
        }

        async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
            self.0.contains_key(scope, key).await
        }
    }

    #[async_trait::async_trait]
    impl ExpiryStore for ExpiryStoreGlue {
        async fn set_expiring(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
            value: Arc<[u8]>,
            expire_in: Duration,
        ) -> Result<()> {
            if let Some(expiry) = self.1.clone() {
                self.0.set(scope.clone(), key.clone(), value).await?;
                expiry.expire(scope, key, expire_in).await
            } else {
                Err(StorageError::MethodNotSupported)
            }
        }

        async fn get_expiring(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
        ) -> Result<Option<(Arc<[u8]>, Option<Duration>)>> {
            if let Some(expiry) = self.1.clone() {
                let val = self.0.get(scope.clone(), key.clone()).await?;
                if let Some(val) = val {
                    let expiry = expiry.expiry(scope, key).await?;
                    Ok(Some((val, expiry)))
                } else {
                    Ok(None)
                }
            } else {
                Err(StorageError::MethodNotSupported)
            }
        }
    }
}
