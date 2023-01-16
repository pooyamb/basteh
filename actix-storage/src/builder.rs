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
pub struct StorageBuilder<S = ()> {
    store: Option<S>,
}

impl StorageBuilder {
    #[must_use = "Builder must be used by calling finish"]
    /// This method can be used to set a [`Store`](trait.Store.html), the second call to this
    /// method will overwrite the store.
    pub fn store<S>(self, store: S) -> StorageBuilder<S>
    where
        S: Store + 'static,
    {
        StorageBuilder { store: Some(store) }
    }
}

impl<S: Store> StorageBuilder<S> {
    #[must_use = "Builder must be used by calling finish"]
    /// This method can be used to set a [`Expiry`](trait.Expiry.html), if the store
    /// already supports expiration methods, this will overwrite that behaviour
    pub fn expiry<E: Expiry>(self, e: E) -> StorageBuilder<impl ExpiryStore> {
        StorageBuilder {
            store: Some(self::private::ExpiryStoreGlue(self.store.unwrap(), e)),
        }
    }

    #[must_use = "Builder must be used by calling finish"]
    /// This method should be called when there is no expiration method support in the store
    /// and there won't be any seperate provider for it.
    /// Calling this method means acknowleding all the expirations methods will fail.(with an error)
    pub fn no_expiry(self) -> StorageBuilder<impl ExpiryStore> {
        StorageBuilder {
            store: Some(self::private::ExpiryStoreGlue(self.store.unwrap(), ())),
        }
    }
}

impl<S: ExpiryStore + 'static> StorageBuilder<S> {
    /// Build the Storage
    pub fn finish(self) -> Storage {
        Storage {
            scope: Arc::new(GLOBAL_SCOPE),
            store: Arc::new(self.store.unwrap()),
        }
    }
}

mod private {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::{
        error::Result,
        provider::{Expiry, ExpiryStore, Store},
        StorageError,
    };

    pub(crate) struct ExpiryStoreGlue<S, E = ()>(pub(super) S, pub(super) E);

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S, E> Expiry for ExpiryStoreGlue<S, E>
    where
        S: Send + Sync,
        E: Send + Sync + Expiry,
    {
        async fn expire(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
            expire_in: Duration,
        ) -> Result<()> {
            self.1.expire(scope, key, expire_in).await
        }

        async fn expiry(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Duration>> {
            self.1.expiry(scope, key).await
        }

        async fn extend(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
            expire_in: Duration,
        ) -> Result<()> {
            self.1.extend(scope, key, expire_in).await
        }

        async fn persist(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
            self.1.persist(scope, key).await
        }
    }

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S, E> Store for ExpiryStoreGlue<S, E>
    where
        S: Send + Sync + Store,
        E: Send + Sync + Expiry,
    {
        async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
            self.0.set(scope, key.clone(), value).await?;
            self.1.set_called(key).await;
            Ok(())
        }

        async fn set_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: i64) -> Result<()> {
            self.0.set_number(scope, key.clone(), value).await?;
            self.1.set_called(key).await;
            Ok(())
        }

        async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
            self.0.get(scope, key).await
        }

        async fn get_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<i64>> {
            self.0.get_number(scope, key).await
        }

        async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
            self.0.delete(scope, key).await
        }

        async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
            self.0.contains_key(scope, key).await
        }
    }

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S, E> ExpiryStore for ExpiryStoreGlue<S, E>
    where
        S: Send + Sync + Store,
        E: Send + Sync + Expiry,
    {
        async fn set_expiring(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
            value: Arc<[u8]>,
            expire_in: Duration,
        ) -> Result<()> {
            self.0.set(scope.clone(), key.clone(), value).await?;
            self.1.expire(scope, key, expire_in).await
        }

        async fn get_expiring(
            &self,
            scope: Arc<[u8]>,
            key: Arc<[u8]>,
        ) -> Result<Option<(Arc<[u8]>, Option<Duration>)>> {
            let val = self.0.get(scope.clone(), key.clone()).await?;
            if let Some(val) = val {
                let expiry = self.1.expiry(scope, key).await?;
                Ok(Some((val, expiry)))
            } else {
                Ok(None)
            }
        }
    }

    /// For simple stores
    #[async_trait::async_trait]
    impl<S> Expiry for ExpiryStoreGlue<S>
    where
        S: Send + Sync,
    {
        async fn expire(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Duration) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }

        async fn expiry(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<Option<Duration>> {
            Err(StorageError::MethodNotSupported)
        }

        async fn extend(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Duration) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }

        async fn persist(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }
    }

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S> Store for ExpiryStoreGlue<S>
    where
        S: Send + Sync + Store,
    {
        async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
            self.0.set(scope, key.clone(), value).await
        }

        async fn set_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: i64) -> Result<()> {
            self.0.set_number(scope, key.clone(), value).await
        }

        async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
            self.0.get(scope, key).await
        }

        async fn get_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<i64>> {
            self.0.get_number(scope, key).await
        }

        async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
            self.0.delete(scope, key).await
        }

        async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
            self.0.contains_key(scope, key).await
        }
    }

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S> ExpiryStore for ExpiryStoreGlue<S>
    where
        S: Send + Sync + Store,
    {
        async fn set_expiring(
            &self,
            _: Arc<[u8]>,
            _: Arc<[u8]>,
            _: Arc<[u8]>,
            _: Duration,
        ) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }

        async fn get_expiring(
            &self,
            _: Arc<[u8]>,
            _: Arc<[u8]>,
        ) -> Result<Option<(Arc<[u8]>, Option<Duration>)>> {
            Err(StorageError::MethodNotSupported)
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use crate::{
        dev::{Expiry, Store},
        Result, Storage,
    };

    #[derive(Clone)]
    struct SampleStore;

    #[async_trait::async_trait]
    impl Store for SampleStore {
        async fn set(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
            Ok(())
        }
        async fn set_number(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: i64) -> Result<()> {
            Ok(())
        }
        async fn get(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
            Ok(Some("v".as_bytes().into()))
        }
        async fn get_number(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<Option<i64>> {
            Ok(Some(123))
        }
        async fn contains_key(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<bool> {
            Ok(false)
        }
        async fn delete(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Expiry for SampleStore {
        async fn expire(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Duration) -> Result<()> {
            Ok(())
        }
        async fn expiry(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<Option<Duration>> {
            Ok(Some(Duration::from_secs(1)))
        }
        async fn extend(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Duration) -> Result<()> {
            Ok(())
        }
        async fn persist(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_no_expiry() {
        struct OnlyStore;
        let storage = Storage::build().store(SampleStore).no_expiry().finish();

        let k = "key";
        let v = "value".as_bytes();
        let d = Duration::from_secs(1);

        // These checks should all result in error as we didn't set any expiry
        assert!(storage.expire(k, d).await.is_err());
        assert!(storage.expiry(k).await.is_err());
        assert!(storage.extend(k, d).await.is_err());
        assert!(storage.persist(k).await.is_err());
        assert!(storage.set_expiring(k, v, d).await.is_err());
        assert!(storage.get_expiring(k).await.is_err());

        // These tests should all succeed
        assert!(storage.set(k, v).await.is_ok());
        assert!(storage.get(k).await.is_ok());
        assert!(storage.delete(k).await.is_ok());
        assert!(storage.contains_key(k).await.is_ok());
    }

    #[tokio::test]
    async fn test_expiry_store_polyfill() {
        let k = "key";
        let v = "value".as_bytes();
        let d = Duration::from_secs(1);

        let store = SampleStore;
        let storage = Storage::build().store(store.clone()).expiry(store).finish();
        assert!(storage
            .set_expiring("key", "value", Duration::from_secs(1))
            .await
            .is_ok());

        // These tests should all succeed
        assert!(storage.expire(k, d).await.is_ok());
        assert!(storage.expiry(k).await.is_ok());
        assert!(storage.extend(k, d).await.is_ok());
        assert!(storage.persist(k).await.is_ok());
        assert!(storage.set_expiring(k, v, d).await.is_ok());
        assert!(storage.get_expiring(k).await.is_ok());
        assert!(storage.set(k, v).await.is_ok());
        assert!(storage.get(k).await.is_ok());
        assert!(storage.delete(k).await.is_ok());
        assert!(storage.contains_key(k).await.is_ok());

        // values should match
        let res = storage.get_expiring("key").await;
        assert!(res.is_ok());
        assert!(res.unwrap() == Some(("v".as_bytes().into(), Some(Duration::from_secs(1)))));
    }
}
