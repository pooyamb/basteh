use std::sync::Arc;

use crate::{
    dev::{Expiry, ExpiryStore, Store},
    Storage,
};

pub const GLOBAL_SCOPE: &str = "STORAGE_GLOBAL_SCOPE";

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
            scope: GLOBAL_SCOPE.into(),
            store: Arc::new(self.store.unwrap()),
        }
    }
}

mod private {
    use std::time::Duration;

    use crate::{
        dev::{Mutation, OwnedValue},
        error::Result,
        provider::{Expiry, ExpiryStore, Store},
        value::Value,
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
        async fn expire(&self, scope: &str, key: &[u8], expire_in: Duration) -> Result<()> {
            self.1.expire(scope, key, expire_in).await
        }

        async fn expiry(&self, scope: &str, key: &[u8]) -> Result<Option<Duration>> {
            self.1.expiry(scope, key).await
        }

        async fn extend(&self, scope: &str, key: &[u8], expire_in: Duration) -> Result<()> {
            self.1.extend(scope, key, expire_in).await
        }

        async fn persist(&self, scope: &str, key: &[u8]) -> Result<()> {
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
        async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
            self.0.keys(scope).await
        }

        async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()> {
            self.0.set(scope, key.clone(), value).await?;
            self.1.set_called(key).await;
            Ok(())
        }

        async fn get(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
            self.0.get(scope, key).await
        }

        async fn delete(&self, scope: &str, key: &[u8]) -> Result<()> {
            self.0.delete(scope, key).await
        }

        async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool> {
            self.0.contains_key(scope, key).await
        }

        async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<()> {
            self.0.mutate(scope, key, mutations).await
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
            scope: &str,
            key: &[u8],
            value: Value<'_>,
            expire_in: Duration,
        ) -> Result<()> {
            self.0.set(scope.clone(), key.clone(), value).await?;
            self.1.expire(scope, key, expire_in).await
        }

        async fn get_expiring(
            &self,
            scope: &str,
            key: &[u8],
        ) -> Result<Option<(OwnedValue, Option<Duration>)>> {
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
        async fn expire(&self, _: &str, _: &[u8], _: Duration) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }

        async fn expiry(&self, _: &str, _: &[u8]) -> Result<Option<Duration>> {
            Err(StorageError::MethodNotSupported)
        }

        async fn extend(&self, _: &str, _: &[u8], _: Duration) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }

        async fn persist(&self, _: &str, _: &[u8]) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }
    }

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S> Store for ExpiryStoreGlue<S>
    where
        S: Send + Sync + Store,
    {
        async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
            self.0.keys(scope).await
        }

        async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()> {
            self.0.set(scope, key.clone(), value).await
        }

        async fn get(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
            self.0.get(scope, key).await
        }

        async fn delete(&self, scope: &str, key: &[u8]) -> Result<()> {
            self.0.delete(scope, key).await
        }

        async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool> {
            self.0.contains_key(scope, key).await
        }

        async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<()> {
            self.0.mutate(scope, key, mutations).await
        }
    }

    /// For sepearate expiry and stores
    #[async_trait::async_trait]
    impl<S> ExpiryStore for ExpiryStoreGlue<S>
    where
        S: Send + Sync + Store,
    {
        async fn set_expiring(&self, _: &str, _: &[u8], _: Value<'_>, _: Duration) -> Result<()> {
            Err(StorageError::MethodNotSupported)
        }

        async fn get_expiring(
            &self,
            _: &str,
            _: &[u8],
        ) -> Result<Option<(OwnedValue, Option<Duration>)>> {
            Err(StorageError::MethodNotSupported)
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{
        dev::{Expiry, Mutation, OwnedValue, Store},
        value::Value,
        Result, Storage,
    };

    #[derive(Clone)]
    struct SampleStore;

    #[async_trait::async_trait]
    impl Store for SampleStore {
        async fn keys(&self, _: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
            Ok(Box::new(Vec::new().into_iter()))
        }
        async fn set(&self, _: &str, _: &[u8], _: Value<'_>) -> Result<()> {
            Ok(())
        }
        async fn get(&self, _: &str, _: &[u8]) -> Result<Option<OwnedValue>> {
            Ok(Some(Value::<'_>::from("v").into_owned()))
        }
        async fn contains_key(&self, _: &str, _: &[u8]) -> Result<bool> {
            Ok(false)
        }
        async fn delete(&self, _: &str, _: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn mutate(&self, _: &str, _: &[u8], _: Mutation) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Expiry for SampleStore {
        async fn expire(&self, _: &str, _: &[u8], _: Duration) -> Result<()> {
            Ok(())
        }
        async fn expiry(&self, _: &str, _: &[u8]) -> Result<Option<Duration>> {
            Ok(Some(Duration::from_secs(1)))
        }
        async fn extend(&self, _: &str, _: &[u8], _: Duration) -> Result<()> {
            Ok(())
        }
        async fn persist(&self, _: &str, _: &[u8]) -> Result<()> {
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
        assert!(storage.get_expiring::<String>(k).await.is_err());

        // These tests should all succeed
        assert!(storage.set(k, v).await.is_ok());
        assert!(storage.get::<Vec<u8>>(k).await.is_ok());
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
        assert!(storage.get_expiring::<String>(k).await.is_ok());
        assert!(storage.set(k, v).await.is_ok());
        assert!(storage.get::<Vec<u8>>(k).await.is_ok());
        assert!(storage.delete(k).await.is_ok());
        assert!(storage.contains_key(k).await.is_ok());

        // values should match
        let res = storage.get_expiring::<String>("key").await;
        assert!(res.is_ok());
        assert!(res.unwrap() == Some(("v".to_owned(), Some(Duration::from_secs(1)))));
    }
}
