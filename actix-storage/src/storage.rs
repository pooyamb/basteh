use std::convert::AsRef;
use std::sync::Arc;
use std::time::Duration;

use crate::dev::{Expiry, ExpiryStore, Store};
use crate::error::Result;

pub const GLOBAL_SCOPE: [u8; 20] = *b"STORAGE_GLOBAL_SCOPE";

/// Takes the underlying backend and provides common methods for it
///
/// It can be stored in actix_web's Data and be used from handlers
/// without specifying the backend itself and provides all the common methods from underlying
/// store and expiry.
/// The backend this struct holds should implement [`ExpiryStore`](dev/trait.ExpiryStore.html)
/// either directly, or by depending on the default polyfill.
/// Look [`StorageBuilder`](dev/struct.StorageBuilder.html) for more details.
///
/// ## Example
///
/// ```rust
/// use actix_storage::Storage;
/// use actix_web::*;
///
/// async fn index(storage: Storage) -> Result<String, Error>{
///     storage.set("key", "value").await;
///     let val = storage.get("key").await?.unwrap();
///     Ok(std::str::from_utf8(&val)
///         .map_err(|err| error::ErrorInternalServerError("Storage error"))?.to_string())
/// }
/// ```
///
///
#[derive(Clone)]
pub struct Storage {
    scope: Arc<[u8]>,
    store: Arc<dyn ExpiryStore>,
}

impl Storage {
    /// Returns the storage builder struct
    pub fn build() -> StorageBuilder {
        StorageBuilder::default()
    }

    /// Return a new Storage struct for the specified scope.
    ///
    /// Scopes may or may not be implemented as key prefixes but should provide
    /// some guarantees to not mutate other scopes.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// let cache = storage.scope("cache");
    /// cache.set("age", "60").await;
    /// #     "set"
    /// # }
    /// ```
    pub fn scope(&self, scope: impl AsRef<[u8]>) -> Storage {
        Storage {
            scope: scope.as_ref().into(),
            store: self.store.clone(),
        }
    }

    /// Stores a sequence of bytes on storage
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// storage.set("age", vec![10]).await;
    /// storage.set("name", "Violet".as_bytes()).await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn set(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        self.store
            .set(
                self.scope.clone(),
                key.as_ref().into(),
                value.as_ref().into(),
            )
            .await
    }

    /// Stores a sequence of bytes on storage and sets expiry on the key
    /// It should be prefered over calling set and expire as providers may define
    /// a more optimized way to do both operations at once.
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// # use std::time::Duration;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// storage.set_expiring("name", "Violet".as_bytes(), Duration::from_secs(10)).await;
    /// #     "set"
    /// # }
    /// ```
    ///
    /// ## Errors
    /// Beside the normal errors caused by the storage itself, it will result in error if
    /// expiry provider is not set.
    pub async fn set_expiring(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_in: Duration,
    ) -> Result<()> {
        self.store
            .set_expiring(
                self.scope.clone(),
                key.as_ref().into(),
                value.as_ref().into(),
                expires_in,
            )
            .await
    }

    /// Gets a sequence of bytes from backend, resulting in an arc
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// let val = storage.get("key").await?;
    /// #     Ok(std::str::from_utf8(&val.unwrap()).unwrap_or_default().to_owned())
    /// # }
    /// ```
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Arc<[u8]>>> {
        self.store
            .get(self.scope.clone(), key.as_ref().into())
            .await
    }

    /// Same as `get` but it also gets expiry.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// let val = storage.get_expiring("key").await?;
    /// #     Ok(std::str::from_utf8(&val.unwrap().0).unwrap_or_default().to_owned())
    /// # }
    /// ```
    pub async fn get_expiring(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(Arc<[u8]>, Option<Duration>)>> {
        if let Some((val, expiry)) = self
            .store
            .get_expiring(self.scope.clone(), key.as_ref().into())
            .await?
        {
            Ok(Some((val, expiry)))
        } else {
            Ok(None)
        }
    }

    /// Deletes/Removes a key value pair from storage.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// storage.delete("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.store
            .delete(self.scope.clone(), key.as_ref().into())
            .await
    }

    /// Checks if storage contains a key.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// let exist = storage.contains_key("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.store
            .contains_key(self.scope.clone(), key.as_ref().into())
            .await
    }

    /// Sets expiry on a key, it won't result in error if the key doesn't exist.
    ///
    /// Calling set methods twice or calling persist will result in expiry being erased
    /// from the key, calling expire itself twice will overwrite the expiry for key.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// storage.expire("key", Duration::from_secs(10)).await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn expire(&self, key: impl AsRef<[u8]>, expire_in: Duration) -> Result<()> {
        self.store
            .expire(self.scope.clone(), key.as_ref().into(), expire_in)
            .await
    }

    /// Gets expiry for the provided key, it will give none if there is no expiry set.
    ///
    /// The result of this method is not guaranteed to be exact and may be inaccurate
    /// depending on sotrage implementation.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// let exp = storage.expiry("key").await?;
    /// if let Some(exp) = exp{
    ///     println!("Key will expire in {} seconds", exp.as_secs());
    /// } else {
    ///     println!("Long live the key");
    /// }
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn expiry(&self, key: impl AsRef<[u8]>) -> Result<Option<Duration>> {
        self.store
            .expiry(self.scope.clone(), key.as_ref().into())
            .await
    }

    /// Extends expiry for a key, it won't result in error if the key doesn't exist.
    ///
    /// If the provided key doesn't have an expiry set, it will set the expiry on that key.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// storage.expire("key", Duration::from_secs(5)).await?;
    /// storage.extend("key", Duration::from_secs(5)).await?; // ket will expire in ~10 seconds
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn extend(&self, key: impl AsRef<[u8]>, expire_in: Duration) -> Result<()> {
        self.store
            .extend(self.scope.clone(), key.as_ref().into(), expire_in)
            .await
    }

    /// Clears expiry from the provided key, making it persistant.
    ///
    /// Calling expire will overwrite persist.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// storage.persist("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn persist(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.store
            .persist(self.scope.clone(), key.as_ref().into())
            .await
    }
}

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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[actix::test]
    async fn test_no_expiry() {
        struct OnlyStore;

        #[async_trait::async_trait]
        impl Store for OnlyStore {
            async fn set(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
                Ok(())
            }
            async fn get(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
                Ok(None)
            }
            async fn contains_key(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<bool> {
                Ok(false)
            }
            async fn delete(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
                Ok(())
            }
        }

        let storage = Storage::build().store(OnlyStore).finish();

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

    #[actix::test]
    async fn test_expiry_store_polyfill() {
        #[derive(Clone)]
        struct SampleStore;

        #[async_trait::async_trait]
        impl Store for SampleStore {
            async fn set(&self, _: Arc<[u8]>, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<()> {
                Ok(())
            }
            async fn get(&self, _: Arc<[u8]>, _: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
                Ok(Some("v".as_bytes().into()))
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

    #[test]
    #[should_panic(expected = "Storage builder needs at least a store")]
    fn test_no_sotre() {
        Storage::build().finish();
    }
}
