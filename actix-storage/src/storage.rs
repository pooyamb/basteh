use std::convert::AsRef;
use std::sync::Arc;
use std::time::Duration;

use crate::dev::{ExpiryStore, StorageBuilder};
use crate::error::Result;

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
    pub(crate) scope: Arc<[u8]>,
    pub(crate) store: Arc<dyn ExpiryStore>,
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

    /// Stores a number on storage
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    /// How the number is represented in storage is decided by the provider, but
    /// by default its simple le_bytes.
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// storage.set_number("age", 10).await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn set_number(&self, key: impl AsRef<[u8]>, value: i64) -> Result<()> {
        self.store
            .set_number(self.scope.clone(), key.as_ref().into(), value)
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

    /// Gets a number from storage
    ///
    /// ## Example
    /// ```rust
    /// # use actix_storage::Storage;
    /// # use actix_web::*;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, Error> {
    /// let val: Option<i64> = storage.get_number("key").await?;
    /// #     Ok(val.unwrap_or(0).to_string())
    /// # }
    /// ```
    pub async fn get_number(&self, key: impl AsRef<[u8]>) -> Result<Option<i64>> {
        self.store
            .get_number(self.scope.clone(), key.as_ref().into())
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
