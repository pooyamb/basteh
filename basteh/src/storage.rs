use std::convert::AsRef;
use std::sync::Arc;
use std::time::Duration;

use crate::dev::{ExpiryStore, Mutation, StorageBuilder};
use crate::error::Result;

/// Takes the underlying backend and provides common methods for it
///
/// As it is type erased, it's suitable to be stored in a web framework's state or extensions.
///
/// The backend this struct holds should implement [`ExpiryStore`](dev/trait.ExpiryStore.html)
/// either directly, or by depending on the default polyfill.
/// Look [`StorageBuilder`](dev/struct.StorageBuilder.html) for more details.
///
/// ## Example
///
/// ```rust
/// use basteh::{Storage, StorageError};
///
/// async fn index(storage: Storage) -> Result<String, StorageError>{
///     storage.set("key", "value").await;
///     let val = storage.get("key").await?.unwrap();
///     Ok(String::from_utf8(val.to_vec()).unwrap())
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
    /// # use basteh::Storage;
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
    /// # use basteh::Storage;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// storage.set("age", vec![10]).await;
    /// storage.set("name", "Violet".as_bytes()).await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn keys(&self) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        self.store.keys(self.scope.as_ref()).await
    }

    /// Stores a sequence of bytes on storage
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Storage;
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
                self.scope.as_ref(),
                key.as_ref().into(),
                value.as_ref().into(),
            )
            .await
    }

    /// Stores a number on storage
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    /// How the number is represented in storage is decided by the backend.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Storage;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// storage.set_number("age", 10).await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn set_number(&self, key: impl AsRef<[u8]>, value: i64) -> Result<()> {
        self.store
            .set_number(self.scope.as_ref(), key.as_ref().into(), value)
            .await
    }

    /// Stores a sequence of bytes on storage and sets expiry on the key
    /// It should be prefered over calling set and expire as backends may define
    /// a more optimized way to do both operations at once.
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Storage;
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
    /// expiry provider is not set.(no_expiry is called on builder)
    pub async fn set_expiring(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_in: Duration,
    ) -> Result<()> {
        self.store
            .set_expiring(
                self.scope.as_ref(),
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
    /// # use basteh::{Storage, StorageError};
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// let val = storage.get("key").await?;
    /// #     Ok(std::str::from_utf8(&val.unwrap()).unwrap_or_default().to_owned())
    /// # }
    /// ```
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.store
            .get(self.scope.as_ref(), key.as_ref().into())
            .await
    }

    /// Gets a number from storage
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// let val: Option<i64> = storage.get_number("key").await?;
    /// #     Ok(val.unwrap_or(0).to_string())
    /// # }
    /// ```
    pub async fn get_number(&self, key: impl AsRef<[u8]>) -> Result<Option<i64>> {
        self.store
            .get_number(self.scope.as_ref(), key.as_ref().into())
            .await
    }

    /// Same as `get` but it also gets expiry.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// let val = storage.get_expiring("key").await?;
    /// #     Ok(std::str::from_utf8(&val.unwrap().0).unwrap_or_default().to_owned())
    /// # }
    /// ```
    pub async fn get_expiring(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(Vec<u8>, Option<Duration>)>> {
        if let Some((val, expiry)) = self
            .store
            .get_expiring(self.scope.as_ref(), key.as_ref().into())
            .await?
        {
            Ok(Some((val, expiry)))
        } else {
            Ok(None)
        }
    }

    /// Mutate a numeric value in the storage
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Storage;
    /// # use std::cmp::Ordering;
    /// #
    /// # async fn index<'a>(storage: Storage) -> &'a str {
    /// storage.mutate("age", |v| v.incr(5)).await;
    /// // Or conditionally set it to 100
    /// storage.mutate("age", |v| v.if_(Ordering::Greater, 100, |m| m.set(100))).await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn mutate<F>(&self, key: impl AsRef<[u8]>, mutate_f: F) -> Result<()>
    where
        F: Fn(Mutation) -> Mutation,
    {
        self.store
            .mutate(
                self.scope.as_ref(),
                key.as_ref().into(),
                mutate_f(Mutation::new()),
            )
            .await
    }

    /// Deletes/Removes a key value pair from storage.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// storage.delete("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.store
            .delete(self.scope.as_ref(), key.as_ref().into())
            .await
    }

    /// Checks if storage contains a key.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// let exist = storage.contains_key("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.store
            .contains_key(self.scope.as_ref(), key.as_ref().into())
            .await
    }

    /// Sets expiry on a key, it won't result in error if the key doesn't exist.
    ///
    /// Calling set methods twice or calling persist will result in expiry being erased
    /// from the key, calling expire itself twice will overwrite the expiry for key.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// storage.expire("key", Duration::from_secs(10)).await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn expire(&self, key: impl AsRef<[u8]>, expire_in: Duration) -> Result<()> {
        self.store
            .expire(self.scope.as_ref(), key.as_ref().into(), expire_in)
            .await
    }

    /// Gets expiry for the provided key, it will give none if there is no expiry set.
    ///
    /// The result of this method is not guaranteed to be exact and may be inaccurate
    /// depending on sotrage implementation.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
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
            .expiry(self.scope.as_ref(), key.as_ref().into())
            .await
    }

    /// Extends expiry for a key, it won't result in error if the key doesn't exist.
    ///
    /// If the provided key doesn't have an expiry set, it will set the expiry on that key.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// storage.expire("key", Duration::from_secs(5)).await?;
    /// storage.extend("key", Duration::from_secs(5)).await?; // ket will expire in ~10 seconds
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn extend(&self, key: impl AsRef<[u8]>, expire_in: Duration) -> Result<()> {
        self.store
            .extend(self.scope.as_ref(), key.as_ref().into(), expire_in)
            .await
    }

    /// Clears expiry from the provided key, making it persistant.
    ///
    /// Calling expire will overwrite persist.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Storage, StorageError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(storage: Storage) -> Result<String, StorageError> {
    /// storage.persist("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn persist(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.store
            .persist(self.scope.as_ref(), key.as_ref().into())
            .await
    }
}
