use std::convert::{AsRef, TryFrom, TryInto};
use std::sync::Arc;
use std::time::Duration;

use crate::dev::{BastehBuilder, OwnedValue, Provider};
use crate::error::Result;
use crate::mutation::Mutation;
use crate::value::Value;
use crate::BastehError;

/// Takes the underlying backend and provides common methods for it
///
/// As it is type erased, it's suitable to be stored in a web framework's state or extensions.
///
/// The backend this struct holds should implement [`ExpiryBasteh`](dev/trait.Expirystore.html)
/// either directly, or by depending on the default polyfill.
/// Look [`BastehBuilder`](dev/struct.BastehBuilder.html) for more details.
///
/// ## Example
///
/// ```rust
/// use basteh::{Basteh, BastehError};
///
/// async fn index(store: Basteh) -> Result<String, BastehError>{
///     store.set("key", "value").await;
///     let val = store.get::<String>("key").await?;
///     Ok(val.unwrap_or_default())
/// }
/// ```
///
///
#[derive(Clone)]
pub struct Basteh {
    pub(crate) scope: Arc<str>,
    pub(crate) provider: Arc<dyn Provider>,
}

impl Basteh {
    /// Returns the Basteh builder struct
    pub fn build() -> BastehBuilder {
        BastehBuilder::default()
    }

    /// Return a new Basteh struct for the specified scope. Calling twice will just change
    /// the current scope.
    ///
    /// Scopes may or may not be implemented as key prefixes but should provide
    /// some guarantees to not mutate other scopes.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// let cache = store.scope("cache");
    /// cache.set("age", "60").await;
    /// #     "set"
    /// # }
    /// ```
    pub fn scope(&self, scope: &str) -> Basteh {
        Basteh {
            scope: scope.into(),
            provider: self.provider.clone(),
        }
    }

    /// Get all keys matching the requested pattern(not implemented yet)
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// store.keys().await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn keys(&self) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        self.provider.keys(self.scope.as_ref()).await
    }

    /// Saves a single key-value on store, use bytes for bytes
    ///
    /// ## Note
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// # use bytes::Bytes;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// store.set("name", "Violet").await; // String
    /// store.set("age", 20).await; // Number
    /// store.set("points", vec![20__u32, 25, 30]).await; // Lists
    /// store.set("data", Bytes::from_static(b"123456")).await; // Or bytes
    /// #     "set"
    /// # }
    /// ```
    pub async fn set<'a>(&self, key: impl AsRef<[u8]>, value: impl Into<Value<'a>>) -> Result<()> {
        self.provider
            .set(self.scope.as_ref(), key.as_ref(), value.into())
            .await
    }

    /// Sets a value on store with expiry on the key
    /// It should be prefered over calling set and expire as backends may define
    /// a more optimized way to do both operations at once.
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// # use std::time::Duration;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// store.set_expiring("name", "Violet", Duration::from_secs(10)).await;
    /// #     "set"
    /// # }
    /// ```
    ///
    /// ## Errors
    /// Beside the normal errors caused by the Basteh itself, it will result in error if
    /// expiry provider is not set.(no_expiry is called on builder)
    pub async fn set_expiring(
        &self,
        key: impl AsRef<[u8]>,
        value: impl Into<Value<'_>>,
        expires_in: Duration,
    ) -> Result<()> {
        self.provider
            .set_expiring(
                self.scope.as_ref(),
                key.as_ref().into(),
                value.into(),
                expires_in,
            )
            .await
    }

    /// Gets a single value from store(use `get_range` for lists)
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// let val = store.get::<String>("key").await?;
    /// #     Ok(val.unwrap_or_default())
    /// # }
    /// ```
    pub async fn get<'a, T: TryFrom<OwnedValue, Error = impl Into<BastehError>>>(
        &'a self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        self.provider
            .get(self.scope.as_ref(), key.as_ref().into())
            .await?
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)
    }

    /// Gets a list of values from store, start/end works like redis with support for negative indexes
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// #
    /// # async fn index(store: Basteh) -> Result<Vec<String>, BastehError> {
    /// let val = store.get_range::<String>("key", 0, -1).await?;
    /// #     Ok(val)
    /// # }
    /// ```
    pub async fn get_range<'a, T: TryFrom<OwnedValue, Error = impl Into<BastehError>>>(
        &'a self,
        key: impl AsRef<[u8]>,
        start: i64,
        end: i64,
    ) -> Result<Vec<T>> {
        self.provider
            .get_range(self.scope.as_ref(), key.as_ref().into(), start, end)
            .await?
            .into_iter()
            .map(|v| v.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Same as `get` but it also gets expiry.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// let val = store.get_expiring::<String>("key").await?;
    /// #     Ok(val.map(|v|v.0).unwrap_or_default())
    /// # }
    /// ```
    pub async fn get_expiring<'a, T: TryFrom<OwnedValue, Error = impl Into<BastehError>>>(
        &'a self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(T, Option<Duration>)>> {
        self.provider
            .get_expiring(self.scope.as_ref(), key.as_ref().into())
            .await?
            .map(|(v, e)| v.try_into().map(|v| (v, e)).map_err(Into::into))
            .transpose()
    }

    /// Push a single value into the list stored for this key
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// store.set("age", vec![10]).await;
    /// store.set("name", "Violet").await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn push<'a>(&self, key: impl AsRef<[u8]>, value: impl Into<Value<'a>>) -> Result<()> {
        self.provider
            .push(self.scope.as_ref(), key.as_ref(), value.into())
            .await
    }

    /// Push all the given values into the list stored for this key
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// store.set("age", vec![10]).await;
    /// store.set("name", "Violet").await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn push_mutiple<'a>(
        &self,
        key: impl AsRef<[u8]>,
        values: impl Iterator<Item = impl Into<Value<'a>>>,
    ) -> Result<()> {
        self.provider
            .push_multiple(
                self.scope.as_ref(),
                key.as_ref(),
                values.map(|v| v.into()).collect(),
            )
            .await
    }

    /// Pop all the value from the list stored for this key
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// let val = store.get::<String>("key").await?;
    /// #     Ok(val.unwrap_or_default())
    /// # }
    /// ```
    pub async fn pop<'a, T: TryFrom<OwnedValue, Error = impl Into<BastehError>>>(
        &'a self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        self.provider
            .pop(self.scope.as_ref(), key.as_ref().into())
            .await?
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)
    }

    /// Mutate a numeric value in the store. It may overwrite the value if it's not a number.
    ///
    /// ## Note
    /// The closure will called in-place(outside the backend store) and only the collected mutations
    /// will be passed.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::Basteh;
    /// # use std::cmp::Ordering;
    /// #
    /// # async fn index<'a>(store: Basteh) -> &'a str {
    /// store.mutate("age", |v| v.incr(5)).await;
    /// // Or conditionally set it to 100
    /// store.mutate("age", |v| v.if_(Ordering::Greater, 100, |m| m.set(100))).await;
    /// #     "set"
    /// # }
    /// ```
    pub async fn mutate(
        &self,
        key: impl AsRef<[u8]>,
        mutate_f: impl Fn(Mutation) -> Mutation,
    ) -> Result<i64> {
        self.provider
            .mutate(
                self.scope.as_ref(),
                key.as_ref().into(),
                mutate_f(Mutation::new()),
            )
            .await
    }

    /// Removes a key value pair from store, returning the value if exist.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// store.remove::<String>("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn remove<T: TryFrom<OwnedValue, Error = impl Into<BastehError>>>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        self.provider
            .remove(self.scope.as_ref(), key.as_ref().into())
            .await?
            .map(TryInto::try_into)
            .transpose()
            .map_err(Into::into)
    }

    /// Checks if store contains a key.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// let exist = store.contains_key("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.provider
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
    /// # use basteh::{Basteh, BastehError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// store.expire("key", Duration::from_secs(10)).await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn expire(&self, key: impl AsRef<[u8]>, expire_in: Duration) -> Result<()> {
        self.provider
            .expire(self.scope.as_ref(), key.as_ref().into(), expire_in)
            .await
    }

    /// Gets expiry for the provided key, it will return none if there is no expiry set.
    ///
    /// The result of this method is not guaranteed to be exact and may be inaccurate
    /// depending on sotrage implementation.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// let exp = store.expiry("key").await?;
    /// if let Some(exp) = exp{
    ///     println!("Key will expire in {} seconds", exp.as_secs());
    /// } else {
    ///     println!("Long live the key");
    /// }
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn expiry(&self, key: impl AsRef<[u8]>) -> Result<Option<Duration>> {
        self.provider
            .expiry(self.scope.as_ref(), key.as_ref().into())
            .await
    }

    /// Extends expiry for a key, it won't result in error if the key doesn't exist.
    ///
    /// If the provided key doesn't have an expiry set, it will set the expiry on that key.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// store.expire("key", Duration::from_secs(5)).await?;
    /// store.extend("key", Duration::from_secs(5)).await?; // ket will expire in ~10 seconds
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn extend(&self, key: impl AsRef<[u8]>, expire_in: Duration) -> Result<()> {
        self.provider
            .extend(self.scope.as_ref(), key.as_ref().into(), expire_in)
            .await
    }

    /// Clears expiry from the provided key, making it persistent.
    ///
    /// Calling expire will overwrite persist.
    ///
    /// ## Example
    /// ```rust
    /// # use basteh::{Basteh, BastehError};
    /// # use std::time::Duration;
    /// #
    /// # async fn index(store: Basteh) -> Result<String, BastehError> {
    /// store.persist("key").await?;
    /// #     Ok("deleted".to_string())
    /// # }
    /// ```
    pub async fn persist(&self, key: impl AsRef<[u8]>) -> Result<()> {
        self.provider
            .persist(self.scope.as_ref(), key.as_ref().into())
            .await
    }
}
