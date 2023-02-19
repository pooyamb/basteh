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

    /// Bastehs a sequence of bytes on Basteh
    ///
    /// Calling set operations twice on the same key, overwrites it's value and
    /// clear the expiry on that key(if it exist).
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

    /// Bastehs a sequence of bytes on Basteh
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
    pub async fn set<'a>(&self, key: impl AsRef<[u8]>, value: impl Into<Value<'a>>) -> Result<()> {
        self.provider
            .set(self.scope.as_ref(), key.as_ref(), value.into())
            .await
    }

    /// Bastehs a sequence of bytes on Basteh and sets expiry on the key
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

    /// Gets a sequence of bytes from backend, resulting in an arc
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
    pub async fn get<'a, T: TryFrom<OwnedValue, Error = BastehError>>(
        &'a self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        self.provider
            .get(self.scope.as_ref(), key.as_ref().into())
            .await?
            .map(|v| v.try_into())
            .transpose()
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
    pub async fn get_expiring<'a, T: TryFrom<OwnedValue, Error = BastehError>>(
        &'a self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<(T, Option<Duration>)>> {
        if let Some((v, expiry)) = self
            .provider
            .get_expiring(self.scope.as_ref(), key.as_ref().into())
            .await?
        {
            Ok(Some((v.try_into()?, expiry)))
        } else {
            Ok(None)
        }
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
    pub async fn remove<T: TryFrom<OwnedValue, Error = BastehError>>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>> {
        self.provider
            .remove(self.scope.as_ref(), key.as_ref().into())
            .await?
            .map(|v| v.try_into())
            .transpose()
    }

    /// Checks if Basteh contains a key.
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

    /// Gets expiry for the provided key, it will give none if there is no expiry set.
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

    /// Clears expiry from the provided key, making it persistant.
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
