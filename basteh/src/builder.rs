use std::sync::Arc;

use crate::{dev::Provider, Basteh};

pub const GLOBAL_SCOPE: &str = "Basteh_GLOBAL_SCOPE";

/// Builder struct for [`Basteh`](../struct.Basteh.html)
///
/// A provider can either implement [`ExpiryBasteh`](trait.ExpiryBasteh.html) directly,
/// or implement [`Basteh`](trait.Basteh.html) and rely on another provider to provide
/// expiration capablities. The builder will polyfill a [`ExpiryBasteh`](trait.ExpiryBasteh.html)
/// by combining an [`Expiry`](trait.Expiry.html) and a [`Basteh`](trait.Basteh.html) itself.
///
/// If there is no [`Expiry`](trait.Expiry.html) set in either of the ways, it will result in runtime
/// errors when calling methods which require that functionality.
#[derive(Default)]
pub struct BastehBuilder<S = ()> {
    provider: Option<S>,
}

impl BastehBuilder {
    #[must_use = "Builder must be used by calling finish"]
    /// This method can be used to set a [`Basteh`](trait.Basteh.html), the second call to this
    /// method will overwrite the store.
    pub fn provider<P>(self, provider: P) -> BastehBuilder<P>
    where
        P: Provider + 'static,
    {
        BastehBuilder {
            provider: Some(provider),
        }
    }
}

impl<S: Provider + 'static> BastehBuilder<S> {
    /// Build the Basteh
    pub fn finish(self) -> Basteh {
        Basteh {
            scope: GLOBAL_SCOPE.into(),
            provider: Arc::new(self.provider.unwrap()),
        }
    }
}
