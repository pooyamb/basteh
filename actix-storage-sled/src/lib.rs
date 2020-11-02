#[cfg(feature = "actor")]
/// An implementation of [`ExpiryStore`](../actix_storage/dev/trait.ExpiryStore.html) based on actix
/// and sled, requires `["actor"]` feature
pub mod actor;
mod basic;

pub use basic::SledStore;

pub use sled::{Config as SledConfig, Error as SledError};
