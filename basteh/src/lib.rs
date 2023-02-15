#![doc = include_str!("../README.md")]

mod basteh;
mod builder;
mod error;
mod mutation;
mod provider;
mod value;

pub use basteh::Basteh;
pub use builder::GLOBAL_SCOPE;
pub use error::{BastehError, Result};

/// Set of traits and structs used for storage backend development
pub mod dev {
    pub use crate::builder::BastehBuilder;
    pub use crate::mutation::{Action, Mutation};
    pub use crate::provider::Provider;
    pub use crate::value::{OwnedValue, Value, ValueKind};
}

#[doc(hidden)]
#[cfg(feature = "test_utils")]
pub mod test_utils;
