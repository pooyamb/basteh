mod builder;
mod error;
mod provider;
mod storage;

pub use builder::GLOBAL_SCOPE;
pub use error::{Result, StorageError};
pub use storage::Storage;

/// Set of traits and structs used for storage backend development
pub mod dev {
    #[cfg(feature = "actor")]
    /// Set of actix messages to help with store and expiry implementation
    pub mod actor {
        pub use crate::actor::*;
    }
    pub use crate::builder::StorageBuilder;
    pub use crate::provider::*;
}

#[cfg(feature = "actor")]
mod actor;

#[cfg(feature = "actix-web")]
mod actix_web;

#[doc(hidden)]
#[cfg(feature = "tests")]
pub mod tests;
