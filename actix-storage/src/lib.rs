mod actor;
mod error;
mod provider;
mod storage;

#[cfg(feature = "with-serde")]
mod format;

#[cfg(feature = "with-serde")]
pub use crate::format::Format;

pub use error::{Result, StorageError};
pub use storage::{Storage, GLOBAL_SCOPE};

/// Set of traits and structs used for storage backend development
pub mod dev {
    /// Set of actix messages to help with store and expiry implementation
    pub mod actor {
        pub use crate::actor::*;
    }
    pub use crate::provider::*;
    pub use crate::storage::StorageBuilder;
}

#[doc(hidden)]
#[cfg(feature = "tests")]
pub mod tests;
