mod builder;
mod error;
mod provider;
mod storage;

pub use builder::GLOBAL_SCOPE;
pub use error::{Result, StorageError};
pub use storage::Storage;

/// Set of traits and structs used for storage backend development
pub mod dev {
    pub use crate::builder::StorageBuilder;
    pub use crate::provider::*;
}

#[doc(hidden)]
#[cfg(feature = "test_utils")]
pub mod test_utils;
