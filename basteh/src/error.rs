use std::error::Error;

use thiserror::Error;

/// Error type that will be returned from all fallible methods of basteh.
///
/// implementers should generally use Custom variant for their own errors.
#[derive(Debug, Error)]
pub enum StorageError {
    /// States that the method(mostly expiration methods) is not supported by the backend
    #[error("StorageError: Method not supported for the storage backend provided")]
    MethodNotSupported,
    /// States that the retrieved number is invalid
    #[error("StorageError: Value is not a valid number or mutation will result in overflow")]
    InvalidNumber,
    /// States that the retrieved number is invalid
    #[error("StorageError: Invalid type requested from backend")]
    TypeConversion,
    /// An error from the underlying backend
    #[error("StorageError: {:?}", self)]
    Custom(Box<dyn Error + Send>),
}

impl StorageError {
    /// Shortcut method to construct Custom variant
    pub fn custom<E>(err: E) -> Self
    where
        E: 'static + Error + Send,
    {
        Self::Custom(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;
