use std::error::Error;

use actix_web::{http::StatusCode, ResponseError};
use thiserror::Error;

/// Error type that will be returned from all fallible methods of actix_storage.
///
/// implementers should generally use Custom variant for their own errors.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Occurs when expiry methods are not available or the implementer doesn't support
    /// the method.
    #[error("StorageError: Method not supported for the storage backend provided")]
    MethodNotSupported,
    /// Occurs when serialization is not possible, either because of wrong format specified
    /// or wrong type.
    #[error("StorageError: Serialization failed")]
    SerializationError,
    /// Occurs when deserialization is not possible, either because of wrong format specified
    /// or wrong type.
    #[error("StorageError: Deserialization failed")]
    DeserializationError,
    /// Occurs when the underlying storage implementer faces error
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

impl ResponseError for StorageError {
    fn status_code(&self) -> StatusCode {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod test {
    use super::*;
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("My error")]
    struct MyError;

    #[test]
    fn test_error() {
        let e = MyError;
        let s = StorageError::custom(e);
        assert!(s.status_code() == StatusCode::INTERNAL_SERVER_ERROR);
        if let StorageError::Custom(err) = s {
            assert!(format!("{}", err) == "My error");
        }
    }
}
