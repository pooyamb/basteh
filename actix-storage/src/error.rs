use std::error::Error;

use actix_web::{http::StatusCode, ResponseError};
use thiserror::Error;

/// Error type that will be returned from all fallible methods of actix_storage.
///
/// implementers should generally use Custom variant for their own errors.
#[derive(Debug, Error)]
pub enum StorageError {
    /// States that the method(mostly expiration methods) is not supported by the backend
    #[error("StorageError: Method not supported for the storage backend provided")]
    MethodNotSupported,
    /// States that the retrieved number is invalid
    #[error("StorageError: Invalid number retrieved from database")]
    InvalidNumber,
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
