use std::error::Error;

use thiserror::Error;

/// Error type that will be returned from all fallible methods of basteh.
///
/// implementers should generally use Custom variant for their own errors.
#[derive(Debug, Error)]
pub enum BastehError {
    /// States that the method(mostly expiration methods) is not supported by the backend
    #[error("BastehError: Method not supported for the Basteh backend provided")]
    MethodNotSupported,
    /// States that the retrieved number is invalid
    #[error("BastehError: Value is not a valid number or mutation will result in overflow")]
    InvalidNumber,
    /// States that the retrieved number is invalid
    #[error("BastehError: Invalid type requested from backend")]
    TypeConversion,
    /// An error from the underlying backend
    #[error("BastehError: {:?}", self)]
    Custom(Box<dyn Error + Send>),
}

impl BastehError {
    /// Shortcut method to construct Custom variant
    pub fn custom<E>(err: E) -> Self
    where
        E: 'static + Error + Send,
    {
        Self::Custom(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, BastehError>;
