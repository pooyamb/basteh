use std::future::{ready, Ready};

use actix_web::{dev::Payload, error::ErrorInternalServerError, FromRequest, HttpRequest};

use crate::Storage;

/// It is pretty much copied as-is from actix-web Data
impl FromRequest for Storage {
    type Error = actix_web::Error;
    type Future = Ready<std::result::Result<Self, actix_web::Error>>;

    #[inline]
    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        if let Some(st) = req.app_data::<Storage>() {
            ready(Ok(st.clone()))
        } else {
            log::debug!(
                "Failed to construct Storage(basteh). \
                 Request path: {:?}",
                req.path(),
            );
            ready(Err(ErrorInternalServerError(
                "Storage is not configured, please refer to basteh documentation\
                for more information.",
            )))
        }
    }
}
