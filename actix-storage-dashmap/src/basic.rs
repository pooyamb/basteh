use std::sync::Arc;

use actix_storage::{dev::Store, Result};
use dashmap::DashMap;

/// A simple implementation of [`Store`](actix_storage::dev::Store) based on DashMap
///
/// This provider doesn't support key expiration thus Storage will return errors when trying to use methods
/// that require expiration functionality if there is no expiry provided.
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_dashmap::DashMapStore;
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     let storage = Storage::build().store(DashMapStore::new()).finish();
///     let server = HttpServer::new(move || {
///         App::new()
///             .data(storage.clone())
///     });
///     server.bind("localhost:5000")?.run().await
/// }
/// ```
#[derive(Debug, Default)]
pub struct DashMapStore {
    map: DashMap<Arc<[u8]>, Arc<[u8]>>,
}

impl DashMapStore {
    /// Make a new store, with default capacity of 0
    pub fn new() -> Self {
        Self::default()
    }

    /// Make a new store, with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: DashMap::with_capacity(capacity),
        }
    }

    /// Make a new store from a hashmap
    pub fn from_dashmap(map: DashMap<Arc<[u8]>, Arc<[u8]>>) -> Self {
        Self { map }
    }
}

#[async_trait::async_trait]
impl Store for DashMapStore {
    async fn set(&self, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    async fn get(&self, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
        let val = match self.map.get(key.as_ref()) {
            Some(val) => Some(val.value().clone()),
            None => None,
        };
        Ok(val)
    }

    async fn delete(&self, key: Arc<[u8]>) -> Result<()> {
        self.map.remove(key.as_ref());
        Ok(())
    }

    async fn contains_key(&self, key: Arc<[u8]>) -> Result<bool> {
        Ok(self.map.contains_key(key.as_ref()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[actix_rt::test]
    async fn test_dashmap_basic_store() {
        let store = DashMapStore::default();
        test_store(store).await;
    }

    #[actix_rt::test]
    async fn test_dashmap_basic_formats() {
        impl Clone for DashMapStore {
            fn clone(&self) -> Self {
                Self {
                    map: self.map.clone(),
                }
            }
        }
        let store = DashMapStore::default();
        test_all_formats(store).await;
    }
}
