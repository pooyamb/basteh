use std::sync::Arc;

use actix_storage::{dev::Store, Result};
use dashmap::DashMap;

type ScopeMap = DashMap<Arc<[u8]>, Arc<[u8]>>;
type InternalMap = DashMap<Arc<[u8]>, ScopeMap>;

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
    map: InternalMap,
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
    pub fn from_dashmap(map: InternalMap) -> Self {
        Self { map }
    }
}

#[async_trait::async_trait]
impl Store for DashMapStore {
    async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
        self.map.entry(scope).or_default().insert(key, value);
        Ok(())
    }

    async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
        let value = if let Some(scope_map) = self.map.get(&scope) {
            scope_map.get(&key).map(|v| v.clone())
        } else {
            None
        };
        Ok(value)
    }

    async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
        self.map
            .get_mut(&scope)
            .and_then(|scope_map| scope_map.remove(&key));
        Ok(())
    }

    async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
        Ok(self
            .map
            .get(&scope)
            .map(|scope_map| scope_map.contains_key(&key))
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[test]
    fn test_dashmap_basic_store() {
        test_store(Box::pin(async { DashMapStore::default() }));
    }

    #[test]
    fn test_dashmap_basic_formats() {
        impl Clone for DashMapStore {
            fn clone(&self) -> Self {
                Self {
                    map: self.map.clone(),
                }
            }
        }
        test_all_formats(Box::pin(async { DashMapStore::default() }));
    }
}
