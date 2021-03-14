use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use actix_storage::{dev::Store, Result, StorageError};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("A proccess obtaining the lock has failed while keeping it.")]
pub struct PoisionLockStorageError;

type ScopeMap = HashMap<Arc<[u8]>, Arc<[u8]>>;
type InternalMap = HashMap<Arc<[u8]>, ScopeMap>;

/// A simple implementation of [`Store`](actix_storage::dev::Store) based on RwLock wrapped HashMap
///
/// This provider doesn't support key expiration thus Storage will return errors when trying to use methods
/// that require expiration functionality.  
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_hashmap::HashMapStore;
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     let storage = Storage::build().store(HashMapStore::new()).finish();
///     let server = HttpServer::new(move || {
///         App::new()
///             .data(storage.clone())
///     });
///     server.bind("localhost:5000")?.run().await
/// }
/// ```
#[derive(Debug, Default)]
pub struct HashMapStore {
    map: RwLock<InternalMap>,
}

impl HashMapStore {
    /// Make a new store, with default capacity of 0
    pub fn new() -> Self {
        Self::default()
    }

    /// Make a new store, with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: RwLock::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Make a new store from a hashmap
    pub fn from_hashmap(map: InternalMap) -> Self {
        Self {
            map: RwLock::new(map),
        }
    }
}

#[async_trait::async_trait]
impl Store for HashMapStore {
    async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
        match self.map.write() {
            Ok(mut h) => {
                h.entry(scope).or_default().insert(key, value);
                Ok(())
            }
            Err(_) => Err(StorageError::custom(PoisionLockStorageError)),
        }
    }

    async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
        match self.map.read() {
            Ok(h) => Ok(h
                .get(&scope)
                .and_then(|scope_map| scope_map.get(&key))
                .cloned()),
            Err(_) => Err(StorageError::custom(PoisionLockStorageError)),
        }
    }

    async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
        match self.map.write() {
            Ok(mut h) => {
                h.get_mut(&scope)
                    .and_then(|scope_map| scope_map.remove(&key));
                Ok(())
            }
            Err(_) => Err(StorageError::custom(PoisionLockStorageError)),
        }
    }

    async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
        match self.map.read() {
            Ok(h) => Ok(h
                .get(&scope)
                .map(|scope_map| scope_map.contains_key(&key))
                .unwrap_or(false)),
            Err(_) => Err(StorageError::custom(PoisionLockStorageError)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[test]
    fn test_hashmap_basic_store() {
        test_store(Box::pin(async { HashMapStore::default() }));
    }

    #[test]
    fn test_hashmap_basic_formats() {
        impl Clone for HashMapStore {
            fn clone(&self) -> Self {
                Self::default()
            }
        }
        test_all_formats(Box::pin(async { HashMapStore::default() }));
    }
}
