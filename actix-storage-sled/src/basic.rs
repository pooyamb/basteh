use std::{convert::TryInto, sync::Arc};

#[cfg(feature = "v01-compat")]
use std::ops::Deref;

use actix_storage::{
    dev::{Mutation, Store},
    Result as StorageResult, StorageError,
};
use sled::Tree;

use crate::{utils::run_mutations, SledConfig, SledError};

/// A simple implementation of [`Store`](actix_storage::dev::Store) based on Sled
///
/// This provider doesn't support key expiration thus Storage will return errors when trying to use methods
/// that require expiration functionality if there is no expiry provided.
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_sled::SledStore;
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     let db = SledStore::new().expect("Error opening the database");
///     let storage = Storage::build().store(db).no_expiry().finish();
///     let server = HttpServer::new(move || {
///         App::new()
///             .app_data(storage.clone())
///     });
///     server.bind("localhost:5000")?.run().await
/// }
/// ```
#[derive(Debug)]
pub struct SledStore {
    db: sled::Db,
}

impl SledStore {
    pub fn new() -> Result<Self, SledError> {
        Ok(Self {
            db: SledConfig::default().open()?,
        })
    }

    pub fn from_db(db: sled::Db) -> Self {
        Self { db }
    }

    #[cfg(not(feature = "v01-compat"))]
    fn get_tree(&self, scope: Arc<[u8]>) -> StorageResult<Tree> {
        self.db.open_tree(scope).map_err(StorageError::custom)
    }

    #[cfg(feature = "v01-compat")]
    fn get_tree(&self, scope: Arc<[u8]>) -> StorageResult<Tree> {
        if scope.as_ref() == &actix_storage::GLOBAL_SCOPE {
            Ok(self.db.deref().clone())
        } else {
            self.db.open_tree(scope).map_err(StorageError::custom)
        }
    }
}

#[async_trait::async_trait]
impl Store for SledStore {
    async fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> StorageResult<()> {
        match self.get_tree(scope)?.insert(key, value.as_ref()) {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::custom(err)),
        }
    }

    async fn set_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: i64) -> StorageResult<()> {
        match self.get_tree(scope)?.insert(key, &value.to_le_bytes()) {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::custom(err)),
        }
    }

    async fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> StorageResult<Option<Arc<[u8]>>> {
        Ok(self
            .get_tree(scope)?
            .get(key)
            .map_err(StorageError::custom)?
            .map(|val| val.as_ref().into()))
    }

    async fn get_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> StorageResult<Option<i64>> {
        self.get_tree(scope)?
            .get(key)
            .map_err(StorageError::custom)?
            .map(|val| {
                val.as_ref()
                    .try_into()
                    .map(i64::from_le_bytes)
                    .map_err(|_| StorageError::InvalidNumber)
            })
            .transpose()
    }

    async fn mutate(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        mutations: Mutation,
    ) -> StorageResult<()> {
        match self.get_tree(scope)?.update_and_fetch(key, |value| {
            let val = value.map(TryInto::<[u8; 8]>::try_into);

            let val = if let Some(val) = val {
                val.map(i64::from_le_bytes).unwrap_or_default()
            } else {
                0
            };

            Some(run_mutations(val, &mutations).to_le_bytes().to_vec())
        }) {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::custom(err)),
        }
    }

    async fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> StorageResult<()> {
        match self.get_tree(scope)?.remove(key) {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::custom(err)),
        }
    }

    async fn contains_key(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> StorageResult<bool> {
        match self.get_tree(scope)?.contains_key(key) {
            Ok(res) => Ok(res),
            Err(err) => Err(StorageError::custom(err)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::test_utils::*;
    use std::time::Duration;

    async fn open_database() -> sled::Db {
        let mut tries: u8 = 0;
        loop {
            tries += 1;
            let db = SledConfig::default().temporary(true).open();
            match db {
                Ok(db) => return db,
                Err(err) => {
                    if tries > 10 {
                        panic!("{}", err)
                    };
                }
            }
            actix::clock::sleep(Duration::from_millis(500)).await;
        }
    }

    #[test]
    fn test_sled_basic_store() {
        test_store(Box::pin(async {
            SledStore::from_db(open_database().await)
        }));
    }

    #[test]
    fn test_sled_basic_store_numbers() {
        test_store_numbers(Box::pin(async {
            SledStore::from_db(open_database().await)
        }));
    }

    #[test]
    fn test_sled_basic_mutate_numbers() {
        test_mutate_numbers(Box::pin(async {
            SledStore::from_db(open_database().await)
        }));
    }
}
