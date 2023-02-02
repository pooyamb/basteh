use std::{collections::HashMap, convert::TryInto, sync::Arc, time::Duration};

use basteh::{
    dev::{Expiry, ExpiryStore, Mutation, Store},
    Result, StorageError,
};
use parking_lot::Mutex;

use crate::delayqueue::{delayqueue, DelayQueueSender};
use crate::utils::run_mutations;

type ScopeMap = HashMap<Arc<[u8]>, Arc<[u8]>>;
type InternalMap = HashMap<Arc<[u8]>, ScopeMap>;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct ExpiryKey {
    pub(crate) scope: Arc<[u8]>,
    pub(crate) key: Arc<[u8]>,
}

impl ExpiryKey {
    pub fn new(scope: Arc<[u8]>, key: Arc<[u8]>) -> Self {
        Self { scope, key }
    }
}

/// An implementation of [`ExpiryStore`](basteh::dev::ExpiryStore) based on Arc-Mutex-Hashmap
/// using tokio's delayqueue for expiration.
///
/// ## Example
/// ```no_run
/// use basteh::Storage;
/// use basteh_memory::{MemoryBackend};
///
/// # async fn your_main() {
/// let store = MemoryBackend::start_default();
/// let storage = Storage::build().store(store).finish();
/// # }
/// ```
///
#[derive(Clone)]
pub struct MemoryBackend {
    map: Arc<Mutex<InternalMap>>,

    // Send part of the channel used to send commands to delayqueue
    dq_tx: DelayQueueSender<ExpiryKey>,
}

impl MemoryBackend {
    pub fn start(buffer_size: usize) -> Self {
        let (dq_tx, mut dq_rx) = delayqueue::<ExpiryKey>(buffer_size, buffer_size);
        let map = Arc::new(Mutex::new(InternalMap::new()));

        let map_clone = map.clone();
        tokio::spawn(async move {
            while let Some(exp) = dq_rx.recv().await {
                map_clone
                    .lock()
                    .get_mut(&exp.scope)
                    .and_then(|scope_map| scope_map.remove(&exp.key));
            }
        });

        Self { map, dq_tx }
    }

    pub fn start_default() -> Self {
        Self::start(2048)
    }
}

#[async_trait::async_trait]
impl Store for MemoryBackend {
    async fn keys(&self, scope: &[u8]) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
        Ok(Box::new(
            self.map
                .lock()
                .entry(scope.into())
                .or_default()
                .keys()
                .map(|k| k.to_vec())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }

    async fn set(&self, scope: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        if self
            .map
            .lock()
            .entry(scope.into())
            .or_default()
            .insert(key.into(), value.into())
            .is_some()
        {
            self.dq_tx
                .remove(ExpiryKey::new(scope.into(), key.into()))
                .await
                .map_err(StorageError::custom)?;
        }
        Ok(())
    }

    async fn set_number(&self, scope: &[u8], key: &[u8], value: i64) -> Result<()> {
        self.set(scope, key, &value.to_le_bytes()).await
    }

    async fn get(&self, scope: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .map
            .lock()
            .get(scope.as_ref())
            .and_then(|scope_map| scope_map.get(key.into()))
            .map(|value| value.to_vec()))
    }

    async fn get_number(&self, scope: &[u8], key: &[u8]) -> Result<Option<i64>> {
        self.get(scope, key)
            .await?
            .map(|val| {
                val.try_into()
                    .map_err(|_| StorageError::InvalidNumber)
                    .map(i64::from_le_bytes)
            })
            .transpose()
    }

    async fn mutate(&self, scope: &[u8], key: &[u8], mutations: Mutation) -> Result<()> {
        let mut guard = self.map.lock();
        let scope_map = guard.entry(scope.into()).or_default();

        let value = if let Some(val) = scope_map.get(key.into()) {
            let num = val
                .as_ref()
                .try_into()
                .map(i64::from_le_bytes)
                .map_err(StorageError::custom)?;
            num
        } else {
            0
        };

        let value = run_mutations(value, mutations);

        if let Some(value) = value {
            scope_map.insert(key.into(), Arc::new(value.to_le_bytes()));
            Ok(())
        } else {
            Err(StorageError::InvalidNumber)
        }
    }

    async fn delete(&self, scope: &[u8], key: &[u8]) -> Result<()> {
        if self
            .map
            .lock()
            .get_mut(scope.into())
            .and_then(|scope_map| scope_map.remove(key.into()))
            .is_some()
        {
            self.dq_tx
                .remove(ExpiryKey::new(scope.into(), key.into()))
                .await
                .ok();
        }
        Ok(())
    }

    async fn contains_key(&self, scope: &[u8], key: &[u8]) -> Result<bool> {
        Ok(self
            .map
            .lock()
            .get(scope.into())
            .map(|scope_map| scope_map.contains_key(key.into()))
            .unwrap_or(false))
    }
}

#[async_trait::async_trait]
impl Expiry for MemoryBackend {
    async fn persist(&self, scope: &[u8], key: &[u8]) -> Result<()> {
        self.dq_tx
            .remove(ExpiryKey::new(scope.into(), key.into()))
            .await
            .map_err(StorageError::custom)
    }

    async fn expire(&self, scope: &[u8], key: &[u8], expire_in: Duration) -> Result<()> {
        self.dq_tx
            .insert_or_update(ExpiryKey::new(scope.into(), key.into()), expire_in)
            .await
            .map_err(StorageError::custom)
    }

    async fn expiry(&self, scope: &[u8], key: &[u8]) -> Result<Option<Duration>> {
        self.dq_tx
            .get(ExpiryKey::new(scope.into(), key.into()))
            .await
            .map_err(StorageError::custom)
    }

    async fn extend(&self, scope: &[u8], key: &[u8], duration: Duration) -> Result<()> {
        self.dq_tx
            .extend(ExpiryKey::new(scope.into(), key.into()), duration)
            .await
            .map_err(|e| StorageError::custom(e))
    }
}

#[async_trait::async_trait]
impl ExpiryStore for MemoryBackend {
    async fn set_expiring(
        &self,
        scope: &[u8],
        key: &[u8],
        value: &[u8],
        expire_in: Duration,
    ) -> Result<()> {
        self.map
            .lock()
            .entry(scope.into())
            .or_default()
            .insert(key.into(), value.into());
        self.dq_tx
            .insert_or_update(ExpiryKey::new(scope.into(), key.into()), expire_in)
            .await
            .map_err(|e| StorageError::custom(e))
    }

    async fn get_expiring(
        &self,
        scope: &[u8],
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Option<Duration>)>> {
        let val = self
            .map
            .lock()
            .get(scope.into())
            .and_then(|scope_map| scope_map.get(key.into()))
            .cloned();
        if let Some(val) = val {
            let exp = self
                .dq_tx
                .get(ExpiryKey::new(scope.into(), key.into()))
                .await
                .map_err(|e| StorageError::custom(e))?;
            Ok(Some((val.as_ref().to_vec(), exp)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basteh::test_utils::*;

    #[tokio::test]
    async fn test_hashmap_store() {
        test_store(MemoryBackend::start_default()).await;
    }

    #[tokio::test]
    async fn test_hashmap_store_numbers() {
        test_store_numbers(MemoryBackend::start_default()).await;
    }

    #[tokio::test]
    async fn test_hashmap_mutate_numbers() {
        test_mutate_numbers(MemoryBackend::start_default()).await;
    }

    #[tokio::test]
    async fn test_hashmap_expiry() {
        let store = MemoryBackend::start_default();
        test_expiry(store.clone(), store, 2).await;
    }

    #[tokio::test]
    async fn test_hashmap_expiry_store() {
        test_expiry_store(MemoryBackend::start_default(), 2).await;
    }
}
