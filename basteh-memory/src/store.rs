use std::{collections::HashMap, sync::Arc, time::Duration};

use basteh::{
    dev::{Mutation, OwnedValue, Provider, Value},
    BastehError, Result,
};
use parking_lot::Mutex;

use crate::delayqueue::{delayqueue, DelayQueueSender};
use crate::utils::run_mutations;

type ScopeMap = HashMap<Arc<[u8]>, OwnedValue>;
type InternalMap = HashMap<Arc<str>, ScopeMap>;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct ExpiryKey {
    pub(crate) scope: Arc<str>,
    pub(crate) key: Arc<[u8]>,
}

impl ExpiryKey {
    pub fn new(scope: Arc<str>, key: Arc<[u8]>) -> Self {
        Self { scope, key }
    }
}

/// An implementation of [`ExpiryStore`](basteh::dev::ExpiryStore) based on Arc-Mutex-Hashmap
/// using tokio's delayqueue for expiration.
///
/// ## Example
/// ```no_run
/// use basteh::Basteh;
/// use basteh_memory::{MemoryBackend};
///
/// # async fn your_main() {
/// let provider = MemoryBackend::start_default();
/// let storage = Basteh::build().provider(provider).finish();
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
impl Provider for MemoryBackend {
    async fn keys(&self, scope: &str) -> Result<Box<dyn Iterator<Item = Vec<u8>>>> {
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

    async fn set(&self, scope: &str, key: &[u8], value: Value<'_>) -> Result<()> {
        let scope: Arc<str> = scope.into();
        let key: Arc<[u8]> = key.into();

        if self
            .map
            .lock()
            .entry(scope.clone())
            .or_default()
            .insert(key.clone(), value.into_owned().into())
            .is_some()
        {
            self.dq_tx
                .remove(ExpiryKey::new(scope, key))
                .await
                .map_err(BastehError::custom)?;
        }
        Ok(())
    }

    async fn get<'a>(&'a self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
        Ok(self
            .map
            .lock()
            .get(scope)
            .and_then(|scope_map| scope_map.get(key))
            .map(|value| value.clone()))
    }

    async fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<i64> {
        let mut guard = self.map.lock();
        let scope_map = guard.entry(scope.into()).or_default();

        let value = if let Some(val) = scope_map.get(key) {
            let num = match val {
                OwnedValue::Number(n) => *n,
                _ => return Err(BastehError::InvalidNumber),
            };
            num
        } else {
            0
        };

        let value = run_mutations(value, mutations);

        if let Some(value) = value {
            scope_map.insert(key.into(), OwnedValue::Number(value));
            Ok(value)
        } else {
            Err(BastehError::InvalidNumber)
        }
    }

    async fn remove(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>> {
        let value = self
            .map
            .lock()
            .get_mut(scope)
            .and_then(|scope_map| scope_map.remove(key));

        if value.is_some() {
            self.dq_tx
                .remove(ExpiryKey::new(scope.into(), key.into()))
                .await
                .ok();
        }

        Ok(value)
    }

    async fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool> {
        Ok(self
            .map
            .lock()
            .get(scope)
            .map(|scope_map| scope_map.contains_key(key))
            .unwrap_or(false))
    }

    async fn persist(&self, scope: &str, key: &[u8]) -> Result<()> {
        self.dq_tx
            .remove(ExpiryKey::new(scope.into(), key.into()))
            .await
            .map_err(BastehError::custom)
    }

    async fn expire(&self, scope: &str, key: &[u8], expire_in: Duration) -> Result<()> {
        self.dq_tx
            .insert_or_update(ExpiryKey::new(scope.into(), key.into()), expire_in)
            .await
            .map_err(BastehError::custom)
    }

    async fn expiry(&self, scope: &str, key: &[u8]) -> Result<Option<Duration>> {
        self.dq_tx
            .get(ExpiryKey::new(scope.into(), key.into()))
            .await
            .map_err(BastehError::custom)
    }

    async fn extend(&self, scope: &str, key: &[u8], duration: Duration) -> Result<()> {
        self.dq_tx
            .extend(ExpiryKey::new(scope.into(), key.into()), duration)
            .await
            .map_err(|e| BastehError::custom(e))
    }

    async fn set_expiring(
        &self,
        scope: &str,
        key: &[u8],
        value: Value<'_>,
        expire_in: Duration,
    ) -> Result<()> {
        let scope: Arc<str> = scope.into();
        let key: Arc<[u8]> = key.into();

        self.map
            .lock()
            .entry(scope.clone())
            .or_default()
            .insert(key.clone(), value.to_owned().into());
        self.dq_tx
            .insert_or_update(ExpiryKey::new(scope, key), expire_in)
            .await
            .map_err(|e| BastehError::custom(e))
    }

    async fn get_expiring(
        &self,
        scope: &str,
        key: &[u8],
    ) -> Result<Option<(OwnedValue, Option<Duration>)>> {
        let val = self
            .map
            .lock()
            .get(scope)
            .and_then(|scope_map| scope_map.get(key))
            .cloned();
        if let Some(val) = val {
            let exp = self
                .dq_tx
                .get(ExpiryKey::new(scope.into(), key.into()))
                .await
                .map_err(|e| BastehError::custom(e))?;
            Ok(Some((val.clone(), exp)))
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
    async fn test_hashmap_mutations() {
        test_mutations(MemoryBackend::start_default()).await;
    }

    #[tokio::test]
    async fn test_hashmap_expiry() {
        test_expiry(MemoryBackend::start_default(), 2).await;
    }

    #[tokio::test]
    async fn test_hashmap_expiry_store() {
        test_expiry_store(MemoryBackend::start_default(), 2).await;
    }
}
