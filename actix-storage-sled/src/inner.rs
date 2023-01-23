use std::time::Duration;
use std::{convert::TryInto, sync::Arc};

use actix_storage::dev::Mutation;
use actix_storage::StorageError;

use crate::utils::run_mutations;

use super::message::{Message, Request, Response};
use crate::{
    decode, decode_mut,
    delayqueue::{DelayQueue, DelayedIem},
    encode, ExpiryFlags,
};

type Result<T> = std::result::Result<T, StorageError>;

#[inline]
pub(crate) fn open_tree(db: &sled::Db, scope: &[u8]) -> Result<sled::Tree> {
    db.open_tree(scope).map_err(StorageError::custom)
}

#[derive(Clone)]
pub(crate) struct SledInner {
    pub(crate) db: sled::Db,
    pub(crate) queue: DelayQueue,
}

impl SledInner {
    pub fn from_db(db: sled::Db) -> Self {
        Self {
            db,
            queue: DelayQueue::new(),
        }
    }

    pub fn scan_db(&mut self) {
        for tree_name in self.db.tree_names() {
            let tree = if let Ok(tree) = open_tree(&self.db, &tree_name) {
                tree
            } else {
                log::warn!("Failed to open tree {:?}", tree_name);
                continue;
            };

            let mut deleted_keys = vec![];
            for kv in tree.iter() {
                let (key, value) = if let Ok((key, value)) = kv {
                    (key, value)
                } else {
                    log::warn!(
                        "Failed to read key-value pair, {:?} in tree {:?}",
                        kv,
                        tree_name
                    );
                    continue;
                };

                if let Some((_, exp)) = decode(&value) {
                    if exp.expired() {
                        deleted_keys.push(key);
                    } else if let Some(dur) = exp.expires_in() {
                        self.queue.push(DelayedIem::new(
                            tree_name.to_vec().into(),
                            key.to_vec().into(),
                            exp.nonce.get(),
                            dur,
                        ));
                    }
                } else {
                    log::warn!("Failed to decode key ({:?}) in tree ({:?})", key, tree_name);
                }
            }
            for key in deleted_keys {
                tree.remove(&key).unwrap();
            }
        }
    }

    pub fn spawn_expiry_thread(&mut self) {
        let db = self.db.clone();
        let mut queue = self.queue.clone();

        tokio::task::spawn_blocking(move || loop {
            if let Some(item) = queue.try_pop_for(Duration::from_millis(500)) {
                let tree = if let Ok(tree) = open_tree(&db, &item.scope) {
                    tree
                } else {
                    log::error!("Failed to open tree {:?}", item.scope);
                    return;
                };

                let res = tree.get(&item.key).and_then(|val| {
                    if let Some(mut bytes) = val {
                        if let Some((_, exp)) = decode_mut(&mut bytes) {
                            if exp.nonce.get() == item.nonce && exp.persist.get() == 0 {
                                tree.remove(&item.key)?;
                            }
                        }
                    }
                    Ok(())
                });

                if let Err(err) = res {
                    log::error!("{}", err);
                }
            }
            if queue.is_dead() {
                break;
            };
        });
    }
}

/// Store methods
impl SledInner {
    pub fn set(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: Arc<[u8]>) -> Result<()> {
        let tree = open_tree(&self.db, &scope)?;
        tree.update_and_fetch(&key, |bytes| {
            let nonce = if let Some(bytes) = bytes {
                decode(&bytes)
                    .map(|(_, exp)| exp.next_nonce())
                    .unwrap_or_default()
            } else {
                0
            };

            let exp = ExpiryFlags::new_persist(nonce);
            let val = encode(&value, &exp);

            Some(val)
        })
        .map_err(StorageError::custom)?;
        Ok(())
    }

    pub fn set_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>, value: i64) -> Result<()> {
        self.set(scope, key, Arc::new(value.to_le_bytes()))
    }

    pub fn get(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Arc<[u8]>>> {
        let tree = open_tree(&self.db, &scope)?;
        tree.get(&key)
            .map(|val| {
                val.and_then(|bytes| {
                    let (val, exp) = decode(&bytes)?;
                    if !exp.expired() {
                        Some(val.into())
                    } else {
                        None
                    }
                })
            })
            .map_err(StorageError::custom)
    }

    pub fn get_number(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<i64>> {
        self.get(scope, key)?
            .map(|v| {
                v.as_ref()
                    .try_into()
                    .map(i64::from_le_bytes)
                    .map_err(|_| StorageError::InvalidNumber)
            })
            .transpose()
    }

    pub fn mutate(&self, scope: Arc<[u8]>, key: Arc<[u8]>, mutations: Mutation) -> Result<()> {
        match open_tree(&self.db, &scope)?.update_and_fetch(key, |existing| {
            let mut bytes = sled::IVec::from(existing?);

            let (val, exp) = decode_mut(&mut bytes)?;
            let val = if !exp.expired() {
                i64::from_le_bytes(val.try_into().unwrap_or_default())
            } else {
                0
            };

            let value = run_mutations(val, &mutations).to_le_bytes();

            let val = encode(&value, exp);

            Some(val)
        }) {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::custom(err)),
        }
    }

    pub fn delete(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
        let tree = open_tree(&self.db, &scope)?;
        tree.remove(&key).map(|_| ()).map_err(StorageError::custom)
    }

    pub fn contains(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<bool> {
        let tree = open_tree(&self.db, &scope)?;
        tree.contains_key(&key).map_err(StorageError::custom)
    }
}

/// Expiry methods
impl SledInner {
    pub fn set_expiry(
        &mut self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        duration: Duration,
    ) -> Result<()> {
        let mut nonce = 0;
        let tree = open_tree(&self.db, &scope)?;
        let val = tree
            .update_and_fetch(&key, |existing| {
                let mut bytes = sled::IVec::from(existing?);

                // If we can't decode the bytes, leave them as they are
                if let Some((_, exp)) = decode_mut(&mut bytes) {
                    exp.increase_nonce();
                    exp.expire_in(duration);
                    exp.persist.set(0);

                    // Sending values to outer scope
                    nonce = exp.nonce.get();
                }
                Some(bytes)
            })
            .map_err(StorageError::custom)?;

        // We can't add item to queue in update_and_fetch as it may run multiple times
        // before taking into effect.
        if val.is_some() {
            self.queue
                .push(DelayedIem::new(scope, key, nonce, duration));
        }
        Ok(())
    }

    pub fn get_expiry(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<Option<Duration>> {
        let tree = open_tree(&self.db, &scope)?;
        tree.get(&key)
            .map(|val| {
                val.and_then(|bytes| {
                    let (_, exp) = decode(&bytes)?;
                    exp.expires_in()
                })
            })
            .map_err(StorageError::custom)
    }

    pub fn persist(&self, scope: Arc<[u8]>, key: Arc<[u8]>) -> Result<()> {
        let tree = open_tree(&self.db, &scope)?;
        tree.update_and_fetch(&key, |existing| {
            let mut bytes = sled::IVec::from(existing?);
            if let Some((_, exp)) = decode_mut(&mut bytes) {
                exp.persist.set(1);
            }
            Some(bytes)
        })
        .map_err(StorageError::custom)?;
        Ok(())
    }

    pub fn extend_expiry(
        &mut self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        duration: Duration,
    ) -> Result<()> {
        let mut nonce = 0;
        let mut total_duration = None;
        let tree = open_tree(&self.db, &scope)?;
        tree.update_and_fetch(&key, |existing| {
            let mut bytes = sled::IVec::from(existing?);

            // If we can't decode the bytes, leave them as they are
            if let Some((_, exp)) = decode_mut(&mut bytes) {
                exp.increase_nonce();
                if let Some(expiry) = exp.expires_in() {
                    exp.expire_in(expiry + duration);
                } else {
                    exp.expire_in(duration);
                }
                exp.persist.set(0);

                // Sending values to outer scope to prevent decoding again
                nonce = exp.nonce.get();
                total_duration = exp.expires_in();
            }
            Some(bytes)
        })
        .map_err(StorageError::custom)?;
        if let Some(total_duration) = total_duration {
            self.queue
                .push(DelayedIem::new(scope, key, nonce, total_duration));
        }
        Ok(())
    }
}

/// Expiring store methods
impl SledInner {
    pub fn set_expiring(
        &mut self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
        value: Arc<[u8]>,
        duration: Duration,
    ) -> Result<()> {
        let tree = open_tree(&self.db, &scope)?;
        let mut nonce = 0;

        tree.update_and_fetch(key.as_ref(), |bytes| {
            nonce = if let Some(bytes) = bytes {
                decode(&bytes)
                    .map(|(_, exp)| exp.next_nonce())
                    .unwrap_or_default()
            } else {
                0
            };

            let exp = ExpiryFlags::new_expiring(nonce, duration);
            let val = encode(&value, &exp);

            Some(val)
        })
        .map_err(StorageError::custom)?;

        self.queue
            .push(DelayedIem::new(scope, key, nonce, duration));

        Ok(())
    }

    pub fn get_expiring(
        &self,
        scope: Arc<[u8]>,
        key: Arc<[u8]>,
    ) -> Result<Option<(Arc<[u8]>, Option<Duration>)>> {
        let tree = open_tree(&self.db, &scope)?;
        let val = tree.get(&key).map_err(StorageError::custom)?;
        Ok(val.and_then(|bytes| {
            let (val, exp) = decode(&bytes)?;
            if !exp.expired() {
                Some((val.into(), exp.expires_in()))
            } else {
                None
            }
        }))
    }
}

impl SledInner {
    pub fn listen(&mut self, rx: crossbeam_channel::Receiver<Message>) {
        while let Ok(Message { req, tx }) = rx.recv() {
            match req {
                // Store methods
                Request::Get(scope, key) => {
                    tx.send(self.get(scope, key).map(Response::Value)).ok();
                }
                Request::GetNumber(scope, key) => {
                    tx.send(self.get_number(scope, key).map(Response::Number))
                        .ok();
                }
                Request::Set(scope, key, value) => {
                    tx.send(self.set(scope, key, value).map(Response::Empty))
                        .ok();
                }
                Request::SetNumber(scope, key, value) => {
                    tx.send(self.set_number(scope, key, value).map(Response::Empty))
                        .ok();
                }
                Request::MutateNumber(scope, key, mutations) => {
                    tx.send(self.mutate(scope, key, mutations).map(Response::Empty))
                        .ok();
                }
                Request::Delete(scope, key) => {
                    tx.send(self.delete(scope, key).map(Response::Empty)).ok();
                }
                Request::Contains(scope, key) => {
                    tx.send(self.contains(scope, key).map(Response::Bool)).ok();
                }
                // Expiry methods
                Request::Persist(scope, key) => {
                    tx.send(self.persist(scope, key).map(Response::Empty)).ok();
                }
                Request::Expire(scope, key, dur) => {
                    tx.send(self.set_expiry(scope, key, dur).map(Response::Empty))
                        .ok();
                }
                Request::Expiry(scope, key) => {
                    tx.send(self.get_expiry(scope, key).map(Response::Duration))
                        .ok();
                }
                Request::Extend(scope, key, dur) => {
                    tx.send(self.extend_expiry(scope, key, dur).map(Response::Empty))
                        .ok();
                }
                // ExpiryStore methods
                Request::SetExpiring(scope, key, value, dur) => {
                    tx.send(
                        self.set_expiring(scope, key, value, dur)
                            .map(Response::Empty),
                    )
                    .ok();
                }
                Request::GetExpiring(scope, key) => {
                    tx.send(self.get_expiring(scope, key).map(Response::ValueDuration))
                        .ok();
                }
            }
        }
    }
}
