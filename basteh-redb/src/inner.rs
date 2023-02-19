use std::{
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant},
};

use basteh::{
    dev::{Action, Mutation, OwnedValue},
    BastehError,
};
use redb::{Error, ReadableTable, TableDefinition};

use crate::{
    delayqueue::DelayQueue,
    flags::ExpiryFlags,
    message::{Message, Request, Response},
    value::OwnedValueWrapper,
};

macro_rules! table_def {
    ($var_name:ident, $name:expr) => {
        let $var_name = TableDefinition::<&[u8], OwnedValueWrapper>::new($name);
    };
}

macro_rules! exp_table_def {
    ($var_name:ident, $name:expr, $postfix:expr) => {
        let $var_name = {
            let mut __name = String::from($name);
            __name.push_str($postfix);
            __name
        };
        let $var_name = TableDefinition::<&[u8], ExpiryFlags>::new(&$var_name);
    };
}

#[derive(Clone)]
pub struct RedbInner {
    db: Arc<redb::Database>,
    exp_table: String,
    queue: DelayQueue,
    queue_started: bool,
}

impl RedbInner {
    pub(crate) fn from_db(db: redb::Database) -> Self {
        Self {
            db: Arc::new(db),
            exp_table: String::from("__EXPIRATIONS_TABLE__"),
            queue: DelayQueue::new(),
            queue_started: false,
        }
    }

    pub fn scan_db(&mut self) -> Result<(), Error> {
        let guard = self.db.begin_write()?;
        for table_name in guard.list_tables()? {
            table_def!(table, &table_name);
            exp_table_def!(exp_table, &table_name, &self.exp_table);

            let exp_table = if let Ok(table) = guard.open_table(exp_table) {
                table
            } else {
                // log::warn!("Failed to open tree {:?}", table_name);
                continue;
            };

            let mut deleted_keys = vec![];

            let exp_table_iter = if let Ok(exp_table_iter) = exp_table.iter() {
                exp_table_iter
            } else {
                // log::warn!("Failed to iterate over table {}", table_name);
                continue;
            };

            for (key, value) in exp_table_iter {
                let exp = value.value();
                if exp.expired() {
                    deleted_keys.push(key);
                } else if let Some(dur) = exp.expires_at() {
                    self.queue.push(&table_name, key.value(), dur);
                }
            }

            let mut table = if let Ok(table) = guard.open_table(table) {
                table
            } else {
                // log::warn!("Failed to open tree {:?}", tree_name);
                continue;
            };

            for key in deleted_keys {
                table.remove(&key.value()).ok();
            }
        }

        guard.commit()
    }

    pub fn spawn_expiry_thread(&mut self) {
        if !self.queue_started {
            self.queue_started = true;
        } else {
            return;
        }

        let db = self.db.clone();
        let mut queue = self.queue.clone();

        tokio::task::spawn_blocking(move || loop {
            if let Some(item) = queue.try_pop_for(Duration::from_millis(500)) {
                table_def!(table, &item.scope);

                (|| {
                    let txn = db.begin_write()?;
                    txn.open_table(table)?.remove(item.key.as_ref())?;
                    txn.commit()
                })()
                .ok();
            }
            if queue.is_dead() {
                break;
            };
        });
    }
}

impl RedbInner {
    fn keys(&self, scope: &str) -> Result<std::vec::IntoIter<Vec<u8>>, Error> {
        table_def!(table, scope);

        match self.db.begin_read()?.open_table(table) {
            Ok(r) => Ok(r
                .iter()?
                .map(|v| v.0.value().to_vec())
                .collect::<Vec<_>>()
                .into_iter()),
            Err(e) => match e {
                Error::TableDoesNotExist(_) => Ok(Vec::new().into_iter()),
                e => Err(e),
            },
        }
    }

    fn set(&self, scope: &str, key: &[u8], value: OwnedValue) -> Result<(), Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        txn.open_table(table)?.insert(key, value)?;
        txn.open_table(exp_table)?.remove(key)?;
        txn.commit()?;

        if self.queue_started {
            self.queue.remove(scope, key);
        }
        Ok(())
    }

    fn get(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>, Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        if let Ok(r) = self.db.begin_read()?.open_table(exp_table) {
            if let Some(true) = r.get(key)?.map(|v| v.value().expired()) {
                return Ok(None);
            }
        };

        match self.db.begin_read()?.open_table(table) {
            Ok(r) => Ok(r.get(key)?.map(|v| v.value())),
            Err(e) => match e {
                Error::TableDoesNotExist(_) => Ok(None),
                e => Err(e),
            },
        }
    }

    fn mutate(&self, scope: &str, key: &[u8], mutations: Mutation) -> Result<i64, Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        let value = {
            let mut table = txn.open_table(table)?;
            let mut expired = false;
            if let Ok(mut r) = txn.open_table(exp_table) {
                if r.get(key)?
                    .map(|v| v.value().expired().then_some(()))
                    .flatten()
                    .is_some()
                {
                    // If the key is already expired, remove it from queue and expiry table
                    // to make sure it won't get deleted or expired after mutation.
                    if self.queue_started {
                        self.queue.remove(scope, key);
                    }
                    r.remove(key)?;

                    expired = true;
                }
            };

            let current = if expired {
                None
            } else {
                table.remove(key)?.and_then(|v| v.value().try_into().ok())
            };
            let value = run_mutations(current.unwrap_or(0), &mutations);

            table.insert(key, OwnedValue::Number(value))?;
            value
        };
        txn.commit()?;

        Ok(value)
    }

    fn remove(&self, scope: &str, key: &[u8]) -> Result<Option<OwnedValue>, Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        let val = txn.open_table(table)?.remove(key)?.map(|v| v.value());
        txn.open_table(exp_table)?.remove(key)?;
        txn.commit()?;

        if self.queue_started {
            self.queue.remove(scope, key);
        }

        Ok(val)
    }

    fn contains_key(&self, scope: &str, key: &[u8]) -> Result<bool, Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        if let Ok(r) = self.db.begin_read()?.open_table(exp_table) {
            if let Some(true) = r.get(key)?.map(|v| v.value().expired()) {
                return Ok(false);
            }
        };

        Ok(self.db.begin_read()?.open_table(table)?.get(key)?.is_some())
    }

    pub fn expire(&mut self, scope: &str, key: &[u8], duration: Duration) -> Result<(), Error> {
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        txn.open_table(exp_table)?
            .insert(key, ExpiryFlags::new_expiring(duration))?;
        txn.commit()?;

        if self.queue_started {
            self.queue.push(scope, key, Instant::now() + duration);
        }
        Ok(())
    }

    pub fn expiry(&self, scope: &str, key: &[u8]) -> Result<Option<Duration>, Error> {
        exp_table_def!(exp_table, scope, &self.exp_table);

        match self.db.begin_read()?.open_table(exp_table) {
            Ok(r) => Ok(r.get(key)?.and_then(|v| v.value().expires_in())),
            Err(e) => match e {
                Error::TableDoesNotExist(_) => Ok(None),
                e => Err(e),
            },
        }
    }

    pub fn persist(&self, scope: &str, key: &[u8]) -> Result<(), Error> {
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        txn.open_table(exp_table)?
            .insert(key, ExpiryFlags::new_persist())?;
        txn.commit()?;

        if self.queue_started {
            self.queue.remove(scope, key);
        }
        Ok(())
    }

    pub fn extend(&mut self, scope: &str, key: &[u8], duration: Duration) -> Result<(), Error> {
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        let exp = {
            let exp = match txn.open_table(exp_table) {
                Ok(r) => r.get(key)?.map(|v| {
                    let mut exp = v.value();
                    exp.expire_in(duration);
                    exp
                }),
                Err(e) => match e {
                    Error::TableDoesNotExist(_) => None,
                    e => return Err(e),
                },
            };
            exp.map(|mut v| {
                v.expire_in(v.expires_in().map(|v| v + duration).unwrap_or(duration));
                v
            })
            .unwrap_or(ExpiryFlags::new_expiring(duration))
        };
        txn.open_table(exp_table)?.insert(key, exp)?;
        txn.commit()?;

        // FIXME
        self.queue.push(
            scope,
            key,
            Instant::now() + exp.expires_in().unwrap_or_default(),
        );

        Ok(())
    }

    pub fn set_expiring(
        &mut self,
        scope: &str,
        key: &[u8],
        value: OwnedValue,
        duration: Duration,
    ) -> Result<(), Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        let txn = self.db.begin_write()?;
        txn.open_table(table)?.insert(key, value)?;
        txn.open_table(exp_table)?
            .insert(key, ExpiryFlags::new_expiring(duration))?;
        txn.commit()?;

        if self.queue_started {
            self.queue.push(scope, key, Instant::now() + duration);
        }
        Ok(())
    }

    pub fn get_expiring(
        &self,
        scope: &str,
        key: &[u8],
    ) -> Result<Option<(OwnedValue, Option<Duration>)>, Error> {
        table_def!(table, scope);
        exp_table_def!(exp_table, scope, &self.exp_table);

        let exp_flags = match self.db.begin_read()?.open_table(exp_table) {
            Ok(r) => r.get(key)?.map(|v| v.value()),
            Err(e) => match e {
                Error::TableDoesNotExist(_) => None,
                e => return Err(e),
            },
        };

        if let Some(exp) = exp_flags {
            if exp.expired() {
                return Ok(None);
            }
        }

        let value = match self.db.begin_read()?.open_table(table) {
            Ok(r) => r.get(key)?.map(|v| v.value()),
            Err(e) => match e {
                Error::TableDoesNotExist(_) => None,
                e => return Err(e),
            },
        };

        Ok(value.map(|v| (v, exp_flags.and_then(|e| e.expires_in()))))
    }
}

pub(crate) fn run_mutations(mut value: i64, mutations: &Mutation) -> i64 {
    for act in mutations.iter() {
        match act {
            Action::Set(rhs) => {
                value = *rhs;
            }
            Action::Incr(rhs) => {
                value = value + rhs;
            }
            Action::Decr(rhs) => {
                value = value - rhs;
            }
            Action::Mul(rhs) => {
                value = value * rhs;
            }
            Action::Div(rhs) => {
                value = value / rhs;
            }
            Action::If(ord, rhs, ref sub) => {
                if value.cmp(&rhs) == *ord {
                    value = run_mutations(value, sub);
                }
            }
            Action::IfElse(ord, rhs, ref sub, ref sub2) => {
                if value.cmp(&rhs) == *ord {
                    value = run_mutations(value, sub);
                } else {
                    value = run_mutations(value, sub2);
                }
            }
        }
    }
    value
}

impl RedbInner {
    pub fn listen(&mut self, rx: crossbeam_channel::Receiver<Message>) {
        while let Ok(Message { req, tx }) = rx.recv() {
            match req {
                // Store methods
                Request::Keys(scope) => {
                    tx.send(
                        self.keys(&scope)
                            .map_err(BastehError::custom)
                            .map(|v| Response::Iterator(Box::new(v))),
                    )
                    .ok();
                }
                Request::Get(scope, key) => {
                    tx.send(
                        self.get(&scope, &key)
                            .map_err(BastehError::custom)
                            .map(Response::Value),
                    )
                    .ok();
                }
                Request::Set(scope, key, value) => {
                    tx.send(
                        self.set(&scope, &key, value)
                            .map_err(BastehError::custom)
                            .map(Response::Empty),
                    )
                    .ok();
                }
                Request::MutateNumber(scope, key, mutations) => {
                    tx.send(
                        self.mutate(&scope, &key, mutations)
                            .map_err(BastehError::custom)
                            .map(Response::Number),
                    )
                    .ok();
                }
                Request::Remove(scope, key) => {
                    tx.send(
                        self.remove(&scope, &key)
                            .map_err(BastehError::custom)
                            .map(Response::Value),
                    )
                    .ok();
                }
                Request::Contains(scope, key) => {
                    tx.send(
                        self.contains_key(&scope, &key)
                            .map_err(BastehError::custom)
                            .map(Response::Bool),
                    )
                    .ok();
                }
                // Expiry methods
                Request::Persist(scope, key) => {
                    tx.send(
                        self.persist(&scope, &key)
                            .map_err(BastehError::custom)
                            .map(Response::Empty),
                    )
                    .ok();
                }
                Request::Expire(scope, key, dur) => {
                    tx.send(
                        self.expire(&scope, &key, dur)
                            .map_err(BastehError::custom)
                            .map(Response::Empty),
                    )
                    .ok();
                }
                Request::Expiry(scope, key) => {
                    tx.send(
                        self.expiry(&scope, &key)
                            .map_err(BastehError::custom)
                            .map(Response::Duration),
                    )
                    .ok();
                }
                Request::Extend(scope, key, dur) => {
                    tx.send(
                        self.extend(&scope, &key, dur)
                            .map_err(BastehError::custom)
                            .map(Response::Empty),
                    )
                    .ok();
                }
                // ExpiryStore methods
                Request::SetExpiring(scope, key, value, dur) => {
                    tx.send(
                        self.set_expiring(&scope, &key, value, dur)
                            .map_err(BastehError::custom)
                            .map(Response::Empty),
                    )
                    .ok();
                }
                Request::GetExpiring(scope, key) => {
                    tx.send(
                        self.get_expiring(&scope, &key)
                            .map_err(BastehError::custom)
                            .map(Response::ValueDuration),
                    )
                    .ok();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc, time::Duration};

    use redb::TableDefinition;

    use super::*;

    impl RedbInner {
        fn from_arc_db(db: Arc<redb::Database>) -> Self {
            Self {
                db,
                exp_table: String::from("__EXPIRATIONS_TABLE__"),
                queue: DelayQueue::new(),
                queue_started: false,
            }
        }
    }

    fn open_database(path: &str) -> redb::Database {
        let p = Path::new(path);
        if p.exists() {
            std::fs::remove_file(p).ok();
        }
        redb::Database::create(path).unwrap()
    }

    #[tokio::test]
    async fn test_redb_perform_deletion() {
        let dur = Duration::from_secs(1);
        let table = TableDefinition::<&[u8], OwnedValueWrapper>::new("some_scope");
        let db = Arc::new(open_database("/tmp/redb.perform_deletion.db"));

        let mut store = RedbInner::from_arc_db(db.clone());
        store.spawn_expiry_thread();

        store
            .set_expiring(
                "some_scope",
                b"key",
                OwnedValue::Bytes(b"value".to_vec()),
                dur,
            )
            .unwrap();

        assert_eq!(
            store
                .get("some_scope", b"key")
                .unwrap()
                .map(|v| TryInto::<Vec<u8>>::try_into(v).unwrap()),
            Some(b"value".to_vec())
        );
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(table)
                .unwrap()
                .get(b"key".as_ref())
                .unwrap()
                .unwrap()
                .value(),
            OwnedValue::Bytes(b"value".to_vec())
        );

        tokio::time::sleep(dur * 2).await;

        assert!(db
            .begin_read()
            .unwrap()
            .open_table(table)
            .unwrap()
            .get(b"key".as_ref())
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_redb_scan_db() {
        let dur = Duration::from_secs(1);
        let table = TableDefinition::<&[u8], OwnedValueWrapper>::new("some_scope");
        let table2 = TableDefinition::<&[u8], OwnedValueWrapper>::new("some_scope2");
        let db = Arc::new(open_database("/tmp/redb.scan_db.db"));

        {
            exp_table_def!(exp_table, "some_scope", "__EXPIRATIONS_TABLE__");
            exp_table_def!(exp_table2, "some_scope2", "__EXPIRATIONS_TABLE__");

            let txn = db.begin_write().unwrap();
            txn.open_table(table)
                .unwrap()
                .insert(b"key".as_ref(), OwnedValue::Bytes(b"value".to_vec()))
                .unwrap();
            txn.open_table(table2)
                .unwrap()
                .insert(b"key2".as_ref(), OwnedValue::Bytes(b"value".to_vec()))
                .unwrap();

            txn.open_table(exp_table)
                .unwrap()
                .insert(
                    b"key".as_ref(),
                    ExpiryFlags::new_expiring(Duration::from_secs(1)),
                )
                .unwrap();

            txn.open_table(exp_table2)
                .unwrap()
                .insert(
                    b"key2".as_ref(),
                    ExpiryFlags::new_expiring(Duration::from_secs(1)),
                )
                .unwrap();

            txn.commit().unwrap();
        }

        let mut store = RedbInner::from_arc_db(db.clone());

        tokio::time::sleep(dur * 2).await;

        store.scan_db().unwrap();

        assert!(db
            .begin_read()
            .unwrap()
            .open_table(table)
            .unwrap()
            .get(b"key".as_ref())
            .unwrap()
            .map(|v| v.value())
            .is_none());

        assert!(db
            .begin_read()
            .unwrap()
            .open_table(table2)
            .unwrap()
            .get(b"key2".as_ref())
            .unwrap()
            .map(|v| v.value())
            .is_none());
    }
}
