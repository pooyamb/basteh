use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

use futures::StreamExt;
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_util::time::DelayQueue;

#[derive(Debug, thiserror::Error)]
#[error("The internal expiry queue channel is closed")]
pub struct DelayQueueChannelError;

#[derive(Debug)]
pub(crate) enum Commands<T> {
    InsertOrUpdate(T, Duration),
    Get(T, oneshot::Sender<Option<Duration>>),
    Remove(T),
    Extend(T, Duration),
}

#[derive(Debug, Clone)]
pub(crate) struct DelayQueueSender<T>(mpsc::Sender<Commands<T>>);

impl<T> DelayQueueSender<T> {
    pub(crate) async fn insert_or_update(
        &self,
        item: T,
        timeout: Duration,
    ) -> Result<(), DelayQueueChannelError> {
        self.0
            .send(Commands::InsertOrUpdate(item, timeout))
            .await
            .map_err(|_| DelayQueueChannelError)
    }

    pub(crate) async fn get(&self, item: T) -> Result<Option<Duration>, DelayQueueChannelError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(Commands::Get(item, tx))
            .await
            .map_err(|_| DelayQueueChannelError)?;

        rx.await.map_err(|_| DelayQueueChannelError)
    }

    pub(crate) async fn remove(&self, item: T) -> Result<(), DelayQueueChannelError> {
        self.0
            .send(Commands::Remove(item))
            .await
            .map_err(|_| DelayQueueChannelError)
    }

    pub(crate) async fn extend(
        &self,
        item: T,
        timeout: Duration,
    ) -> Result<(), DelayQueueChannelError> {
        self.0
            .send(Commands::Extend(item, timeout))
            .await
            .map_err(|_| DelayQueueChannelError)
    }
}

pub(crate) fn delayqueue<T>(
    input_buffer: usize,
    output_buffer: usize,
) -> (DelayQueueSender<T>, mpsc::Receiver<T>)
where
    T: 'static + Debug + Hash + Eq + Send + Clone,
{
    let mut dq = DelayQueue::new();

    // A map used to associate values with expiry times and internal keys
    let mut ids = HashMap::new();

    // Command channel to receive add/delete/query orders
    let (queue_write, mut rx) = mpsc::channel::<Commands<T>>(input_buffer);

    // Stream channel to return expired items
    let (tx, queue_read) = mpsc::channel::<T>(output_buffer);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(expired) = dq.next(), if !dq.is_empty() => {
                    let value = expired.into_inner();
                    ids.remove(&value);
                    tx.send(value).await.ok();
                },
                message = rx.recv() => {
                    if let Some(command) = message {
                        match command {
                            Commands::InsertOrUpdate(value, timeout) => {
                                if let Some((key, _)) = ids.remove(&value) {
                                    dq.reset(&key, timeout);
                                    ids.insert(value, (key, Instant::now() + timeout));
                                } else {
                                    let key = dq.insert(value.clone(), timeout);
                                    ids.insert(value, (key, Instant::now() + timeout));
                                }
                            }
                            Commands::Get(value, oneshottx) => {
                                // We don't care if the receiver has dropped
                                // as it doesn't affect our internal state
                                let _ = if let Some((_, timeout)) = ids.get(&value) {
                                    oneshottx.send(
                                        timeout
                                            .checked_duration_since(Instant::now()),
                                    )
                                } else {
                                    oneshottx.send(None)
                                };
                            }
                            Commands::Remove(value) => {
                                if let Some((key, _)) = ids.get(&value) {
                                    dq.remove(key);
                                    ids.remove(&value);
                                }
                            }
                            Commands::Extend(value, extend_by) => {
                                if let Some((key, timeout)) = ids.remove(&value) {
                                    let new_timeout = timeout + extend_by;
                                    dq.reset_at(&key, new_timeout);
                                    ids.insert(value, (key, new_timeout));
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    (DelayQueueSender(queue_write), queue_read)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::sleep;

    use super::*;

    #[tokio::test]
    async fn test_read() {
        let dur1 = Duration::from_secs(1);
        let dur2 = Duration::from_secs(2);
        let (tx, mut rx) = delayqueue::<i32>(1, 1);

        tx.insert_or_update(5, dur1).await.unwrap();
        tx.insert_or_update(10, dur2).await.unwrap();
        assert!(rx.try_recv().is_err());

        sleep(dur1).await;
        let next_item = rx.recv().await;
        assert!(next_item.is_some());
        assert!(next_item.unwrap() == 5);
        assert!(rx.try_recv().is_err());

        sleep(dur1).await;
        let next_item = rx.recv().await;
        assert!(next_item.is_some());
        assert!(next_item.unwrap() == 10);
    }

    #[tokio::test]
    async fn test_send() {
        let dur1 = Duration::from_secs(1);
        let dur2 = Duration::from_secs(2);
        let (tx, _) = delayqueue::<i32>(1, 1);

        tx.insert_or_update(5, dur1).await.unwrap();
        let exp = tx.get(5).await;
        assert!(exp.is_ok());
        assert!(exp.unwrap().unwrap().as_secs() <= 1);

        tx.extend(5, dur2).await.unwrap();
        let exp = tx.get(5).await;
        assert!(exp.is_ok());
        let exp = exp.unwrap().unwrap();
        assert!(exp.as_secs() <= 3);
        assert!(exp.as_secs() >= 2);

        let r = tx.remove(5).await;
        assert!(r.is_ok());

        let exp = tx.get(5).await;
        assert!(exp.is_ok());
        assert!(exp.unwrap().is_none())
    }
}
