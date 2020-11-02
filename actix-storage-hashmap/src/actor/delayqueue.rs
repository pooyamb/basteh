#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::StreamExt;
use tokio::{
    stream::Stream,
    sync::{mpsc, oneshot},
    time::{
        delay_queue::{self, DelayQueue},
        Instant,
    },
};

#[derive(Debug)]
pub(crate) enum Commands<T> {
    InsertOrUpdate(T, Duration),
    Get(T, oneshot::Sender<Option<Duration>>),
    Remove(T),
    Extend(T, Duration),
}

#[derive(Debug)]
pub(crate) enum EmergencyCommand<T> {
    Kill,
    Restart(oneshot::Sender<DelayQueueReceiver<T>>),
}

pub(crate) struct Expired<T> {
    item: T,
    deadline: Instant,
}

impl<T> Expired<T> {
    pub fn into_inner(self) -> T {
        self.item
    }
}

impl<T> From<delay_queue::Expired<T>> for Expired<T> {
    fn from(item: delay_queue::Expired<T>) -> Self {
        Self {
            deadline: item.deadline(),
            item: item.into_inner(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("Channel is closed")]
    Closed,
    #[error("Channel is full and can't accept new items")]
    Full,
    #[error("Channel is empty and there is nothing to read")]
    Empty,
}

#[derive(Debug, Clone)]
pub(crate) struct DelayQueueSender<T>(mpsc::Sender<Commands<T>>);

impl<T> DelayQueueSender<T> {
    pub(crate) async fn insert_or_update(
        &mut self,
        item: T,
        timeout: Duration,
    ) -> Result<(), ChannelError> {
        self.0
            .send(Commands::InsertOrUpdate(item, timeout))
            .await
            .map_err(|_| ChannelError::Closed)
    }

    pub(crate) async fn get(&mut self, item: T) -> Result<Option<Duration>, ChannelError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(Commands::Get(item, tx))
            .await
            .map_err(|_| ChannelError::Closed)?;

        rx.await.map_err(|_| ChannelError::Closed)
    }

    pub(crate) async fn remove(&mut self, item: T) -> Result<(), ChannelError> {
        self.0
            .send(Commands::Remove(item))
            .await
            .map_err(|_| ChannelError::Closed)
    }

    pub(crate) async fn extend(&mut self, item: T, timeout: Duration) -> Result<(), ChannelError> {
        self.0
            .send(Commands::Extend(item, timeout))
            .await
            .map_err(|_| ChannelError::Closed)
    }
}

#[derive(Debug)]
pub(crate) struct DelayQueueReceiver<T>(mpsc::Receiver<Expired<T>>);

impl<T> DelayQueueReceiver<T>
where
    T: Clone,
{
    pub(crate) async fn receive(&mut self) -> Option<Expired<T>> {
        self.0.recv().await
    }

    pub(crate) fn try_receive(&mut self) -> Result<Expired<T>, ChannelError> {
        self.0.try_recv().map_err(|err| match err {
            mpsc::error::TryRecvError::Empty => ChannelError::Empty,
            mpsc::error::TryRecvError::Closed => ChannelError::Closed,
        })
    }
}

impl<T> Stream for DelayQueueReceiver<T> {
    type Item = Expired<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DelayQueueEmergency<T>(mpsc::Sender<EmergencyCommand<T>>);

impl<T> DelayQueueEmergency<T> {
    pub async fn kill(&mut self) -> Result<(), ChannelError> {
        self.0
            .send(EmergencyCommand::Kill)
            .await
            .map_err(|_| ChannelError::Closed)?;
        Ok(())
    }

    pub async fn restart(&mut self) -> Result<DelayQueueReceiver<T>, ChannelError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(EmergencyCommand::Restart(tx))
            .await
            .map_err(|_| ChannelError::Closed)?;

        rx.await.map_err(|_| ChannelError::Closed)
    }
}

pub(crate) fn delayqueue<T>(
    input_buffer: usize,
    output_buffer: usize,
) -> (
    DelayQueueSender<T>,
    DelayQueueReceiver<T>,
    DelayQueueEmergency<T>,
)
where
    T: 'static + Debug + Hash + Eq + Send + Clone,
{
    let mut dq = DelayQueue::new();
    let mut ids = HashMap::new();

    // Command channel to receive add/delete/query orders
    let (queue_write, mut rx) = mpsc::channel::<Commands<T>>(input_buffer);

    // Stream channel to return expired items
    let (mut tx, queue_read) = mpsc::channel::<Expired<T>>(output_buffer);

    // Emergency channel
    let (etx, erx) = mpsc::channel::<EmergencyCommand<T>>(1);

    let mut erx = erx.peekable();

    let mut senders_dropped = false;

    tokio::spawn(async move {
        'main: loop {
            'task: loop {
                tokio::select! {
                    Some(item) = dq.next(), if !dq.is_empty() => {
                        if let Ok(expired) = item {
                            ids.remove(expired.get_ref());

                            // If the receiver is dropped, we close the task
                            // and put the last item back in queue
                            if let Err(err) = tx.send(expired.into()).await{
                                let item = err.0.into_inner();
                                let key = dq.insert(item.clone(), Duration::default());
                                ids.insert(item, (key, Instant::now()));
                                break 'task;
                            };
                        }
                    },
                    message = rx.next(), if !senders_dropped => {
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
                                    if let Some((_, timeout)) = ids.get(&value) {
                                        oneshottx.send(
                                            timeout
                                                .checked_duration_since(Instant::now()),
                                        )
                                    } else {
                                        oneshottx.send(None)
                                    }
                                    .ok();
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
                        } else if message.is_none() {
                            // If we got None, all the senders have dropped
                            // we continue giving away all the remaining expired items
                            senders_dropped = true;
                        }
                    },
                    Some(_) = Pin::new(&mut erx).peek() => {
                        break 'task;
                    }
                }
            }

            // We wait on emergency channel to stablish a new possible connection
            'emergency: loop {
                match erx.next().await {
                    Some(cm) => match cm {
                        EmergencyCommand::Kill => {
                            // We are called to close explicity
                            break 'main;
                        }
                        EmergencyCommand::Restart(ch) => {
                            let new_channel = mpsc::channel::<Expired<T>>(16);
                            tx = new_channel.0;
                            if ch.send(DelayQueueReceiver(new_channel.1)).is_err() {
                                // Receiver channel have dropped, we go back to emergency mode
                                continue;
                            } else {
                                break 'emergency;
                            }
                        }
                    },
                    None => {
                        // emergency channel have dropped, there is no way to reconver
                        break 'main;
                    }
                }
            }
        }
    });

    (
        DelayQueueSender(queue_write),
        DelayQueueReceiver(queue_read),
        DelayQueueEmergency(etx),
    )
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::time::delay_for;

    use super::*;

    #[actix_rt::test]
    async fn test_read() {
        let dur1 = Duration::from_secs(1);
        let dur2 = Duration::from_secs(2);
        let (mut tx, mut rx, _) = delayqueue::<i32>(1, 1);

        tx.insert_or_update(5, dur1).await.unwrap();
        tx.insert_or_update(10, dur2).await.unwrap();
        assert!(rx.try_receive().is_err());

        delay_for(dur1).await;
        let next_item = rx.next().await.map(|val| val.item);
        assert!(next_item.is_some());
        assert!(next_item.unwrap() == 5);
        assert!(rx.try_receive().is_err());

        delay_for(dur1).await;
        let next_item = rx.receive().await.map(|val| val.item);
        assert!(next_item.is_some());
        assert!(next_item.unwrap() == 10);
    }

    #[actix_rt::test]
    async fn test_send() {
        let dur1 = Duration::from_secs(1);
        let dur2 = Duration::from_secs(2);
        let (mut tx, _, _) = delayqueue::<i32>(1, 1);

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

    #[actix_rt::test]
    async fn test_emergency() {
        let dur1 = Duration::from_secs(1);
        let (mut tx, mut rx, mut etx) = delayqueue::<i32>(1, 1);

        drop(rx);

        assert!(tx.insert_or_update(10, dur1).await.is_ok());
        assert!(tx.get(10).await.unwrap().is_some());

        let new_channel = etx.restart().await;
        assert!(new_channel.is_ok());
        rx = new_channel.unwrap();
        delay_for(Duration::from_secs(2)).await;
        let next = rx.try_receive().unwrap();
        assert!(next.item == 10);

        assert!(etx.kill().await.is_ok());
        delay_for(Duration::from_millis(10)).await;
        assert!(tx.insert_or_update(5, dur1).await.is_err());
    }

    #[actix_rt::test]
    async fn test_emergency_droped() {
        let dur1 = Duration::from_secs(1);

        let mut tx = {
            let (tx, rx, etx) = delayqueue::<i32>(1, 1);
            drop(rx);
            drop(etx);
            tx
        };

        assert!(tx.insert_or_update(5, Duration::from_secs(0)).await.is_ok());
        // Wait for dealy queue to notice the problem and drop
        delay_for(Duration::from_millis(100)).await;
        assert!(tx.insert_or_update(5, dur1).await.is_err());
    }
}
