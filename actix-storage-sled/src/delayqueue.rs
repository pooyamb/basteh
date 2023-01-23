use std::{
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};

#[derive(Default)]
pub(crate) struct DelayQueueInner {
    queue: Mutex<BinaryHeap<DelayedIem>>,
    condvar_new_head: Condvar,
}

#[derive(Default)]
pub(crate) struct DelayQueue {
    inner: Arc<DelayQueueInner>,
    owner_count: Arc<AtomicU64>,
}

impl Clone for DelayQueue {
    fn clone(&self) -> Self {
        self.owner_count.fetch_add(1, Ordering::SeqCst);

        Self {
            inner: self.inner.clone(),
            owner_count: self.owner_count.clone(),
        }
    }
}

impl Drop for DelayQueue {
    fn drop(&mut self) {
        self.owner_count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl DelayQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, item: DelayedIem) {
        let mut queue = self.inner.queue.lock();

        let curr_head = queue.peek();
        if curr_head.is_none() || (item.until < curr_head.unwrap().until) {
            self.inner.condvar_new_head.notify_one();
        }

        queue.push(item);
    }

    pub fn try_pop_for(&mut self, duration: Duration) -> Option<DelayedIem> {
        let try_until = Instant::now() + duration;
        let mut queue = self.inner.queue.lock();

        // Loop until an element can be popped or the timeout expires, waiting if necessary
        loop {
            let now = Instant::now();
            if now >= try_until {
                return None;
            }

            let loop_try_until = match queue.peek() {
                Some(elem) if elem.until <= now => break,
                Some(elem) => elem.until.min(try_until),
                None => try_until,
            };

            self.inner
                .condvar_new_head
                .wait_until(&mut queue, loop_try_until);
        }

        if queue.len() > 1 {
            self.inner.condvar_new_head.notify_one();
        }

        queue.pop()
    }

    pub fn is_dead(&mut self) -> bool {
        if self.owner_count.load(Ordering::SeqCst) == 0 {
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub(crate) struct DelayedIem {
    pub scope: Arc<[u8]>,
    pub key: Arc<[u8]>,
    pub until: Instant,
    pub nonce: u64,
}

impl DelayedIem {
    pub fn new(scope: Arc<[u8]>, key: Arc<[u8]>, nonce: u64, duration: Duration) -> Self {
        Self {
            scope,
            key,
            nonce,
            until: Instant::now() + duration,
        }
    }
}

impl Ord for DelayedIem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.until.cmp(&other.until)
    }
}

impl PartialOrd for DelayedIem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DelayedIem {
    fn eq(&self, other: &Self) -> bool {
        self.until == other.until
    }
}

impl Eq for DelayedIem {}
