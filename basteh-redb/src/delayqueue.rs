use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};
use priority_queue::PriorityQueue;

#[derive(Default)]
pub(crate) struct DelayQueueInner {
    queue: Mutex<PriorityQueue<DelayedIem, Instant>>,
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

    pub fn remove(&self, scope: &str, key: &[u8]) {
        let item = DelayedIem {
            scope: String::from(scope),
            key: key.into(),
        };
        let mut queue = self.inner.queue.lock();
        let curr_head_until = queue.peek().map(|v| *v.1);

        if let Some(item) = queue.remove(&item) {
            if let Some(curr_head_until) = curr_head_until {
                if item.1 <= curr_head_until {
                    self.inner.condvar_new_head.notify_one();
                }
            }
        }
    }

    pub fn push(&self, scope: &str, key: &[u8], until: Instant) {
        let item = DelayedIem {
            scope: String::from(scope),
            key: key.into(),
        };

        let mut queue = self.inner.queue.lock();
        let curr_head = queue.peek();

        if curr_head.is_none() || (until < *curr_head.unwrap().1) {
            self.inner.condvar_new_head.notify_one();
        }

        queue.push(item, until);
    }

    pub fn try_pop_for(&self, duration: Duration) -> Option<DelayedIem> {
        let try_until = Instant::now() + duration;
        let mut queue = self.inner.queue.lock();

        // Loop until an element can be popped or the timeout expires, waiting if necessary
        loop {
            let now = Instant::now();
            if now >= try_until {
                return None;
            }

            let loop_try_until = match queue.peek() {
                Some(elem) if *elem.1 <= now => break,
                Some(elem) => (*elem.1).min(try_until),
                None => try_until,
            };

            self.inner
                .condvar_new_head
                .wait_until(&mut queue, loop_try_until);
        }

        if queue.len() > 1 {
            self.inner.condvar_new_head.notify_one();
        }

        queue.pop().map(|v| v.0)
    }

    pub fn is_dead(&mut self) -> bool {
        if self.owner_count.load(Ordering::SeqCst) == 0 {
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub(crate) struct DelayedIem {
    pub(crate) scope: String,
    pub(crate) key: Box<[u8]>,
}
