use std::{sync::Arc, time::Duration};

use delay_queue::{Delay, DelayQueue as DQ};

pub(crate) struct DelayedIem {
    pub scope: Arc<[u8]>,
    pub key: Arc<[u8]>,
    pub nonce: u64,
}

impl DelayedIem {
    pub fn new(scope: Arc<[u8]>, key: Arc<[u8]>, nonce: u64) -> Self {
        Self { scope, key, nonce }
    }
}

#[derive(Default, Clone)]
pub(crate) struct DelayQueue(DQ<Delay<DelayedIem>>);

impl DelayQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_for_duration(&mut self, item: DelayedIem, duration: Duration) {
        self.0.push(Delay::for_duration(item, duration))
    }

    pub fn try_pop_for(&mut self, duration: Duration) -> Option<DelayedIem> {
        if let Some(item) = self.0.try_pop_for(duration) {
            Some(item.value)
        } else {
            None
        }
    }
}
