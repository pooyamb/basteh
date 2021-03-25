use std::time::Duration;

use byteorder::LittleEndian;
use zerocopy::{AsBytes, FromBytes, Unaligned, U16, U64};

use super::utils::get_current_timestamp;

/// Represents expiry data and is stored as suffix to the value.
///
/// Nonce is used to ignore expiration requests after the value has changed as we don't have direct access to delay-queue
/// for removing notifications from it.
#[derive(Debug, Default, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
pub struct ExpiryFlags {
    pub nonce: U64<LittleEndian>,
    pub expires_at: U64<LittleEndian>,
    pub persist: U16<LittleEndian>,
}

impl ExpiryFlags {
    /// Make a new flags struct with persist flag set to true. Provide 0 for nonce if it's a new key.
    pub fn new_persist(nonce: u64) -> Self {
        Self {
            nonce: U64::new(nonce),
            expires_at: U64::new(0),
            persist: U16::new(1),
        }
    }

    /// Make a new flags struct with persist flag set to false. Provide 0 for nonce if it's a new key.
    pub fn new_expiring(nonce: u64, expires_in: Duration) -> Self {
        let expires_at = get_current_timestamp() + expires_in.as_secs();
        Self {
            nonce: U64::new(nonce),
            expires_at: U64::new(expires_at),
            persist: U16::new(0),
        }
    }

    /// Increase the nonce in place
    pub fn increase_nonce(&mut self) {
        self.nonce = U64::new(self.next_nonce());
    }

    /// Get the next nonce without mutating the current value
    pub fn next_nonce(&self) -> u64 {
        if self.nonce == U64::MAX_VALUE {
            0
        } else {
            self.nonce.get() + 1
        }
    }

    /// Change the expiration time
    pub fn expire_in(&mut self, duration: Duration) {
        self.expires_at
            .set(get_current_timestamp() + duration.as_secs())
    }

    /// Get the expiration time, returns None if persist flag is true.
    pub fn expires_in(&self) -> Option<Duration> {
        if self.persist.get() == 1 {
            return None;
        }
        let expires_at = self.expires_at.get();
        let now = get_current_timestamp();
        if expires_at <= now {
            Some(Duration::default())
        } else {
            Some(Duration::from_secs(expires_at - now))
        }
    }

    /// Check if the key is expired
    pub fn expired(&self) -> bool {
        let expires_at = self.expires_at.get();
        self.persist.get() == 0 && expires_at <= get_current_timestamp()
    }
}
