use std::{
    convert::TryInto,
    time::{Duration, Instant, SystemTime},
};

use redb::TypeName;

pub(crate) fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Represent the expiration timestamp, we reserve 4 words but use only one of them for now
/// TODO: What if SystemTime changes?
#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
pub struct ExpiryFlags(u64);

impl redb::RedbValue for ExpiryFlags {
    type SelfType<'a> = ExpiryFlags;

    type AsBytes<'a> = [u8; 32];

    fn fixed_width() -> Option<usize> {
        Some(32)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self(u64::from_be_bytes(data[0..8].try_into().unwrap()))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut arr = [0_u8; 32];
        arr[0..8].copy_from_slice(&value.0.to_be_bytes());
        arr
    }

    fn type_name() -> TypeName {
        TypeName::new("Expiration flags")
    }
}

impl ExpiryFlags {
    /// Make a new flags struct with persist flag set to true. Provide 0 for nonce if it's a new key.
    pub fn new_persist() -> Self {
        Self(0)
    }

    /// Make a new flags struct with persist flag set to false. Provide 0 for nonce if it's a new key.
    pub fn new_expiring(expires_in: Duration) -> Self {
        let expires_at = get_current_timestamp() + expires_in.as_secs();
        Self(expires_at)
    }

    /// Change the expiration time
    pub fn expire_in(&mut self, duration: Duration) {
        self.0 = get_current_timestamp() + duration.as_secs()
    }

    /// Get the expiration time, returns None if persist flag is true.
    pub fn expires_in(&self) -> Option<Duration> {
        if self.0 == 0 {
            return None;
        }
        let now = get_current_timestamp();
        if self.0 <= now {
            Some(Duration::default())
        } else {
            Some(Duration::from_secs(self.0 - now))
        }
    }

    /// Get the expiration time, returns None if persist flag is true.
    pub fn expires_at(&self) -> Option<Instant> {
        if self.0 == 0 {
            return None;
        }
        let now = get_current_timestamp();
        if self.0 <= now {
            Some(Instant::now())
        } else {
            Some(Instant::now() + Duration::from_secs(self.0 - now))
        }
    }

    /// Check if the key is expired
    pub fn expired(&self) -> bool {
        self.0 != 0 && self.0 <= get_current_timestamp()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persist_flag() {
        let mut flags = ExpiryFlags::new_persist();
        assert_eq!(flags.expired(), false);
        assert_eq!(flags.expires_in(), None);

        // Setting expiry shouldn't mutate persist state
        flags.expire_in(Duration::from_millis(100));

        // We don't support durations under 1 seconds so it should be considered expired
        assert_eq!(flags.expired(), true);
        assert_eq!(flags.expires_in(), Some(Duration::from_secs(0)));

        // Changing the flag manually should do
        flags.0 = 0;
        assert_eq!(flags.expired(), false);
        assert_eq!(flags.expires_in(), None);
    }

    #[test]
    fn test_expiry() {
        let mut flags = ExpiryFlags::new_expiring(Duration::from_millis(1000));
        assert_eq!(flags.expired(), false);

        let expires_in = flags.expires_in();
        assert!(expires_in.is_some());
        assert!(expires_in.unwrap().as_millis() <= 1000);
        assert!(expires_in.unwrap().as_millis() > 0);

        flags.expire_in(Duration::from_millis(1000));

        let expires_in = flags.expires_in();
        assert!(expires_in.is_some());
        assert!(expires_in.unwrap().as_millis() <= 2000);
        assert!(expires_in.unwrap().as_millis() >= 1000);
    }
}
