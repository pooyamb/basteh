use std::time::SystemTime;

use zerocopy::{AsBytes, LayoutVerified};

use super::flags::ExpiryFlags;

pub(crate) fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Takes an IVec and returns value bytes with its expiry flags as mutable
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode_mut(bytes: &mut [u8]) -> Option<(&mut [u8], &mut ExpiryFlags)> {
    let (val, exp): (&mut [u8], LayoutVerified<&mut [u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_mut())?;
    Some((val, exp.into_mut()))
}

/// Takes an IVec and returns value bytes with its expiry flags
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode(bytes: &[u8]) -> Option<(&[u8], &ExpiryFlags)> {
    let (val, exp): (&[u8], LayoutVerified<&[u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_ref())?;
    Some((val, exp.into_ref()))
}

/// Takes a value as bytes and an ExpiryFlags and turns them into bytes
#[allow(clippy::type_complexity)]
#[inline]
pub fn encode(value: &[u8], exp: &ExpiryFlags) -> Vec<u8> {
    let mut buff = vec![];
    buff.extend_from_slice(value);
    buff.extend_from_slice(exp.as_bytes());
    buff
}
