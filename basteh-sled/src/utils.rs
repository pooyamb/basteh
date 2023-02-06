use std::time::SystemTime;

use basteh::dev::{Action, Mutation, Value};
use zerocopy::{AsBytes, LayoutVerified};

use crate::{flags::ExpiryFlags, value::SledValue};

pub(crate) fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Takes an IVec and returns value bytes with its expiry flags as mutable
#[allow(clippy::type_complexity)]
#[inline]
pub(crate) fn decode_mut(bytes: &mut [u8]) -> Option<(Value<'_>, &mut ExpiryFlags)> {
    let (val, exp): (&mut [u8], LayoutVerified<&mut [u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_mut())?;
    Some((SledValue::from_bytes(val)?.0, exp.into_mut()))
}

/// Takes an IVec and returns value bytes with its expiry flags
#[allow(clippy::type_complexity)]
#[inline]
pub fn decode(bytes: &[u8]) -> Option<(Value<'_>, &ExpiryFlags)> {
    let (val, exp): (&[u8], LayoutVerified<&[u8], ExpiryFlags>) =
        LayoutVerified::new_unaligned_from_suffix(bytes.as_ref())?;
    Some((SledValue::from_bytes(val)?.0, exp.into_ref()))
}

/// Takes a value as bytes and an ExpiryFlags and turns them into bytes
#[allow(clippy::type_complexity)]
#[inline]
pub fn encode(value: Value<'_>, exp: &ExpiryFlags) -> Vec<u8> {
    let mut buff = vec![];
    buff.extend_from_slice(&SledValue(value).to_bytes());
    buff.extend_from_slice(exp.as_bytes());
    buff
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
