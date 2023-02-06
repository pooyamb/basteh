use std::convert::TryInto;

use basteh::dev::{OwnedValue, ValueKind};

#[derive(Debug)]
pub(crate) struct OwnedValueWrapper(pub(crate) OwnedValue);

impl redb::RedbValue for OwnedValueWrapper {
    type SelfType<'a> = OwnedValue;

    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let kind = match data.get(0).and_then(|v| ValueKind::from_u8(*v)) {
            Some(kind) => kind,
            None => {
                // Invalid data found, should we panic?
                return OwnedValue::Number(0);
            }
        };

        match kind {
            ValueKind::Number => {
                if data.len() < std::mem::size_of::<i64>() + 1 {
                    // Invalid data found, should we panic?
                    return OwnedValue::Number(0);
                } else {
                    OwnedValue::Number(i64::from_le_bytes(data[1..9].try_into().unwrap()))
                }
            }
            ValueKind::String => {
                OwnedValue::String(String::from_utf8_lossy(&data[1..]).into_owned())
            }
            ValueKind::Bytes => OwnedValue::Bytes(data[1..].to_vec()),
        }
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut res = Vec::new();
        let kind = value.kind() as u8;
        match &value {
            OwnedValue::Number(n) => {
                res.reserve(std::mem::size_of::<i64>() + 1);
                res.push(kind);
                res.extend_from_slice(&n.to_le_bytes())
            }
            OwnedValue::Bytes(b) => {
                res.reserve(b.len() + 1);
                res.push(kind);
                res.extend_from_slice(&b)
            }
            OwnedValue::String(s) => {
                res.reserve(s.len() + 1);
                res.push(kind);
                res.extend_from_slice(&s.as_bytes())
            }
        }

        res
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Generic value")
    }
}
