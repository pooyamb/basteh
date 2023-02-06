use std::convert::TryInto;

use basteh::dev::{Value, ValueKind};

pub struct SledValue<'a>(pub Value<'a>);

impl<'a> SledValue<'a> {
    pub(crate) fn from_bytes(data: &'a [u8]) -> Option<Self> {
        let kind = data.get(0).and_then(|v| ValueKind::from_u8(*v))?;

        Some(Self(match kind {
            ValueKind::Number => {
                if data.len() < std::mem::size_of::<i64>() + 1 {
                    // Invalid data found, should we panic?
                    return None;
                } else {
                    Value::Number(i64::from_le_bytes(data[1..9].try_into().unwrap()))
                }
            }
            ValueKind::String => Value::String(String::from_utf8_lossy(&data[1..])),
            ValueKind::Bytes => Value::Bytes(data[1..].into()),
        }))
    }
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::new();
        let kind = self.0.kind() as u8;
        match &self.0 {
            Value::Number(n) => {
                res.reserve(std::mem::size_of::<i64>() + 1);
                res.push(kind);
                res.extend_from_slice(&n.to_le_bytes())
            }
            Value::Bytes(b) => {
                res.reserve(b.len() + 1);
                res.push(kind);
                res.extend_from_slice(&b)
            }
            Value::String(s) => {
                res.reserve(s.len() + 1);
                res.push(kind);
                res.extend_from_slice(&s.as_bytes())
            }
        }

        res
    }
}
