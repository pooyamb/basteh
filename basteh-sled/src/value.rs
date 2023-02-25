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
            ValueKind::List => {
                let mut index = 1;
                let mut values = Vec::new();

                while index < data.len() {
                    let kind = ValueKind::from_u8(data[index]).unwrap_or(ValueKind::Number);
                    index += 1;

                    let len = u64::from_le_bytes(data[index..(index + 8)].try_into().unwrap());
                    index += 8;

                    match kind {
                        ValueKind::List => {
                            panic!("List of lists is not supported");
                        }
                        ValueKind::Number => {
                            let n =
                                i64::from_le_bytes(data[index..(index + 8)].try_into().unwrap());
                            index += 8;
                            values.push(Value::Number(n));
                        }
                        ValueKind::Bytes => {
                            let b = data[index..(index + len as usize)].to_vec();
                            index += b.len();
                            values.push(Value::Bytes(b.into()));
                        }
                        ValueKind::String => {
                            let s = data[index..(index + len as usize)].to_vec();
                            index += s.len();
                            values.push(Value::String(
                                String::from_utf8_lossy(&data[1..]).into_owned().into(),
                            ));
                        }
                    }
                }

                Value::List(values)
            }
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
            Value::List(l) => {
                res.reserve(std::mem::size_of::<u64>() + 1);
                res.push(ValueKind::List as u8);

                for item in l {
                    match item {
                        Value::List(_) => {
                            panic!("List of lists is not supported")
                        }
                        Value::Number(n) => {
                            res.reserve(17);
                            res.push(ValueKind::Number as u8);
                            res.extend_from_slice(&4__u64.to_le_bytes());
                            res.extend_from_slice(&n.to_le_bytes())
                        }
                        Value::Bytes(b) => {
                            res.reserve(b.len() + 9);
                            res.push(ValueKind::Bytes as u8);
                            res.extend_from_slice(&(b.len() as u64).to_le_bytes());
                            res.extend_from_slice(&b)
                        }
                        Value::String(s) => {
                            res.reserve(s.len() + 9);
                            res.push(ValueKind::Bytes as u8);
                            res.extend_from_slice(&(s.len() as u64).to_le_bytes());
                            res.extend_from_slice(&s.as_bytes())
                        }
                    }
                }
            }
        }

        res
    }
}
