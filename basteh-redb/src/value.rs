use std::convert::TryInto;

use basteh::dev::{OwnedValue, ValueKind};
use bytes::BytesMut;

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
            ValueKind::Bytes => OwnedValue::Bytes(BytesMut::from(&data[1..])),
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
                            values.push(OwnedValue::Number(n));
                        }
                        ValueKind::Bytes => {
                            let b = BytesMut::from(&data[index..(index + len as usize)]);
                            index += b.len();
                            values.push(OwnedValue::Bytes(b));
                        }
                        ValueKind::String => {
                            let s = data[index..(index + len as usize)].to_vec();
                            index += s.len();
                            values
                                .push(OwnedValue::String(String::from_utf8_lossy(&s).into_owned()));
                        }
                    }
                }

                OwnedValue::List(values)
            }
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
            OwnedValue::List(l) => {
                res.reserve(std::mem::size_of::<u64>() + 1);
                res.push(ValueKind::List as u8);

                for item in l {
                    match item {
                        OwnedValue::List(_) => {
                            panic!("List of lists is not supported")
                        }
                        OwnedValue::Number(n) => {
                            res.reserve(17);
                            res.push(ValueKind::Number as u8);
                            res.extend_from_slice(&4__u64.to_le_bytes());
                            res.extend_from_slice(&n.to_le_bytes());
                        }
                        OwnedValue::Bytes(b) => {
                            res.reserve(b.len() + 9);
                            res.push(ValueKind::Bytes as u8);
                            res.extend_from_slice(&(b.len() as u64).to_le_bytes());
                            res.extend_from_slice(&b);
                        }
                        OwnedValue::String(s) => {
                            res.reserve(s.len() + 9);
                            res.push(ValueKind::String as u8);
                            res.extend_from_slice(&(s.len() as u64).to_le_bytes());
                            res.extend_from_slice(&s.as_bytes());
                        }
                    }
                }
            }
        }

        res
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("Generic value")
    }
}
