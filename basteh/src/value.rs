use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    rc::Rc,
    sync::Arc,
};

use crate::BastehError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum ValueKind {
    Number = 0,
    String = 1,
    Bytes = 2,
    List = 3,
}

impl ValueKind {
    pub fn from_u8(n: u8) -> Option<Self> {
        match n {
            0 => Some(ValueKind::Number),
            1 => Some(ValueKind::String),
            2 => Some(ValueKind::Bytes),
            3 => Some(ValueKind::List),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value<'a> {
    Number(i64),
    String(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
    List(Vec<Value<'a>>),
}

impl<'a> Value<'a> {
    pub fn kind(&self) -> ValueKind {
        match self {
            Self::Number(_) => ValueKind::Number,
            Self::String(_) => ValueKind::String,
            Self::Bytes(_) => ValueKind::Bytes,
            Self::List(_) => ValueKind::List,
        }
    }

    pub fn to_owned(&self) -> OwnedValue {
        match &self {
            Value::Number(n) => OwnedValue::Number(*n),
            Value::String(s) => OwnedValue::String(s.as_ref().to_owned()),
            Value::Bytes(b) => OwnedValue::Bytes(b.as_ref().to_owned()),
            Value::List(l) => OwnedValue::List(l.iter().map(|v| v.to_owned()).collect()),
        }
    }

    pub fn into_owned(self) -> OwnedValue {
        match self {
            Value::Number(n) => OwnedValue::Number(n),
            Value::String(s) => OwnedValue::String(s.into_owned()),
            Value::Bytes(b) => OwnedValue::Bytes(b.into_owned()),
            Value::List(l) => OwnedValue::List(l.into_iter().map(|v| v.into_owned()).collect()),
        }
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(value: &'a str) -> Self {
        Self::String(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a Rc<str>> for Value<'a> {
    fn from(value: &'a Rc<str>) -> Self {
        Self::String(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a Arc<str>> for Value<'a> {
    fn from(value: &'a Arc<str>) -> Self {
        Self::String(Cow::Borrowed(value))
    }
}

impl<'a> From<String> for Value<'a> {
    fn from(value: String) -> Self {
        Self::String(Cow::Owned(value))
    }
}

impl<'a> From<&'a String> for Value<'a> {
    fn from(value: &'a String) -> Self {
        Self::String(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a [u8]> for Value<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Bytes(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a Rc<[u8]>> for Value<'a> {
    fn from(value: &'a Rc<[u8]>) -> Self {
        Self::Bytes(Cow::Borrowed(&value))
    }
}

impl<'a> From<&'a Arc<[u8]>> for Value<'a> {
    fn from(value: &'a Arc<[u8]>) -> Self {
        Self::Bytes(Cow::Borrowed(&value))
    }
}

impl<'a> From<&'a Vec<u8>> for Value<'a> {
    fn from(value: &'a Vec<u8>) -> Self {
        Self::Bytes(Cow::Borrowed(&value))
    }
}

impl<'a> From<Vec<u8>> for Value<'a> {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(Cow::Owned(value))
    }
}

macro_rules! impl_from_number {
    ($number:ty) => {
        impl<'a> From<$number> for Value<'a> {
            fn from(value: $number) -> Self {
                Self::Number(value as i64)
            }
        }
    };
}

impl_from_number!(u8);
impl_from_number!(i8);
impl_from_number!(u16);
impl_from_number!(i16);
impl_from_number!(u32);
impl_from_number!(i32);
impl_from_number!(i64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedValue {
    Number(i64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<OwnedValue>),
}

impl OwnedValue {
    pub fn kind(&self) -> ValueKind {
        match self {
            Self::Number(_) => ValueKind::Number,
            Self::String(_) => ValueKind::String,
            Self::Bytes(_) => ValueKind::Bytes,
            Self::List(_) => ValueKind::List,
        }
    }

    pub fn as_value(&self) -> Value<'_> {
        match &self {
            OwnedValue::Number(n) => Value::Number(*n),
            OwnedValue::String(s) => Value::String(Cow::Borrowed(&s)),
            OwnedValue::Bytes(b) => Value::Bytes(Cow::Borrowed(&b)),
            OwnedValue::List(l) => Value::List(l.into_iter().map(|v| v.as_value()).collect()),
        }
    }
}

impl<'a> TryFrom<OwnedValue> for String {
    type Error = BastehError;

    fn try_from(value: OwnedValue) -> Result<Self, Self::Error> {
        match value {
            OwnedValue::String(val) => Ok(val),
            OwnedValue::Number(n) => Ok(n.to_string()),
            OwnedValue::Bytes(b) => Ok(String::from_utf8_lossy(&b).into_owned()),
            OwnedValue::List(_) => Err(BastehError::TypeConversion),
        }
    }
}

impl<'a> TryFrom<OwnedValue> for Vec<u8> {
    type Error = BastehError;

    fn try_from(value: OwnedValue) -> Result<Self, Self::Error> {
        match value {
            OwnedValue::String(val) => Ok(val.into_bytes()),
            OwnedValue::Bytes(b) => Ok(b),
            _ => Err(BastehError::TypeConversion),
        }
    }
}

macro_rules! impl_from_value_for_number {
    ($number:ty) => {
        impl<'a> TryFrom<OwnedValue> for $number {
            type Error = BastehError;

            fn try_from(value: OwnedValue) -> Result<Self, Self::Error> {
                match value {
                    OwnedValue::Number(val) => {
                        val.try_into().map_err(|_| BastehError::TypeConversion)
                    }
                    _ => Err(BastehError::TypeConversion),
                }
            }
        }
    };
}

impl_from_value_for_number!(u8);
impl_from_value_for_number!(i8);
impl_from_value_for_number!(u16);
impl_from_value_for_number!(i16);
impl_from_value_for_number!(u32);
impl_from_value_for_number!(i32);
impl_from_value_for_number!(i64);
impl_from_value_for_number!(u64);
