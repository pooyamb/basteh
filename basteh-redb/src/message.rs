use std::time::Duration;

use basteh::{dev::Mutation, Result};
use tokio::sync::oneshot;

pub enum Request {
    Keys(Box<str>),
    Get(Box<str>, Box<[u8]>),
    GetNumber(Box<str>, Box<[u8]>),
    Set(Box<str>, Box<[u8]>, Box<[u8]>),
    SetNumber(Box<str>, Box<[u8]>, i64),
    Delete(Box<str>, Box<[u8]>),
    Contains(Box<str>, Box<[u8]>),
    MutateNumber(Box<str>, Box<[u8]>, Mutation),
    Expire(Box<str>, Box<[u8]>, Duration),
    Persist(Box<str>, Box<[u8]>),
    Expiry(Box<str>, Box<[u8]>),
    Extend(Box<str>, Box<[u8]>, Duration),
    SetExpiring(Box<str>, Box<[u8]>, Box<[u8]>, Duration),
    GetExpiring(Box<str>, Box<[u8]>),
}

pub enum Response {
    Iterator(Box<dyn Iterator<Item = Vec<u8>> + Send + Sync>),
    Value(Option<Vec<u8>>),
    Number(Option<i64>),
    Duration(Option<Duration>),
    ValueDuration(Option<(Vec<u8>, Option<Duration>)>),
    Bool(bool),
    Empty(()),
}

pub struct Message {
    pub req: Request,
    pub tx: oneshot::Sender<Result<Response>>,
}
