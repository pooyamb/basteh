use std::time::Duration;

use basteh::{
    dev::{Mutation, OwnedValue},
    Result,
};
use tokio::sync::oneshot;

pub enum Request {
    Keys(Box<str>),
    Get(Box<str>, Box<[u8]>),
    GetRange(Box<str>, Box<[u8]>, i64, i64),
    Set(Box<str>, Box<[u8]>, OwnedValue),
    Pop(Box<str>, Box<[u8]>),
    Push(Box<str>, Box<[u8]>, OwnedValue),
    PushMulti(Box<str>, Box<[u8]>, Vec<OwnedValue>),
    Remove(Box<str>, Box<[u8]>),
    Contains(Box<str>, Box<[u8]>),
    MutateNumber(Box<str>, Box<[u8]>, Mutation),
    Expire(Box<str>, Box<[u8]>, Duration),
    Persist(Box<str>, Box<[u8]>),
    Expiry(Box<str>, Box<[u8]>),
    Extend(Box<str>, Box<[u8]>, Duration),
    SetExpiring(Box<str>, Box<[u8]>, OwnedValue, Duration),
    GetExpiring(Box<str>, Box<[u8]>),
}

pub enum Response {
    Iterator(Box<dyn Iterator<Item = Vec<u8>> + Send + Sync>),
    Value(Option<OwnedValue>),
    ValueVec(Vec<OwnedValue>),
    Number(i64),
    Duration(Option<Duration>),
    ValueDuration(Option<(OwnedValue, Option<Duration>)>),
    Bool(bool),
    Empty(()),
}

pub struct Message {
    pub req: Request,
    pub tx: oneshot::Sender<Result<Response>>,
}
