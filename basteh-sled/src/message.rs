use std::time::Duration;

use basteh::{
    dev::{Mutation, OwnedValue},
    Result,
};
use sled::IVec;
use tokio::sync::oneshot;

type Scope = IVec;
type Key = IVec;
type Value = OwnedValue;

pub enum Request {
    Keys(Scope),
    Get(Scope, Key),
    GetRange(Scope, Key, i64, i64),
    Set(Scope, Key, Value),
    Pop(Scope, Key),
    Push(Scope, Key, Value),
    PushMulti(Scope, Key, Vec<Value>),
    Remove(Scope, Key),
    Contains(Scope, Key),
    MutateNumber(Scope, Key, Mutation),
    Expire(Scope, Key, Duration),
    Persist(Scope, Key),
    Expiry(Scope, Key),
    Extend(Scope, Key, Duration),
    SetExpiring(Scope, Key, Value, Duration),
    GetExpiring(Scope, Key),
}

pub enum Response {
    Iterator(Box<dyn Iterator<Item = Vec<u8>> + Send + Sync>),
    Value(Option<Value>),
    ValueVec(Vec<Value>),
    Number(i64),
    Duration(Option<Duration>),
    ValueDuration(Option<(Value, Option<Duration>)>),
    Bool(bool),
    Empty(()),
}

pub struct Message {
    pub req: Request,
    pub tx: oneshot::Sender<Result<Response>>,
}
