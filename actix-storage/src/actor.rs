use std::sync::Arc;
use std::time::Duration;

use actix::{
    dev::{MessageResponse, ResponseChannel, ToEnvelope},
    Actor, Addr, Handler, Message,
};

use crate::dev::{Expiry, ExpiryStore, Store};
use crate::error::{Result, StorageError};

type Scope = Arc<[u8]>;
type Key = Arc<[u8]>;
type Value = Arc<[u8]>;

/// Actix message for [`Store`](../trait.Store.html) requests
///
/// Every store methods are mirrored to an enum variant of the same name, and should
/// result in its corresponding variant in [`StoreResponse`](enum.StoreResponse.html).
/// [`Store`](../trait.Store.html) is automatically implemented for actors handling
/// this message.
#[derive(Debug, Message)]
#[rtype(StoreResponse)]
pub enum StoreRequest {
    Get(Scope, Key),
    Set(Scope, Key, Value),
    Delete(Scope, Key),
    Contains(Scope, Key),
}

/// Actix message reply for [`Store`](../trait.Store.html) requests
///
/// Every store methods are mirrored to an enum variant of the same name, and should
/// be a result for its corresponding variant in [`StoreResponse`](enum.StoreResponse.html)
/// Returning anything beside the requested variant, will result in panic at runtime.
pub enum StoreResponse {
    Get(Result<Option<Value>>),
    Set(Result<()>),
    Delete(Result<()>),
    Contains(Result<bool>),
}

impl<A: Actor> MessageResponse<A, StoreRequest> for StoreResponse {
    fn handle<R: ResponseChannel<StoreRequest>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[async_trait::async_trait]
impl<T> Store for Addr<T>
where
    T: Actor + Handler<StoreRequest> + Sync + Send,
    T::Context: ToEnvelope<T, StoreRequest>,
{
    async fn set(&self, scope: Scope, key: Key, value: Value) -> Result<()> {
        match self
            .send(StoreRequest::Set(scope, key, value))
            .await
            .map_err(StorageError::custom)?
        {
            StoreResponse::Set(val) => val,
            _ => panic!(),
        }
    }

    async fn delete(&self, scope: Scope, key: Key) -> Result<()> {
        match self
            .send(StoreRequest::Delete(scope, key))
            .await
            .map_err(StorageError::custom)?
        {
            StoreResponse::Delete(val) => val,
            _ => panic!(),
        }
    }

    async fn contains_key(&self, scope: Scope, key: Key) -> Result<bool> {
        match self
            .send(StoreRequest::Contains(scope, key))
            .await
            .map_err(StorageError::custom)?
        {
            StoreResponse::Contains(val) => val,
            _ => panic!(),
        }
    }

    async fn get(&self, scope: Scope, key: Key) -> Result<Option<Value>> {
        match self
            .send(StoreRequest::Get(scope, key))
            .await
            .map_err(StorageError::custom)?
        {
            StoreResponse::Get(val) => val,
            _ => panic!(),
        }
    }
}

/// Actix message for [`Expiry`](../trait.Expiry.html) requests
///
/// Every store methods are mirrored to an enum variant of the same name, and should
/// result in its corresponding variant in [`ExpiryResponse`](enum.ExpiryResponse.html).
/// [`Expiry`](../trait.Expiry.html) is automatically implemented for actors handling
/// this message.
#[derive(Debug, Message)]
#[rtype(ExpiryResponse)]
pub enum ExpiryRequest {
    Set(Scope, Key, Duration),
    Persist(Scope, Key),
    Get(Scope, Key),
    Extend(Scope, Key, Duration),
}

/// Actix message reply for [`Expiry`](../trait.Expiry.html) requests
///
/// Every store methods are mirrored to an enum variant of the same name, and should
/// be a result for its corresponding variant in [`ExpiryRequest`](enum.ExpiryRequest.html)
/// Returning anything beside the requested variant, will result in panic at runtime.
pub enum ExpiryResponse {
    Set(Result<()>),
    Persist(Result<()>),
    Get(Result<Option<Duration>>),
    Extend(Result<()>),
}

impl<A: Actor> MessageResponse<A, ExpiryRequest> for ExpiryResponse {
    fn handle<R: ResponseChannel<ExpiryRequest>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[async_trait::async_trait]
impl<T> Expiry for Addr<T>
where
    T: Actor + Handler<ExpiryRequest> + Sync + Send,
    T::Context: ToEnvelope<T, ExpiryRequest>,
{
    async fn expire(&self, scope: Scope, key: Key, expire_in: Duration) -> Result<()> {
        match self
            .send(ExpiryRequest::Set(scope, key, expire_in))
            .await
            .map_err(StorageError::custom)?
        {
            ExpiryResponse::Set(val) => val,
            _ => panic!(),
        }
    }

    async fn persist(&self, scope: Scope, key: Key) -> Result<()> {
        match self
            .send(ExpiryRequest::Persist(scope, key))
            .await
            .map_err(StorageError::custom)?
        {
            ExpiryResponse::Persist(val) => val,
            _ => panic!(),
        }
    }

    async fn expiry(&self, scope: Scope, key: Key) -> Result<Option<Duration>> {
        match self
            .send(ExpiryRequest::Get(scope, key))
            .await
            .map_err(StorageError::custom)?
        {
            ExpiryResponse::Get(val) => val,
            _ => panic!(),
        }
    }

    async fn extend(&self, scope: Scope, key: Key, expire_in: Duration) -> Result<()> {
        match self
            .send(ExpiryRequest::Extend(scope, key, expire_in))
            .await
            .map_err(StorageError::custom)?
        {
            ExpiryResponse::Extend(val) => val,
            _ => panic!(),
        }
    }
}

/// Actix message for [`ExpiryStore`](../trait.ExpiryStore.html) requests
///
/// Every store methods are mirrored to an enum variant of the same name, and should
/// result in its corresponding variant in [`ExpiryStoreResponse`](enum.ExpiryStoreResponse.html).
/// [`ExpiryStore`](../trait.ExpiryStore.html) is automatically implemented for actors handling
/// this message.
#[derive(Debug, Message)]
#[rtype(ExpiryStoreResponse)]
pub enum ExpiryStoreRequest {
    SetExpiring(Key, Value, Duration),
    GetExpiring(Key),
}

/// Actix message reply for [`ExpiryStore`](../trait.ExpiryStore.html) requests
///
/// Every store methods are mirrored to an enum variant of the same name, and should
/// be a result for its corresponding variant in [`ExpiryStoreResponse`](enum.ExpiryStoreResponse.html)
/// Returning anything beside the requested variant, will result in panic at runtime.
pub enum ExpiryStoreResponse {
    SetExpiring(Result<()>),
    GetExpiring(Result<Option<(Value, Option<Duration>)>>),
}

impl<A: Actor> MessageResponse<A, ExpiryStoreRequest> for ExpiryStoreResponse {
    fn handle<R: ResponseChannel<ExpiryStoreRequest>>(self, _: &mut A::Context, tx: Option<R>) {
        if let Some(tx) = tx {
            tx.send(self)
        }
    }
}

#[async_trait::async_trait]
impl<T> ExpiryStore for Addr<T>
where
    T: Actor
        + Handler<ExpiryStoreRequest>
        + Handler<ExpiryRequest>
        + Handler<StoreRequest>
        + Sync
        + Send,
    T::Context: ToEnvelope<T, ExpiryStoreRequest>
        + ToEnvelope<T, ExpiryRequest>
        + ToEnvelope<T, StoreRequest>,
{
    async fn set_expiring(
        &self,
        scope: Scope,
        key: Key,
        value: Value,
        expire_in: Duration,
    ) -> Result<()> {
        match self
            .send(ExpiryStoreRequest::SetExpiring(key, value, expire_in))
            .await
            .map_err(StorageError::custom)?
        {
            ExpiryStoreResponse::SetExpiring(val) => val,
            _ => panic!(),
        }
    }

    async fn get_expiring(
        &self,
        scope: Scope,
        key: Key,
    ) -> Result<Option<(Value, Option<Duration>)>> {
        match self
            .send(ExpiryStoreRequest::GetExpiring(key))
            .await
            .map_err(StorageError::custom)?
        {
            ExpiryStoreResponse::GetExpiring(val) => val,
            _ => panic!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::dev::*;
    use actix::prelude::*;

    #[derive(Default)]
    struct TestActor;

    impl Actor for TestActor {
        type Context = Context<Self>;
    }

    impl Handler<StoreRequest> for TestActor {
        type Result = StoreResponse;
        fn handle(&mut self, msg: StoreRequest, _: &mut Self::Context) -> Self::Result {
            match msg {
                StoreRequest::Get(_, _) => StoreResponse::Get(Ok(None)),
                StoreRequest::Set(_, _, _) => StoreResponse::Set(Ok(())),
                StoreRequest::Delete(_, _) => StoreResponse::Get(Ok(None)),
                StoreRequest::Contains(_, _) => StoreResponse::Contains(Ok(true)),
            }
        }
    }

    impl Handler<ExpiryRequest> for TestActor {
        type Result = ExpiryResponse;
        fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
            match msg {
                ExpiryRequest::Get(_, _) => ExpiryResponse::Get(Ok(None)),
                ExpiryRequest::Set(_, _, _) => ExpiryResponse::Set(Ok(())),
                ExpiryRequest::Persist(_, _) => ExpiryResponse::Persist(Ok(())),
                ExpiryRequest::Extend(_, _, _) => ExpiryResponse::Extend(Ok(())),
            }
        }
    }

    impl Handler<ExpiryStoreRequest> for TestActor {
        type Result = ExpiryStoreResponse;
        fn handle(&mut self, msg: ExpiryStoreRequest, _: &mut Self::Context) -> Self::Result {
            match msg {
                ExpiryStoreRequest::SetExpiring(_, _, _) => {
                    ExpiryStoreResponse::SetExpiring(Ok(()))
                }
                ExpiryStoreRequest::GetExpiring(_) => ExpiryStoreResponse::GetExpiring(Ok(None)),
            }
        }
    }

    #[actix_rt::test]
    #[should_panic(expected = "explicit panic")]
    async fn test_actor() {
        let actor = TestActor::start_default();
        let scope: Arc<[u8]> = "scope".as_bytes().into();
        let key: Arc<[u8]> = "key".as_bytes().into();
        let val: Arc<[u8]> = "val".as_bytes().into();
        let dur = Duration::from_secs(1);
        assert!(actor
            .set(scope.clone(), key.clone(), val.clone())
            .await
            .is_ok());
        assert!(actor.get(scope.clone(), key.clone()).await.is_ok());
        assert!(actor.contains_key(scope.clone(), key.clone()).await.is_ok());
        assert!(actor.expire(scope.clone(), key.clone(), dur).await.is_ok());
        assert!(actor.expiry(scope.clone(), key.clone()).await.is_ok());
        assert!(actor.persist(scope.clone(), key.clone()).await.is_ok());
        assert!(actor.extend(scope.clone(), key.clone(), dur).await.is_ok());
        assert!(actor
            .set_expiring(scope.clone(), key.clone(), val, dur)
            .await
            .is_ok());
        assert!(actor.get_expiring(scope.clone(), key.clone()).await.is_ok());
        // should panic here
        actor.delete(scope, key).await.unwrap();
    }
}
