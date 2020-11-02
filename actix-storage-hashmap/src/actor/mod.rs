use std::collections::HashMap;
use std::sync::Arc;

use actix::{
    Actor, ActorContext, ActorFuture, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    StreamHandler, WrapFuture,
};
use actix_storage::dev::actor::{
    ExpiryRequest, ExpiryResponse, ExpiryStoreRequest, ExpiryStoreResponse, StoreRequest,
    StoreResponse,
};

mod delayqueue;
use delayqueue::{delayqueue, DelayQueueEmergency, DelayQueueReceiver, DelayQueueSender, Expired};

/// An implementation of [`ExpiryStore`](../actix_storage/dev/trait.ExpiryStore.html) based on async
/// actix actors and HashMap
///
/// It relies on tokio's DelayQueue internally to manage expiration, and it doesn't have any lock as
/// it runs in single threaded async arbiter.
///
/// ## Example
/// ```no_run
/// use actix_storage::Storage;
/// use actix_storage_hashmap::HashMapActor;
/// use actix_web::{App, HttpServer};
///
/// #[actix_web::main]
/// async fn main() -> std::io::Result<()> {
///     let store = HashMapActor::start_default();
///     // OR
///     let store = HashMapActor::with_capacity(100).start();
///
///     let storage = Storage::build().expiry_store(store).finish();
///     let server = HttpServer::new(move || {
///         App::new()
///             .data(storage.clone())
///     });
///     server.bind("localhost:5000")?.run().await
/// }
/// ```
///
/// requires ["actor"] feature
#[derive(Debug)]
pub struct HashMapActor {
    map: HashMap<Arc<[u8]>, Arc<[u8]>>,
    exp: DelayQueueSender<Arc<[u8]>>,
    emergency_channel: DelayQueueEmergency<Arc<[u8]>>,

    #[doc(hidden)]
    exp_receiver: Option<DelayQueueReceiver<Arc<[u8]>>>,
}

const DEFAULT_INPUT_CHANNEL_SIZE: usize = 2;
const DEFAULT_OUTPUT_CHANNEL_SIZE: usize = 4;

impl HashMapActor {
    /// Makes a new HashMapActor without starting it
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes a new HashMapActor with specified HashMap capacity without starting it
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx, etx) =
            delayqueue::<Arc<[u8]>>(DEFAULT_INPUT_CHANNEL_SIZE, DEFAULT_OUTPUT_CHANNEL_SIZE);
        Self {
            map: HashMap::with_capacity(capacity),
            exp: tx,
            exp_receiver: Some(rx),
            emergency_channel: etx,
        }
    }

    /// Makes a new HashMapActor with specified HashMap and channel capacity without starting it
    ///
    /// Buffer sizes are used for internal expiry channel provider, input is for the channel
    /// providing commands expire/extend/expiry/persist and output is the other channel that
    /// sends back expired items.
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn with_capacity_and_channel_size(
        capacity: usize,
        input_buffer: usize,
        output_buffer: usize,
    ) -> Self {
        let (tx, rx, etx) = delayqueue::<Arc<[u8]>>(input_buffer, output_buffer);
        Self {
            map: HashMap::with_capacity(capacity),
            exp: tx,
            exp_receiver: Some(rx),
            emergency_channel: etx,
        }
    }

    /// Equivalent of actix::Actor::start for when actix::Actor is not in scope
    pub fn start(self) -> Addr<Self> {
        Actor::start(self)
    }

    /// Equivalent of actix::Actor::start_default for when actix::Actor is not in scope
    pub fn start_default() -> Addr<Self> {
        <Self as Actor>::start_default()
    }
}

impl Default for HashMapActor {
    fn default() -> Self {
        let (tx, rx, etx) =
            delayqueue::<Arc<[u8]>>(DEFAULT_INPUT_CHANNEL_SIZE, DEFAULT_OUTPUT_CHANNEL_SIZE);
        Self {
            map: HashMap::new(),
            exp: tx,
            exp_receiver: Some(rx),
            emergency_channel: etx,
        }
    }
}

impl Actor for HashMapActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // if there receiver channel is available, we take it
        // otherwise we stablish a new connection through emergency channel
        if self.exp_receiver.is_some() {
            let rx = std::mem::take(&mut self.exp_receiver).unwrap();
            ctx.add_stream(rx);
        } else {
            let mut etx = self.emergency_channel.clone();
            ctx.wait(Box::pin(
                async move { etx.restart().await }
                    .into_actor(self)
                    .map(|message, _, ctx| {
                        match message {
                            Ok(ch) => {
                                ctx.add_stream(ch);
                            }
                            Err(_) => {
                                // Something went wrong with the channel, we stop
                                ctx.terminate();
                            }
                        }
                    }),
            ));
        }
    }
}

impl StreamHandler<Expired<Arc<[u8]>>> for HashMapActor {
    fn handle(&mut self, item: Expired<Arc<[u8]>>, _: &mut Self::Context) {
        self.map.remove(&item.into_inner());
    }
}

impl Handler<StoreRequest> for HashMapActor {
    type Result = ResponseActFuture<Self, StoreResponse>;

    fn handle(&mut self, msg: StoreRequest, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            StoreRequest::Set(key, value) => {
                if self.map.insert(key.clone(), value).is_some() {
                    // Remove the key from expiry if it already exists
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move { exp.remove(key).await }
                            .into_actor(self)
                            .map(move |_, _, _| StoreResponse::Set(Ok(()))),
                    )
                } else {
                    Box::pin(async { StoreResponse::Set(Ok(())) }.into_actor(self))
                }
            }
            StoreRequest::Get(key) => {
                let val = self.map.get(&key).cloned();
                Box::pin(async move { StoreResponse::Get(Ok(val)) }.into_actor(self))
            }
            StoreRequest::Delete(key) => {
                if self.map.remove(&key).is_some() {
                    // Remove key from expiry if the item actually existed and was removed
                    let mut exp = self.exp.clone();
                    ctx.spawn(
                        async move {
                            exp.remove(key).await.ok().unwrap();
                        }
                        .into_actor(self),
                    );
                }
                Box::pin(async { StoreResponse::Delete(Ok(())) }.into_actor(self))
            }
            StoreRequest::Contains(key) => {
                let con = self.map.contains_key(&key);
                Box::pin(async move { StoreResponse::Contains(Ok(con)) }.into_actor(self))
            }
        }
    }
}

impl Handler<ExpiryRequest> for HashMapActor {
    type Result = ResponseActFuture<Self, ExpiryResponse>;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(key, expires_in) => {
                if self.map.contains_key(&key) {
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move { exp.insert_or_update(key, expires_in).await }
                            .into_actor(self)
                            .map(move |_, _, _| ExpiryResponse::Set(Ok(()))),
                    )
                } else {
                    Box::pin(async { ExpiryResponse::Set(Ok(())) }.into_actor(self))
                }
            }
            ExpiryRequest::Persist(key) => {
                if self.map.contains_key(&key) {
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move { exp.remove(key).await }
                            .into_actor(self)
                            .map(move |_, _, _| ExpiryResponse::Persist(Ok(()))),
                    )
                } else {
                    Box::pin(async { ExpiryResponse::Persist(Ok(())) }.into_actor(self))
                }
            }
            ExpiryRequest::Get(key) => {
                let mut exp = self.exp.clone();
                Box::pin(
                    async move { exp.get(key).await.ok().flatten() }
                        .into_actor(self)
                        .map(|val, _, _| ExpiryResponse::Get(Ok(val))),
                )
            }
            ExpiryRequest::Extend(key, duration) => {
                let mut exp = self.exp.clone();
                Box::pin(
                    async move { exp.extend(key, duration).await }
                        .into_actor(self)
                        .map(|_, _, _| ExpiryResponse::Extend(Ok(()))),
                )
            }
        }
    }
}

impl Handler<ExpiryStoreRequest> for HashMapActor {
    type Result = ResponseActFuture<Self, ExpiryStoreResponse>;

    fn handle(&mut self, msg: ExpiryStoreRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryStoreRequest::SetExpiring(key, value, expires_in) => {
                self.map.insert(key.clone(), value);
                let mut exp = self.exp.clone();
                Box::pin(
                    async move { exp.insert_or_update(key, expires_in).await }
                        .into_actor(self)
                        .map(move |_, _, _| ExpiryStoreResponse::SetExpiring(Ok(()))),
                )
            }
            ExpiryStoreRequest::GetExpiring(key) => {
                let val = self.map.get(&key).cloned();
                if let Some(val) = val {
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move { exp.get(key).await.ok().flatten() }
                            .into_actor(self)
                            .map(|expiry, _, _| {
                                ExpiryStoreResponse::GetExpiring(Ok(Some((val, expiry))))
                            }),
                    )
                } else {
                    Box::pin(async { ExpiryStoreResponse::GetExpiring(Ok(None)) }.into_actor(self))
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use actix_storage::tests::*;

    #[actix_rt::test]
    async fn test_hashmap_store() {
        let store = HashMapActor::start_default();
        test_store(store).await;
    }

    #[actix_rt::test]
    async fn test_hashmap_expiry() {
        let store = HashMapActor::start_default();
        test_expiry(store.clone(), store).await;
    }

    #[actix_rt::test]
    async fn test_hashmap_expiry_store() {
        let store = HashMapActor::start_default();
        test_expiry_store(store).await;
    }

    #[actix_rt::test]
    async fn test_hashmap_formats() {
        let store = HashMapActor::start_default();
        test_all_formats(store).await;
    }
}
