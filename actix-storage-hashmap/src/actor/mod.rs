use std::collections::HashMap;
use std::sync::Arc;

use actix::{
    Actor, ActorContext, ActorFutureExt, Addr, AsyncContext, Context, Handler, ResponseActFuture,
    StreamHandler, WrapFuture,
};
use actix_storage::dev::actor::{
    ExpiryRequest, ExpiryResponse, ExpiryStoreRequest, ExpiryStoreResponse, StoreRequest,
    StoreResponse,
};

mod delayqueue;
use delayqueue::{delayqueue, DelayQueueEmergency, DelayQueueReceiver, DelayQueueSender, Expired};

type ScopeMap = HashMap<Arc<[u8]>, Arc<[u8]>>;
type InternalMap = HashMap<Arc<[u8]>, ScopeMap>;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct ExpiryKey {
    pub(crate) scope: Arc<[u8]>,
    pub(crate) key: Arc<[u8]>,
}

impl ExpiryKey {
    pub fn new(scope: Arc<[u8]>, key: Arc<[u8]>) -> Self {
        Self { scope, key }
    }
}

/// An implementation of [`ExpiryStore`](actix_storage::dev::ExpiryStore) based on async
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
    map: InternalMap,
    exp: DelayQueueSender<ExpiryKey>,
    emergency_channel: DelayQueueEmergency<ExpiryKey>,

    #[doc(hidden)]
    exp_receiver: Option<DelayQueueReceiver<ExpiryKey>>,
}

const DEFAULT_INPUT_CHANNEL_SIZE: usize = 16;
const DEFAULT_OUTPUT_CHANNEL_SIZE: usize = 16;

impl HashMapActor {
    /// Makes a new HashMapActor without starting it
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes a new HashMapActor with specified HashMap capacity without starting it
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn with_capacity(capacity: usize) -> Self {
        let (tx, rx, etx) = delayqueue(DEFAULT_INPUT_CHANNEL_SIZE, DEFAULT_OUTPUT_CHANNEL_SIZE);
        Self {
            map: HashMap::with_capacity(capacity),
            exp: tx,
            exp_receiver: Some(rx),
            emergency_channel: etx,
        }
    }

    /// Makes a new HashMapActor with specified channel capacity without starting it
    ///
    /// Buffer sizes are used for internal expiry channel provider, input is for the channel
    /// providing commands expire/extend/expiry/persist and output is the other channel that
    /// sends back expired items.
    #[must_use = "Actor should be started to work by calling `start`"]
    pub fn with_channel_size(input_buffer: usize, output_buffer: usize) -> Self {
        let (tx, rx, etx) = delayqueue(input_buffer, output_buffer);
        Self {
            map: HashMap::new(),
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
        let (tx, rx, etx) = delayqueue(input_buffer, output_buffer);
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
        let (tx, rx, etx) = delayqueue(DEFAULT_INPUT_CHANNEL_SIZE, DEFAULT_OUTPUT_CHANNEL_SIZE);
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
        // otherwise we establish a new receiving channel through emergency channel
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
                            Err(err) => {
                                // Something went wrong with the channel, we stop
                                log::error!(
                                    "Expiration channel closed and could not be recovered. {}",
                                    err
                                );
                                ctx.terminate();
                            }
                        }
                    }),
            ));
        }
    }
}

impl StreamHandler<Expired<ExpiryKey>> for HashMapActor {
    fn handle(&mut self, item: Expired<ExpiryKey>, _: &mut Self::Context) {
        let item = item.into_inner();
        self.map
            .get_mut(&item.scope)
            .and_then(|scope_map| scope_map.remove(&item.key));
    }
}

impl Handler<StoreRequest> for HashMapActor {
    type Result = ResponseActFuture<Self, StoreResponse>;

    fn handle(&mut self, msg: StoreRequest, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            StoreRequest::Set(scope, key, value) => {
                if self
                    .map
                    .entry(scope.clone())
                    .or_default()
                    .insert(key.clone(), value)
                    .is_some()
                {
                    // Remove the key from expiry if it already exists
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move {
                            if let Err(err) = exp.remove(ExpiryKey::new(scope, key)).await {
                                log::error!("{}", err);
                            }
                        }
                        .into_actor(self)
                        .map(move |_, _, _| StoreResponse::Set(Ok(()))),
                    )
                } else {
                    Box::pin(async { StoreResponse::Set(Ok(())) }.into_actor(self))
                }
            }
            StoreRequest::Get(scope, key) => {
                let val = self
                    .map
                    .get(&scope)
                    .and_then(|scope_map| scope_map.get(&key))
                    .cloned();
                Box::pin(async move { StoreResponse::Get(Ok(val)) }.into_actor(self))
            }
            StoreRequest::Delete(scope, key) => {
                if self
                    .map
                    .get_mut(&scope)
                    .and_then(|scope_map| scope_map.remove(&key))
                    .is_some()
                {
                    // Remove key from expiry if the item actually existed and was removed
                    let mut exp = self.exp.clone();
                    ctx.spawn(
                        async move {
                            if let Err(err) = exp.remove(ExpiryKey::new(scope, key)).await {
                                log::error!("{}", err);
                            }
                        }
                        .into_actor(self),
                    );
                }
                Box::pin(async { StoreResponse::Delete(Ok(())) }.into_actor(self))
            }
            StoreRequest::Contains(scope, key) => {
                let con = self
                    .map
                    .get(&scope)
                    .map(|scope_map| scope_map.contains_key(&key))
                    .unwrap_or(false);
                Box::pin(async move { StoreResponse::Contains(Ok(con)) }.into_actor(self))
            }
        }
    }
}

impl Handler<ExpiryRequest> for HashMapActor {
    type Result = ResponseActFuture<Self, ExpiryResponse>;

    fn handle(&mut self, msg: ExpiryRequest, _: &mut Self::Context) -> Self::Result {
        match msg {
            ExpiryRequest::Set(scope, key, expires_in) => {
                if self
                    .map
                    .get(&scope)
                    .map(|scope_map| scope_map.contains_key(&key))
                    .unwrap_or(false)
                {
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move {
                            if let Err(err) = exp
                                .insert_or_update(ExpiryKey::new(scope, key), expires_in)
                                .await
                            {
                                log::error!("{}", err);
                            }
                        }
                        .into_actor(self)
                        .map(move |_, _, _| ExpiryResponse::Set(Ok(()))),
                    )
                } else {
                    // The key does not exist, we return Ok here as the non-existent key may be expired before
                    Box::pin(async { ExpiryResponse::Set(Ok(())) }.into_actor(self))
                }
            }
            ExpiryRequest::Persist(scope, key) => {
                if self
                    .map
                    .get(&scope)
                    .map(|scope_map| scope_map.contains_key(&key))
                    .unwrap_or(false)
                {
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move {
                            if let Err(err) = exp.remove(ExpiryKey::new(scope, key)).await {
                                log::error!("{}", err);
                            }
                        }
                        .into_actor(self)
                        .map(move |_, _, _| ExpiryResponse::Persist(Ok(()))),
                    )
                } else {
                    Box::pin(async { ExpiryResponse::Persist(Ok(())) }.into_actor(self))
                }
            }
            ExpiryRequest::Get(scope, key) => {
                let mut exp = self.exp.clone();
                Box::pin(
                    async move {
                        match exp.get(ExpiryKey::new(scope, key)).await {
                            Ok(val) => val,
                            Err(err) => {
                                log::error!("{}", err);
                                None
                            }
                        }
                    }
                    .into_actor(self)
                    .map(|val, _, _| ExpiryResponse::Get(Ok(val))),
                )
            }
            ExpiryRequest::Extend(scope, key, duration) => {
                let mut exp = self.exp.clone();
                Box::pin(
                    async move { exp.extend(ExpiryKey::new(scope, key), duration).await }
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
            ExpiryStoreRequest::SetExpiring(scope, key, value, expires_in) => {
                self.map
                    .entry(scope.clone())
                    .or_default()
                    .insert(key.clone(), value);
                let mut exp = self.exp.clone();
                Box::pin(
                    async move {
                        exp.insert_or_update(ExpiryKey::new(scope, key), expires_in)
                            .await
                    }
                    .into_actor(self)
                    .map(move |_, _, _| ExpiryStoreResponse::SetExpiring(Ok(()))),
                )
            }
            ExpiryStoreRequest::GetExpiring(scope, key) => {
                let val = self
                    .map
                    .get(&scope)
                    .and_then(|scope_map| scope_map.get(&key))
                    .cloned();
                if let Some(val) = val {
                    let mut exp = self.exp.clone();
                    Box::pin(
                        async move {
                            match exp.get(ExpiryKey::new(scope, key)).await {
                                Ok(val) => val,
                                Err(err) => {
                                    log::error!("{}", err);
                                    None
                                }
                            }
                        }
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

    #[test]
    fn test_hashmap_store() {
        test_store(Box::pin(async { HashMapActor::start_default() }));
    }

    #[test]
    fn test_hashmap_expiry() {
        test_expiry(
            Box::pin(async {
                let store = HashMapActor::start_default();
                (store.clone(), store)
            }),
            2,
        );
    }

    #[test]
    fn test_hashmap_expiry_store() {
        test_expiry_store(Box::pin(async { HashMapActor::start_default() }), 2);
    }
}
