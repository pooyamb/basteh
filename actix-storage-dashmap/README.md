# actix-storage-dashmap

It's an implementation for actix-storage based on dashmap, please refer to actix-storage crate documentations.

There are 2 different implementors available in this crate

## DashMapStore
`DashMapStore` is a simple store without expiration functionality.

## DashMapActor
`DashMapActor` is a full expiry_store implementation available under `actor` feature.

### Implementation details
`DashMapActor` is a `SyncActor` running in a thread-pool by actix which uses `delay-queue` crate internally in a thread for expiration notifications.

It is possible to specify the number of instances being used in thread-pool, and the default capacity of dashmap is also configurable.

```rust
let store = DashMapActor::start_default(THREADS_NUMBER);
// OR
let store = DashMapActor::with_capacity(100).start(THREADS_NUMBER);
```

