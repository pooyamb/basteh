# actix-storage-hashmap

This crate provides implementations for actix-storage based on std lib's hashmap.

> Please refer to actix-storage crate documentations for full details about usage and use cases.

There are 2 different implementers available in this crate

## HashMapStore
`HashMapStore` is a simple store without expiration functionality.

## HashMapActor
`HashMapActor` is a full store with expiration implementation available under `actor` feature.

### Implementation details
`HashMapActor` is an `AsyncActor` running in actix's arbiter which uses tokio's `delayqueue` internally for expiration notifications.

It is possible to specify the size for the underlying channel between tokio's `delayqueue` and the actor. The default capacity of hashmap is also configurable.

```rust
let store = HashMapActor::with_capacity(10_000).start();
// OR
let store = HashMapActor::with_channel_size(16, 16).start();
// OR
let store = HashMapActor::with_capacity_and_channel_size(10_000, 16, 16).start();
```

