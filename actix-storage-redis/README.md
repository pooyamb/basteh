# actix-storage-redis

This crate provides an implementation for actix-storage based on `redis`.

> Please refer to actix-storage crate documentations for full details about usage and use cases.

There are 2 different implementers available in this crate

## RedisBackend
`RedisBackend` is a full store with expiration implementation.

```rust
// Connecting to the redis instance on localhost without username/password(for dev env)
let store = RedisBackend::connect_default().await?;
// OR connect with the provided redis::ConnectionInfo
let store = RedisBackend::connect(connection_info).await?;
```

