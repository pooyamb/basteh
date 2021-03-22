# actix-storage-redis

It's an implementation for actix-storage based on `redis`, please refer to actix-storage crate documentations.

There are 2 different implementers available in this crate

## RedisBackend
`RedisBackend` is a full expiry_store implementation.

```rust
// Connecting to the redis instance on localhost without username/password(for dev env)
let store = RedisBackend::connect_default().await?;
// OR connect with the provided redis::ConnectionInfo
let store = RedisBackend::connect(connection_info).await?;
```

