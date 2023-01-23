# actix-storage-redis

This crate provides an implementation for actix-storage based on `redis`.

> Please refer to actix-storage crate documentations for full details about usage and use cases.

## RedisBackend

`RedisBackend` is a full store with expiration implementation.

```rust,no_run
use actix_storage_redis::{RedisBackend, ConnectionInfo, RedisConnectionInfo, ConnectionAddr};

async fn my_main() {
    // Connecting to the redis instance on localhost without username/password(for dev env)
    let store = RedisBackend::connect_default().await.expect("Redis connection failed");
    // OR connect with the provided redis::ConnectionInfo
    let connection_info = ConnectionInfo {
        addr: ConnectionAddr::Tcp("127.0.0.1".to_string(), 1234).into(),
        redis: RedisConnectionInfo{
            db: 0,
            username: Some("god".to_string()),
            password: Some("bless".to_string()),
        }
    };
    let store = RedisBackend::connect(connection_info).await.expect("Redis connection failed");
}
```
