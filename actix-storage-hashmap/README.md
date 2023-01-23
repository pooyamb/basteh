# actix-storage-hashmap

This crate provides implementations for actix-storage based on std lib's hashmap.

> Please refer to actix-storage crate documentations for full details about usage and use cases.

### Implementation details

`HashMapBackend` uses tokio's `delayqueue` internally for expiration notifications.

It is possible to specify the size for the underlying channel between tokio's `delayqueue` and the actor.

```rust,no_run
use actix_storage_hashmap::HashMapBackend;

let store = HashMapBackend::start(2048);
// OR
let store = HashMapBackend::start_default();
```
