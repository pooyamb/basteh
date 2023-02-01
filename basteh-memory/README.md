# basteh-hashmap

This crate provides implementations for basteh based on std lib's hashmap.

> Please refer to basteh crate documentations for full details about usage and use cases.

### Implementation details

`HashMapBackend` uses tokio's `delayqueue` internally for expiration notifications.

It is possible to specify the size for the underlying channel between tokio's `delayqueue` and the actor.

```rust,no_run
use basteh_memory::HashMapBackend;

let store = HashMapBackend::start(2048);
// OR
let store = HashMapBackend::start_default();
```
