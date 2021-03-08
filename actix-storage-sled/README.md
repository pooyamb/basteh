# actix-storage-sled

It's an implementation for actix-storage based on `sled` database, please refer to actix-storage crate documentations.

There are 2 different implementors available in this crate

## SledStore
`SledStore` is a simple store without expiration functionality.

## SledActor
`SledActor` is a full expiry_store implementation available under `actor` feature.

### Implementation details
`SledActor` is a `SyncActor` running in a thread-pool by actix which uses `delay-queue` crate internally in a thread for expiration notifications.

It is possible to specify the number of instances being used in thread-pool.

```rust
// You'll need to have the provided sled trait extension in scope
use actix_storage_sled::{actor::ToActorExt, SledConfig};

// Refer to sled's documentation for more options
let sled_db = SledConfig::default().temporary(true);

// Open the database and make an actor(not started yet)
let actor = sled_db.to_actor()?;

let store = actor
            // If you want to scan the database on start for expiration
            .scan_db_on_start(true)
            // If you want the expiration thread to perform deletion instead of soft deleting items
            .perform_deletion(true)
            // Finally start the actor
            .start(THREADS_NUMBER);
```

### Important note
`SledActor` stores the expiration flags in the same place as the data, it is not yet possible to store these flags in a different subtree.
For this very reason there are 3 public methods provided by this crate if you want to access the data outside this crate's scope, or mutate them, the expiry flags struct is also public.

`actix_storage_sled::actor::encode` To encode the data with expiration flags.

`actix_storage_sled::actor::decode` To decode the data with expiration flags.

`actix_storage_sled::actor::decode_mut` Same as `decode` but mutable.

`actix_storage_sled::actor::ExpiryFlags` The expiry flags

