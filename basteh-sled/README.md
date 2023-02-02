# basteh-sled

This crate provides implementations for basteh based on `sled` database.

> This crate is not released and is not updated yet, you shouldn't rely on its stablity
> Please refer to basteh crate documentations for full details about usage and use cases.

### Implementation details

`SledBackend` will spawn tasks on tokio's threadpool.

It is possible to specify the number of instances being used in thread-pool.

```rust,no_run
// You'll need to have the provided sled trait extension in scope
use basteh_sled::{SledBackend, SledConfig};

// Refer to sled's documentation for more options
let sled_db = SledConfig::default().temporary(true).open().unwrap();

// Open the database and make a store(not started yet)
let store = SledBackend::from_db(sled_db);

let store = store
            // If you want to scan the database on start for expiration
            .scan_db_on_start(true)
            // If you want the expiration thread to perform deletion instead of soft deleting items
            .perform_deletion(true)
            // Finally start the tasks
            .start(4); // Number of threads
```

### Important note

`SledBackend` stores the expiration flags in the same place as the data, it is not yet possible to store these flags in a different subtree.
For this very reason there are 3 public methods provided by this crate if you want to access the data outside this crate's scope, or mutate them, the expiry flags struct is also public.

`basteh_sled::encode` To encode the data with expiration flags.

`basteh_sled::decode` To decode the data with expiration flags.

`basteh_sled::decode_mut` Same as `decode` but mutable.

`basteh_sled::ExpiryFlags` The expiry flags
