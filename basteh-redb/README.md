# basteh-sled

This crate provides implementations for basteh based on `redb` database.

> Please refer to basteh crate documentations for full details about usage and use cases.

### Implementation details

`RedbBackend` will spawn tasks on tokio's threadpool.

It is possible to specify the number of instances being used in thread-pool.

```rust,no_run
use basteh_sled::{RedbBackend, Database};

// Refer to redb's documentation for more options
let redb_db = Database::create("test.db").unwrap();

// Open the database and make a store(not started yet)
let store = RedbBackend::from_db(redb_db);

let store = store
            // If you want to scan the database on start for expiration
            .scan_db_on_start(true)
            // If you want the expiration thread to perform deletion instead of soft deleting items
            .perform_deletion(true)
            // Finally start the tasks
            .start(4); // Number of threads
```

### Impl notes

Each scope has its own table with the same as scope. Expiration flags are stored inside the database and in a seperate table for each scope. We also use a priority-queue to get notifications about expirations if perform_deletion is true. If scan_db_on_start is set, the database will be scanned to find expired items, which may cause loss of data if system's time have changed.
