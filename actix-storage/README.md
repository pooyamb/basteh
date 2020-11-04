<h1 align="center">Actix-storage</h1>
<br />

<div align="center">
  <a href="https://crates.io/crates/actix-storage">
    <img src="https://img.shields.io/crates/v/actix-storage.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <a href="https://docs.rs/actix-storage">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
  <img src="https://img.shields.io/github/workflow/status/pooyamb/actix-storage/Storage?style=flat-square" alt="actions status" />
  <img alt="Codecov" src="https://img.shields.io/codecov/c/github/pooyamb/actix-storage">
  <img alt="Crates.io" src="https://img.shields.io/crates/l/actix-storage">
</div>

<br>

Actix storage is a simple wrapper around some key-value storages to provide basic operations without knowing the backend in advance.

## Install

Actix-storage is meant to be used alongside one the implementor crates, ex:

```toml
# Cargo.toml
[dependencies]
actix-storage = "0.1"
actix-storage-hashmap = "0.1"
```

## Usage

After you picked an implementor:

```rust
use actix_storage::{Storage, Format};
use actix_storage_hashmap::HashMapActor;
use actix_web::{App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
   // Intialize the implementor according to its docs
   let store = HashMapActor::start_default();

   // Give it to the Storage struct
   let storage = Storage::build().expiry_store(store).finish();

   // Or if it doesn't support expiring functionality
   // it will give errors if those methods are called
   let storage = Storage::build().store(store).finish();

   // It is also possible to feed a seprate expiry,
   // as long as it works on the same storage backend
   let storage = Storage::build().expiry(expiry).finish();

   // It is also possible to add a format to directly
   // set and get values using serde.
   let storage = Storage::build().store(expiry).format(Format::Json).finish();


   // Store it with .data
   let server = HttpServer::new(move || {
      App::new()
            .data(storage.clone())
   });
   server.bind("localhost:5000")?.run().await
}
```

And later in your handlers

```rust
async fn index(storage: web::Data<Storage>) -> Result<String, Error>{
   storage.set_bytes("key", "value").await;
   let val = storage.get_bytes("key").await?.unwrap_or_default();

   // Ot if you defined a serde format
   let number: i32 = 5
   storage.set("number", number);
   let x: i32 = storage.get("number");

   Ok(std::str::from_utf8(&val)
      .map_err(|err| error::ErrorInternalServerError("Storage error"))?.to_string())
}
```

## Implementations

actix-storage-hashmap
<a href="https://docs.rs/actix-storage-hashmap">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

actix-storage-dashmap
<a href="https://docs.rs/actix-storage-dashmap">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

actix-storage-sled
<a href="https://docs.rs/actix-storage-sled">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

actix-storage-redis
<a href="https://docs.rs/actix-storage-redis">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

## Why?

It can be usefull when:

1. You don't know what kind of storage you'll want.
2. You can't afford the long time compilation of some dbs while developing
   - hashmap store compiles pretty fast
3. You're writing a general purpose library and you want to let the user decide

## Why not?

If you really care about every drop of your application performance then actix-storage may not be for you, as it uses dynamic dispatching internally.

## Examples

There are bunch of examples in the `examples` folder, very basic ones thought, but it will give you the idea.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
