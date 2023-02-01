<h1 align="center">basteh</h1>
<br />

<div align="center">
  <a href="https://crates.io/crates/basteh">
    <img src="https://img.shields.io/crates/v/basteh.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <a href="https://docs.rs/basteh">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
  <img src="https://img.shields.io/github/actions/workflow/status/pooyamb/basteh/storage.yml?style=flat-square" alt="actions status" />
  <img alt="Codecov" src="https://img.shields.io/codecov/c/github/pooyamb/basteh?style=flat-square">
  <img alt="Crates.io" src="https://img.shields.io/crates/l/basteh?style=flat-square">
</div>

<br>

Actix storage is a simple wrapper around some key-value storages to provide basic operations without knowing the backend in advance.

## Install

basteh is meant to be used alongside one the implementer crates, ex:

```toml
# Cargo.toml
[dependencies]
basteh = "0.3.0"
basteh-hashmap = "0.3.0"
```

## Usage

After you picked an implementer:

```rust
use basteh::{Storage, Format};
use basteh_memory::HashMapActor;
use actix_web::{App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
   // Intialize the implementer according to its docs
   let store = HashMapActor::start_default();

   // Give it to the Storage struct
   let storage = Storage::build().store(store).finish();

   // Or if it doesn't support expiring functionality
   // it will give errors if those methods are called
   let storage = Storage::build().store(store).no_expiry().finish();

   // It is also possible to feed a seprate expiry,
   // as long as it works on the same storage backend
   let storage = Storage::build().store(store).expiry(expiry).finish();

   // Store it in you application state with actix_web::App.app_data
   let server = HttpServer::new(move || {
      App::new()
            .app_data(storage.clone())
   });
   server.bind("localhost:5000")?.run().await
}
```

And later in your handlers

```rust
async fn index(storage: Storage) -> Result<String, Error>{
   storage.set("key", b"value").await;
   let val = storage.get(b"key").await?.unwrap_or_default();

   Ok(std::str::from_utf8(&val)
      .map_err(|err| error::ErrorInternalServerError("Storage error"))?.to_string())
}
```

## Implementations

basteh-hashmap
<a href="https://docs.rs/basteh-hashmap">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

basteh-sled
<a href="https://docs.rs/basteh-sled">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

basteh-redis
<a href="https://docs.rs/basteh-redis">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

## Why?

It can be usefull when:

1. You don't know which key-value database you'll need later.
2. You can't afford the long time compilation of some dbs while developing.
   - hashmap store compiles pretty fast
3. You're writing an actix-web extension library and need to support multiple storage backends.

## Why not?

If you really care about every drop of your application performance then basteh may not be for you, as it uses dynamic dispatching internally.

## Examples

There are bunch of examples in the `examples` folder, very basic ones thought, but it will give you the idea.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.