<h1 align="center">Basteh</h1>
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

> **Note** The main branch includes many breaking changes, switch to v0.3 branch for the released version.

<br>

Basteh(previously actix-storage) is a type erased wrapper around some key-value storages to provide common operations.

## Install

Basteh is meant to be used alongside one the implementer crates, ex:

```toml
# Cargo.toml
[dependencies]
basteh = "0.4.0-alpha.1"
basteh-memory = "0.4.0-alpha.1"
```

## Usage

After you picked a backend:

```rust,ignore
use basteh::{Storage, Format};
use basteh_memory::MemoryBackend;

async fn make_storage() -> Storage {
   // Intialize the implementer according to its docs
   let store = MemoryBackend::start_default();

   // Give it to the Storage struct
   let storage = Storage::build().store(store).finish();

   // Or if it doesn't support expiring functionality
   // it will give errors if those methods are called
   let storage = Storage::build().store(store).no_expiry().finish();

   // It is also possible to feed a seprate expiry,
   // as long as it works on the same storage backend
   let storage = Storage::build().store(store).expiry(expiry).finish();
   
   storage
}
```

## Implementations

basteh-memory
<a href="https://docs.rs/basteh-memory">
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

## Use cases

It can be usefull when:

1. You don't know which key-value database you'll need later.
2. You can't afford the long time compilation of some dbs while developing.
   - memory store compiles pretty fast
3. You're writing an extension library and need to support multiple storage backends.

## Examples

There are bunch of examples in the `examples` folder, very basic ones thought, but it will give you the idea.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
