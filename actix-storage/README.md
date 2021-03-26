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
  <img alt="Codecov" src="https://img.shields.io/codecov/c/github/pooyamb/actix-storage?style=flat-square">
  <img alt="Crates.io" src="https://img.shields.io/crates/l/actix-storage?style=flat-square">
</div>

<br>

Actix storage is a simple wrapper around some key-value storages to provide basic operations without knowing the backend in advance.

## Install

Actix-storage is meant to be used alongside one the implementer crates, ex:

```toml
# Cargo.toml
[dependencies]
actix-storage = "0.2.0"
actix-storage-hashmap = "0.2.0"
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

## Examples

There are bunch of examples in the `examples` folder, very basic ones thought, but it will give you the idea.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
