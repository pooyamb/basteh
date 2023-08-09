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
basteh = "=0.4.0-alpha.5"
basteh-memory = "=0.4.0-alpha.5"
```

## Usage

After you picked a backend:

```rust,ignore
use basteh::{Basteh, BastehError};
use basteh_memory::MemoryBackend;

async fn handler() -> Result<String, BastehError> {
   // Intialize the implementer according to its docs
   let provider = MemoryBackend::start_default();

   // Give it to the Storage struct
   let basteh = Basteh::build().provider(provider).finish();

   // Set a key, value can be stringy, bytes or numbers
   basteh.set("key", "value").await;

   // And make it expire after 5 seconds
   basteh.expire("key", Duration::from_secs(5)).await;

   // Or just do both in one command(usually faster)
   basteh.set_expiring("key", "value", Duration::from_secs(5)).await;

   basteh.get::<String>("key").await
}
```

## Implementations

basteh-memory
<a href="https://docs.rs/basteh-memory">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

basteh-redb
<a href="https://docs.rs/basteh-redb">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

basteh-redis
<a href="https://docs.rs/basteh-redis">
<img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
</a>

There is also an implementation based on sled, but because of sled's situation, it has not been released to crates.io

## What does basteh mean?

The word `basteh` is persian for box/package, that simple! Does it sound like some other words in english? sure! But I'm bad at naming packages and my other option was `testorage` as in type-erased storage... so... `basteh` is cool.

## Use cases

The main idea for having a kv storage around while developing applications, came from my love for django framework and how it makes your life easier. There is a cache framework in django that does just that! Basteh is a bit more, it is not just for a cache but also for persistent values which is why there will always be some persistent implementations around.

Although it is designed to work with small, mostly temporary data, I don't have any plans to make it less effecient for other workflows, and pull requests are always welcome.

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
