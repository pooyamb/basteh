# Actix-storage

Actix storage is a simple wrapper around some key-value storages to provide basic operations without knowing the backend in advance.

## Why?

There are times you're writing an actix-web handler and you need a key-value storage to store some state or cache some value, but you don't know what kind of storage you're gonna need in advance, generally:

1. When you don't know what kind of storage you'll want.
2. When you can't afford the long time compilation of some dbs while developing
   - hashmap store compiles pretty fast
3. When you're writing a general purpose library and you want to let the user decide

## Why not?

If you really care about every drop of your application performance then actix-storage may not be for you, as it uses dynamic dispatching internally.

## Examples

There are bunch of examples in the `examples` folder, very basic ones thought, but it will give you the idea.

## Implementations

- actix-storage-hashmap: using tokio's delayqueue for expiration
- actix-storage-dashmap: using delay-queue crate for expiration
- actix-storage-sled: using delay-queue crate for expiration
- actix-storage-redis

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
