#![doc = include_str!("../README.md")]

mod delayqueue;
mod flags;
mod inner;
mod message;
mod store;
mod utils;

pub use flags::ExpiryFlags;
pub use sled::Config as SledConfig;
pub use store::SledBackend;
pub use utils::{decode, decode_mut, encode};
