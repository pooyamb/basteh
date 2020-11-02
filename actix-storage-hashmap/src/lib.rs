#[cfg(feature = "actor")]
mod actor;
mod basic;

#[cfg(feature = "actor")]
pub use actor::HashMapActor;
pub use basic::HashMapStore;
