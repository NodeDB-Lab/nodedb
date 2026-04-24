//! CRDT state management backed by loro.
//!
//! Each `CrdtState` wraps a `LoroDoc` representing one tenant/namespace's
//! state. Collections within the doc are `LoroMap` instances keyed by row ID,
//! where each row is itself a `LoroMap` of field→value.

pub mod core;
pub mod history;
pub mod snapshot;

#[cfg(test)]
mod tests;

pub use core::CrdtState;
