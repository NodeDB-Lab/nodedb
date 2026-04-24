//! Bitemporal EdgeStore primitives.
//!
//! Key layout appends a 20-digit zero-padded `system_from` ordinal suffix to
//! the legacy composite key:
//!
//! ```text
//! {collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}
//! ```
//!
//! `system_from` is an HLC-aligned `i64` ordinal (nanosecond-precision,
//! strictly monotonic per write) produced by
//! [`nodedb_types::OrdinalClock::next_ordinal`]. User-facing "AS OF SYSTEM
//! TIME `<ms>`" queries translate via
//! [`nodedb_types::ms_to_ordinal_upper`].
//!
//! Values are either an [`EdgeValuePayload`] (zerompk fixarray-3, first byte
//! `0x93`) or a single-byte sentinel ([`TOMBSTONE_SENTINEL`] `0xFF`,
//! [`GDPR_ERASURE_SENTINEL`] `0xFE`). Sentinels never collide with fixarray
//! prefixes (`0x90..=0x9f`).

pub mod keys;
pub mod payload;
pub mod query;
pub mod read;
#[cfg(test)]
mod tests;
pub mod write;

pub use keys::{
    EdgeRef, GDPR_ERASURE_SENTINEL, SYSTEM_TIME_WIDTH, TOMBSTONE_SENTINEL, edge_version_prefix,
    is_gdpr_erasure, is_sentinel, is_tombstone, parse_versioned_edge_key, versioned_edge_key,
};
pub use payload::EdgeValuePayload;
