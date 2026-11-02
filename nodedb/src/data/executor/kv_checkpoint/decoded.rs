// SPDX-License-Identifier: BUSL-1.1

//! The in-memory shape of a fully decoded generation.
//!
//! Kept apart from the on-disk types because it is what the loader holds AFTER
//! every fallible step is done: the whole generation decodes into this before
//! any of it is installed, so a file that cannot be rebuilt costs a WAL replay
//! rather than a half-restored collection under a floor that suppresses the
//! records which would have completed it.

use std::collections::HashMap;

use super::format::{KvCheckpointEntry, KvCheckpointKeyHistory};
use super::index_decode::DecodedKvIndexes;

/// One collection's decoded state.
pub(super) struct DecodedKvCollection {
    /// Every live row at flush time.
    pub entries: Vec<KvCheckpointEntry>,
    /// Every ended version of every key.
    pub history: Vec<KvCheckpointKeyHistory>,
    /// Every index registration, with its content.
    pub indexes: DecodedKvIndexes,
}

/// A fully decoded generation: `(tenant_id, db-qualified collection)` → state.
pub(super) type DecodedKvGeneration = HashMap<(u64, String), DecodedKvCollection>;
