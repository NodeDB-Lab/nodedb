// SPDX-License-Identifier: BUSL-1.1

//! Wire shapes of a committed redo body: inline, or chunked across entries.

use nodedb_physical::physical_plan::{RedoPins, RedoSumTargets};

use super::transaction_redo_wire::ReplicatedIdentity;
use crate::wal::{RedoStreamId, RedoStreamKeys};

/// The row-sized part of a committed redo: what a chunked redo splits.
/// A chunk stream carries it framed: `RedoContent::to_framed`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoContent {
    /// The resolved post-images.
    pub redo: crate::wal::RedoRecord,
    /// Materialized-sum resolution the document writes fold into their
    /// targets, keyed by source collection.
    pub sum_targets: Vec<RedoSumTargets>,
    /// Identities every replica binds before the apply.
    pub identities: Vec<ReplicatedIdentity>,
    /// The transaction's reads homed on this vShard. Every replica's apply
    /// validates them at the entry's snapshot ordinal before it installs.
    /// They travel in the content, so a large read set chunks with the redo.
    pub reads: Vec<nodedb_types::calvin::VersionedReadEntry>,
    /// The config content the proposer pinned for every replica's validate.
    /// They travel in the content, so their bytes count in the entry size.
    pub pins: RedoPins,
}

/// Where a `TransactionRedo` entry's content is.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RedoBody {
    /// The entry carries the content.
    Inline(Box<RedoContent>),
    /// Earlier `RedoChunk` entries of `stream` carry the content: `count`
    /// chunks of `len` bytes in all, one framed [`RedoContent`]. `keys` is
    /// what the content touches beside the entry's collections, as the
    /// proposer walked it.
    Chunked {
        stream: RedoStreamId,
        count: u32,
        len: u64,
        keys: RedoStreamKeys,
    },
}
