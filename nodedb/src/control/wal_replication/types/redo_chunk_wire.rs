// SPDX-License-Identifier: BUSL-1.1

//! Wire shapes of a committed redo body: inline, or chunked across entries.

use nodedb_physical::physical_plan::RedoSumTargets;

use super::transaction_redo_wire::ReplicatedIdentity;
use crate::wal::RedoStreamId;

/// The row-sized part of a committed redo: what a chunked redo splits.
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
}

impl RedoContent {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo content encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo content decode: {e}"),
        })
    }
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
    /// chunks of `len` bytes in all, encoded as one [`RedoContent`].
    Chunked {
        stream: RedoStreamId,
        count: u32,
        len: u64,
    },
}
