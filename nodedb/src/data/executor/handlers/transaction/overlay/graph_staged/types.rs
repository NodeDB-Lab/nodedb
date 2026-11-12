// SPDX-License-Identifier: BUSL-1.1

//! Shared value types for the GRAPH transaction staging overlay: the
//! collection key, the edge identity, the per-collection staged-write state,
//! and the node-label delta shape `SetNodeLabels` / `RemoveNodeLabels` stage.

use std::collections::HashSet;

use zerompk::{FromMessagePack, Read, ToMessagePack, Write};

use crate::data::executor::handlers::spill::tiered::{SpillSpace, SpillWeight, TieredMap};
use crate::types::{DatabaseId, TenantId};

/// Collection overlay key: `(database, tenant, collection)`. Same shape as
/// the surrogate overlay's `CollKey`, re-declared here (not exported from
/// `stage_write::context`, which is module-private) so this type can be
/// shared between the staging handlers and the Neighbors/Hop read-merge.
pub type GraphCollKey = (DatabaseId, TenantId, String);

/// One staged edge identity: `(src_id, label, dst_id)`, exactly the tuple
/// `EdgeStore` / `ShardedCsrIndex` key an edge by.
pub(super) type EdgeKey = (String, String, String);

/// A borrowed edge identity `(src_id, label, dst_id)`. It hashes and
/// compares like an [`EdgeKey`] and encodes to the same MessagePack bytes,
/// so an edge lookup builds no owned key.
#[derive(Debug, Clone, Copy, Hash)]
pub(super) struct EdgeRef<'a>(pub &'a str, pub &'a str, pub &'a str);

impl hashbrown::Equivalent<EdgeKey> for EdgeRef<'_> {
    fn equivalent(&self, key: &EdgeKey) -> bool {
        self.0 == key.0 && self.1 == key.1 && self.2 == key.2
    }
}

impl ToMessagePack for EdgeRef<'_> {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        (self.0, self.1, self.2).write(writer)
    }
}

/// One staged edge put: `(collection, (src, label, dst), properties)`.
pub type StagedEdgePut = (String, (String, String, String), Vec<u8>);

/// One staged edge put of a known collection: `(src, label, dst,
/// properties, valid time)`.
pub type CollectionEdgePut = (String, String, String, Vec<u8>, EdgeValidTime);

/// Staged node-label delta: labels added and labels removed in this
/// transaction. A label that appears in both (added then removed, or vice
/// versa within the same statement sequence) is resolved by insertion order:
/// `GraphTxnOverlay::stage_node_labels_set` / `stage_node_labels_remove`
/// each clear the opposite set's membership for the labels they touch, so
/// the two sets stay disjoint and the last write wins.
#[derive(Debug, Default, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct NodeLabelDelta {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}

impl SpillWeight for NodeLabelDelta {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<NodeLabelDelta>()
            + self
                .added
                .iter()
                .chain(&self.removed)
                .map(String::spill_weight)
                .sum::<usize>()
    }
}

/// The valid time a staged edge put names: `VALID_TIME <from> [UNTIL
/// <until>]`. `None` takes the apply's default.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct EdgeValidTime {
    pub valid_from_ms: Option<i64>,
    pub valid_until_ms: Option<i64>,
}

/// One staged edge: a put with its properties and valid time, or a
/// tombstone. A put and a tombstone of one edge replace each other, so an
/// edge is never in both states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EdgeSlot {
    Put {
        properties: Vec<u8>,
        valid: EdgeValidTime,
    },
    Tombstone,
}

/// One staged edge of a collection, as a resolve reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StagedEdge {
    Put {
        src: String,
        label: String,
        dst: String,
        properties: Vec<u8>,
        valid: EdgeValidTime,
    },
    Tombstone {
        src: String,
        label: String,
        dst: String,
    },
}

/// A put spills as `[bin properties, valid]`, a tombstone as `nil`.
impl ToMessagePack for EdgeSlot {
    fn write<W: Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            EdgeSlot::Put { properties, valid } => {
                writer.write_array_len(2)?;
                writer.write_binary(properties)?;
                valid.write(writer)
            }
            EdgeSlot::Tombstone => writer.write_nil(),
        }
    }
}

impl<'a> FromMessagePack<'a> for EdgeSlot {
    fn read<R: Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        const NIL: u8 = 0xc0;
        if reader.peek_marker()? == NIL {
            reader.read_nil()?;
            return Ok(EdgeSlot::Tombstone);
        }
        reader.check_array_len(2)?;
        let properties = reader.read_binary()?.into_owned();
        let valid = EdgeValidTime::read(reader)?;
        Ok(EdgeSlot::Put { properties, valid })
    }
}

impl SpillWeight for EdgeSlot {
    fn spill_weight(&self) -> usize {
        std::mem::size_of::<EdgeSlot>()
            + match self {
                EdgeSlot::Put { properties, .. } => properties.len(),
                EdgeSlot::Tombstone => 0,
            }
    }
}

/// Staged graph mutations for a single collection within one transaction.
#[derive(Debug, Default)]
pub(super) struct GraphCollectionOverlay {
    /// Staged edges by identity.
    pub(super) edges: TieredMap<EdgeKey, EdgeSlot>,
    /// Staged node-label deltas, keyed by raw node id.
    pub(super) node_labels: TieredMap<String, NodeLabelDelta>,
}

impl GraphCollectionOverlay {
    pub(super) fn hot_bytes(&self) -> usize {
        self.edges.hot_bytes() + self.node_labels.hot_bytes()
    }

    pub(super) fn spilled_heap_bytes(&self) -> usize {
        self.edges.run_heap_bytes() + self.node_labels.run_heap_bytes()
    }

    pub(super) fn spill(&mut self, space: &mut SpillSpace) -> crate::Result<()> {
        self.edges.spill(space, "edge")?;
        self.node_labels.spill(space, "labels")
    }

    pub(super) fn rollback(&mut self, bound: u64, space: &mut SpillSpace) -> crate::Result<()> {
        self.edges.rollback(bound, space, "edge")?;
        self.node_labels.rollback(bound, space, "labels")
    }
}
