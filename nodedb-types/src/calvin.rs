// SPDX-License-Identifier: Apache-2.0

//! Primitive Calvin scheduling types shared between `nodedb-physical`
//! (the physical-plan IR layer) and `nodedb-cluster` (the distributed
//! Calvin sequencer / scheduler).
//!
//! Provides [`SortedVec`], [`EngineKeySet`], and [`PassiveReadKey`] —
//! the building blocks of Calvin read/write sets. `DependentReadSpec`
//! and other scheduler-internal aggregates stay in `nodedb-cluster`.

use serde::{Deserialize, Serialize};

use crate::{KeyRepr, WriteVersion};

pub use crate::calvin_passive::{PassiveKey, PassiveReadKeyId};

/// A newtype over `Vec<T>` that guarantees sorted, deduplicated contents.
///
/// Constructed via [`SortedVec::new`], which sorts and deduplicates at
/// construction time. This property is load-bearing for byte-determinism:
/// two `SortedVec`s built from the same logical set (in any insertion order)
/// produce identical serialized bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortedVec<T>(Vec<T>);

impl<T: zerompk::ToMessagePack> zerompk::ToMessagePack for SortedVec<T> {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        self.0.write(writer)
    }
}

impl<'de, T> zerompk::FromMessagePack<'de> for SortedVec<T>
where
    T: zerompk::FromMessagePack<'de> + Ord + Clone,
{
    fn read<R: zerompk::Read<'de>>(reader: &mut R) -> zerompk::Result<Self> {
        let v = Vec::<T>::read(reader)?;
        Ok(Self::new(v))
    }
}

impl<T: Ord + Clone> SortedVec<T> {
    /// Build from any slice. Sorts and deduplicates in place.
    pub fn new(mut items: Vec<T>) -> Self {
        items.sort();
        items.dedup();
        Self(items)
    }

    pub fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T: Ord + Clone> From<Vec<T>> for SortedVec<T> {
    fn from(v: Vec<T>) -> Self {
        Self::new(v)
    }
}

/// A typed key set for one engine within a read or write set.
///
/// Keys are normalized to surrogates (or byte keys for KV) at admission, so
/// all engine-specific naming is resolved upstream of the sequencer.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum EngineKeySet {
    /// Document engine (schemaless or strict): identified by surrogate.
    Document {
        collection: String,
        surrogates: SortedVec<u32>,
    },
    /// Vector engine: identified by surrogate.
    Vector {
        collection: String,
        surrogates: SortedVec<u32>,
    },
    /// Key-Value engine: identified by raw byte keys.
    Kv {
        collection: String,
        keys: SortedVec<Vec<u8>>,
    },
    /// Graph edge engine: identified by (src_surrogate, dst_surrogate) pairs.
    ///
    /// `edges` carries the surrogate-pair IDENTITY used for Calvin locking and
    /// conflict detection. `home_vshards` carries the from_key ROUTING homes —
    /// the set of `VShardId::from_key(src_str)` / `from_key(dst_str)` u32 ids
    /// for every edge in this set. Routing (which vShards participate) is driven
    /// by `home_vshards`, NOT by the collection name, because a graph edge is
    /// dual-homed across its two endpoint key-hashed vShards.
    Edge {
        collection: String,
        edges: SortedVec<(u32, u32)>,
        home_vshards: SortedVec<u32>,
    },
    /// Array engine: the array's cells on each vShard in `vshards`.
    ///
    /// An array is tile-partitioned: its cells live on the vShards their
    /// tiles hash to, not on the array's collection home. Each vShard in
    /// `vshards` participates, and the transaction locks the whole array on
    /// it. Appended last: the encoding is positional.
    Array {
        collection: String,
        vshards: SortedVec<u32>,
    },
    /// A whole collection on each vShard in `vshards`, or on the
    /// collection's own vShard when `vshards` is empty.
    ///
    /// In a write set the transaction locks the collection exclusively: a
    /// truncate, bulk, predicate, or identity-less write. In a read set it
    /// locks the collection shared: a predicate, scan, or index read. A read
    /// that observed the collection on named vShards, such as a cross-shard
    /// graph read, names them in `vshards`.
    Collection {
        collection: String,
        vshards: SortedVec<u32>,
    },
    /// UNIQUE values a write claims in one index of `collection`.
    ///
    /// Each value is the canonical index value the Data Plane judges. Two
    /// transactions that claim one value lock one key, so they serialize.
    Unique {
        collection: String,
        index: String,
        values: SortedVec<Vec<u8>>,
    },
}

impl EngineKeySet {
    /// O(1) estimate of the serialized byte size of this key set.
    ///
    /// Used by the dependent-read cap check at sequencer admission to bound
    /// the total bytes that would be Raft-replicated in a `CalvinReadResult`
    /// entry.  This is an estimate, not an exact count; do NOT use it as a
    /// correctness check — only as a pre-flight guard.
    pub fn serialized_size_hint(&self) -> usize {
        match self {
            // u32 surrogates: 4 bytes each.
            Self::Document { surrogates, .. } | Self::Vector { surrogates, .. } => {
                surrogates.len() * 4
            }
            // KV keys: sum of key byte lengths.
            Self::Kv { keys, .. } => keys.iter().map(|k| k.len()).sum(),
            // Edge: two u32 per edge = 8 bytes each.
            Self::Edge { edges, .. } => edges.len() * 8,
            // Array: one u32 vShard each.
            Self::Array { vshards, .. } => vshards.len() * 4,
            // Collection: the name plus one u32 vShard each.
            Self::Collection {
                collection,
                vshards,
            } => collection.len() + vshards.len() * 4,
            // Unique: the index name plus each value's bytes.
            Self::Unique { index, values, .. } => {
                index.len() + values.iter().map(|v| v.len()).sum::<usize>()
            }
        }
    }

    /// The collection this key set belongs to.
    pub fn collection(&self) -> &str {
        match self {
            Self::Document { collection, .. }
            | Self::Vector { collection, .. }
            | Self::Kv { collection, .. }
            | Self::Edge { collection, .. }
            | Self::Array { collection, .. }
            | Self::Collection { collection, .. }
            | Self::Unique { collection, .. } => collection,
        }
    }

    /// Returns `true` if this key set contains no keys.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Document { surrogates, .. } => surrogates.is_empty(),
            Self::Vector { surrogates, .. } => surrogates.is_empty(),
            Self::Kv { keys, .. } => keys.is_empty(),
            Self::Edge { edges, .. } => edges.is_empty(),
            Self::Array { vshards, .. } => vshards.is_empty(),
            Self::Collection { .. } => false,
            Self::Unique { values, .. } => values.is_empty(),
        }
    }
}

/// A key set a passive participant reads and broadcasts.
///
/// Wraps an [`EngineKeySet`]. A passive vShard reads document rows by
/// surrogate and key-value rows by key. It refuses every other key set. The
/// sequencer bounds the bytes a transaction reads passively through
/// `DependentReadSpec::total_bytes()` in `nodedb-cluster`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct PassiveReadKey {
    /// The engine key set to read on the passive vshard.
    pub engine_key: EngineKeySet,
}

/// Which peer engine served a read. Mirrors the top-level physical-plan
/// engine variants one-to-one so the classifier is total and a new engine
/// forces a decision at compile time.
///
/// Encoded as a plain integer discriminant (`#[msgpack(c_enum)]`): stable,
/// compact, and comparable across the wire. The discriminant assignment is
/// load-bearing for on-wire stability — do NOT reorder variants.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum EngineTag {
    Vector,
    Graph,
    Document,
    Kv,
    Text,
    Columnar,
    Timeseries,
    Spatial,
    Crdt,
    Query,
    Meta,
    Array,
    ClusterArray,
}

/// The identity a read observed within a collection, carried on the
/// replicated Calvin `TxClass`.
///
/// `Point` carries the exact row identity ([`KeyRepr`]) for a keyed lookup
/// (per-key optimistic-concurrency validation). `Predicate` is the coarse,
/// collection-scoped observation for scans / searches / aggregates — safe
/// against phantoms, never under-approximating. `IndexEq` / `IndexRange` carry
/// the indexed dimension of a secondary-index equality / range read (canonical
/// stringified index value, identical to the index-key segment) for narrower
/// per-value validation.
///
/// New variants are APPENDED only — the on-wire encoding is positional, so
/// reordering would break cross-version decode. Do NOT reorder variants.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ReadKeyIdent {
    /// A single-row keyed observation.
    Point(KeyRepr),
    /// A collection-scoped predicate observation.
    Predicate,
    /// A secondary-index equality observation on one indexed field.
    IndexEq { field: String, value: String },
    /// A secondary-index range observation on one indexed field. `lo`/`hi` are
    /// optional so a one-sided native range is representable; both `None` is
    /// never emitted.
    IndexRange {
        field: String,
        lo: Option<String>,
        hi: Option<String>,
    },
}

/// One versioned, predicate-aware read observed by a transaction, carried on
/// the replicated Calvin `TxClass` so participants can validate it at the
/// commit serialization point.
///
/// `read_version` is a data-group log position, so any replica of the read's
/// vShard validates it. The enclosing `TxClass` scopes the tenant;
/// per-database scoping is carried by the transaction as a whole.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VersionedReadEntry {
    /// Which engine served the read.
    pub engine: EngineTag,
    /// The collection the read observed.
    pub collection: String,
    /// Point-key or collection-scoped-predicate identity of the observation.
    pub key: ReadKeyIdent,
    /// The version of what the read observed on its home vShard.
    pub read_version: WriteVersion,
    /// The vShard whose write versions validate this read. `None` homes the
    /// read to its collection's vShard. A graph read names the key vShard it
    /// read edges on, because edges live on their endpoints' vShards. A homed
    /// read with an empty `collection` observed every collection there, so it
    /// validates against the vShard's latest version.
    pub home_vshard: Option<u32>,
}

/// The versioned read-set of a Calvin transaction.
///
/// Empty for pure-write transactions and for autocommit statements (which
/// accumulate no session read-set). Populated at commit time from the neutral
/// session read-set, and validated per-participant against the local
/// write-version index at Calvin stage time (the commit vote on
/// a stage response).
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VersionedReadSet(pub Vec<VersionedReadEntry>);

impl VersionedReadSet {
    /// Build from a vector of entries.
    pub fn new(entries: Vec<VersionedReadEntry>) -> Self {
        Self(entries)
    }

    /// Returns `true` if no reads were recorded.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Number of recorded read entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate the recorded read entries.
    pub fn iter(&self) -> std::slice::Iter<'_, VersionedReadEntry> {
        self.0.iter()
    }

    /// Borrow the entries as a slice.
    pub fn as_slice(&self) -> &[VersionedReadEntry] {
        &self.0
    }
}
