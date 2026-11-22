// SPDX-License-Identifier: BUSL-1.1

//! The keys a stream marker declares: the state its content touches beside
//! the collections its plan names.
//!
//! The proposer walks the content as it frames it (see [`super::walk`]). The
//! final entry of a chunked redo carries the keys, and every replica puts
//! them on the marker's stream reference. So the marker's plan and its WAL
//! record hold them. A waiting marker's install fence footprint and its
//! install's first fence both read them. Keys that pass the room the final
//! entry leaves them are not declared: the marker then waits as a core
//! task.

use nodedb_types::sync::wire::SyncProvenance;

/// The sync stream of one producer, whose high-water mark a sync frame
/// advances.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RedoSyncStream {
    pub producer_id: u64,
    pub stream_id: u64,
}

impl RedoSyncStream {
    /// The stream whose mark `provenance` advances. Producer zero takes no
    /// part in producer sequencing: its frame advances no mark.
    pub fn of(provenance: &SyncProvenance) -> Option<Self> {
        (provenance.producer_id != 0).then_some(Self {
            producer_id: provenance.producer_id,
            stream_id: provenance.stream_id,
        })
    }
}

/// What a stream marker declares for its content.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RedoStreamKeys {
    /// Every key the content touches beside the plan's collections.
    Declared(DeclaredStreamKeys),
    /// The keys pass the room the final entry leaves them, so the marker
    /// declares none. Its fence footprint is a core task.
    Unbounded,
}

impl Default for RedoStreamKeys {
    /// A declaration of no key.
    fn default() -> Self {
        Self::Declared(DeclaredStreamKeys::default())
    }
}

impl RedoStreamKeys {
    /// The declared keys, `None` when the marker declares none.
    pub fn declared(&self) -> Option<&DeclaredStreamKeys> {
        match self {
            Self::Declared(keys) => Some(keys),
            Self::Unbounded => None,
        }
    }

    /// An estimate of the heap bytes the keys hold.
    pub fn heap_bytes(&self) -> usize {
        self.declared().map_or(0, DeclaredStreamKeys::heap_bytes)
    }
}

/// What a stream marker's content touches beside its plan's collections.
/// Each list is sorted and holds each item once.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct DeclaredStreamKeys {
    /// The collection each entry of the read set names. An empty name
    /// reads the whole vShard.
    pub reads: Vec<String>,
    /// Each materialized-sum source and target the content header names.
    pub sum_collections: Vec<String>,
    /// The sync stream of each sync frame among the ops.
    pub sync_streams: Vec<RedoSyncStream>,
    /// Whether an op writes an edge, an edge cut or a node label.
    pub graph: bool,
}

impl DeclaredStreamKeys {
    /// An estimate of the heap bytes the keys hold.
    pub fn heap_bytes(&self) -> usize {
        let names: usize = self
            .reads
            .iter()
            .chain(&self.sum_collections)
            .map(|name| std::mem::size_of::<String>() + name.len())
            .sum();
        names + std::mem::size_of_val(self.sync_streams.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keys_round_trip_through_msgpack() {
        let declared = RedoStreamKeys::Declared(DeclaredStreamKeys {
            reads: vec!["".into(), "ledger".into()],
            sum_collections: vec!["totals".into()],
            sync_streams: vec![RedoSyncStream {
                producer_id: 7,
                stream_id: 42,
            }],
            graph: true,
        });
        for keys in [declared, RedoStreamKeys::Unbounded] {
            let bytes = zerompk::to_msgpack_vec(&keys).expect("encode");
            let decoded: RedoStreamKeys = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(decoded, keys);
        }
    }

    #[test]
    fn producer_zero_names_no_stream() {
        let provenance = |producer_id| SyncProvenance {
            producer_id,
            epoch: 1,
            stream_id: 42,
            seq: 1,
        };
        assert_eq!(RedoSyncStream::of(&provenance(0)), None);
        assert_eq!(
            RedoSyncStream::of(&provenance(7)),
            Some(RedoSyncStream {
                producer_id: 7,
                stream_id: 42,
            })
        );
    }
}
