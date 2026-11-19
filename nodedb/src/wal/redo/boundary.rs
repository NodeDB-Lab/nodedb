// SPDX-License-Identifier: BUSL-1.1

//! The commit boundary of a committed redo record, as the WAL holds it.
//!
//! A committed record's install validates the record against what its plan
//! carries beside the redo: who committed it, the snapshot ordinal, the read
//! set and the instant retention is judged at. The record's WAL copy carries
//! none of them. So the write funnel appends the boundary in the same WAL
//! lock as the record, as `RedoBoundary` records right below it. Each piece
//! names the LSN the record takes. The WAL is durable in LSN order, so a
//! durable record has its whole boundary. A refusal of the install cancels
//! both. A boundary over the WAL record limit spreads over several pieces,
//! one record each, so no read set is too large to commit.
//!
//! Boot validates a record whose install has no outcome against its
//! boundary, at the record's place in replay. A Calvin slice gets no
//! boundary: its stage vote validated it, and its transaction committed on
//! every shard it writes.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::RedoOrigin;
use nodedb_types::calvin::VersionedReadEntry;
use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;

/// What the install of a committed record validates beside the record, as
/// the funnel hands it to the append. The append adds the record's CRC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitBoundary {
    /// Who committed the record, which decides the commit-boundary checks.
    pub origin: RedoOrigin,
    /// The one ordinal every read of the transaction observed. `None` for a
    /// transaction with no snapshot.
    pub snapshot_ordinal: Option<i64>,
    /// The transaction's read set.
    pub reads: Vec<VersionedReadEntry>,
    /// The instant the install judges retention at, in ms since the Unix
    /// epoch.
    pub judged_at_ms: i64,
}

impl CommitBoundary {
    /// The stored boundary of the record whose payload has CRC32C
    /// `redo_crc`.
    pub fn with_crc(self, redo_crc: u32) -> RedoBoundary {
        RedoBoundary {
            origin: self.origin,
            snapshot_ordinal: self.snapshot_ordinal,
            reads: self.reads,
            judged_at_ms: self.judged_at_ms,
            redo_crc,
        }
    }
}

/// What the install of a committed record validates beside the record, as
/// the WAL stores it.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct RedoBoundary {
    /// Who committed the record, which decides the commit-boundary checks.
    pub origin: RedoOrigin,
    /// The one ordinal every read of the transaction observed. `None` for a
    /// transaction with no snapshot.
    pub snapshot_ordinal: Option<i64>,
    /// The transaction's read set. A stream marker's reads sit in its
    /// stream content, so its boundary holds none.
    pub reads: Vec<VersionedReadEntry>,
    /// The instant the install judges retention at, in ms since the Unix
    /// epoch. The funnel fixes it once, and the dispatched install judges at
    /// it too.
    pub judged_at_ms: i64,
    /// The CRC32C of the payload of the `TransactionRedo` record this
    /// boundary belongs to. A boundary whose CRC does
    /// not match the record at the LSN it names is left over from a failed
    /// append, and belongs to no record.
    pub redo_crc: u32,
}

impl RedoBoundary {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo commit boundary encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo commit boundary decode: {e}"),
        })
    }

    /// The encoded boundary cut into piece bodies, each of which fits one
    /// payload of at most `max_payload` bytes. An encoded byte takes at most
    /// two bytes in a piece's encoding. The count does not depend on the
    /// record's LSN.
    pub fn piece_bodies(&self, max_payload: usize) -> crate::Result<Vec<Vec<u8>>> {
        let encoded = self.to_bytes()?;
        let room = max_payload.saturating_sub(super::continuation::RECORD_OVERHEAD) / 2;
        if room == 0 {
            return Err(crate::Error::Internal {
                detail: format!(
                    "a WAL record limit of {max_payload} bytes holds no commit boundary piece"
                ),
            });
        }
        Ok(encoded.chunks(room).map(<[u8]>::to_vec).collect())
    }
}

/// One piece of an encoded boundary: the payload of one `RedoBoundary`
/// record.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct RedoBoundaryPiece {
    /// The LSN of the `TransactionRedo` record the boundary belongs to.
    pub redo_lsn: u64,
    /// The piece's place among the boundary's pieces, from 0.
    pub index: u32,
    /// How many pieces the boundary spreads over.
    pub count: u32,
    /// This piece's bytes of the encoded boundary.
    pub bytes: Vec<u8>,
}

impl RedoBoundaryPiece {
    /// The pieces of the boundary cut into `bodies`, for the record at
    /// `redo_lsn`.
    pub fn of(redo_lsn: u64, bodies: Vec<Vec<u8>>) -> crate::Result<Vec<Self>> {
        let count = u32::try_from(bodies.len()).map_err(|_| crate::Error::Internal {
            detail: format!(
                "the commit boundary of the redo at lsn {redo_lsn} spreads over {} pieces, more \
                 than a boundary counts",
                bodies.len()
            ),
        })?;
        Ok((0u32..)
            .zip(bodies)
            .map(|(index, bytes)| Self {
                redo_lsn,
                index,
                count,
                bytes,
            })
            .collect())
    }

    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!(
                "commit boundary piece {} of the redo at lsn {} encode: {e}",
                self.index, self.redo_lsn
            ),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo commit boundary piece decode: {e}"),
        })
    }
}

/// A boundary and the LSNs of the records that hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBoundary {
    /// The LSN of each `RedoBoundary` record of the boundary, in piece
    /// order.
    pub lsns: Vec<u64>,
    pub boundary: RedoBoundary,
}

/// A boundary piece found at the LSN its record expects.
struct PlacedPiece {
    lsn: u64,
    bytes: Vec<u8>,
}

/// Every complete boundary in `records`, by the LSN of the redo record each
/// names. Several can name one LSN: pieces of an append that failed before
/// its record landed sit below the LSN the record would have taken.
/// [`boundary_of`] picks the one that belongs to the record there.
///
/// The pieces of a boundary of `n` pieces sit right below the LSN they
/// name: piece `i` at `redo_lsn - n + i`. A piece elsewhere is left over and
/// ignored, and so is a boundary whose pieces are not all there. A piece
/// that does not decode is an error: the WAL verified its checksum, so its
/// bytes are what the funnel wrote.
pub fn boundaries_in(records: &[WalRecord]) -> crate::Result<BTreeMap<u64, Vec<StoredBoundary>>> {
    let mut placed: BTreeMap<(u64, u32), BTreeMap<u32, PlacedPiece>> = BTreeMap::new();
    for record in records {
        if RecordType::from_raw(record.logical_record_type()) != Some(RecordType::RedoBoundary) {
            continue;
        }
        let piece = RedoBoundaryPiece::from_bytes(&record.payload)?;
        let at = piece
            .redo_lsn
            .checked_sub(u64::from(piece.count))
            .and_then(|below| below.checked_add(u64::from(piece.index)));
        if piece.index >= piece.count || at != Some(record.header.lsn) {
            continue;
        }
        placed
            .entry((piece.redo_lsn, piece.count))
            .or_default()
            .insert(
                piece.index,
                PlacedPiece {
                    lsn: record.header.lsn,
                    bytes: piece.bytes,
                },
            );
    }
    let mut boundaries: BTreeMap<u64, Vec<StoredBoundary>> = BTreeMap::new();
    for ((redo_lsn, count), pieces) in placed {
        if !(0..count).all(|index| pieces.contains_key(&index)) {
            continue;
        }
        let mut lsns = Vec::with_capacity(pieces.len());
        let mut encoded = Vec::new();
        for piece in pieces.into_values() {
            lsns.push(piece.lsn);
            encoded.extend_from_slice(&piece.bytes);
        }
        boundaries
            .entry(redo_lsn)
            .or_default()
            .push(StoredBoundary {
                lsns,
                boundary: RedoBoundary::from_bytes(&encoded)?,
            });
    }
    Ok(boundaries)
}

/// The boundary of the `TransactionRedo` record `record` among `found`:
/// the one that names its LSN and holds the CRC32C of its payload. `None`
/// for a record no boundary belongs to.
pub fn boundary_of<'a>(
    found: &'a BTreeMap<u64, Vec<StoredBoundary>>,
    record: &WalRecord,
) -> Option<&'a StoredBoundary> {
    let crc = crc32c::crc32c(&record.payload);
    found
        .get(&record.header.lsn)?
        .iter()
        .find(|stored| stored.boundary.redo_crc == crc)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(reads: Vec<VersionedReadEntry>) -> CommitBoundary {
        CommitBoundary {
            origin: RedoOrigin::Commit,
            snapshot_ordinal: Some(9),
            reads,
            judged_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn a_boundary_round_trips() {
        let boundary = boundary(Vec::new()).with_crc(7);
        let bytes = boundary.to_bytes().expect("encode");
        assert_eq!(RedoBoundary::from_bytes(&bytes).expect("decode"), boundary);
        assert!(RedoBoundary::from_bytes(b"\x01").is_err());
    }

    /// A read set past the record limit spreads over pieces that each fit,
    /// all below the record they name, and the pieces join back.
    #[test]
    fn a_large_boundary_spreads_over_pieces_below_its_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = crate::wal::WalManager::open_for_testing(&dir.path().join("w.wal")).expect("wal");
        // Each entry encodes to more than 40 bytes.
        let entries = u32::try_from(wal.max_payload() / 40 + 1).expect("entry count");
        let reads = (0..entries)
            .map(|index| VersionedReadEntry {
                engine: nodedb_types::calvin::EngineTag::Document,
                collection: format!("collection-with-a-long-name-{index}"),
                key: nodedb_types::calvin::ReadKeyIdent::Predicate,
                home_vshard: Some(index),
            })
            .collect();
        let boundary = boundary(reads);
        let stored = boundary.clone().with_crc(0);
        assert!(stored.to_bytes().expect("encode").len() > wal.max_payload());
        let bodies = stored.piece_bodies(wal.max_payload()).expect("bodies");
        assert!(bodies.len() > 1);
        for piece in RedoBoundaryPiece::of(u64::MAX, bodies.clone()).expect("pieces") {
            assert!(piece.to_bytes().expect("encode piece").len() <= wal.max_payload());
        }
        let record = crate::wal::RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
        };
        let lsn = wal
            .appender(crate::wal::manager::NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_bounded_transaction_redo_bytes(
                crate::types::TenantId::new(1),
                crate::types::VShardId::new(0),
                crate::types::DatabaseId::DEFAULT,
                &record.to_bytes().expect("encode record"),
                boundary.clone(),
            )
            .expect("append")
            .as_u64();
        wal.sync().expect("sync");
        let records = wal.replay().expect("replay");
        let found = boundaries_in(&records).expect("boundaries");
        let redo = records
            .iter()
            .find(|record| record.header.lsn == lsn)
            .expect("redo record");
        let stored = boundary_of(&found, redo).expect("its boundary");
        assert_eq!(stored.boundary.reads, boundary.reads);
        assert_eq!(stored.lsns.len(), bodies.len());
        assert!(stored.lsns.iter().all(|piece| *piece < lsn));
    }
}
