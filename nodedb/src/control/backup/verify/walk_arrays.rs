// SPDX-License-Identifier: BUSL-1.1

//! Canonical rows of the array section.
//!
//! One row per cell version, keyed by its coordinate and stamped with its
//! system time. The restore keeps both, and assigns the cell's surrogate
//! anew, so the surrogate stays out of the hash. The newest version of each
//! coordinate also carries the coordinate's current state: a RESTORE erases
//! no history, so the destination can hold later versions of a coordinate,
//! and its current state must still equal the backup's.

use std::collections::HashMap;

use nodedb_types::backup_envelope::VerifiedPart;

use crate::Error;
use crate::engine::array::export::ArrayCellVersion;
use crate::types::ArrayCellsBlob;

use super::canonical::{Row, RowHasher, key_hash};
use super::walk::Walk;

const LIVE: u8 = b'l';
const TOMBSTONE: u8 = b't';
const ERASED: u8 = b'e';
/// The state of a coordinate whose newest version is a tombstone or an
/// erasure.
const ABSENT: u8 = b'a';

fn encode_error(array: &str, e: impl std::fmt::Display) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("backup verification: array '{array}': {e}"),
    }
}

/// One decoded cell version and its encoded coordinate.
struct Version {
    coord: Vec<u8>,
    version: ArrayCellVersion,
}

impl Walk<'_> {
    pub(super) fn arrays(
        &self,
        blobs: &[ArrayCellsBlob],
        sink: &mut dyn FnMut(Row),
    ) -> Result<(), Error> {
        let mut decoded: Vec<(&str, Vec<Version>)> = Vec::with_capacity(blobs.len());
        for blob in blobs {
            let versions: Vec<ArrayCellVersion> =
                zerompk::from_msgpack(&blob.cells).map_err(|e| encode_error(&blob.array, e))?;
            let mut encoded = Vec::with_capacity(versions.len());
            for version in versions {
                let coord = zerompk::to_msgpack_vec(&version.coord)
                    .map_err(|e| encode_error(&blob.array, e))?;
                encoded.push(Version { coord, version });
            }
            decoded.push((blob.array.as_str(), encoded));
        }
        // The newest stamp of each coordinate, across every blob.
        let mut newest: HashMap<(&str, &[u8]), i64> = HashMap::new();
        for (array, versions) in &decoded {
            for v in versions {
                let held = newest
                    .entry((*array, v.coord.as_slice()))
                    .or_insert(v.version.system_from_ms);
                *held = (*held).max(v.version.system_from_ms);
            }
        }
        for (array, versions) in &decoded {
            for v in versions {
                let is_newest =
                    newest.get(&(*array, v.coord.as_slice())) == Some(&v.version.system_from_ms);
                sink(array_row(array, &v.coord, &v.version, is_newest)?);
            }
        }
        Ok(())
    }
}

fn array_row(
    array: &str,
    coord: &[u8],
    version: &ArrayCellVersion,
    is_newest: bool,
) -> Result<Row, Error> {
    let key: [&[u8]; 1] = [coord];
    let mut hasher = RowHasher::new(VerifiedPart::Array, &key);
    // RESTORE installs each version at its own system time.
    hasher.int(b's', version.system_from_ms);
    match &version.payload {
        Some(payload) => {
            hasher.bytes(b'r', &[LIVE]);
            hasher.int(b'f', payload.valid_from_ms);
            hasher.int(b'u', payload.valid_until_ms);
            let attrs =
                zerompk::to_msgpack_vec(&payload.attrs).map_err(|e| encode_error(array, e))?;
            hasher.bytes(b'a', &attrs);
        }
        None if version.erased => hasher.bytes(b'r', &[ERASED]),
        None => hasher.bytes(b'r', &[TOMBSTONE]),
    }
    let state = if is_newest {
        Some(array_state(array, coord, version)?)
    } else {
        None
    };
    Ok(Row {
        collection: array.to_string(),
        part: VerifiedPart::Array,
        key: Some(key_hash(VerifiedPart::Array, &key)),
        hash: hasher.finish(),
        expire_at_ms: 0,
        stamp: Some(version.system_from_ms),
        state,
    })
}

/// The current state of a coordinate: its newest version without its system
/// time. A live version hashes its valid time and attributes. A tombstone
/// and an erasure state the same absence.
fn array_state(array: &str, coord: &[u8], newest: &ArrayCellVersion) -> Result<[u8; 32], Error> {
    let mut hasher = RowHasher::state(VerifiedPart::Array, &[coord]);
    match &newest.payload {
        Some(payload) => {
            hasher.bytes(b'r', &[LIVE]);
            hasher.int(b'f', payload.valid_from_ms);
            hasher.int(b'u', payload.valid_until_ms);
            let attrs =
                zerompk::to_msgpack_vec(&payload.attrs).map_err(|e| encode_error(array, e))?;
            hasher.bytes(b'a', &attrs);
        }
        None => hasher.bytes(b'r', &[ABSENT]),
    }
    Ok(hasher.finish())
}

#[cfg(test)]
mod tests {
    use nodedb_array::tile::cell_payload::CellPayload;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{OPEN_UPPER, Surrogate};

    use super::*;

    fn live(x: i64, v: i64, surrogate: u32) -> ArrayCellVersion {
        ArrayCellVersion {
            coord: vec![CoordValue::Int64(x)],
            system_from_ms: 10,
            payload: Some(CellPayload {
                valid_from_ms: 0,
                valid_until_ms: OPEN_UPPER,
                attrs: vec![CellValue::Int64(v)],
                surrogate: Surrogate::new(surrogate),
            }),
            erased: false,
            placement: 0,
        }
    }

    fn row(version: &ArrayCellVersion) -> Row {
        let coord = zerompk::to_msgpack_vec(&version.coord).expect("coord");
        array_row("grid", &coord, version, true).expect("row")
    }

    /// A restored cell under another surrogate hashes the same. A changed
    /// value, system time or row kind does not. Every version of one
    /// coordinate shares its key and carries its own stamp.
    #[test]
    fn the_digest_ignores_surrogates_only() {
        let source = row(&live(1, 7, 3));
        assert_eq!(source.hash, row(&live(1, 7, 99)).hash);
        assert_eq!(source.key, row(&live(1, 7, 99)).key);
        assert_ne!(source.hash, row(&live(1, 8, 3)).hash);
        let later = ArrayCellVersion {
            system_from_ms: 11,
            ..live(1, 7, 3)
        };
        assert_eq!(source.key, row(&later).key);
        assert_ne!(source.hash, row(&later).hash);
        assert_eq!(row(&later).stamp, Some(11));
        let tombstone = ArrayCellVersion {
            payload: None,
            ..live(1, 7, 3)
        };
        let erased = ArrayCellVersion {
            erased: true,
            ..tombstone.clone()
        };
        assert_ne!(row(&tombstone).hash, row(&erased).hash);
        assert_ne!(row(&tombstone).hash, source.hash);
    }

    /// The current state ignores the system time and the surrogate. A
    /// tombstone and an erasure state the same absence, apart from a live
    /// cell.
    #[test]
    fn the_state_is_the_newest_version_without_its_system_time() {
        let source = row(&live(1, 7, 3));
        let later = ArrayCellVersion {
            system_from_ms: 99,
            ..live(1, 7, 4)
        };
        assert_eq!(source.state, row(&later).state);
        assert_ne!(source.state, row(&live(1, 8, 3)).state);
        let tombstone = ArrayCellVersion {
            payload: None,
            ..live(1, 7, 3)
        };
        let erased = ArrayCellVersion {
            erased: true,
            ..tombstone.clone()
        };
        assert_eq!(row(&tombstone).state, row(&erased).state);
        assert_ne!(row(&tombstone).state, source.state);
    }

    /// Only the newest version of a coordinate carries the state.
    #[test]
    fn only_the_newest_version_carries_the_state() {
        let older = live(1, 7, 3);
        let newer = ArrayCellVersion {
            system_from_ms: 20,
            payload: None,
            ..live(1, 7, 3)
        };
        let blob = ArrayCellsBlob {
            database_id: 0,
            tenant_id: 1,
            array: "grid".into(),
            vshard: 0,
            cells: zerompk::to_msgpack_vec(&vec![older, newer]).expect("encode"),
        };
        let (shapes, binds) = (
            super::super::walk::Shapes::new(),
            super::super::walk::BindIndex::default(),
        );
        let walk = Walk {
            tenant_id: 1,
            database_id: crate::types::DatabaseId::DEFAULT,
            shapes: &shapes,
            binds: &binds,
            kek: None,
        };
        let mut rows = Vec::new();
        walk.arrays(&[blob], &mut |row| rows.push(row))
            .expect("walk");
        let states: Vec<(Option<i64>, bool)> = rows
            .iter()
            .map(|row| (row.stamp, row.state.is_some()))
            .collect();
        assert_eq!(states, vec![(Some(10), false), (Some(20), true)]);
    }
}
