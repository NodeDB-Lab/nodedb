// SPDX-License-Identifier: BUSL-1.1

//! The destination side of restore verification.
//!
//! After the last re-issue, the restore captures every restored collection
//! from the whole cluster, with the capture a backup takes, and recomputes each
//! collection part's tally. It checks what the RESTORE guarantees at its cut
//! `T`, and nothing a write after `T` legitimately changed.
//!
//! - A keyed part without versions (CRDT entries, unbound vectors) counts
//!   only the destination rows whose key the envelope holds.
//! - A versioned part (documents, edges, KV, array cells, columnar rows,
//!   key-bound vectors) checks each backed-up key against the destination's
//!   own versions of it: those at a `(key, stamp)` the envelope does not
//!   hold. A key with an own version above `T` was written after the
//!   RESTORE began, or set again by it at the batch's ordinal: the key is
//!   not checked. Every other key's current state must equal the backup's,
//!   and each backed-up version must be present unless an own version
//!   stands at or above its stamp: the RESTORE installs only the versions
//!   above the destination's newest (`restore_key_plan`).
//! - A keyless part (timeseries) must hold every backed-up row, once per
//!   copy. The destination's own rows beside them are not the restore's. A
//!   collection a TRUNCATE ended above `T` is not checked: that TRUNCATE
//!   ends every row the RESTORE wrote.
//! - A re-issue with no cut (MOVE TENANT) fills a drained target: every
//!   backed-up version must be present, and every key's state must match.
//!
//! A KV row whose TTL has passed by the time of the capture counts on neither
//! side: the restore skips it, and the destination can drop it.

use std::collections::{BTreeMap, HashMap, HashSet};

use nodedb_types::backup_envelope::{VerificationMismatch, VerificationPhase, VerifiedPart};

use crate::Error;
use crate::control::backup::capture::capture_collections;
use crate::control::state::SharedState;
use crate::data::executor::handlers::timeseries::ended_install::ENDED_INSTALL_PREFIX;
use crate::types::{DatabaseId, TenantDataSnapshot};
use nodedb_wal::crypto::WalEncryptionKey;

use super::canonical::{KeyHash, Row};
use super::expect::{DatabaseExpectation, Expectation, ExpectedRows, compare};
use super::walk::{BindIndex, PartKey, Tallies, Walk};

/// What the destination check verified.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Verified {
    /// Collection parts whose tally matched.
    pub collections: usize,
    /// Rows those parts hold.
    pub rows: u64,
    /// Rows per collection, keyed by (destination database id, collection).
    pub per_collection: BTreeMap<(u64, String), u64>,
}

/// Check that the destination holds every backed-up row the RESTORE's cut
/// rule guarantees. `dest_of` maps a source database id to its destination
/// database and the cut the RESTORE took effect at there.
///
/// A mismatch fails with [`Error::RestoreVerificationFailed`] in `phase`,
/// naming every mismatched collection. Nothing is rolled back.
pub(crate) async fn verify_destination(
    state: &SharedState,
    tenant_id: u64,
    expectation: Expectation,
    dest_of: impl Fn(u64) -> Result<(DatabaseId, i64), Error>,
    phase: VerificationPhase,
) -> Result<Verified, Error> {
    let mut verified = Verified::default();
    let mut mismatches = Vec::new();
    for (source, database) in expectation.databases {
        let collections = database.collections();
        if collections.is_empty() {
            continue;
        }
        let (dest, cut) = dest_of(source)?;
        let arrays = database
            .rows
            .tallies
            .keys()
            .any(|(_, part)| *part == nodedb_types::backup_envelope::VerifiedPart::Array);
        let snap = capture_collections(state, tenant_id, dest, &collections, arrays).await?;
        // Taken after the capture: a row whose TTL passed during it counts on
        // neither side.
        let cutoff_ms = now_ms();
        let scope = Scope {
            tenant_id,
            database_id: dest,
            kek: state.wal.encryption_key(),
            cut,
        };
        let found = compare_destination(scope, database, &snap, cutoff_ms)?;
        verified.collections += found.verified.collections;
        verified.rows += found.verified.rows;
        for (key, rows) in found.verified.per_collection {
            *verified.per_collection.entry(key).or_default() += rows;
        }
        mismatches.extend(found.mismatches);
    }
    if mismatches.is_empty() {
        Ok(verified)
    } else {
        Err(Error::RestoreVerificationFailed { phase, mismatches })
    }
}

struct Compared {
    verified: Verified,
    mismatches: Vec<VerificationMismatch>,
}

/// The destination database a capture came from.
#[derive(Clone, Copy)]
struct Scope<'a> {
    tenant_id: u64,
    database_id: DatabaseId,
    kek: Option<&'a WalEncryptionKey>,
    /// The RESTORE's cut: `i64::MAX` for a re-issue no write follows.
    cut: i64,
}

/// Compare `snap`, captured from the destination database of `scope`, with
/// `expected`. The capture carries the destination's own binds.
fn compare_destination(
    scope: Scope<'_>,
    mut expected: DatabaseExpectation,
    snap: &TenantDataSnapshot,
    cutoff_ms: u64,
) -> Result<Compared, Error> {
    let rows = &mut expected.rows;
    for row in &rows.expiring {
        if row.expire_at_ms > cutoff_ms {
            continue;
        }
        if let Some(tally) = rows.tallies.get_mut(&row.part) {
            tally.remove(&row.hash);
        }
        match row.stamp {
            Some(stamp) => {
                if let Some(versions) = rows.versions.get_mut(&row.part) {
                    versions.remove(&(row.key, stamp));
                }
            }
            None => {
                if let Some(keys) = rows.keys.get_mut(&row.part) {
                    keys.remove(&row.key);
                }
            }
        }
        // The destination can still hold the expired row or have dropped
        // it, so its key's state is not checked.
        if let Some(states) = rows.states.get_mut(&row.part) {
            states.remove(&row.key);
        }
    }

    let binds = BindIndex::new(
        snap.surrogate_pk
            .iter()
            .filter(|b| b.tenant_id == scope.tenant_id)
            .map(|b| (b.collection.as_str(), b.surrogate, b.pk.as_slice())),
    );
    let walk = Walk {
        tenant_id: scope.tenant_id,
        database_id: scope.database_id,
        shapes: &expected.shapes,
        binds: &binds,
        kek: scope.kek,
    };
    let mut dest_rows = Vec::new();
    walk.snapshot(snap, &mut |row| {
        if rows
            .tallies
            .contains_key(&(row.collection.clone(), row.part))
        {
            dest_rows.push(row);
        }
    })?;
    apply_cut_rule(
        rows,
        &dest_rows,
        truncated_after_cut(&walk, snap, scope.cut)?,
        scope.cut,
    );
    rows.tallies.retain(|_, tally| tally.count != 0);
    let rows = &expected.rows;

    let mut found = Tallies::new();
    let mut found_states: HashMap<PartKey, HashMap<KeyHash, [u8; 32]>> = HashMap::new();
    let mut keyless_left = rows.keyless.clone();
    for row in dest_rows {
        let part = (row.collection, row.part);
        if !rows.tallies.contains_key(&part) {
            continue;
        }
        match row.key {
            Some(key) => {
                if let Some(state) = row.state
                    && rows
                        .states
                        .get(&part)
                        .is_some_and(|states| states.contains_key(&key))
                {
                    found_states
                        .entry(part.clone())
                        .or_default()
                        .insert(key, state);
                }
                let backed_up = match row.stamp {
                    Some(stamp) => rows
                        .versions
                        .get(&part)
                        .is_some_and(|versions| versions.contains_key(&(key, stamp))),
                    None => rows.keys.get(&part).is_some_and(|keys| keys.contains(&key)),
                };
                if !backed_up {
                    continue;
                }
            }
            // A keyless row counts once per backed-up copy: the
            // destination can hold rows of its own beside them.
            None => {
                let left = keyless_left
                    .get_mut(&part)
                    .and_then(|hashes| hashes.get_mut(&row.hash));
                match left {
                    Some(left) if *left > 0 => *left -= 1,
                    _ => continue,
                }
            }
        }
        found.entry(part).or_default().add(&row.hash);
    }

    let database_id = scope.database_id.as_u64();
    let mut mismatches = compare(database_id, &rows.tallies, &found);
    // A part whose versions already differ reports once.
    let state_mismatches = compare(
        database_id,
        &state_tallies(&rows.states, |part, key| {
            rows.states
                .get(part)
                .and_then(|states| states.get(key))
                .copied()
        }),
        &state_tallies(&rows.states, |part, key| {
            found_states
                .get(part)
                .and_then(|states| states.get(key))
                .copied()
        }),
    );
    for mismatch in state_mismatches {
        let reported = mismatches
            .iter()
            .any(|m| m.collection == mismatch.collection && m.part == mismatch.part);
        if !reported {
            mismatches.push(mismatch);
        }
    }
    let collections = {
        let mismatched: HashSet<(&str, VerifiedPart)> = mismatches
            .iter()
            .map(|m| (m.collection.as_str(), m.part))
            .collect();
        rows.tallies
            .keys()
            .filter(|(collection, part)| !mismatched.contains(&(collection.as_str(), *part)))
            .count()
    };
    let mut per_collection = BTreeMap::new();
    for ((collection, _), tally) in &rows.tallies {
        *per_collection
            .entry((database_id, collection.clone()))
            .or_default() += tally.count;
    }
    let verified = Verified {
        collections,
        rows: rows.tallies.values().map(|t| t.count).sum(),
        per_collection,
    };
    Ok(Compared {
        verified,
        mismatches,
    })
}

/// Drop from `rows` what the RESTORE does not guarantee, given the
/// destination's rows `dest` and its cut `cut` (see the module doc).
/// `truncated` names the timeseries collections a TRUNCATE ended above the
/// cut.
fn apply_cut_rule(rows: &mut ExpectedRows, dest: &[Row], truncated: HashSet<String>, cut: i64) {
    // A re-issue with no cut (MOVE TENANT) writes into a drained target: the
    // target holds no version of its own, and every backed-up version must
    // be present.
    if cut == i64::MAX {
        return;
    }
    // The newest stamp of each key's own versions.
    let mut own: HashMap<(PartKey, KeyHash), i64> = HashMap::new();
    for row in dest {
        let (Some(key), Some(stamp)) = (row.key, row.stamp) else {
            continue;
        };
        let part = (row.collection.clone(), row.part);
        let backed_up = rows
            .versions
            .get(&part)
            .is_some_and(|versions| versions.contains_key(&(key, stamp)));
        if !backed_up {
            let newest = own.entry((part, key)).or_insert(stamp);
            *newest = (*newest).max(stamp);
        }
    }
    for (part, versions) in &mut rows.versions {
        let mut dropped = Vec::new();
        versions.retain(|(key, stamp), hashes| {
            let newest_own = own.get(&(part.clone(), *key)).copied();
            let required = newest_own.is_none_or(|newest| newest <= cut && newest < *stamp);
            if !required {
                dropped.append(hashes);
            }
            required
        });
        if let Some(tally) = rows.tallies.get_mut(part) {
            for hash in &dropped {
                tally.remove(hash);
            }
        }
    }
    for (part, states) in &mut rows.states {
        states.retain(|key, _| {
            own.get(&(part.clone(), *key))
                .is_none_or(|newest| *newest <= cut)
        });
    }
    rows.tallies.retain(|(collection, part), _| {
        !(*part == VerifiedPart::Timeseries && truncated.contains(collection))
    });
}

/// The timeseries collections of `snap` a TRUNCATE of the destination ended
/// above `cut`. A partition a RESTORE installed carries the end its source
/// stamped, which is no TRUNCATE here.
fn truncated_after_cut(
    walk: &Walk<'_>,
    snap: &TenantDataSnapshot,
    cut: i64,
) -> Result<HashSet<String>, Error> {
    let mut truncated = HashSet::new();
    for blob in &snap.flushed_ts_segments {
        let ended_after_cut = blob.partitions.iter().any(|part| {
            !part.dir_name.starts_with(ENDED_INSTALL_PREFIX)
                && zerompk::from_msgpack::<nodedb_types::timeseries::PartitionMeta>(
                    &part.meta_bytes,
                )
                .ok()
                .and_then(|meta| meta.ended_at)
                .is_some_and(|end| end > cut)
        });
        if ended_after_cut {
            truncated.insert(walk.bare(walk.scoped_rest(&blob.collection_key)?));
        }
    }
    Ok(truncated)
}

/// One tally per versioned part over its keys' current states: `state`
/// answers the state of each expected key, `None` for a key with none. A
/// missing or different state changes the tally.
fn state_tallies(
    expected: &HashMap<PartKey, HashMap<KeyHash, [u8; 32]>>,
    state: impl Fn(&PartKey, &KeyHash) -> Option<[u8; 32]>,
) -> Tallies {
    let mut tallies = Tallies::new();
    for (part, keys) in expected {
        let tally = tallies.entry(part.clone()).or_default();
        for key in keys.keys() {
            if let Some(state) = state(part, key) {
                tally.add(&state);
            }
        }
    }
    tallies
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use nodedb_types::backup_envelope::VerifiedPart;

    use super::*;
    use crate::control::backup::verify::expect::expect_rows;
    use crate::control::backup::verify::walk::Shapes;

    const TENANT: u64 = 7;

    fn kv_snapshot(db: u64, rows: &[(&str, &str, u64)]) -> TenantDataSnapshot {
        let rows: Vec<crate::engine::kv::hash_table::KvSnapshotRow> = rows
            .iter()
            .zip(1u32..)
            .map(
                |((k, v, at), surrogate)| crate::engine::kv::hash_table::KvSnapshotRow {
                    key: k.as_bytes().to_vec(),
                    value: v.as_bytes().to_vec(),
                    expire_at_ms: *at,
                    surrogate,
                    sys_from: 100,
                },
            )
            .collect();
        let table = crate::engine::kv::hash_table::KvSnapshotTable {
            rows,
            history: Vec::new(),
        };
        TenantDataSnapshot {
            kv_tables: vec![(
                format!("{db}:{TENANT}:sessions"),
                zerompk::to_msgpack_vec(&table).expect("encode"),
            )],
            ..Default::default()
        }
    }

    fn scope() -> Scope<'static> {
        Scope {
            tenant_id: TENANT,
            database_id: DatabaseId::DEFAULT,
            kek: None,
            cut: i64::MAX,
        }
    }

    /// The envelope-side expectation of `snap`.
    fn expect(snap: &TenantDataSnapshot) -> DatabaseExpectation {
        let (shapes, binds) = (Shapes::new(), BindIndex::default());
        let walk = Walk {
            tenant_id: TENANT,
            database_id: DatabaseId::DEFAULT,
            shapes: &shapes,
            binds: &binds,
            kek: None,
        };
        DatabaseExpectation {
            shapes: Shapes::new(),
            rows: expect_rows(&walk, snap).expect("walk"),
        }
    }

    fn check(expected: DatabaseExpectation, found: &TenantDataSnapshot, cutoff: u64) -> Compared {
        compare_destination(scope(), expected, found, cutoff).expect("compare")
    }

    #[test]
    fn a_faithful_destination_verifies() {
        let backup = kv_snapshot(0, &[("a", "1", 0), ("b", "2", 0)]);
        let dest = kv_snapshot(0, &[("b", "2", 0), ("a", "1", 0)]);
        let compared = check(expect(&backup), &dest, 1_000);
        assert!(compared.mismatches.is_empty());
        assert_eq!(
            compared.verified,
            Verified {
                collections: 1,
                rows: 2,
                per_collection: BTreeMap::from([((0, "sessions".to_string()), 2)]),
            }
        );
    }

    #[test]
    fn a_deleted_row_fails_and_names_the_collection() {
        let backup = kv_snapshot(0, &[("a", "1", 0), ("b", "2", 0)]);
        let dest = kv_snapshot(0, &[("a", "1", 0)]);
        let compared = check(expect(&backup), &dest, 1_000);
        assert_eq!(compared.mismatches.len(), 1);
        let mismatch = &compared.mismatches[0];
        assert_eq!(mismatch.collection, "sessions");
        assert_eq!(mismatch.part, VerifiedPart::KeyValue);
        assert_eq!((mismatch.expected.count, mismatch.found.count), (2, 1));
    }

    #[test]
    fn a_changed_value_fails() {
        let backup = kv_snapshot(0, &[("a", "1", 0)]);
        let dest = kv_snapshot(0, &[("a", "2", 0)]);
        assert_eq!(check(expect(&backup), &dest, 1_000).mismatches.len(), 1);
    }

    #[test]
    fn a_row_under_another_key_is_not_the_restores() {
        let backup = kv_snapshot(0, &[("a", "1", 0)]);
        let dest = kv_snapshot(0, &[("a", "1", 0), ("older", "x", 0)]);
        assert!(check(expect(&backup), &dest, 1_000).mismatches.is_empty());
    }

    #[test]
    fn an_expired_row_counts_on_neither_side() {
        let backup = kv_snapshot(0, &[("a", "1", 0), ("gone", "x", 500)]);
        let dest = kv_snapshot(0, &[("a", "1", 0)]);
        assert!(check(expect(&backup), &dest, 1_000).mismatches.is_empty());
        // Before the deadline the row must be there.
        let compared = check(expect(&backup), &dest, 100);
        assert_eq!(compared.mismatches.len(), 1);
    }

    /// The `sessions` table with key `a`: its live row `(value, sys_from)`,
    /// if any, and its ended versions `(sys_from, sys_to, value)`.
    fn kv_key(live: Option<(&str, i64)>, ended: &[(i64, i64, &str)]) -> TenantDataSnapshot {
        let rows = live
            .map(
                |(value, sys_from)| crate::engine::kv::hash_table::KvSnapshotRow {
                    key: b"a".to_vec(),
                    value: value.as_bytes().to_vec(),
                    expire_at_ms: 0,
                    surrogate: 1,
                    sys_from,
                },
            )
            .into_iter()
            .collect();
        let versions: Vec<_> = ended
            .iter()
            .map(
                |(sys_from, sys_to, value)| crate::engine::kv::hash_table::KvSnapshotVersion {
                    sys_from: *sys_from,
                    sys_to: *sys_to,
                    value: value.as_bytes().to_vec(),
                    expire_at_ms: 0,
                    surrogate: 1,
                },
            )
            .collect();
        let history = if versions.is_empty() {
            Vec::new()
        } else {
            vec![crate::engine::kv::hash_table::KvSnapshotKeyHistory {
                key: b"a".to_vec(),
                versions,
            }]
        };
        let table = crate::engine::kv::hash_table::KvSnapshotTable { rows, history };
        TenantDataSnapshot {
            kv_tables: vec![(
                format!("0:{TENANT}:sessions"),
                zerompk::to_msgpack_vec(&table).expect("encode"),
            )],
            ..Default::default()
        }
    }

    /// Every backed-up version must sit at its own ordinal: a destination
    /// that holds the live value at another ordinal, or lost a version,
    /// fails.
    #[test]
    fn kv_ordinals_and_history_verify() {
        let backup = kv_key(Some(("1", 200)), &[(100, 200, "0")]);
        let compared = check(expect(&backup), &backup, 1_000);
        assert!(compared.mismatches.is_empty());
        assert_eq!(
            compared.verified.rows, 2,
            "the live row and its ended version"
        );
        assert_eq!(
            check(
                expect(&backup),
                &kv_key(Some(("1", 300)), &[(100, 300, "0")]),
                1_000
            )
            .mismatches
            .len(),
            1,
            "a live row at another ordinal fails"
        );
        assert_eq!(
            check(
                expect(&backup),
                &kv_key(Some(("1", 200)), &[(90, 200, "0")]),
                1_000
            )
            .mismatches
            .len(),
            1,
            "an ended version at another ordinal fails"
        );
    }

    /// A destination that wrote the key after the backup keeps that history.
    /// It verifies once the backup's value is current again, and fails while
    /// the later value is current.
    #[test]
    fn kv_later_history_verifies_only_under_the_backups_state() {
        let backup = kv_key(Some(("1", 200)), &[(100, 200, "0")]);
        let reasserted = kv_key(
            Some(("1", 400)),
            &[(100, 200, "0"), (200, 300, "1"), (300, 400, "x")],
        );
        assert!(
            check(expect(&backup), &reasserted, 1_000)
                .mismatches
                .is_empty()
        );
        let later = kv_key(Some(("x", 300)), &[(100, 200, "0"), (200, 300, "1")]);
        assert_eq!(check(expect(&backup), &later, 1_000).mismatches.len(), 1);
        let deleted = kv_key(None, &[(100, 200, "0"), (200, 300, "1")]);
        assert_eq!(
            check(expect(&backup), &deleted, 1_000).mismatches.len(),
            1,
            "a key deleted after the backup fails"
        );
    }

    /// `snap` holding the versions of document row `alice` of `people`:
    /// `(sys_from, body)`, a `None` body for a tombstone.
    fn doc_versions(versions: &[(i64, Option<&str>)]) -> TenantDataSnapshot {
        use crate::engine::sparse::btree_versioned::{TAG_LIVE, TAG_TOMBSTONE, encode_value};
        let documents_versioned = versions
            .iter()
            .map(|(sys, city)| {
                let value = match city {
                    Some(city) => {
                        let body =
                            nodedb_types::Value::Object(std::collections::HashMap::from([(
                                "city".to_string(),
                                nodedb_types::Value::String((*city).into()),
                            )]));
                        let body = nodedb_types::value_to_msgpack(&body).expect("encode");
                        encode_value(TAG_LIVE, i64::MIN, i64::MAX, &body)
                    }
                    None => encode_value(TAG_TOMBSTONE, 0, 0, &[]),
                };
                (format!("0:{TENANT}:people:0000002a\x00{sys:020}"), value)
            })
            .collect();
        TenantDataSnapshot {
            documents_versioned,
            surrogate_pk: vec![crate::types::SurrogateBindEntry {
                database_id: 0,
                tenant_id: TENANT,
                collection: "people".into(),
                surrogate: 0x2a,
                pk: b"alice".to_vec(),
            }],
            ..Default::default()
        }
    }

    /// The destination truncated the row after the backup. It verifies once
    /// the backed-up body is current again above the tombstone, and fails
    /// while the tombstone is current.
    #[test]
    fn a_document_truncated_after_the_backup_verifies_only_when_current_again() {
        let backup = doc_versions(&[(100, Some("paris"))]);
        let truncated = doc_versions(&[(100, Some("paris")), (200, None)]);
        let restored = doc_versions(&[(100, Some("paris")), (200, None), (300, Some("paris"))]);
        let expected = || DatabaseExpectation {
            shapes: Shapes::new(),
            rows: expect_rows(
                &Walk {
                    tenant_id: TENANT,
                    database_id: DatabaseId::DEFAULT,
                    shapes: &Shapes::new(),
                    binds: &BindIndex::new([("people", 0x2a, &b"alice"[..])]),
                    kek: None,
                },
                &backup,
            )
            .expect("walk"),
        };
        assert!(check(expected(), &restored, 1_000).mismatches.is_empty());
        let compared = check(expected(), &truncated, 1_000);
        assert_eq!(compared.mismatches.len(), 1);
        assert_eq!(compared.mismatches[0].part, VerifiedPart::Documents);
    }

    /// `snap` holding the versions of cell `x = 1` of array `grid`:
    /// `(system_from_ms, value)`, a `None` value for a tombstone.
    fn array_versions(versions: &[(i64, Option<i64>)]) -> TenantDataSnapshot {
        use nodedb_array::tile::cell_payload::CellPayload;
        use nodedb_array::types::cell_value::value::CellValue;
        use nodedb_array::types::coord::value::CoordValue;
        let cells: Vec<crate::engine::array::export::ArrayCellVersion> = versions
            .iter()
            .map(
                |(sys, value)| crate::engine::array::export::ArrayCellVersion {
                    coord: vec![CoordValue::Int64(1)],
                    system_from_ms: *sys,
                    payload: value.map(|v| CellPayload {
                        valid_from_ms: 0,
                        valid_until_ms: nodedb_types::OPEN_UPPER,
                        attrs: vec![CellValue::Int64(v)],
                        surrogate: nodedb_types::Surrogate::new(5),
                    }),
                    erased: false,
                    placement: 0,
                },
            )
            .collect();
        TenantDataSnapshot {
            arrays: vec![crate::types::ArrayCellsBlob {
                database_id: 0,
                tenant_id: TENANT,
                array: "grid".into(),
                vshard: 0,
                cells: zerompk::to_msgpack_vec(&cells).expect("encode"),
            }],
            ..Default::default()
        }
    }

    /// The destination deleted the cell after the backup. It verifies once
    /// the backed-up value is current again above the tombstone, and fails
    /// while the tombstone is current or a backed-up version is gone.
    #[test]
    fn an_array_cell_deleted_after_the_backup_verifies_only_when_current_again() {
        let backup = array_versions(&[(100, Some(7))]);
        let restored = array_versions(&[(100, Some(7)), (200, None), (300, Some(7))]);
        assert!(
            check(expect(&backup), &restored, 1_000)
                .mismatches
                .is_empty()
        );
        let deleted = array_versions(&[(100, Some(7)), (200, None)]);
        let compared = check(expect(&backup), &deleted, 1_000);
        assert_eq!(compared.mismatches.len(), 1);
        assert_eq!(compared.mismatches[0].part, VerifiedPart::Array);
        let changed = array_versions(&[(100, Some(7)), (200, Some(8))]);
        assert_eq!(check(expect(&backup), &changed, 1_000).mismatches.len(), 1);
        let moved = array_versions(&[(150, Some(7))]);
        assert_eq!(
            check(expect(&backup), &moved, 1_000).mismatches.len(),
            1,
            "a version at another stamp fails"
        );
    }

    fn check_at(expected: DatabaseExpectation, found: &TenantDataSnapshot, cut: i64) -> Compared {
        let scope = Scope { cut, ..scope() };
        compare_destination(scope, expected, found, 1_000).expect("compare")
    }

    /// A key the destination wrote above the RESTORE's cut is not checked:
    /// the write followed the RESTORE.
    #[test]
    fn a_write_above_the_cut_is_no_mismatch() {
        let backup = kv_key(Some(("1", 200)), &[(100, 200, "0")]);
        let after = kv_key(Some(("9", 5_000)), &[(100, 200, "0"), (200, 5_000, "1")]);
        assert!(
            check_at(expect(&backup), &after, 3_000)
                .mismatches
                .is_empty()
        );
        // With no cut the same destination fails: its state is not the
        // backup's.
        assert!(
            !check_at(expect(&backup), &after, i64::MAX)
                .mismatches
                .is_empty()
        );
    }

    /// A key whose own newest version is at or below the cut keeps the
    /// backup's state, and every backed-up version above that version.
    #[test]
    fn a_key_below_the_cut_is_checked() {
        let backup = kv_key(Some(("1", 200)), &[(100, 200, "0")]);
        // The destination's own version at 150 sits below the backed-up
        // version at 200, which the RESTORE installed: it must be there.
        let restored = kv_key(Some(("1", 200)), &[(100, 150, "0"), (150, 200, "x")]);
        assert!(
            check_at(expect(&backup), &restored, 3_000)
                .mismatches
                .is_empty()
        );
        let lost = kv_key(Some(("x", 150)), &[(100, 150, "0")]);
        assert!(
            !check_at(expect(&backup), &lost, 3_000)
                .mismatches
                .is_empty()
        );
    }
}
