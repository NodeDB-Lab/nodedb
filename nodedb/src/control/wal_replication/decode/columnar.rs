// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Columnar`.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{ColumnarOp, TimeseriesOp};
use nodedb_types::RlsWriteCheck;

/// Reconstruct a `ColumnarOp::Truncate` plan. Same idempotent-replay
/// contract as `kv::truncate`: whole-collection clear, no surrogate binding.
pub(super) fn truncate(collection: &str, restart_identity: bool) -> PhysicalPlan {
    PhysicalPlan::Columnar(ColumnarOp::Truncate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        restart_identity,
    })
}

/// Reconstruct a `TimeseriesOp::Truncate` plan. Same contract as
/// [`truncate`].
pub(super) fn timeseries_truncate(collection: &str, restart_identity: bool) -> PhysicalPlan {
    PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
        restart_identity,
    })
}

/// Reconstruct the columnar predicate-DML plan. The apply re-scans local
/// columnar state at this committed log position and mutates the predicate
/// matches — deterministic across replicas by Raft log order (identical
/// prior state ⇒ identical matching set).
///
/// No RLS predicate travels here: this shape only ever carries a collection
/// with NO write policy attached — `entry_columnar_family::columnar_write`
/// refuses to encode `ColumnarBulkDml` for a governed collection, so a
/// governed predicate DML never reaches this decoder. A collection that DOES
/// carry a write policy goes through [`bulk_dml_resolved`] instead, which
/// carries the already-decided rows rather than a predicate.
pub(super) fn bulk_dml(
    collection: &str,
    filters: &[u8],
    is_update: bool,
    updates: &[(String, Vec<u8>)],
) -> PhysicalPlan {
    if is_update {
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        })
    } else {
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            filters: filters.to_vec(),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        })
    }
}

/// Reconstruct the columnar resolved-row-set DML plan
/// (`ColumnarOp::ResolvedUpdate` / `ColumnarOp::ResolvedDelete`).
///
/// Stamps `RlsWriteCheck::decided_earlier_in_request()`, not
/// `already_decided_elsewhere()`: the identity that authored this write
/// decided these exact rows against the write policy upstream, in the
/// Control Plane, and shipped the verdict — it did not go missing the way a
/// follower's own writing identity does. `decided_earlier_in_request` is the
/// tag for "a live identity already decided this row image"; every replica
/// applying this entry, including the leader that proposed it, is in that
/// same position.
pub(super) fn bulk_dml_resolved(
    collection: &str,
    is_update: bool,
    rows: &[super::super::types::ColumnarResolvedRow],
) -> crate::Result<PhysicalPlan> {
    let decode_value = |bytes: &[u8]| -> crate::Result<nodedb_types::Value> {
        nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Internal {
            detail: format!("columnar resolved dml row decode failed: {e}"),
        })
    };
    if is_update {
        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let pk = decode_value(&row.pk_msgpack)?;
            let new_row = match decode_value(&row.new_row_msgpack)? {
                nodedb_types::Value::Array(values) => values,
                other => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "columnar resolved dml row: expected an array post-image, got {other:?}"
                        ),
                    });
                }
            };
            decoded.push((pk, new_row));
        }
        Ok(PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            rows: decoded,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }))
    } else {
        let pks = rows
            .iter()
            .map(|row| decode_value(&row.pk_msgpack))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            pks,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::wal_replication::decode;
    use crate::control::wal_replication::types::ReplicatedEntry;
    use crate::types::{DatabaseId, TenantId, VShardId};
    use nodedb_types::QualifiedCollection;

    /// Decide + encode in one call, so each test names only the plan it encodes.
    fn to_replicated_entry(plan: &PhysicalPlan) -> crate::Result<Option<ReplicatedEntry>> {
        let write = crate::control::wal_replication::ReplicableWrite::decide_for_replication(plan)?;
        crate::control::wal_replication::encode::to_replicated_entry(
            TenantId::new(1),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            &write,
        )
    }

    fn round_trip(plan: &PhysicalPlan) -> PhysicalPlan {
        let entry = to_replicated_entry(plan)
            .expect("encode must not error")
            .expect("a truncate must replicate");
        let (_, _, decoded, _) = decode::from_replicated_entry(&entry.to_bytes(), None)
            .expect("decode")
            .expect("a replicated entry");
        decoded
    }

    /// A columnar truncate replicates with its `restart_identity` flag, so
    /// every applying node resets the same sequences.
    #[test]
    fn columnar_truncate_round_trips_with_restart_identity() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cols"),
            restart_identity: true,
        });
        let PhysicalPlan::Columnar(ColumnarOp::Truncate {
            collection,
            restart_identity,
        }) = round_trip(&plan)
        else {
            panic!("decoded to the wrong shape");
        };
        assert_eq!(collection.as_str(), "cols");
        assert!(restart_identity);
    }

    #[test]
    fn timeseries_truncate_round_trips_with_restart_identity() {
        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "ts"),
            restart_identity: true,
        });
        let PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection,
            restart_identity,
        }) = round_trip(&plan)
        else {
            panic!("decoded to the wrong shape");
        };
        assert_eq!(collection.as_str(), "ts");
        assert!(restart_identity);
    }
}
