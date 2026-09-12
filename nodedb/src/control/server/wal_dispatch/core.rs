// SPDX-License-Identifier: BUSL-1.1

#![deny(clippy::wildcard_enum_match_arm)]

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::credential::CredentialStore;
use crate::types::{DatabaseId, TenantId, VShardId};
use crate::wal::manager::WalManager;

use super::super::wal_dispatch_kv;

/// Outcome of [`wal_append_if_write`] / [`wal_append_if_write_with_creds`]:
/// the allocated WAL LSN (if a durable record was appended) and, for a
/// TTL-bearing KV write, the wall-clock instant resolved at append time.
///
/// `resolved_now_ms` mirrors `lsn`'s cross-plane contract: the caller stamps
/// it onto the dispatched `Request` (via `WriteDispatch` / `DataPlaneDispatch`)
/// so the Data Plane's live apply installs the SAME instant the durable WAL
/// record carries, rather than re-reading the wall clock at apply time — the
/// two must agree by construction, or a crash between WAL append and apply
/// lets replay recompute `now_ms` at restart time and drift the TTL's expiry
/// forward by the crash-to-restart delay. A plain struct rather than a
/// `(Option<Lsn>, Option<u64>)` tuple: both fields are the same "maybe a
/// number" shape and trivially swappable by position across the several call
/// sites this threads through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalAppendOutcome {
    /// WAL LSN allocated for this write, or `None` for reads / control ops /
    /// WAL-bypassed writes.
    pub lsn: Option<crate::types::Lsn>,
    /// Wall-clock instant (ms since epoch) resolved for a TTL-bearing KV
    /// write's `expire_at_ms`. `None` for every non-KV plan and every KV write
    /// without a TTL.
    pub resolved_now_ms: Option<u64>,
}

/// Inputs for [`wal_append`]: the write's target coordinates, the plan whose
/// redo record is to be encoded, and the two optional knobs only some callers
/// need.
pub struct WalAppendRequest<'a> {
    pub wal: &'a WalManager,
    pub tenant_id: TenantId,
    pub vshard_id: VShardId,
    pub database_id: DatabaseId,
    pub plan: &'a PhysicalPlan,
    /// Credential store for the timeseries `wal=false` bypass check.
    pub credentials: Option<&'a CredentialStore>,
    /// Wall-clock instant (ms since epoch) to resolve a TTL-bearing KV write's
    /// `expire_at_ms` against, instead of reading this node's clock. `Some`
    /// only when the instant was decided elsewhere and the durable record must
    /// carry that exact value: a Raft-committed entry carries the instant the
    /// proposing node resolved, and every replica's redo record — like every
    /// replica's live apply — must install it verbatim, or a replica's WAL
    /// replay resurrects a different `expire_at_ms` than its peers.
    pub now_override: Option<u64>,
}

/// Append a write operation to the WAL for single-node durability.
///
/// Serializes the write as MessagePack and appends to the appropriate
/// WAL record type. Read operations are no-ops (return Ok immediately).
///
/// Returns the WAL LSN allocated for writes it appended (`Some`), or `None`
/// for reads / control ops that need no WAL record. The caller stamps the
/// returned LSN onto the dispatched `Request` so the Data Plane can record the
/// committed write version.
pub fn wal_append_if_write(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<WalAppendOutcome> {
    wal_append_if_write_with_creds(wal, tenant_id, vshard_id, database_id, plan, None)
}

/// WAL append with optional credential store for timeseries WAL bypass check.
///
/// Returns the appended write's WAL LSN (`Some`), or `None` for reads / control
/// ops / WAL-bypassed writes.
pub fn wal_append_if_write_with_creds(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
    credentials: Option<&CredentialStore>,
) -> crate::Result<WalAppendOutcome> {
    wal_append(WalAppendRequest {
        wal,
        tenant_id,
        vshard_id,
        database_id,
        plan,
        credentials,
        now_override: None,
    })
}

/// Encode and append the redo record for `plan`, if it is a write.
///
/// The full-control entry point behind [`wal_append_if_write`] /
/// [`wal_append_if_write_with_creds`]; callers that must pin a TTL-bearing KV
/// write's resolved instant reach for this one.
pub fn wal_append(req: WalAppendRequest<'_>) -> crate::Result<WalAppendOutcome> {
    let WalAppendRequest {
        wal,
        tenant_id,
        vshard_id,
        database_id,
        plan,
        credentials,
        now_override,
    } = req;
    let mut resolved_now_ms: Option<u64> = None;
    // Every engine routes through one exhaustive per-engine match (no `_`
    // catch-all anywhere, enforced by `deny(wildcard_enum_match_arm)`), so a
    // future write variant of any engine fails to compile until its durability
    // is decided by name — it can never silently become non-durable.
    let appended: Option<crate::types::Lsn> = match plan {
        PhysicalPlan::Document(op) => {
            super::document::wal_append_document_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Vector(op) => {
            super::vector::wal_append_vector_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Crdt(op) => {
            super::crdt::wal_append_crdt_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Graph(op) => {
            super::graph::wal_append_graph_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Columnar(op) => {
            super::columnar::wal_append_columnar_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Timeseries(op) => super::timeseries::wal_append_timeseries_op(
            wal,
            tenant_id,
            vshard_id,
            database_id,
            op,
            credentials,
        )?,
        // KV write operations — delegated to wal_dispatch_kv. The only engine
        // that resolves a wall-clock instant (TTL `expire_at_ms`), threaded back
        // out via `resolved_now_ms`.
        PhysicalPlan::Kv(kv_op) => {
            let outcome = wal_dispatch_kv::wal_append_kv_op(
                wal,
                tenant_id,
                vshard_id,
                database_id,
                kv_op,
                now_override,
            )?;
            resolved_now_ms = outcome.resolved_now_ms;
            outcome.lsn
        }
        PhysicalPlan::Array(op) => {
            super::array::wal_append_array_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Text(op) => {
            super::text::wal_append_text_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        PhysicalPlan::Spatial(op) => {
            super::spatial::wal_append_spatial_op(wal, tenant_id, vshard_id, database_id, op)?
        }
        // NotAWrite — reads / query ops / control commands. `Meta` durable
        // writes (WAL append, transaction batch, Calvin apply) are logged on
        // their own dedicated paths, never through this autocommit oracle;
        // `Query` is joins/aggregates/scans; `ClusterArray` is a routing wrapper
        // resolved by the coordinator before the SPSC bridge — it fans its cells
        // out to the owning shards, and each owner's apply mints the redo for
        // the cells it actually holds. All produce no durable record here.
        PhysicalPlan::Meta(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    };
    Ok(WalAppendOutcome {
        lsn: appended,
        resolved_now_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{SpatialOp, TextOp};
    use nodedb_types::QualifiedCollection;
    use nodedb_types::Surrogate;
    use nodedb_types::geometry::Geometry;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn last_record_of_type(
        wal: &WalManager,
        record_type: nodedb_wal::record::RecordType,
    ) -> nodedb_wal::WalRecord {
        wal.sync().expect("sync wal");
        wal.replay()
            .expect("read wal")
            .into_iter()
            .rfind(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(record_type)
            })
            .expect("expected record of this type")
    }

    #[test]
    fn fts_index_doc_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            surrogate: Surrogate::new(7),
            text: "hello world".to_string(),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "FtsIndexDoc must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::FtsIndex);
        let decoded =
            nodedb_wal::record::FtsIndexPayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "docs");
        assert_eq!(decoded.text, "hello world");
        assert_eq!(
            decoded.doc_id,
            crate::engine::document::store::StorageKey::for_surrogate(Surrogate::new(7))
                .to_string()
        );
    }

    #[test]
    fn fts_delete_doc_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            surrogate: Surrogate::new(7),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "FtsDeleteDoc must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::FtsDelete);
        let decoded =
            nodedb_wal::record::FtsDeletePayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "docs");
        assert_eq!(
            decoded.doc_id,
            crate::engine::document::store::StorageKey::for_surrogate(Surrogate::new(7))
                .to_string()
        );
    }

    #[test]
    fn spatial_insert_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(9),
            geometry: Geometry::point(10.0, 20.0),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "SpatialOp::Insert must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::SpatialPut);
        let decoded =
            nodedb_wal::record::SpatialPutPayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "places");
        assert_eq!(decoded.field, "loc");
        let geometry: Geometry =
            zerompk::from_msgpack(&decoded.geometry_bytes).expect("decode geometry");
        assert_eq!(geometry, Geometry::point(10.0, 20.0));
    }

    #[test]
    fn spatial_delete_appends_and_decodes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "places"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(9),
            provenance: None,
        });

        let outcome = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "SpatialOp::Delete must produce a durable LSN"
        );

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::SpatialDelete);
        let decoded =
            nodedb_wal::record::SpatialDeletePayload::from_bytes(&record.payload).expect("decode");
        assert_eq!(decoded.collection, "places");
        assert_eq!(decoded.field, "loc");
    }

    /// Whether the on-disk WAL segments hold a `VectorIndexDrop` record.
    /// Reads the files, never the writer's buffer, so this is exactly what a
    /// restart would replay.
    fn drop_record_on_disk(wal: &WalManager) -> bool {
        wal.replay().expect("read wal").iter().any(|r| {
            nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                == Some(nodedb_wal::record::RecordType::VectorIndexDrop)
        })
    }

    /// `DROP INDEX` on a vector index is not durable until its WAL record is
    /// fsynced: the records it cancels were already synced by the writes that
    /// acked them, so a buffered-only drop is lost and replay resurrects it.
    #[tokio::test]
    async fn vector_index_drop_reaches_disk_only_after_the_durability_barrier() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());

        // Stand in for the writes the drop cancels: appended, then fsynced by
        // their own acknowledgement, so the segment already holds records.
        let created = PhysicalPlan::Spatial(SpatialOp::Insert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            field: "loc".to_string(),
            surrogate: Surrogate::new(1),
            geometry: Geometry::point(1.0, 2.0),
            provenance: None,
        });
        wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &created,
        )
        .expect("append");
        wal.sync().expect("sync wal");

        let plan = PhysicalPlan::Vector(nodedb_physical::physical_plan::VectorOp::DropIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            field_name: "emb".to_string(),
        });

        let lsn = wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append")
        .lsn
        .expect("VectorOp::DropIndex must mint a WAL record");

        assert!(
            !drop_record_on_disk(&wal),
            "an appended drop record is buffered only; nothing is on disk yet"
        );
        assert!(wal.durable_through() < lsn.as_u64());

        wal.wait_durable(lsn).await.expect("group-commit fsync");

        assert!(wal.durable_through() >= lsn.as_u64());
        assert!(
            drop_record_on_disk(&wal),
            "after the barrier the drop record must be replayable from disk"
        );
    }
}
