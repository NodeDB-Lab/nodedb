// SPDX-License-Identifier: BUSL-1.1

//! The check on a point write whose key was unbound at planning.
//!
//! An unbound key has no row, so the settlement folds nothing and stamps a
//! collection-scoped read at its cut. That read catches an insert that commits
//! after the cut. An insert that bound the key between planning and the cut
//! commits below it, and the apply then rebinds the write onto that row with
//! no settlement. So the key is looked up after the cut, and a key bound by
//! then refuses the write.

use nodedb_physical::physical_plan::MaterializedSumBinding;
use nodedb_types::CollectionKey;

use super::stored::StoredRowScope;
use crate::control::server::surrogate_exchange::lookup_surrogate_routed;
use crate::control::state::SharedState;
use crate::query::sum_target_is_co_resident;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Refuse a point write whose key was unbound at planning and is bound now.
///
/// Answers [`CalvinSerializationConflict`](crate::Error::CalvinSerializationConflict),
/// so the client retries and the retry plans the binding. A source whose
/// targets are all co-resident folds on the source core from the real row and
/// needs no check.
pub(super) async fn refuse_key_bound_since_planning(
    state: &SharedState,
    bindings: &[MaterializedSumBinding],
    scope: &StoredRowScope<'_>,
    (tenant_id, database_id): (TenantId, DatabaseId),
    trace_id: TraceId,
) -> crate::Result<()> {
    let key = CollectionKey::from_qualified_str(database_id, scope.collection)?;
    if bindings
        .iter()
        .all(|binding| sum_target_is_co_resident(key, &binding.target_collection))
    {
        return Ok(());
    }
    match lookup_surrogate_routed(
        state,
        key,
        tenant_id,
        scope.document_id.as_bytes(),
        trace_id,
    )
    .await?
    {
        Some(_) => Err(crate::Error::CalvinSerializationConflict),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::super::resolve::resolve_materialized_sum_targets;
    use super::super::settle::{SettleInput, settle_cross_shard_images};
    use super::super::stored::{PostImage, stored_row_scope};
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::shared::session::read_set::{ReadKey, ReadPoint};
    use crate::wal::WalManager;

    const TENANT: TenantId = TenantId::new(7);
    const DB: DatabaseId = DatabaseId::DEFAULT;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("unbound.wal")).expect("open WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct state");
        (state, directory)
    }

    /// A source and a target that do not share a vShard.
    fn cross_shard_pair() -> (String, String) {
        let source = "unbound_entries".to_string();
        let home = CollectionKey::from_bare(DB, &source).vshard();
        let target = (0..2048)
            .map(|i| format!("unbound_accounts_{i}"))
            .find(|candidate| CollectionKey::from_bare(DB, candidate).vshard() != home)
            .expect("a second vShard");
        (source, target)
    }

    fn binding(target: &str) -> MaterializedSumBinding {
        MaterializedSumBinding {
            target_collection: target.to_string(),
            target_column: "balance".to_string(),
            join_column: "account_id".to_string(),
            value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
            declared_primary_key: None,
        }
    }

    fn unbound_scope(collection: &str) -> StoredRowScope<'_> {
        StoredRowScope {
            collection,
            document_id: "e9",
            surrogate: None,
            updates: &[],
            post_image: PostImage::Removed,
            txn_id: None,
        }
    }

    fn unbound_delete() -> DocumentOp {
        DocumentOp::PointDelete {
            collection: QualifiedCollection::new(DB, "entries"),
            document_id: "e9".to_string(),
            surrogate: None,
            pk_bytes: b"e9".to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
        }
    }

    /// A delete of an unbound key that names no scope reads nothing, and an
    /// insert of the key goes unseen.
    #[test]
    fn an_unbound_point_write_names_its_row_with_no_surrogate() {
        let op = unbound_delete();
        let scope = stored_row_scope(&op, None).expect("an unbound delete names its row");
        assert_eq!(scope.collection, "entries");
        assert_eq!(scope.document_id, "e9");
        assert_eq!(scope.surrogate, None);
    }

    /// An unbound key with no image still observed the source, and only a
    /// collection-scoped entry can see the surrogate an insert mints.
    #[test]
    fn an_unbound_key_stamps_a_collection_scoped_read() {
        let (source, target) = cross_shard_pair();
        let input = SettleInput {
            source_collection: &source,
            images: &[],
            source_row: None,
            observed_at: 42,
            crs: &nodedb_crs::CrsContext::builtin_only(),
        };
        let settlement =
            settle_cross_shard_images(&[binding(&target)], &input, &[], None, TENANT, DB)
                .expect("settle");
        assert!(settlement.tasks.is_empty());
        assert_eq!(settlement.reads.len(), 1);
        assert_eq!(settlement.reads[0].key, ReadKey::Predicate);
        assert_eq!(settlement.reads[0].observed_at, Some(42));
    }

    /// A deleted row keeps its binding, and a re-insert reuses the surrogate,
    /// so the absent-row entry names that surrogate.
    #[test]
    fn an_absent_row_stamps_its_surrogate() {
        let (source, target) = cross_shard_pair();
        let input = SettleInput {
            source_collection: &source,
            images: &[],
            source_row: Some(Surrogate::new(11)),
            observed_at: 42,
            crs: &nodedb_crs::CrsContext::builtin_only(),
        };
        let settlement =
            settle_cross_shard_images(&[binding(&target)], &input, &[], None, TENANT, DB)
                .expect("settle");
        assert_eq!(
            settlement.reads[0].key,
            ReadKey::Point {
                repr: ReadPoint::Surrogate(11)
            }
        );
    }

    /// A join-less row folds nothing, and a write that sets its join value
    /// after the read owes a delta the stamp must catch.
    #[test]
    fn a_row_with_no_join_value_stamps_its_read() {
        let (source, target) = cross_shard_pair();
        let images = vec![(
            Some(serde_json::json!({"amount": 3})),
            Some(serde_json::json!({"amount": 7})),
        )];
        let input = SettleInput {
            source_collection: &source,
            images: &images,
            source_row: Some(Surrogate::new(11)),
            observed_at: 42,
            crs: &nodedb_crs::CrsContext::builtin_only(),
        };
        let settlement =
            settle_cross_shard_images(&[binding(&target)], &input, &[], None, TENANT, DB)
                .expect("settle");
        assert!(settlement.shipped.is_empty());
        assert_eq!(settlement.reads.len(), 1);
    }

    /// A co-resident write stamps nothing: a read would cost it a Calvin round.
    #[test]
    fn a_co_resident_absent_row_stamps_nothing() {
        let (source, _) = cross_shard_pair();
        let input = SettleInput {
            source_collection: &source,
            images: &[],
            source_row: None,
            observed_at: 42,
            crs: &nodedb_crs::CrsContext::builtin_only(),
        };
        let settlement =
            settle_cross_shard_images(&[binding(&source)], &input, &[], None, TENANT, DB)
                .expect("settle");
        assert!(settlement.reads.is_empty());
    }

    /// A key bound since planning that passes lets the apply rewrite the row
    /// with no settlement.
    #[tokio::test]
    async fn a_key_bound_since_planning_refuses_the_write() {
        let (state, _directory) = test_state();
        let (source, target) = cross_shard_pair();
        state
            .surrogate_assigner
            .assign(CollectionKey::from_bare(DB, &source), TENANT, b"e9")
            .await
            .expect("bind the key");
        let end = refuse_key_bound_since_planning(
            &state,
            &[binding(&target)],
            &unbound_scope(&source),
            (TENANT, DB),
            TraceId::ZERO,
        )
        .await;
        assert!(
            matches!(end, Err(crate::Error::CalvinSerializationConflict)),
            "{end:?}"
        );
    }

    /// A key still unbound must pass: a conflict here aborts a write no insert
    /// raced.
    #[tokio::test]
    async fn a_key_still_unbound_passes() {
        let (state, _directory) = test_state();
        let (source, target) = cross_shard_pair();
        let end = refuse_key_bound_since_planning(
            &state,
            &[binding(&target)],
            &unbound_scope(&source),
            (TENANT, DB),
            TraceId::ZERO,
        )
        .await;
        assert!(end.is_ok(), "{end:?}");
    }

    /// A co-resident source folds from the real row, so a bound key must not
    /// abort it.
    #[tokio::test]
    async fn a_co_resident_source_skips_the_check() {
        let (state, _directory) = test_state();
        let (source, _) = cross_shard_pair();
        state
            .surrogate_assigner
            .assign(CollectionKey::from_bare(DB, &source), TENANT, b"e9")
            .await
            .expect("bind the key");
        let end = refuse_key_bound_since_planning(
            &state,
            &[binding(&source)],
            &unbound_scope(&source),
            (TENANT, DB),
            TraceId::ZERO,
        )
        .await;
        assert!(end.is_ok(), "{end:?}");
    }

    /// A source that drives no binding never reaches a read.
    #[tokio::test]
    async fn an_unbound_write_of_a_plain_collection_reads_nothing() {
        let (state, _directory) = test_state();
        let mut tasks = vec![nodedb_physical::physical_task::PhysicalTask {
            tenant_id: TENANT,
            vshard_id: crate::types::VShardId::new(0),
            database_id: DB,
            plan: PhysicalPlan::Document(unbound_delete()),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }];
        let reads =
            resolve_materialized_sum_targets(&state, &mut tasks, TENANT, DB, None, TraceId::ZERO)
                .await
                .expect("a plain collection settles nothing");
        assert!(reads.is_empty());
    }
}
