// SPDX-License-Identifier: BUSL-1.1

//! Finish the drop of a full-text index.
//!
//! `DROP INDEX` of a full-text index first marks its record `Dropping`. A
//! `Dropping` record declares no text field and names no analyzer, so once
//! the mark applies every node's writes stop indexing its fields and tokenize
//! with the analysis the other records name. [`finish_text_index_drop`] then
//! runs, in this order:
//!
//! 1. Every node rebuilds the collection's text from the fields and the
//!    analysis the other records declare. The dropped fields leave every
//!    node's index.
//! 2. The kind's teardown runs. A full-text index holds no other state.
//! 3. The record and its owner row leave the catalog.
//!
//! An error at any step leaves the record `Dropping`. A retried `DROP
//! INDEX` and the boot reconcile both run this function again. Every step
//! is idempotent: the rebuild replaces the collection's whole text index,
//! and a core answers a repeat rebuild over the same fields at once.
//!
//! Inside an explicit transaction the mark is buffered and lands at COMMIT.
//! This function runs after it, as a deferred effect. A ROLLBACK leaves the
//! record as it was.

use crate::control::security::catalog::{IndexKind, IndexRecordState, StoredIndexRecord};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::super::super::index_registry::propose_delete_index_record;
use super::super::super::super::result::DdlError;
use super::text_build::{RebuildScope, rebuild_text_index};

/// Rebuild `collection`'s text without the fields of the `Dropping`
/// full-text record `index_name`, tear it down, then remove its record and
/// owner row.
///
/// A record the catalog no longer holds gets only the rebuild: another run
/// of this function finished the drop. A full-text record of `collection`
/// in another state keeps its place after the teardown. The same
/// transaction re-created the index after the drop, and the new record owns
/// the name. The rebuild then indexes the new record's fields with the new
/// record's analysis.
///
/// A record of another kind, or on another collection, also owns the name:
/// the same transaction created that index after the drop. The dropped
/// full-text index still gets its teardown. Its owner row leaves the
/// catalog when the new kind files its owner under another object type.
pub(crate) async fn finish_text_index_drop(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    index_name: &str,
) -> Result<(), DdlError> {
    rebuild_text_index(
        state,
        tenant_id,
        database_id,
        collection,
        RebuildScope::EveryNode,
    )
    .await?;

    let current = state
        .credentials
        .catalog()
        .get_index_record(database_id.as_u64(), tenant_id.as_u64(), index_name)
        .map_err(|e| DdlError::from_error_in_context("read index registry", &e))?;
    let Some(record) = current else {
        return Ok(());
    };
    if record.kind != IndexKind::FullText || record.collection != collection {
        return finish_replaced_drop(state, &record, tenant_id, database_id, collection).await;
    }

    super::teardown::teardown(state, &record, database_id, tenant_id).await?;
    if record.state != IndexRecordState::Dropping {
        return Ok(());
    }
    propose_delete_index_record(
        state,
        database_id,
        tenant_id,
        index_name,
        collection,
        IndexKind::FullText,
    )
    .await?;
    crate::control::server::shared::ddl::owner::propose_delete_owner(
        state,
        IndexKind::FullText.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        index_name,
    )
    .await
}

/// Tear down the dropped full-text index `replacement.name` of `collection`,
/// whose record `replacement` replaced: an index of another kind, or one on
/// another collection. The replacement's record stays.
async fn finish_replaced_drop(
    state: &SharedState,
    replacement: &StoredIndexRecord,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let dropped = StoredIndexRecord {
        database_id: database_id.as_u64(),
        tenant_id: tenant_id.as_u64(),
        name: replacement.name.clone(),
        kind: IndexKind::FullText,
        collection: collection.to_string(),
        fields: Vec::new(),
        is_active: true,
        state: IndexRecordState::Dropping,
        analyzer: None,
        fuzzy_default: None,
    };
    super::teardown::teardown(state, &dropped, database_id, tenant_id).await?;
    if replacement.kind.owner_object_type() == IndexKind::FullText.owner_object_type() {
        return Ok(());
    }
    crate::control::server::shared::ddl::owner::propose_delete_owner(
        state,
        IndexKind::FullText.owner_object_type(),
        database_id.as_u64(),
        tenant_id,
        &replacement.name,
    )
    .await
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use nodedb_physical::physical_plan::{TextOp, TextRebuildProgress};

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide};
    use crate::bridge::envelope::{ErrorCode, Payload, PhysicalPlan, Response, Status};
    use crate::control::security::catalog::auth_types::object_type;
    use crate::control::security::catalog::{StoredCollection, StoredIndexRecord, StoredOwner};
    use crate::control::security::identity::AuthenticatedIdentity;
    use crate::types::Lsn;

    pub(crate) const TENANT: u64 = 1;
    pub(crate) const COLLECTION: &str = "docs";

    /// Answer every request with `Ok`, except a `TextOp::RebuildIndex`
    /// while `fail_rebuild` is set, which gets an error. A rebuild op that
    /// succeeds answers `Installed`, as a core whose rebuild cut over does.
    pub(crate) async fn answer_failing_rebuilds(
        state: Arc<SharedState>,
        mut side: CoreChannelDataSide,
        fail_rebuild: Arc<AtomicBool>,
    ) {
        let installed = zerompk::to_msgpack_vec(&TextRebuildProgress::Installed { replayed: 0 })
            .expect("encode rebuild progress");
        loop {
            while let Ok(request) = side.request_rx.try_pop() {
                let rebuild = matches!(
                    request.inner.plan,
                    PhysicalPlan::Text(TextOp::RebuildIndex { .. })
                );
                let refused = fail_rebuild.load(Ordering::Acquire) && rebuild;
                let error_code = refused.then(|| {
                    Box::new(ErrorCode::Internal {
                        detail: "injected rebuild refusal".into(),
                    })
                });
                let payload = if rebuild && !refused {
                    Payload::from_vec(installed.clone())
                } else {
                    Payload::empty()
                };
                side.response_tx
                    .try_push(BridgeResponse {
                        inner: Response {
                            request_id: request.inner.request_id,
                            status: if refused { Status::Error } else { Status::Ok },
                            attempt: 1,
                            partial: false,
                            payload,
                            watermark_lsn: Lsn::ZERO,
                            error_code,
                            stage_vote: None,
                            read_vshards: crate::types::ReadShards::new(),
                            write_set: Vec::new(),
                        },
                    })
                    .expect("fake data-plane response queue has capacity");
            }
            state.poll_and_route_responses();
            tokio::task::yield_now().await;
        }
    }

    /// Store the active collection `COLLECTION` and its owner row. The
    /// metadata applier halts on a collection row with no owner row.
    pub(crate) fn seed_collection(state: &SharedState) {
        let mut coll = StoredCollection::stamped_for_test(TENANT, COLLECTION, "owner");
        coll.database_id = DatabaseId::DEFAULT;
        let catalog = state.credentials.catalog();
        catalog
            .put_collection(DatabaseId::DEFAULT, &coll)
            .expect("seed collection");
        catalog
            .put_owner(&StoredOwner {
                database_id: DatabaseId::DEFAULT.as_u64(),
                object_type: object_type::COLLECTION.to_string(),
                object_name: COLLECTION.to_string(),
                tenant_id: TENANT,
                owner_username: "owner".to_string(),
            })
            .expect("seed collection owner");
    }

    /// Store a full-text record `name` on `COLLECTION` over `body`.
    pub(crate) fn seed_record(state: &SharedState, name: &str, record_state: IndexRecordState) {
        state
            .credentials
            .catalog()
            .put_index_record(&StoredIndexRecord {
                database_id: DatabaseId::DEFAULT.as_u64(),
                tenant_id: TENANT,
                name: name.to_string(),
                kind: IndexKind::FullText,
                collection: COLLECTION.to_string(),
                fields: vec!["body".to_string()],
                is_active: true,
                state: record_state,
                analyzer: None,
                fuzzy_default: None,
            })
            .expect("seed index record");
    }

    /// The committed record `name`, if any.
    pub(crate) fn record(state: &SharedState, name: &str) -> Option<StoredIndexRecord> {
        state
            .credentials
            .catalog()
            .get_committed_index_record(DatabaseId::DEFAULT.as_u64(), TENANT, name)
            .expect("read index record")
    }

    fn superuser() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_internal_service(
            1,
            "text-drop-test",
            TenantId::new(TENANT),
            Vec::new(),
            true,
            None,
            AuthenticatedIdentity::default_database_set(true),
        )
    }

    /// A `Dropping` mark of this transaction frees the name for a CREATE of
    /// any kind. A committed `Dropping` record holds it with 55000 until a
    /// DROP INDEX finishes it, and a live record holds it as itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_a_drop_of_this_transaction_frees_an_index_name() {
        use crate::control::server::shared::ddl::index_registry::{
            index_name_holder, propose_index_dropping,
        };
        use crate::control::server::shared::session::{conn_scope, ddl_buffer};

        let fail = Arc::new(AtomicBool::new(false));
        let cluster = crate::control::cluster::test_one_node::boot_with_core(
            |_| {},
            move |state, side| tokio::spawn(answer_failing_rebuilds(state, side, fail)),
        )
        .await;
        let state = Arc::clone(&cluster.state);
        seed_collection(&state);
        seed_record(&state, "ft_live", IndexRecordState::Ready);
        seed_record(&state, "ft_held", IndexRecordState::Dropping);
        let holder = |name: &str| {
            index_name_holder(
                &state,
                DatabaseId::DEFAULT,
                TenantId::new(TENANT),
                name,
                "CREATE INDEX",
            )
        };

        let live = holder("ft_live").expect("a live record reads");
        assert_eq!(live.map(|r| r.kind), Some(IndexKind::FullText));
        let held = holder("ft_held").expect_err("a committed Dropping record holds its name");
        assert_eq!(held.sqlstate, "55000");
        assert!(
            held.message.contains("DROP INDEX ft_held"),
            "{}",
            held.message
        );

        conn_scope::scoped(async {
            ddl_buffer::activate();
            let live = record(&state, "ft_live").expect("seeded");
            propose_index_dropping(&state, &live)
                .await
                .expect("buffer the Dropping mark");
            assert!(
                holder("ft_live").expect("read").is_none(),
                "the transaction's own drop frees the name"
            );
            ddl_buffer::discard();
        })
        .await;

        drop(state);
        cluster.shutdown().await;
    }

    /// A rebuild error fails the DROP and leaves the record `Dropping`, so
    /// it still holds the name and declares no field. A retry finishes the
    /// drop and removes the record.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_drop_whose_rebuild_fails_keeps_its_record_and_a_retry_finishes_it() {
        let fail = Arc::new(AtomicBool::new(true));
        let core_fail = Arc::clone(&fail);
        let cluster = crate::control::cluster::test_one_node::boot_with_core(
            |_| {},
            move |state, side| tokio::spawn(answer_failing_rebuilds(state, side, core_fail)),
        )
        .await;
        let state = Arc::clone(&cluster.state);
        seed_collection(&state);
        seed_record(&state, "ft_body", IndexRecordState::Ready);
        let identity = superuser();
        let request = super::super::drop::DropIndexRequest {
            index_name: "ft_body",
            if_exists: false,
            kind: None,
            database_id: DatabaseId::DEFAULT,
        };

        let refused = super::super::drop::drop_index(&state, &identity, &request).await;
        assert!(refused.is_err(), "a failed rebuild fails the DROP");
        assert_eq!(
            record(&state, "ft_body").map(|r| r.state),
            Some(IndexRecordState::Dropping),
            "the record stays, marked Dropping"
        );

        fail.store(false, Ordering::Release);
        super::super::drop::drop_index(&state, &identity, &request)
            .await
            .expect("a retried DROP finishes");
        assert!(record(&state, "ft_body").is_none(), "the record is gone");

        drop(state);
        cluster.shutdown().await;
    }
}
