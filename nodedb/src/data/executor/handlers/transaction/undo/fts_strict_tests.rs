// SPDX-License-Identifier: BUSL-1.1

//! FTS rollback coverage for a STRICT (Binary Tuple) collection — the
//! first-time strict FTS-undo code path.
//!
//! A rolled-back transactional DELETE must re-index the restored strict body
//! (decoded via the schema-aware `decode_stored_document`) so the document is
//! searchable again; a rolled-back PUT must remove the postings it wrote.

use std::time::{Duration, Instant};

use nodedb_physical::physical_plan::{DocumentOp, StorageMode};
use nodedb_types::Surrogate;
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::handlers::transaction::sub_plan_doc::{TxPointDelete, TxPointPut};
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

const DB: u64 = 0;
const TID: u64 = 1;
const COLL: &str = "strict_docs";
const PK: &str = "row1";

/// Register a strict collection whose first column is a non-null `_rowid`
/// (so `apply_point_put` injects the surrogate) plus a nullable `body` text
/// column that feeds the inverted index.
fn register_strict(core: &mut CoreLoop) {
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("body", ColumnType::String),
    ])
    .unwrap();
    core.doc_configs.insert(
        (TenantId::new(TID), COLL.to_string()),
        CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema }),
    );
}

/// MessagePack input document (no `_rowid` — the strict path injects it).
fn doc_bytes() -> Vec<u8> {
    use nodedb_types::Value;
    let mut obj = std::collections::HashMap::new();
    obj.insert(
        "body".to_string(),
        Value::String("searchable elephant paragraph".into()),
    );
    zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
}

fn fts_searchable(core: &CoreLoop) -> bool {
    !core
        .inverted
        .search(
            DB,
            TenantId::new(TID),
            COLL,
            nodedb_fts::FtsSearchParams {
                query: "elephant",
                top_k: 10,
                fuzzy_enabled: false,
                mode: nodedb_fts::posting::QueryMode::And,
                prefilter: None,
            },
        )
        .unwrap()
        .is_empty()
}

fn dummy_task() -> ExecutionTask {
    ExecutionTask::new(Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(TID),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Document(DocumentOp::PointGet {
            collection: COLL.into(),
            document_id: PK.into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
        // no-determinism: test-only deadline is not written to Calvin state.
        deadline: Instant::now() + Duration::from_secs(30),
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::Read,
        ),
    })
}

/// Commit an insert by driving `tx_point_put` and discarding its undo log
/// (the txn commits internally).
fn commit_put(core: &mut CoreLoop) {
    let task = dummy_task();
    let value = doc_bytes();
    let mut throwaway = Vec::new();
    core.tx_point_put(
        TxPointPut {
            task: &task,
            tid: TID,
            collection: COLL,
            document_id: PK,
            surrogate: Surrogate::new(1),
            value: &value,
            user_roles: &[],
            insert_if_absent: None,
        },
        &mut throwaway,
    )
    .unwrap();
}

#[test]
fn strict_tx_delete_rollback_restores_fts_postings() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _t, _r) = make_core_with_dir(dir.path());
    register_strict(&mut core);

    commit_put(&mut core);
    assert!(
        fts_searchable(&core),
        "strict body must be searchable after insert"
    );

    let task = dummy_task();
    let mut undo_log = Vec::new();
    core.tx_point_delete(
        TxPointDelete {
            task: &task,
            tid: TID,
            collection: COLL,
            document_id: PK,
            surrogate: Surrogate::new(1),
            user_roles: &[],
        },
        &mut undo_log,
    )
    .unwrap();
    assert!(
        !fts_searchable(&core),
        "delete cascade must remove strict FTS postings"
    );

    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");
    assert!(
        fts_searchable(&core),
        "strict FTS postings must be restored (searchable again) after delete-rollback"
    );
}

#[test]
fn strict_tx_put_rollback_removes_fts_postings() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _t, _r) = make_core_with_dir(dir.path());
    register_strict(&mut core);

    assert!(!fts_searchable(&core));

    let task = dummy_task();
    let value = doc_bytes();
    let mut undo_log = Vec::new();
    core.tx_point_put(
        TxPointPut {
            task: &task,
            tid: TID,
            collection: COLL,
            document_id: PK,
            surrogate: Surrogate::new(1),
            value: &value,
            user_roles: &[],
            insert_if_absent: None,
        },
        &mut undo_log,
    )
    .unwrap();
    assert!(fts_searchable(&core), "strict body searchable mid-tx");

    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");
    assert!(
        !fts_searchable(&core),
        "strict FTS postings must be gone after put-rollback"
    );
}
