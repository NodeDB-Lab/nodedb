// SPDX-License-Identifier: BUSL-1.1

//! Resolved document statements commit and publish one installed unit.

use nodedb_physical::physical_plan::{
    DocumentOp, DocumentResolvedMutation, MaterializedSumBinding, PhysicalPlan, ResolvedSumTarget,
};
use nodedb_query::expr::SqlExpr;
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, TenantId,
};

use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{ErrorCode, Response, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{
    make_core_with_dir, make_default_task, make_request,
};
use crate::data::executor::doc_format;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{CollectionConfig, IndexPath};
use crate::types::Lsn;
use crate::wal::WriteSetCapture;

const SOURCE: &str = "point_txns";
const TARGET: &str = "point_holders";
const TARGET_ID: Surrogate = Surrogate(4001);

fn key(id: u32) -> StorageKey {
    StorageKey::for_surrogate(Surrogate::new(id))
}

fn config_key(collection: &str) -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(1), collection.to_owned())
}

fn targets() -> Vec<ResolvedSumTarget> {
    vec![ResolvedSumTarget::new(TARGET, "a1", TARGET_ID)]
}

fn put(
    collection: &str,
    id: u32,
    value: serde_json::Value,
    precondition: Option<Vec<u8>>,
) -> DocumentResolvedMutation {
    DocumentResolvedMutation::Put {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
        document_id: format!("row-{id}"),
        surrogate: Surrogate::new(id),
        pk_bytes: Vec::new(),
        value: doc_format::encode_to_msgpack(&value),
        precondition,
        resolved_sum_targets: if collection == SOURCE {
            targets()
        } else {
            Vec::new()
        },
    }
}

fn seed(core: &mut CoreLoop, mutations: &[DocumentResolvedMutation]) {
    let response = core.execute_document_resolved_write(
        &make_default_task(),
        1,
        mutations,
        b"seed",
        &RlsWriteCheck::NoPolicyApplies,
    );
    assert_eq!(response.status, Status::Ok, "{response:?}");
}

fn apply(
    core: &mut CoreLoop,
    collection: &str,
    mutations: Vec<DocumentResolvedMutation>,
) -> Response {
    let mut request = make_request(PhysicalPlan::Document(DocumentOp::ResolvedWrite {
        mutations,
        response_payload: b"accepted".to_vec(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
    }));
    request.wal_lsn = Some(Lsn::new(99));
    let task = ExecutionTask::new(request);
    let permit = core.begin_task_journal(
        &task,
        JournalGroup {
            origin: Lsn::new(99),
            collection: collection.to_owned(),
            apply_key: 0,
            commit_hlc: None,
            change_position: None,
        },
    );
    let mut response = core.execute_with_journal(&task, permit);
    core.finish_journalled_task(&task, &mut response)
        .expect("finish capture");
    response
}

fn versions(core: &CoreLoop, collection: &str) -> Vec<(StorageKey, i64, Option<Vec<u8>>)> {
    let mut rows = Vec::new();
    core.sparse
        .versioned_for_each_version(0, 1, collection, |key, ordinal, body| {
            rows.push((*key, ordinal, body.map(<[u8]>::to_vec)));
            Ok(())
        })
        .expect("history");
    rows
}

fn owners(core: &CoreLoop, collection: &str, field: &str, value: &str) -> Vec<StorageKey> {
    core.sparse
        .versioned_index_lookup_as_of(0, 1, collection, field, value, None)
        .expect("index")
}

#[test]
fn resolved_unique_handover_uses_the_complete_installed_statement() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    let collection = "resolved_codes";
    let mut config = CollectionConfig::new(collection);
    config.bitemporal = true;
    config.index_paths.push(IndexPath {
        unique: true,
        ..IndexPath::new("code")
    });
    core.doc_configs.insert(config_key(collection), config);
    seed(
        &mut core,
        &[
            put(collection, 1, serde_json::json!({"code": "A"}), None),
            put(collection, 2, serde_json::json!({"code": "B"}), None),
        ],
    );
    let first = core
        .sparse
        .get(0, 1, collection, &key(1))
        .expect("first row");
    let second = core
        .sparse
        .get(0, 1, collection, &key(2))
        .expect("second row");
    let response = apply(
        &mut core,
        collection,
        vec![
            put(collection, 1, serde_json::json!({"code": "B"}), first),
            put(collection, 2, serde_json::json!({"code": "A"}), second),
        ],
    );
    assert_eq!(response.status, Status::Ok, "{response:?}");
    assert_eq!(response.payload.as_bytes(), b"accepted");
    assert_eq!(response.write_set.len(), 2);
    assert_eq!(owners(&core, collection, "code", "A"), vec![key(2)]);
    assert_eq!(owners(&core, collection, "code", "B"), vec![key(1)]);
}

#[test]
fn resolved_late_refusal_restores_source_target_history_indexes_and_publication() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    assert!(crate::query::sum_target_is_co_resident(
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, SOURCE),
        TARGET,
    ));
    let mut config = CollectionConfig::new(SOURCE).with_index("amount");
    config.bitemporal = true;
    config.enforcement.hash_chain = true;
    config.enforcement.append_only = true;
    config.enforcement.materialized_sum_sources = vec![MaterializedSumBinding {
        target_collection: TARGET.to_owned(),
        target_column: "balance".into(),
        join_column: "account_id".into(),
        value_expr: SqlExpr::Column("amount".into()),
        declared_primary_key: None,
    }];
    core.doc_configs.insert(config_key(SOURCE), config);
    core.doc_configs.insert(
        config_key(TARGET),
        CollectionConfig::new(TARGET).with_index("balance"),
    );
    core.sparse
        .put(
            0,
            1,
            TARGET,
            &StorageKey::for_surrogate(TARGET_ID),
            &doc_format::encode_to_msgpack(&serde_json::json!({"id": "a1", "balance": "0"})),
            1,
        )
        .expect("target seed");
    seed(
        &mut core,
        &[put(
            SOURCE,
            1,
            serde_json::json!({"account_id": "a1", "amount": 10}),
            None,
        )],
    );
    let source_before = versions(&core, SOURCE);
    let target_before = versions(&core, TARGET);
    let heads_before = core.chain.heads.clone();
    let head_before = core.sparse.get_chain_head(0, 1, SOURCE).expect("head");
    let source_indexes: Vec<_> = ["10", "5"]
        .into_iter()
        .map(|value| owners(&core, SOURCE, "amount", value))
        .collect();
    let target_indexes: Vec<_> = ["10", "15"]
        .into_iter()
        .map(|value| owners(&core, TARGET, "balance", value))
        .collect();
    let prior = core.sparse.get(0, 1, SOURCE, &key(1)).expect("prior");
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 64);
    core.set_event_producer(producers.pop().expect("producer"));
    let mut events = consumers.pop().expect("consumer");
    let response = apply(
        &mut core,
        SOURCE,
        vec![
            put(
                SOURCE,
                2,
                serde_json::json!({"account_id": "a1", "amount": 5}),
                None,
            ),
            DocumentResolvedMutation::Delete {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, SOURCE),
                document_id: "row-1".into(),
                surrogate: Surrogate::new(1),
                pk_bytes: Vec::new(),
                precondition: prior,
                resolved_sum_targets: targets(),
            },
        ],
    );
    assert_eq!(response.status, Status::Error, "{response:?}");
    assert!(matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::AppendOnlyViolation { .. })
    ));
    assert!(response.write_set.is_empty());
    assert!(events.try_recv().is_none());
    assert_eq!(versions(&core, SOURCE), source_before);
    assert_eq!(versions(&core, TARGET), target_before);
    assert_eq!(core.chain.heads, heads_before);
    assert!(core.chain.intents.is_empty());
    assert_eq!(
        core.sparse.get_chain_head(0, 1, SOURCE).expect("head"),
        head_before
    );
    assert_eq!(
        ["10", "5"]
            .into_iter()
            .map(|value| owners(&core, SOURCE, "amount", value))
            .collect::<Vec<_>>(),
        source_indexes
    );
    assert_eq!(
        ["10", "15"]
            .into_iter()
            .map(|value| owners(&core, TARGET, "balance", value))
            .collect::<Vec<_>>(),
        target_indexes
    );
    assert!(core.doc_cache.get(0, 1, SOURCE, &key(2)).is_none());
    assert!(
        core.doc_cache
            .get(0, 1, TARGET, &StorageKey::for_surrogate(TARGET_ID))
            .is_none()
    );
    let captures = core.sparse.load_write_set_captures().expect("captures");
    assert_eq!(captures.len(), 1);
    let capture = WriteSetCapture::from_bytes(&captures[0].1).expect("capture");
    assert!(capture.entries.is_empty());
}
