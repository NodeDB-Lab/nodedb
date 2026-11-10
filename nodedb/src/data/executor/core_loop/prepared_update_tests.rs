// SPDX-License-Identifier: BUSL-1.1

//! Predicate UPDATE statements publish all source and derived rows together.

use super::{
    CoreLoop,
    tests::{make_core_with_dir, make_request},
};
use crate::bridge::dispatch::JournalGroup;
use crate::bridge::envelope::{Response, Status};
use crate::data::executor::{doc_format, task::ExecutionTask};
use crate::engine::document::store::CollectionConfig;
use crate::event::WriteEvent;
use crate::types::{Lsn, RequestId};
use crate::wal::WriteSetCapture;
use nodedb_physical::physical_plan::{
    DocumentOp, MaterializedSumBinding, PhysicalPlan, ResolvedSumTarget, ReturningColumns,
    ReturningSpec, RowSource, UpdateValue,
};
use nodedb_query::expr::{BinaryOp, SqlExpr};
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, SystemTimeScope,
    TenantId, Value,
};

const SOURCE: &str = "point_txns";
const TARGET: &str = "point_holders";
const ACCOUNT: &str = "a1";
const TARGET_SURROGATE: Surrogate = Surrogate(4001);

fn collection(name: &str) -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, name)
}

fn targets() -> Vec<ResolvedSumTarget> {
    vec![ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)]
}

fn request(op: DocumentOp, id: u64) -> ExecutionTask {
    let mut request = make_request(PhysicalPlan::Document(op));
    request.request_id = RequestId::new(id);
    request.wal_lsn = Some(Lsn::new(id));
    ExecutionTask::new(request)
}

fn configure_and_seed(core: &mut CoreLoop) {
    assert!(crate::query::sum_target_is_co_resident(
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, SOURCE),
        TARGET,
    ));
    let mut source = CollectionConfig::new(SOURCE).with_index("amount");
    source.bitemporal = true;
    source.vector_fields = vec![("embedding".into(), 2)];
    source.enforcement.materialized_sum_sources = vec![MaterializedSumBinding {
        target_collection: TARGET.into(),
        target_column: "balance".into(),
        join_column: "account_id".into(),
        value_expr: SqlExpr::Column("amount".into()),
        declared_primary_key: None,
    }];
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(1), SOURCE.into()),
        source,
    );
    let mut target = CollectionConfig::new(TARGET).with_index("balance");
    target.bitemporal = true;
    core.doc_configs.insert(
        (DatabaseId::DEFAULT, TenantId::new(1), TARGET.into()),
        target,
    );
    core.sparse
        .put(
            0,
            1,
            TARGET,
            &StorageKey::for_surrogate(TARGET_SURROGATE),
            &doc_format::encode_to_msgpack(&serde_json::json!({"id": ACCOUNT, "balance": "0"})),
            1,
        )
        .expect("seed SUM target");
    for id in 1..=2 {
        let next = if id == 1 {
            vec![0.0, 1.0]
        } else {
            vec![0.0, 1.0, 0.0]
        };
        let body = doc_format::encode_to_msgpack(&serde_json::json!({
            "account_id": ACCOUNT, "amount": id * 10,
            "embedding": [1.0, 0.0], "next_embedding": next,
        }));
        let task = request(
            DocumentOp::PointPut {
                collection: collection(SOURCE),
                document_id: format!("row-{id}"),
                value: body,
                surrogate: Surrogate::new(id as u32),
                pk_bytes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: targets(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            },
            id,
        );
        let response = core.execute(&task);
        assert_eq!(response.status, Status::Ok, "{response:?}");
    }
    assert_eq!(balance(core), "30");
}

fn events(core: &mut CoreLoop) -> impl FnMut() -> Vec<WriteEvent> + use<> {
    let (mut producers, mut consumers) = crate::event::bus::create_event_bus_with_capacity(1, 64);
    core.set_event_producer(producers.pop().expect("producer"));
    let mut consumer = consumers.pop().expect("consumer");
    move || {
        let mut events = Vec::new();
        while let Some(event) = consumer.try_recv() {
            events.push(event);
        }
        events
    }
}

fn update(join: bool, replace_embedding: bool) -> DocumentOp {
    let mut updates = vec![(
        "amount".into(),
        UpdateValue::Expr(SqlExpr::BinaryOp {
            left: Box::new(SqlExpr::Column("amount".into())),
            op: BinaryOp::Add,
            right: Box::new(SqlExpr::Literal(Value::Integer(5))),
        }),
    )];
    if replace_embedding {
        updates.push((
            "embedding".into(),
            UpdateValue::Expr(SqlExpr::Column("next_embedding".into())),
        ));
    }
    let returning = Some(ReturningSpec {
        columns: ReturningColumns::Star,
        precommit: None,
    });
    if join {
        DocumentOp::UpdateFromJoin {
            target_collection: collection(SOURCE),
            source: RowSource::whole_collection(collection("account_changes")),
            source_alias: "s".into(),
            target_join_col: "account_id".into(),
            source_join_col: "account_id".into(),
            updates,
            target_filters: Vec::new(),
            returning,
            source_rows: Some(vec![doc_format::encode_to_msgpack(
                &serde_json::json!({"account_id": ACCOUNT}),
            )]),
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: targets(),
            declared_primary_key: None,
            target_system_time: SystemTimeScope::Current,
        }
    } else {
        DocumentOp::BulkUpdate {
            collection: collection(SOURCE),
            filters: Vec::new(),
            updates,
            returning,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: targets(),
            declared_primary_key: None,
            system_time: SystemTimeScope::Current,
        }
    }
}

fn apply(core: &mut CoreLoop, op: DocumentOp) -> Response {
    let task = request(op, 3);
    let permit = core.begin_task_journal(
        &task,
        JournalGroup {
            origin: Lsn::new(3),
            collection: SOURCE.into(),
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
    let mut versions = Vec::new();
    core.sparse
        .versioned_for_each_version(0, 1, collection, |key, ordinal, body| {
            versions.push((*key, ordinal, body.map(<[u8]>::to_vec)));
            Ok(())
        })
        .expect("read complete history");
    versions
}

fn index(core: &CoreLoop, collection: &str, field: &str, value: &str) -> Vec<StorageKey> {
    core.sparse
        .versioned_index_lookup_as_of(0, 1, collection, field, value, None)
        .expect("read index")
}

fn balance(core: &CoreLoop) -> String {
    let bytes = core
        .sparse
        .get(0, 1, TARGET, &StorageKey::for_surrogate(TARGET_SURROGATE))
        .expect("read SUM target")
        .expect("SUM target");
    doc_format::decode_document(&bytes).expect("decode SUM target")["balance"]
        .as_str()
        .expect("decimal balance")
        .to_owned()
}

#[test]
fn predicate_update_late_vector_error_restores_source_sum_history_and_indexes() {
    for join in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        configure_and_seed(&mut core);
        let source_before = versions(&core, SOURCE);
        let target_before = versions(&core, TARGET);
        let index_before: Vec<_> = ["10", "20", "15", "25"]
            .into_iter()
            .map(|value| index(&core, SOURCE, "amount", value))
            .collect();
        let sum_index_before: Vec<_> = ["30", "35", "40"]
            .into_iter()
            .map(|value| index(&core, TARGET, "balance", value))
            .collect();
        let mappings = core.vector_doc_map.clone();
        let vector_key = CoreLoop::vector_index_key(0, 1, SOURCE, "embedding");
        let vector_before: Vec<_> = (1..=2)
            .map(|id| {
                let vectors = core
                    .vector_collections
                    .get(&vector_key)
                    .expect("vector index");
                let surrogate = Surrogate::new(id);
                (
                    vectors.local_for_surrogate(surrogate),
                    vectors.vector_for_surrogate(surrogate),
                    vectors.row_newest_ordinal(surrogate),
                )
            })
            .collect();
        let mut events = events(&mut core);
        let response = apply(&mut core, update(join, true));
        assert_eq!(response.status, Status::Error, "{response:?}");
        assert!(format!("{:?}", response.error_code).contains("embedding"));
        assert!(response.write_set.is_empty());
        assert!(events().is_empty());
        assert_eq!(versions(&core, SOURCE), source_before);
        assert_eq!(versions(&core, TARGET), target_before);
        assert_eq!(balance(&core), "30");
        assert_eq!(
            ["10", "20", "15", "25"]
                .into_iter()
                .map(|value| index(&core, SOURCE, "amount", value))
                .collect::<Vec<_>>(),
            index_before
        );
        assert_eq!(
            ["30", "35", "40"]
                .into_iter()
                .map(|value| index(&core, TARGET, "balance", value))
                .collect::<Vec<_>>(),
            sum_index_before
        );
        assert_eq!(core.vector_doc_map, mappings);
        let vectors = core
            .vector_collections
            .get(&vector_key)
            .expect("vector index");
        assert_eq!(vectors.live_count(), 2);
        for (id, before) in (1..=2).zip(vector_before) {
            let surrogate = Surrogate::new(id);
            assert_eq!(
                (
                    vectors.local_for_surrogate(surrogate),
                    vectors.vector_for_surrogate(surrogate),
                    vectors.row_newest_ordinal(surrogate)
                ),
                before
            );
        }
        let captures = core.sparse.load_write_set_captures().expect("captures");
        assert_eq!(captures.len(), 1);
        let capture = WriteSetCapture::from_bytes(&captures[0].1).expect("decode capture");
        assert!(capture.entries.is_empty());
    }
}

#[test]
fn predicate_update_repeated_sum_target_folds_every_row_in_one_transaction() {
    for join in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        configure_and_seed(&mut core);
        let response = apply(&mut core, update(join, false));
        assert_eq!(response.status, Status::Ok, "{response:?}");
        assert_eq!(balance(&core), "40");
        assert_eq!(
            index(&core, SOURCE, "amount", "15"),
            vec![StorageKey::for_surrogate(Surrogate::new(1))]
        );
        assert_eq!(
            index(&core, SOURCE, "amount", "25"),
            vec![StorageKey::for_surrogate(Surrogate::new(2))]
        );
        let rows: crate::data::executor::response_codec::RowsPayload =
            zerompk::from_msgpack(response.payload.as_bytes()).expect("RETURNING rows");
        assert_eq!(rows.rows.len(), 2);
        let source_entries = response
            .write_set
            .iter()
            .filter(|entry| entry.collection.is_none())
            .count();
        let target_entries = response
            .write_set
            .iter()
            .filter(|entry| entry.collection.as_deref() == Some(TARGET))
            .count();
        assert_eq!(source_entries, 2);
        assert_eq!(target_entries, 2);
    }
}
