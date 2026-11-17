// SPDX-License-Identifier: BUSL-1.1

//! Statement policies and coalesced requests judge their actual stored images.

use super::{
    CoreLoop,
    tests::{make_core_with_dir, make_request},
};
use crate::bridge::envelope::{Response, RowEffect, Status};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::{CollectionConfig, IndexPath};
use crate::event::WriteEvent;
use crate::types::RequestId;
use nodedb_physical::physical_plan::{DocumentOp, GeneratedColumnSpec, PhysicalPlan, StorageMode};
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, TenantId,
};

const COLLECTION: &str = "materialized_policy_rows";

fn configure(core: &mut CoreLoop, unique: bool, generated: bool) -> StrictSchema {
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("raw", ColumnType::String),
        ColumnDef::nullable("code", ColumnType::Int64).with_default("7"),
        ColumnDef::nullable("derived", ColumnType::Int64),
    ])
    .expect("schema");
    let mut config = CollectionConfig::new(COLLECTION).with_storage_mode(StorageMode::Strict {
        schema: schema.clone(),
    });
    if unique {
        config.index_paths.push(IndexPath {
            unique: true,
            ..IndexPath::new("code")
        });
    }
    if generated {
        let (expr, depends_on) =
            nodedb_query::expr_parse::parse_generated_expr("raw").expect("expression");
        config
            .enforcement
            .generated_columns
            .push(GeneratedColumnSpec {
                name: "derived".into(),
                expr,
                depends_on,
            });
    }
    core.seed_doc_configs(&[(
        (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
        config,
    )]);
    schema
}

fn policy(derived: i64) -> RlsWriteCheck {
    let filters: Vec<_> = [("code", 7), ("derived", derived)]
        .into_iter()
        .map(|(field, value)| ScanFilter {
            field: field.into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::Integer(value),
            clauses: Vec::new(),
            expr: None,
        })
        .collect();
    RlsWriteCheck::Predicate(zerompk::to_msgpack_vec(&filters).expect("policy"))
}

fn input(value: serde_json::Value) -> Vec<u8> {
    nodedb_types::json_to_msgpack(&value).expect("document")
}

fn put(surrogate: u32, value: serde_json::Value, check: RlsWriteCheck) -> DocumentOp {
    DocumentOp::PointPut {
        collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
        document_id: format!("row-{surrogate}"),
        surrogate: Surrogate::new(surrogate),
        value: input(value),
        pk_bytes: Vec::new(),
        returning: None,
        rls_filters: Vec::new(),
        rls_write_check: check,
        resolved_sum_targets: Vec::new(),
    }
}

fn execute(core: &mut CoreLoop, op: DocumentOp) -> Response {
    core.execute(&ExecutionTask::new(make_request(PhysicalPlan::Document(
        op,
    ))))
}

fn row(core: &CoreLoop, schema: &StrictSchema, surrogate: u32) -> Option<serde_json::Value> {
    core.sparse
        .get(
            0,
            1,
            COLLECTION,
            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
        )
        .expect("read row")
        .map(|bytes| {
            crate::data::executor::strict_format::binary_tuple_to_json(&bytes, schema)
                .expect("strict row")
        })
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

#[test]
fn point_and_upsert_policies_read_defaults_generated_and_coerced_stored_values() {
    for kind in 0..3 {
        for (body, accepted) in [
            (serde_json::json!({"raw":"08"}), true),
            (serde_json::json!({"raw":"08","code":"07"}), true),
            (serde_json::json!({"raw":"09","code":"07"}), false),
        ] {
            let directory = tempfile::tempdir().expect("directory");
            let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
            let schema = configure(&mut core, false, true);
            let mut emitted = events(&mut core);
            let op = match kind {
                0 => put(41, body, policy(8)),
                1 => DocumentOp::PointInsert {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                    document_id: "row-41".into(),
                    surrogate: Surrogate::new(41),
                    value: input(body),
                    if_absent: false,
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: policy(8),
                    resolved_sum_targets: Vec::new(),
                    deferred_sum_targets: Vec::new(),
                },
                _ => DocumentOp::Upsert {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                    document_id: "row-41".into(),
                    surrogate: Surrogate::new(41),
                    value: input(body),
                    on_conflict_updates: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: policy(8),
                    resolved_sum_targets: Vec::new(),
                },
            };
            let response = execute(&mut core, op);
            assert_eq!(
                response.status,
                if accepted { Status::Ok } else { Status::Error },
                "{response:?}"
            );
            if accepted {
                let stored = row(&core, &schema, 41).expect("accepted row");
                assert_eq!(stored["code"], 7);
                assert_eq!(stored["derived"], 8);
                assert_eq!(emitted().len(), 1);
                assert!(
                    response
                        .write_set
                        .iter()
                        .any(|entry| matches!(entry.effect, RowEffect::InstalledPut { .. }))
                );
            } else {
                assert!(row(&core, &schema, 41).is_none());
                assert!(response.write_set.is_empty());
                assert!(emitted().is_empty());
                assert!(!core.is_fail_stopped());
            }
        }
    }
}

#[test]
fn upsert_overwrite_policy_rejects_the_actual_generated_postimage() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    let schema = configure(&mut core, false, true);
    assert_eq!(
        execute(
            &mut core,
            put(41, serde_json::json!({"raw":"08"}), policy(8))
        )
        .status,
        Status::Ok
    );
    let before = row(&core, &schema, 41).expect("prior");
    let mut emitted = events(&mut core);
    let response = execute(
        &mut core,
        DocumentOp::Upsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
            document_id: "row-41".into(),
            surrogate: Surrogate::new(41),
            value: input(serde_json::json!({"raw":"09"})),
            on_conflict_updates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: policy(8),
            resolved_sum_targets: Vec::new(),
        },
    );
    assert_eq!(response.status, Status::Error, "{response:?}");
    assert_eq!(row(&core, &schema, 41), Some(before));
    assert!(response.write_set.is_empty());
    assert!(emitted().is_empty());
    assert!(!core.is_fail_stopped());
}

fn enqueue(core: &mut CoreLoop, id: u64, op: DocumentOp) {
    let mut request = make_request(PhysicalPlan::Document(op));
    request.request_id = RequestId::new(id);
    core.task_queue.push(ExecutionTask::new(request));
}

#[test]
fn coalesced_unique_checks_actual_coercion_and_defaults_per_request() {
    for bodies in [
        [
            serde_json::json!({"code":"01"}),
            serde_json::json!({"code":"1"}),
        ],
        [serde_json::json!({}), serde_json::json!({})],
    ] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
        let schema = configure(&mut core, true, false);
        let mut emitted = events(&mut core);
        for (index, body) in bodies.into_iter().enumerate() {
            enqueue(
                &mut core,
                index as u64 + 1,
                put(index as u32 + 41, body, RlsWriteCheck::NoPolicyApplies),
            );
        }
        assert_eq!(core.poll_write_batch(), 2);
        let first = responses.try_pop().expect("first response").inner;
        let second = responses.try_pop().expect("second response").inner;
        assert_eq!(first.status, Status::Ok, "{first:?}");
        assert_eq!(second.status, Status::Error, "{second:?}");
        assert!(
            matches!(second.error_code.as_deref(), Some(crate::bridge::envelope::ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique")
        );
        assert!(row(&core, &schema, 41).is_some());
        assert!(row(&core, &schema, 42).is_none());
        assert!(second.write_set.is_empty());
        assert_eq!(emitted().len(), 1);
        assert!(!core.is_fail_stopped());
    }
}

#[test]
fn coalesced_unique_handover_observes_only_previously_accepted_requests() {
    for release_first in [true, false] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
        let schema = configure(&mut core, true, false);
        assert_eq!(
            execute(
                &mut core,
                put(
                    41,
                    serde_json::json!({"code":1}),
                    RlsWriteCheck::NoPolicyApplies
                )
            )
            .status,
            Status::Ok
        );
        let release = put(
            41,
            serde_json::json!({"code":2}),
            RlsWriteCheck::NoPolicyApplies,
        );
        let claim = put(
            42,
            serde_json::json!({"code":1}),
            RlsWriteCheck::NoPolicyApplies,
        );
        let ops = if release_first {
            [release, claim]
        } else {
            [claim, release]
        };
        for (index, op) in ops.into_iter().enumerate() {
            enqueue(&mut core, index as u64 + 1, op);
        }
        assert_eq!(core.poll_write_batch(), 2);
        let first = responses.try_pop().expect("first response").inner;
        let second = responses.try_pop().expect("second response").inner;
        assert_eq!(
            first.status,
            if release_first {
                Status::Ok
            } else {
                Status::Error
            },
            "{first:?}"
        );
        assert_eq!(second.status, Status::Ok, "{second:?}");
        assert_eq!(row(&core, &schema, 41).expect("released owner")["code"], 2);
        assert_eq!(row(&core, &schema, 42).is_some(), release_first);
        assert!(!core.is_fail_stopped());
    }
}

#[test]
fn coalesced_policy_refusal_does_not_release_committed_unique_claims() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
    let schema = configure(&mut core, true, false);
    assert_eq!(
        execute(
            &mut core,
            put(
                41,
                serde_json::json!({"code":1}),
                RlsWriteCheck::NoPolicyApplies
            )
        )
        .status,
        Status::Ok
    );
    enqueue(
        &mut core,
        1,
        put(41, serde_json::json!({"code":2}), policy(8)),
    );
    enqueue(
        &mut core,
        2,
        put(
            42,
            serde_json::json!({"code":1}),
            RlsWriteCheck::NoPolicyApplies,
        ),
    );
    assert_eq!(core.poll_write_batch(), 2);
    assert_eq!(
        responses.try_pop().expect("denied release").inner.status,
        Status::Error
    );
    assert_eq!(
        responses.try_pop().expect("blocked claim").inner.status,
        Status::Error
    );
    assert_eq!(row(&core, &schema, 41).expect("original owner")["code"], 1);
    assert!(row(&core, &schema, 42).is_none());
    assert!(!core.is_fail_stopped());
}

#[test]
fn coalesced_requests_preserve_carried_ordinals_and_release_schema_scope() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
    configure(&mut core, false, false);
    for id in [1_u64, 2] {
        let mut request = make_request(PhysicalPlan::Document(put(
            id as u32 + 40,
            serde_json::json!({"code":id}),
            RlsWriteCheck::NoPolicyApplies,
        )));
        request.request_id = RequestId::new(id);
        request.system_ordinal = Some(id as i64 * 10_000);
        core.task_queue.push(ExecutionTask::new(request));
    }
    assert!(core.apply_scope.storage_schemas.borrow().is_empty());
    assert_eq!(core.poll_write_batch(), 2);
    for id in [1_i64, 2] {
        let response = responses.try_pop().expect("response").inner;
        assert_eq!(response.status, Status::Ok, "{response:?}");
        let image = response
            .write_set
            .iter()
            .find_map(|entry| match &entry.effect {
                RowEffect::InstalledPut { image, .. } => Some(image),
                _ => None,
            })
            .expect("installed image");
        assert_eq!(image.version.sys_from, id * 10_000);
    }
    assert!(core.apply_scope.replicated_ordinal.is_none());
    assert!(core.apply_scope.storage_schemas.borrow().is_empty());
}

#[test]
fn coalesced_repeated_key_raises_one_carried_ordinal_above_pending_version() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, mut responses) = make_core_with_dir(directory.path());
    let schema = configure(&mut core, false, false);
    for id in [1_u64, 2] {
        let mut request = make_request(PhysicalPlan::Document(put(
            41,
            serde_json::json!({"code":id}),
            RlsWriteCheck::NoPolicyApplies,
        )));
        request.request_id = RequestId::new(id);
        request.system_ordinal = Some(10_000);
        core.task_queue.push(ExecutionTask::new(request));
    }
    assert_eq!(core.poll_write_batch(), 2);
    for expected in [10_000, 10_001] {
        let response = responses.try_pop().expect("response").inner;
        assert_eq!(response.status, Status::Ok, "{response:?}");
        let image = response
            .write_set
            .iter()
            .find_map(|entry| match &entry.effect {
                RowEffect::InstalledPut { image, .. } => Some(image),
                _ => None,
            })
            .expect("installed image");
        assert_eq!(image.version.sys_from, expected);
    }
    let mut versions = Vec::new();
    core.sparse
        .versioned_for_each_version(0, 1, COLLECTION, |_, ordinal, body| {
            let body = body.expect("live version");
            let value = crate::data::executor::strict_format::binary_tuple_to_json(body, &schema)
                .expect("strict version");
            versions.push((ordinal, value["code"].as_i64().expect("code")));
            Ok(())
        })
        .expect("retained versions");
    versions.sort_unstable();
    assert_eq!(versions, [(10_000, 1), (10_001, 2)]);
}

#[test]
fn single_source_overwrite_allows_final_unique_handover_between_sum_targets() {
    use nodedb_physical::physical_plan::{MaterializedSumBinding, ResolvedSumTarget};
    const TARGET: &str = "materialized_policy_totals";
    for upsert in [false, true] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let mut target = CollectionConfig::new(TARGET);
        target.index_paths.push(IndexPath {
            unique: true,
            ..IndexPath::new("balance")
        });
        core.seed_doc_configs(&[
            (
                (DatabaseId::DEFAULT, TenantId::new(1), TARGET.into()),
                target,
            ),
            (
                (DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()),
                CollectionConfig::new(COLLECTION),
            ),
        ]);
        for (surrogate, id, balance) in [(51, "a", "1"), (52, "b", "0")] {
            let response = execute(
                &mut core,
                DocumentOp::PointPut {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, TARGET),
                    document_id: id.into(),
                    surrogate: Surrogate::new(surrogate),
                    value: input(serde_json::json!({"id":id,"balance":balance})),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: RlsWriteCheck::NoPolicyApplies,
                    resolved_sum_targets: Vec::new(),
                },
            );
            assert_eq!(response.status, Status::Ok, "{response:?}");
        }
        assert_eq!(
            execute(
                &mut core,
                put(
                    41,
                    serde_json::json!({"account":"a","amount":1}),
                    RlsWriteCheck::NoPolicyApplies
                )
            )
            .status,
            Status::Ok
        );
        let config = core
            .doc_configs
            .get_mut(&(DatabaseId::DEFAULT, TenantId::new(1), COLLECTION.into()))
            .expect("source config");
        config
            .enforcement
            .materialized_sum_sources
            .push(MaterializedSumBinding {
                target_collection: TARGET.into(),
                target_column: "balance".into(),
                join_column: "account".into(),
                value_expr: nodedb_query::expr::SqlExpr::Column("amount".into()),
                declared_primary_key: None,
            });
        let resolved = vec![
            ResolvedSumTarget::new(TARGET, "a", Surrogate::new(51)),
            ResolvedSumTarget::new(TARGET, "b", Surrogate::new(52)),
        ];
        let op = if upsert {
            DocumentOp::Upsert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                document_id: "row-41".into(),
                surrogate: Surrogate::new(41),
                value: input(serde_json::json!({"account":"b"})),
                on_conflict_updates: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                rls_write_check: RlsWriteCheck::NoPolicyApplies,
                resolved_sum_targets: resolved,
            }
        } else {
            let mut op = put(
                41,
                serde_json::json!({"account":"b","amount":1}),
                RlsWriteCheck::NoPolicyApplies,
            );
            let DocumentOp::PointPut {
                resolved_sum_targets,
                ..
            } = &mut op
            else {
                unreachable!("point fixture");
            };
            *resolved_sum_targets = resolved;
            op
        };
        let response = execute(&mut core, op);
        assert_eq!(response.status, Status::Ok, "{response:?}");
        for (surrogate, expected) in [(51, "0"), (52, "1")] {
            let bytes = core
                .sparse
                .get(
                    0,
                    1,
                    TARGET,
                    &StorageKey::for_surrogate(Surrogate::new(surrogate)),
                )
                .expect("target read")
                .expect("target");
            let target =
                crate::data::executor::doc_format::decode_document(&bytes).expect("target body");
            assert_eq!(target["balance"], expected);
        }
        assert!(!core.is_fail_stopped());
    }
}
