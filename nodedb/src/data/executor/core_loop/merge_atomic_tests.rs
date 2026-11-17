// SPDX-License-Identifier: BUSL-1.1

//! MERGE judges and publishes the complete installed statement.

use super::{
    CoreLoop,
    tests::{make_core_with_dir, make_request},
};
use crate::bridge::{
    dispatch::JournalGroup,
    envelope::{ErrorCode, Response, Status},
};
use crate::data::executor::{
    doc_format, enforcement::typeguard::CompiledTypeGuard, task::ExecutionTask,
};
use crate::engine::document::store::{CollectionConfig, IndexPath};
use crate::event::WriteEvent;
use crate::types::{Lsn, RequestId};
use crate::wal::WriteSetCapture;
use nodedb_physical::physical_plan::{
    BalancedDef, DocumentOp, GeneratedColumnSpec, MaterializedSumBinding, PhysicalPlan,
    ResolvedSumTarget, ReturningColumns, ReturningSpec, RowSource, StorageMode, UpdateValue,
    document::merge_types::{MergeActionOp, MergeClauseKind, MergeClauseOp},
};
use nodedb_query::expr::SqlExpr;
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::{
    DatabaseId, QualifiedCollection, RlsWriteCheck, StorageKey, Surrogate, SystemTimeScope,
    TenantId, TypeGuardFieldDef,
};

const SOURCE: &str = "point_txns";
const TARGET: &str = "point_holders";
const ACCOUNT: &str = "a1";
const TARGET_SURROGATE: Surrogate = Surrogate(4001);

fn collection(name: &str) -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, name)
}

fn config_key(name: &str) -> (DatabaseId, TenantId, String) {
    (DatabaseId::DEFAULT, TenantId::new(1), name.into())
}

fn targets() -> Vec<ResolvedSumTarget> {
    vec![ResolvedSumTarget::new(TARGET, ACCOUNT, TARGET_SURROGATE)]
}

fn task(op: DocumentOp, id: u64) -> ExecutionTask {
    let mut request = make_request(PhysicalPlan::Document(op));
    request.request_id = RequestId::new(id);
    request.wal_lsn = Some(Lsn::new(id));
    ExecutionTask::new(request)
}

fn seed(
    core: &mut CoreLoop,
    name: &str,
    id: u32,
    body: serde_json::Value,
    sum_targets: Vec<ResolvedSumTarget>,
) {
    let task = task(
        DocumentOp::PointPut {
            collection: collection(name),
            document_id: format!("row-{id}"),
            value: doc_format::encode_to_msgpack(&body),
            surrogate: Surrogate::new(id),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: sum_targets,
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        },
        u64::from(id),
    );
    let response = core.execute(&task);
    assert_eq!(response.status, Status::Ok, "{response:?}");
}

fn merge(
    name: &str,
    rows: Vec<serde_json::Value>,
    columns: &[&str],
    surrogates: &[u32],
    delete: bool,
) -> DocumentOp {
    let mut clauses = vec![MergeClauseOp {
        kind: MergeClauseKind::NotMatched,
        extra_predicate: Vec::new(),
        action: MergeActionOp::Insert {
            columns: columns.iter().map(|column| (*column).into()).collect(),
            values: columns
                .iter()
                .map(|column| UpdateValue::Expr(SqlExpr::Column(format!("s.{column}"))))
                .collect(),
        },
    }];
    if delete {
        clauses.push(MergeClauseOp {
            kind: MergeClauseKind::NotMatchedBySource,
            extra_predicate: Vec::new(),
            action: MergeActionOp::Delete,
        });
    }
    DocumentOp::Merge {
        target_collection: collection(name),
        source: RowSource::whole_collection(collection("merge_source")),
        source_alias: "s".into(),
        target_join_col: "k".into(),
        source_join_col: "k".into(),
        clauses,
        returning: Some(ReturningSpec {
            columns: ReturningColumns::Star,
            precommit: None,
        }),
        resolved_inserts: Some(
            surrogates
                .iter()
                .enumerate()
                .map(|(index, surrogate)| (index.to_string(), *surrogate))
                .collect(),
        ),
        resolved_insert_identities: Vec::new(),
        source_rows: Some(rows.iter().map(doc_format::encode_to_msgpack).collect()),
        rls_filters: Vec::new(),
        rls_write_check: RlsWriteCheck::NoPolicyApplies,
        resolved_sum_targets: if name == SOURCE {
            targets()
        } else {
            Vec::new()
        },
        declared_primary_key: None,
        target_system_time: SystemTimeScope::Current,
    }
}

fn apply(core: &mut CoreLoop, name: &str, op: DocumentOp) -> Response {
    let task = task(op, 99);
    let permit = core.begin_task_journal(
        &task,
        JournalGroup {
            origin: Lsn::new(99),
            collection: name.into(),
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

fn versions(core: &CoreLoop, name: &str) -> Vec<(StorageKey, i64, Option<Vec<u8>>)> {
    let mut rows = Vec::new();
    core.sparse
        .versioned_for_each_version(0, 1, name, |key, ordinal, bytes| {
            rows.push((*key, ordinal, bytes.map(<[u8]>::to_vec)));
            Ok(())
        })
        .expect("complete history");
    rows
}

fn index(core: &CoreLoop, name: &str, field: &str, value: &str) -> Vec<StorageKey> {
    core.sparse
        .versioned_index_lookup_as_of(0, 1, name, field, value, None)
        .expect("index")
}

fn empty_capture(core: &CoreLoop) {
    let captures = core.sparse.load_write_set_captures().expect("captures");
    assert_eq!(captures.len(), 1);
    let capture = WriteSetCapture::from_bytes(&captures[0].1).expect("capture");
    assert!(capture.entries.is_empty());
}

#[test]
fn merge_delete_releases_a_unique_value_for_an_insert_in_the_same_statement() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    let mut unique = IndexPath::new("code");
    unique.unique = true;
    let mut config = CollectionConfig::new("merge_rows");
    config.bitemporal = true;
    config.index_paths.push(unique);
    core.doc_configs.insert(config_key("merge_rows"), config);
    seed(
        &mut core,
        "merge_rows",
        1,
        serde_json::json!({"k": "old", "code": "shared"}),
        Vec::new(),
    );
    let response = apply(
        &mut core,
        "merge_rows",
        merge(
            "merge_rows",
            vec![serde_json::json!({"k": "new", "code": "shared"})],
            &["k", "code"],
            &[2],
            true,
        ),
    );
    assert_eq!(response.status, Status::Ok, "{response:?}");
    assert_eq!(
        index(&core, "merge_rows", "code", "shared"),
        vec![StorageKey::for_surrogate(Surrogate::new(2))]
    );
    assert!(
        core.sparse
            .get(
                0,
                1,
                "merge_rows",
                &StorageKey::for_surrogate(Surrogate::new(1))
            )
            .expect("old row")
            .is_none()
    );
    assert!(
        core.sparse
            .get(
                0,
                1,
                "merge_rows",
                &StorageKey::for_surrogate(Surrogate::new(2))
            )
            .expect("new row")
            .is_some()
    );
    let rows: crate::data::executor::response_codec::RowsPayload =
        zerompk::from_msgpack(response.payload.as_bytes()).expect("RETURNING");
    assert_eq!(rows.rows.len(), 2);
}

#[test]
fn merge_late_delete_refusal_restores_insert_sum_history_indexes_and_chain_head() {
    let directory = tempfile::tempdir().expect("directory");
    let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
    assert!(crate::query::sum_target_is_co_resident(
        nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, SOURCE),
        TARGET
    ));
    let mut config = CollectionConfig::new(SOURCE).with_index("amount");
    config.bitemporal = true;
    config.enforcement.hash_chain = true;
    config.enforcement.append_only = true;
    config.enforcement.materialized_sum_sources = vec![MaterializedSumBinding {
        target_collection: TARGET.into(),
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
            &StorageKey::for_surrogate(TARGET_SURROGATE),
            &doc_format::encode_to_msgpack(&serde_json::json!({"id": ACCOUNT, "balance": "0"})),
            1,
        )
        .expect("target");
    for id in 1..=2 {
        seed(
            &mut core,
            SOURCE,
            id,
            serde_json::json!({"k": format!("old-{id}"), "account_id": ACCOUNT, "amount": id * 10}),
            targets(),
        );
    }
    let source_before = versions(&core, SOURCE);
    let target_before = versions(&core, TARGET);
    let heads = core.chain.heads.clone();
    let stored_head = core.sparse.get_chain_head(0, 1, SOURCE).expect("head");
    let source_indexes: Vec<_> = ["10", "20", "5"]
        .into_iter()
        .map(|value| index(&core, SOURCE, "amount", value))
        .collect();
    let target_indexes: Vec<_> = ["30", "35"]
        .into_iter()
        .map(|value| index(&core, TARGET, "balance", value))
        .collect();
    let mut events = events(&mut core);
    let response = apply(
        &mut core,
        SOURCE,
        merge(
            SOURCE,
            vec![serde_json::json!({"k": "new", "account_id": ACCOUNT, "amount": 5})],
            &["k", "account_id", "amount"],
            &[3],
            true,
        ),
    );
    assert_eq!(response.status, Status::Error, "{response:?}");
    assert!(matches!(
        response.error_code.as_deref(),
        Some(ErrorCode::AppendOnlyViolation { .. })
    ));
    assert!(response.write_set.is_empty());
    assert!(events().is_empty());
    assert_eq!(versions(&core, SOURCE), source_before);
    assert_eq!(versions(&core, TARGET), target_before);
    assert_eq!(core.chain.heads, heads);
    assert!(core.chain.intents.is_empty());
    assert_eq!(
        core.sparse.get_chain_head(0, 1, SOURCE).expect("head"),
        stored_head
    );
    assert_eq!(
        ["10", "20", "5"]
            .into_iter()
            .map(|value| index(&core, SOURCE, "amount", value))
            .collect::<Vec<_>>(),
        source_indexes
    );
    assert_eq!(
        ["30", "35"]
            .into_iter()
            .map(|value| index(&core, TARGET, "balance", value))
            .collect::<Vec<_>>(),
        target_indexes
    );
    empty_capture(&core);
}

fn balanced_config() -> (CollectionConfig, StrictSchema) {
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("k", ColumnType::String),
        ColumnDef::nullable("raw", ColumnType::String),
        ColumnDef::nullable("kind", ColumnType::String),
        ColumnDef::nullable("journal_id", ColumnType::String),
        ColumnDef::nullable("entry_type", ColumnType::String),
        ColumnDef::nullable("amount", ColumnType::Int64),
    ])
    .expect("schema");
    let mut config = CollectionConfig::new("merge_ledger").with_storage_mode(StorageMode::Strict {
        schema: schema.clone(),
    });
    config.type_guards = vec![CompiledTypeGuard::compile(
        "merge_ledger",
        &TypeGuardFieldDef {
            field: "journal_id".into(),
            type_expr: "STRING".into(),
            required: false,
            check_expr: None,
            default_expr: Some("'journal'".into()),
            value_expr: None,
        },
    )];
    config.enforcement.generated_columns = [("entry_type", "kind"), ("amount", "raw")]
        .into_iter()
        .map(|(name, source)| {
            let (expr, depends_on) =
                nodedb_query::expr_parse::parse_generated_expr(source).expect("generated");
            GeneratedColumnSpec {
                name: name.into(),
                expr,
                depends_on,
            }
        })
        .collect();
    config.enforcement.balanced = Some(BalancedDef {
        group_key_column: "journal_id".into(),
        entry_type_column: "entry_type".into(),
        debit_value: "DEBIT".into(),
        credit_value: "CREDIT".into(),
        amount_column: "amount".into(),
    });
    (config, schema)
}

#[test]
fn merge_balanced_uses_defaulted_generated_and_coerced_installed_values() {
    for credit in ["10", "11"] {
        let directory = tempfile::tempdir().expect("directory");
        let (mut core, _requests, _responses) = make_core_with_dir(directory.path());
        let (config, schema) = balanced_config();
        core.doc_configs.insert(config_key("merge_ledger"), config);
        let mut events = events(&mut core);
        let response = apply(
            &mut core,
            "merge_ledger",
            merge(
                "merge_ledger",
                vec![
                    serde_json::json!({"k": "d", "raw": "10", "kind": "DEBIT"}),
                    serde_json::json!({"k": "c", "raw": credit, "kind": "CREDIT"}),
                ],
                &["k", "raw", "kind"],
                &[1, 2],
                false,
            ),
        );
        if credit == "11" {
            assert_eq!(response.status, Status::Error, "{response:?}");
            assert!(matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::BalanceViolation { .. })
            ));
            assert!(versions(&core, "merge_ledger").is_empty());
            assert!(events().is_empty());
            assert!(response.write_set.is_empty());
            empty_capture(&core);
        } else {
            assert_eq!(response.status, Status::Ok, "{response:?}");
            assert_eq!(events().len(), 2);
            for (id, kind) in [(1, "DEBIT"), (2, "CREDIT")] {
                let bytes = core
                    .sparse
                    .get(
                        0,
                        1,
                        "merge_ledger",
                        &StorageKey::for_surrogate(Surrogate::new(id)),
                    )
                    .expect("row")
                    .expect("installed row");
                let row =
                    crate::data::executor::strict_format::binary_tuple_to_json(&bytes, &schema)
                        .expect("tuple");
                assert_eq!(row["journal_id"], "journal");
                assert_eq!(row["entry_type"], kind);
                assert_eq!(row["amount"], 10);
            }
        }
    }
}
