// SPDX-License-Identifier: BUSL-1.1

use redb::TableDefinition;

use super::DocumentBatchInsertParams;
use crate::bridge::envelope::{Priority, Request, Status};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::{declare_text_fields, make_core_with_dir};
use crate::data::executor::doc_format;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::CollectionConfig;
use crate::engine::sparse::fts_redb::tables::DOC_LENGTHS;
use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, ResolvedSumTarget};
use nodedb_types::{StorageKey, Surrogate};
use std::time::{Duration, Instant};

const TID: u64 = 1;
const COLL: &str = "articles";

/// Raw JSON bodies with real words in them, so each row has text the
/// inverted index actually has to accept for the write to be searchable.
fn bodies() -> Vec<(String, Vec<u8>)> {
    vec![
        ("d1".to_string(), br#"{"title":"alpha bravo"}"#.to_vec()),
        ("d2".to_string(), br#"{"title":"charlie delta"}"#.to_vec()),
    ]
}

/// A table sharing `DOC_LENGTHS`'s redb name but with incompatible
/// key/value types, so every inverted-index write fails structurally.
const POISONED_DOC_LENGTHS: TableDefinition<u64, u64> = TableDefinition::new("text.doc_lengths");

fn poison_inverted_index(core: &CoreLoop) {
    let db = core.sparse.db().clone();
    let txn = db.begin_write().unwrap();
    txn.delete_table(DOC_LENGTHS).unwrap();
    txn.open_table(POISONED_DOC_LENGTHS).unwrap();
    txn.commit().unwrap();
}

fn batch_task(documents: &[(String, Vec<u8>)], surrogates: &[Surrogate]) -> ExecutionTask {
    ExecutionTask::new(Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(TID),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
            documents: documents.to_vec(),
            if_absent: false,
            surrogates: surrogates.to_vec(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        }),
        deadline: Instant::now() + Duration::from_secs(5),
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
        commit_hlc: None,
        system_ordinal: None,
        config_pins: None,
        admission: crate::bridge::envelope::Admission::Admitted,
    })
}

fn stored(core: &CoreLoop, surrogate: Surrogate) -> Option<Vec<u8>> {
    core.sparse
        .get(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            COLL,
            &nodedb_types::StorageKey::for_surrogate(surrogate),
        )
        .unwrap()
}

fn corpus_size(core: &CoreLoop) -> u32 {
    core.inverted
        .corpus_stats(DatabaseId::DEFAULT.as_u64(), TenantId::new(TID), COLL)
        .unwrap()
        .0
}

/// Control: with a healthy index the batch lands AND both rows are counted
/// into the FTS corpus. Without this, a passing failure test could not tell
/// "the poison caused the rejection" from "this batch was never insertable".
#[test]
fn a_healthy_batch_commits_every_row_and_indexes_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    declare_text_fields(&mut core, TID, COLL, &["title"]);
    let documents = bodies();
    let surrogates = vec![Surrogate(11), Surrogate(12)];

    let task = batch_task(&documents, &surrogates);
    let resp = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: COLL,
            documents: &documents,
            surrogates: &surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        },
    );

    assert_eq!(resp.status, Status::Ok);
    assert_eq!(resp.write_set.len(), 2);
    for (entry, surrogate) in resp.write_set.iter().zip(&surrogates) {
        let crate::bridge::envelope::RowEffect::InstalledPut { image, .. } = &entry.effect else {
            panic!("batch capture must carry an installed image");
        };
        assert_eq!(Some(image.stored.clone()), stored(&core, *surrogate));
        assert_eq!(
            Some(image.version.sys_from),
            core.sparse
                .versioned_latest_sys_from(0, TID, COLL, &StorageKey::for_surrogate(*surrogate),)
                .unwrap(),
        );
    }
    assert!(stored(&core, Surrogate(11)).is_some());
    assert!(stored(&core, Surrogate(12)).is_some());
    assert_eq!(
        corpus_size(&core),
        2,
        "both committed rows must be in the FTS corpus"
    );
}

fn insert_page(
    core: &mut CoreLoop,
    documents: &[(String, Vec<u8>)],
    surrogates: &[Surrogate],
    if_absent: bool,
) -> crate::bridge::envelope::Response {
    let task = batch_task(documents, surrogates);
    core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: COLL,
            documents,
            surrogates,
            if_absent,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        },
    )
}

fn inserted(resp: &crate::bridge::envelope::Response) -> u64 {
    let value: serde_json::Value =
        nodedb_types::json_from_msgpack(resp.payload.as_ref()).expect("count payload");
    value
        .get("inserted")
        .and_then(serde_json::Value::as_u64)
        .expect("inserted count")
}

/// A page row whose key is already stored is refused, and the page writes
/// nothing: the page once overwrote the stored row.
#[test]
fn a_page_row_with_a_stored_key_refuses_the_page() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let first = vec![("d1".to_string(), br#"{"title":"original"}"#.to_vec())];
    assert_eq!(
        insert_page(&mut core, &first, &[Surrogate(31)], false).status,
        Status::Ok
    );
    let original = stored(&core, Surrogate(31));

    let page = vec![
        ("d9".to_string(), br#"{"title":"new"}"#.to_vec()),
        ("d1".to_string(), br#"{"title":"overwrite"}"#.to_vec()),
    ];
    let resp = insert_page(&mut core, &page, &[Surrogate(39), Surrogate(31)], false);
    assert_eq!(
        resp.status,
        Status::Error,
        "a duplicate key refuses the page"
    );
    assert_eq!(
        stored(&core, Surrogate(31)),
        original,
        "the stored row is untouched"
    );
    assert!(
        stored(&core, Surrogate(39)).is_none(),
        "no row of the page lands"
    );
}

/// A key the page names twice is refused.
#[test]
fn a_key_the_page_names_twice_refuses_the_page() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let page = vec![
        ("d1".to_string(), br#"{"title":"a"}"#.to_vec()),
        ("d1".to_string(), br#"{"title":"b"}"#.to_vec()),
    ];
    let resp = insert_page(&mut core, &page, &[Surrogate(41), Surrogate(41)], false);
    assert_eq!(resp.status, Status::Error);
    assert!(stored(&core, Surrogate(41)).is_none());
}

/// `ON CONFLICT DO NOTHING` skips stored and repeated keys, lands the
/// rest, and counts only what landed.
#[test]
fn skip_mode_lands_only_new_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let first = vec![("d1".to_string(), br#"{"title":"original"}"#.to_vec())];
    insert_page(&mut core, &first, &[Surrogate(51)], false);
    let original = stored(&core, Surrogate(51));

    let page = vec![
        ("d1".to_string(), br#"{"title":"skipped"}"#.to_vec()),
        ("d2".to_string(), br#"{"title":"lands"}"#.to_vec()),
        ("d2".to_string(), br#"{"title":"repeat"}"#.to_vec()),
    ];
    let resp = insert_page(
        &mut core,
        &page,
        &[Surrogate(51), Surrogate(52), Surrogate(52)],
        true,
    );
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(inserted(&resp), 1);
    assert_eq!(stored(&core, Surrogate(51)), original);
    assert!(stored(&core, Surrogate(52)).is_some());
    assert_eq!(
        resp.write_set.len(),
        1,
        "the redo record carries only the row that landed"
    );
}

/// The contract this guards: a row whose indexing fails must not be committed
/// and counted into `inserted` — that would tell the client the write
/// succeeded while full-text search could never return the row and nothing,
/// not replay, not the next write, would re-index it. The batch must
/// fail as a whole, leaving no row behind and no partial corpus.
#[test]
fn a_row_that_cannot_be_indexed_fails_the_whole_batch() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    declare_text_fields(&mut core, TID, COLL, &["title"]);
    poison_inverted_index(&core);
    let documents = bodies();
    let surrogates = vec![Surrogate(21), Surrogate(22)];

    let task = batch_task(&documents, &surrogates);
    let resp = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: COLL,
            documents: &documents,
            surrogates: &surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        },
    );

    assert_eq!(
        resp.status,
        Status::Error,
        "the client must be told the batch failed, not receive a success count \
         for rows full-text search will never return"
    );
    assert!(
        stored(&core, Surrogate(21)).is_none() && stored(&core, Surrogate(22)).is_none(),
        "an indexing failure on any row must roll the whole batch back — a stored \
         row whose index update failed is invisible to search forever"
    );
}

/// A geometry + vector row body, in the MessagePack every handler gets.
fn geo_vector_body(x: f64, embedding: &[f64]) -> Vec<u8> {
    doc_format::encode_to_msgpack(&serde_json::json!({
        "loc": format!(r#"{{"type":"Point","coordinates":[{x},1.0]}}"#),
        "embedding": embedding,
    }))
}

/// A batch whose last row is refused leaves no in-memory trace of the
/// rows before it. Dropping the transaction reverses the stored rows
/// only, so without the undo rows 1-2 keep answering spatial predicates
/// and vector search. Row 3 is refused in its vector step, after its own
/// R-tree entry landed, so it also checks the refused row's own undo.
#[test]
fn a_refused_last_row_leaves_no_spatial_or_vector_trace() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let db = DatabaseId::DEFAULT;
    core.vector_params.insert(
        (db, TenantId::new(TID), COLL.to_string()),
        crate::engine::vector::hnsw::HnswParams::default(),
    );
    let documents = vec![
        ("g1".to_string(), geo_vector_body(1.0, &[1.0, 0.0, 0.0])),
        ("g2".to_string(), geo_vector_body(2.0, &[0.0, 1.0, 0.0])),
        // The index is three wide, so this row is refused.
        ("g3".to_string(), geo_vector_body(3.0, &[1.0, 1.0])),
    ];
    let surrogates = vec![Surrogate(31), Surrogate(32), Surrogate(33)];

    let task = batch_task(&documents, &surrogates);
    let resp = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: COLL,
            documents: &documents,
            surrogates: &surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        },
    );

    assert_eq!(resp.status, Status::Error, "row 3 must refuse the batch");
    for surrogate in &surrogates {
        assert!(stored(&core, *surrogate).is_none(), "no row may be stored");
    }
    let spatial_key = (db, TenantId::new(TID), COLL.to_string(), "loc".to_string());
    assert!(
        core.spatial_indexes
            .get(&spatial_key)
            .is_none_or(|rtree| rtree.entries().is_empty()),
        "the R-tree must hold no entry of the abandoned rows"
    );
    assert!(
        core.spatial_doc_map.is_empty(),
        "the reverse spatial map must hold no entry of the abandoned rows"
    );
    let vector_key = CoreLoop::vector_index_key(db.as_u64(), TID, COLL, "embedding");
    assert!(
        !core.vector_collections.contains_key(&vector_key),
        "row 1 created the vector index, so the abandoned page must remove it"
    );
    assert!(
        core.vector_doc_map.is_empty(),
        "the vector reverse map must hold no entry of the abandoned rows"
    );
}

/// A batch carrying fewer surrogates than documents has no cross-engine
/// identity for its rows, so every index would silently omit them. It is
/// refused outright rather than stored-and-reported-successful.
#[test]
fn a_batch_without_a_surrogate_per_row_is_refused_and_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    let documents = bodies();
    let surrogates = vec![Surrogate(31)];

    let task = batch_task(&documents, &surrogates);
    let resp = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: COLL,
            documents: &documents,
            surrogates: &surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        },
    );

    assert_eq!(resp.status, Status::Error);
    assert!(
        stored(&core, Surrogate(31)).is_none(),
        "a batch with no identity for every row must write no row at all"
    );
    assert_eq!(
        corpus_size(&core),
        0,
        "and must leave nothing in the FTS corpus"
    );
}

// ── The batch insert an `INSERT ... SELECT` page ships must credit its
// materialized-sum targets, exactly like every other write path ─────────

const SUM_SOURCE: &str = "point_txns";
const SUM_TARGET: &str = "point_holders";
const SUM_A1: &str = "a1";
const SUM_T1: Surrogate = Surrogate(4001);

fn sum_binding() -> nodedb_physical::physical_plan::MaterializedSumBinding {
    nodedb_physical::physical_plan::MaterializedSumBinding {
        target_collection: SUM_TARGET.to_string(),
        target_column: "balance".to_string(),
        join_column: "account_id".to_string(),
        value_expr: nodedb_query::expr::SqlExpr::Column("amount".to_string()),
        declared_primary_key: None,
    }
}

fn sum_config_key(collection: &str) -> (DatabaseId, TenantId, String) {
    (
        DatabaseId::DEFAULT,
        TenantId::new(TID),
        collection.to_string(),
    )
}

/// A source collection bound to the sum, and a target row starting at zero.
fn sum_seeded_core(dir: &std::path::Path) -> CoreLoop {
    let (mut core, _req, _resp) = make_core_with_dir(dir);

    let mut source = CollectionConfig::new(SUM_SOURCE);
    source.enforcement.materialized_sum_sources = vec![sum_binding()];
    core.doc_configs.insert(sum_config_key(SUM_SOURCE), source);
    core.doc_configs.insert(
        sum_config_key(SUM_TARGET),
        CollectionConfig::new(SUM_TARGET),
    );

    let seed = serde_json::json!({"id": SUM_A1, "balance": "100"});
    core.sparse
        .put(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            SUM_TARGET,
            &nodedb_types::StorageKey::for_surrogate(SUM_T1),
            &doc_format::encode_to_msgpack(&seed),
            core.bitemporal_ordinal(),
        )
        .expect("seed target row");
    core
}

/// A source row body, in the MessagePack every handler receives.
fn sum_entry(account: &str, amount: i64) -> Vec<u8> {
    doc_format::encode_to_msgpack(&serde_json::json!({
        "account_id": account,
        "amount": amount,
    }))
}

/// The balance the target row currently holds.
fn sum_balance(core: &CoreLoop, surrogate: Surrogate) -> String {
    let stored = core
        .sparse
        .get(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            SUM_TARGET,
            &nodedb_types::StorageKey::for_surrogate(surrogate),
        )
        .expect("read target row")
        .expect("target row must still exist");
    doc_format::decode_document(&stored)
        .expect("target row must decode")
        .get("balance")
        .and_then(|v| v.as_str())
        .expect("target row must carry a balance")
        .to_string()
}

/// The orchestrator re-issues the copy through `dispatch_local`, which
/// never passes through the statement-level resolution pass — so a page
/// shipping an empty resolution would leave the total short of the rows it
/// inserted.
#[test]
fn insert_select_page_credits_its_targets() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut core = sum_seeded_core(dir.path());

    let documents = vec![
        (
            StorageKey::for_surrogate(Surrogate(1)).to_string(),
            sum_entry(SUM_A1, 25),
        ),
        (
            StorageKey::for_surrogate(Surrogate(2)).to_string(),
            sum_entry(SUM_A1, 75),
        ),
    ];
    let surrogates = vec![Surrogate(1), Surrogate(2)];
    let resolved = vec![ResolvedSumTarget::new(SUM_TARGET, SUM_A1, SUM_T1)];
    let task = batch_task(&documents, &surrogates);
    let response = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: SUM_SOURCE,
            documents: &documents,
            surrogates: &surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &resolved,
            deferred_sum_targets: &[],
        },
    );

    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(sum_balance(&core, SUM_T1), "200");
}

#[test]
fn strict_batch_sums_installed_defaults_generated_and_coerced_amounts() {
    use crate::data::executor::enforcement::typeguard::CompiledTypeGuard;
    use nodedb_physical::physical_plan::{GeneratedColumnSpec, StorageMode};
    use nodedb_types::TypeGuardFieldDef;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    let directory = tempfile::tempdir().expect("directory");
    let mut core = sum_seeded_core(directory.path());
    let schema = StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::required("account_id", ColumnType::String),
        ColumnDef::required("base", ColumnType::Int64),
        ColumnDef::required("amount", ColumnType::Int64),
        ColumnDef::required("doubled", ColumnType::Int64),
    ])
    .expect("strict source schema");
    let mut source = CollectionConfig::new(SUM_SOURCE).with_storage_mode(StorageMode::Strict {
        schema: schema.clone(),
    });
    source.type_guards = vec![CompiledTypeGuard::compile(
        SUM_SOURCE,
        &TypeGuardFieldDef {
            field: "amount".into(),
            type_expr: "INT|STRING".into(),
            required: false,
            check_expr: None,
            default_expr: Some("25".into()),
            value_expr: None,
        },
    )];
    let (expr, depends_on) =
        nodedb_query::expr_parse::parse_generated_expr("base * 2").expect("generated amount");
    source.enforcement.generated_columns = vec![GeneratedColumnSpec {
        name: "doubled".into(),
        expr,
        depends_on,
    }];
    let mut generated_sum = sum_binding();
    generated_sum.value_expr = nodedb_query::expr::SqlExpr::Column("doubled".into());
    let mut amount_sum = sum_binding();
    amount_sum.target_column = "amount_balance".into();
    source.enforcement.materialized_sum_sources = vec![generated_sum, amount_sum];
    core.doc_configs.insert(sum_config_key(SUM_SOURCE), source);
    core.sparse
        .put(
            0,
            TID,
            SUM_TARGET,
            &StorageKey::for_surrogate(SUM_T1),
            &doc_format::encode_to_msgpack(
                &serde_json::json!({"id": SUM_A1, "balance": "100", "amount_balance": "100"}),
            ),
            core.bitemporal_ordinal(),
        )
        .expect("seed both totals");
    let documents = vec![
        (
            "first".into(),
            doc_format::encode_to_msgpack(&serde_json::json!({"account_id": SUM_A1, "base": 3})),
        ),
        (
            "second".into(),
            doc_format::encode_to_msgpack(
                &serde_json::json!({"account_id": SUM_A1, "base": 4, "amount": "7"}),
            ),
        ),
    ];
    let surrogates = [Surrogate(4101), Surrogate(4102)];
    let resolved = [ResolvedSumTarget::new(SUM_TARGET, SUM_A1, SUM_T1)];
    let task = batch_task(&documents, &surrogates);
    let response = core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
            tid: TID,
            collection: SUM_SOURCE,
            documents: &documents,
            surrogates: &surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            resolved_sum_targets: &resolved,
            deferred_sum_targets: &[],
        },
    );
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert_eq!(sum_balance(&core, SUM_T1), "114");
    let target = core
        .sparse
        .get(0, TID, SUM_TARGET, &StorageKey::for_surrogate(SUM_T1))
        .expect("target read")
        .expect("target row");
    let target = doc_format::decode_document(&target).expect("target document");
    assert_eq!(target["amount_balance"].as_str(), Some("132"));
    for (surrogate, amount, doubled) in [(surrogates[0], 25, 6), (surrogates[1], 7, 8)] {
        let row = core
            .sparse
            .get(0, TID, SUM_SOURCE, &StorageKey::for_surrogate(surrogate))
            .expect("source read")
            .expect("source row");
        let row = crate::data::executor::strict_format::binary_tuple_to_value(&row, &schema)
            .expect("stored source decode");
        let nodedb_types::Value::Object(fields) = row else {
            panic!("source must be an object");
        };
        assert_eq!(fields["amount"], nodedb_types::Value::Integer(amount));
        assert_eq!(fields["doubled"], nodedb_types::Value::Integer(doubled));
    }
}

fn strict_unique_core(dir: &std::path::Path, default: bool) -> CoreLoop {
    use nodedb_physical::physical_plan::StorageMode;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
    let (mut core, _req, _resp) = make_core_with_dir(dir);
    let mut code = ColumnDef::nullable("code", ColumnType::Int64);
    if default {
        code = code.with_default("7");
    }
    let schema = StrictSchema::new(vec![
        ColumnDef::required("id", ColumnType::String).with_primary_key(),
        code,
    ])
    .expect("schema");
    let mut config = CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema });
    config
        .index_paths
        .push(crate::engine::document::store::IndexPath {
            unique: true,
            ..crate::engine::document::store::IndexPath::new("code")
        });
    core.seed_doc_configs(&[(
        (DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_owned()),
        config,
    )]);
    core
}

fn strict_unique_page(
    core: &mut CoreLoop,
    upsert: bool,
    documents: &[(String, Vec<u8>)],
    surrogates: &[Surrogate],
) -> crate::bridge::envelope::Response {
    let task = batch_task(documents, surrogates);
    if upsert {
        core.dispatch_upsert_batch(
            &task,
            TID,
            &DocumentOp::BatchUpsert {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                documents: documents.to_vec(),
                surrogates: surrogates.to_vec(),
                on_conflict_updates: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            },
        )
    } else {
        insert_page(core, documents, surrogates, false)
    }
}

fn assert_refused_unique_page_keeps_handle_healthy(
    core: &mut CoreLoop,
    upsert: bool,
    documents: &[(String, Vec<u8>)],
) {
    let response = strict_unique_page(core, upsert, documents, &[Surrogate(71), Surrogate(72)]);
    assert_eq!(response.status, Status::Error);
    assert!(matches!(response.error_code.as_deref(),
        Some(crate::bridge::envelope::ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"));
    assert!(response.write_set.is_empty());
    assert!(stored(core, Surrogate(71)).is_none());
    assert!(stored(core, Surrogate(72)).is_none());
    assert!(!core.is_fail_stopped());
    let healthy = [(
        "c".to_owned(),
        doc_format::encode_to_msgpack(&serde_json::json!({"id":"c","code":8})),
    )];
    let response = strict_unique_page(core, upsert, &healthy, &[Surrogate(73)]);
    assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
    assert!(stored(core, Surrogate(73)).is_some());
}

#[test]
fn strict_numeric_coercion_collisions_refuse_the_complete_page() {
    for upsert in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = strict_unique_core(dir.path(), false);
        let documents = [
            (
                "a".to_owned(),
                doc_format::encode_to_msgpack(&serde_json::json!({"id":"a","code":"01"})),
            ),
            (
                "b".to_owned(),
                doc_format::encode_to_msgpack(&serde_json::json!({"id":"b","code":"1"})),
            ),
        ];
        assert_refused_unique_page_keeps_handle_healthy(&mut core, upsert, &documents);
    }
}

#[test]
fn strict_default_collisions_refuse_the_complete_page() {
    for upsert in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = strict_unique_core(dir.path(), true);
        let documents = [
            (
                "a".to_owned(),
                doc_format::encode_to_msgpack(&serde_json::json!({"id":"a"})),
            ),
            (
                "b".to_owned(),
                doc_format::encode_to_msgpack(&serde_json::json!({"id":"b"})),
            ),
        ];
        assert_refused_unique_page_keeps_handle_healthy(&mut core, upsert, &documents);
    }
}

#[test]
fn target_unique_claims_use_the_complete_source_statement() {
    use crate::engine::document::store::{DocumentEngine, IndexPath};
    const SECOND: Surrogate = Surrogate(4002);
    for (second_delta, accepted) in [(1, true), (0, false)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = sum_seeded_core(dir.path());
        let mut target = CollectionConfig::new(SUM_TARGET);
        target.index_paths.push(IndexPath {
            unique: true,
            ..IndexPath::new("balance")
        });
        core.doc_configs
            .insert(sum_config_key(SUM_TARGET), target.clone());
        {
            let mut engine = DocumentEngine::new(&core.sparse, 0, TID);
            engine.register_collection(target);
            for (surrogate, id, balance) in [(SUM_T1, SUM_A1, "1"), (SECOND, "a2", "2")] {
                engine
                    .put(
                        SUM_TARGET,
                        &StorageKey::for_surrogate(surrogate),
                        &serde_json::json!({"id":id,"balance":balance}),
                    )
                    .expect("seed unique target");
            }
        }
        let documents = [
            ("s1".to_owned(), sum_entry(SUM_A1, 1)),
            ("s2".to_owned(), sum_entry("a2", second_delta)),
        ];
        let surrogates = [Surrogate(4201), Surrogate(4202)];
        let resolved = [
            ResolvedSumTarget::new(SUM_TARGET, SUM_A1, SUM_T1),
            ResolvedSumTarget::new(SUM_TARGET, "a2", SECOND),
        ];
        let task = batch_task(&documents, &surrogates);
        let response = core.execute_document_batch_insert(
            &task,
            DocumentBatchInsertParams {
                rls_write_check: &nodedb_types::RlsWriteCheck::NoPolicyApplies,
                tid: TID,
                collection: SUM_SOURCE,
                documents: &documents,
                surrogates: &surrogates,
                if_absent: false,
                returning: None,
                rls_filters: &[],
                resolved_sum_targets: &resolved,
                deferred_sum_targets: &[],
            },
        );
        assert_eq!(
            response.status,
            if accepted { Status::Ok } else { Status::Error },
            "{:?}",
            response.error_code
        );
        assert_eq!(sum_balance(&core, SUM_T1), if accepted { "2" } else { "1" });
        assert_eq!(sum_balance(&core, SECOND), if accepted { "3" } else { "2" });
        if !accepted {
            assert!(response.write_set.is_empty());
            for surrogate in surrogates {
                assert!(
                    core.sparse
                        .get(0, TID, SUM_SOURCE, &StorageKey::for_surrogate(surrogate))
                        .expect("source read")
                        .is_none()
                );
            }
            assert!(!core.is_fail_stopped());
        }
    }
}

#[test]
fn batch_upsert_policy_reads_the_materialized_stored_row() {
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    for (body, expected) in [
        (serde_json::json!({"id":"a"}), 7),
        (serde_json::json!({"id":"a","code":"08"}), 8),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = strict_unique_core(dir.path(), true);
        let filter = ScanFilter {
            field: "code".into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::Integer(expected),
            clauses: Vec::new(),
            expr: None,
        };
        let policy = nodedb_types::RlsWriteCheck::Predicate(
            zerompk::to_msgpack_vec(&vec![filter]).expect("policy"),
        );
        let documents = [("a".to_owned(), doc_format::encode_to_msgpack(&body))];
        let surrogates = [Surrogate(4301)];
        let task = batch_task(&documents, &surrogates);
        let response = core.dispatch_upsert_batch(
            &task,
            TID,
            &DocumentOp::BatchUpsert {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
                documents: documents.to_vec(),
                surrogates: surrogates.to_vec(),
                on_conflict_updates: Vec::new(),
                rls_write_check: policy,
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            },
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert!(stored(&core, surrogates[0]).is_some());
    }
}

fn code_write_policy(code: i64) -> nodedb_types::RlsWriteCheck {
    use crate::bridge::scan_filter::{FilterOp, ScanFilter};
    nodedb_types::RlsWriteCheck::Predicate(
        zerompk::to_msgpack_vec(&vec![ScanFilter {
            field: "code".into(),
            op: FilterOp::Eq,
            value: nodedb_types::Value::Integer(code),
            clauses: Vec::new(),
            expr: None,
        }])
        .expect("write policy"),
    )
}

fn policy_insert_page(
    core: &mut CoreLoop,
    documents: &[(String, Vec<u8>)],
    surrogates: &[Surrogate],
    policy: &nodedb_types::RlsWriteCheck,
) -> crate::bridge::envelope::Response {
    let task = batch_task(documents, surrogates);
    core.execute_document_batch_insert(
        &task,
        DocumentBatchInsertParams {
            tid: TID,
            collection: COLL,
            documents,
            surrogates,
            if_absent: false,
            returning: None,
            rls_filters: &[],
            rls_write_check: policy,
            resolved_sum_targets: &[],
            deferred_sum_targets: &[],
        },
    )
}

#[test]
fn batch_insert_policy_reads_defaults_coercion_and_generated_values() {
    for (body, expected, generated) in [
        (serde_json::json!({"id":"a"}), 7, false),
        (serde_json::json!({"id":"a","code":"08"}), 8, false),
        (serde_json::json!({"id":"a","base":3}), 6, true),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = strict_unique_core(dir.path(), true);
        if generated {
            use nodedb_physical::physical_plan::{GeneratedColumnSpec, StorageMode};
            use nodedb_types::columnar::{ColumnDef, ColumnType};
            let config = core
                .doc_configs
                .get_mut(&(DatabaseId::DEFAULT, TenantId::new(TID), COLL.to_owned()))
                .expect("config");
            let StorageMode::Strict { schema } = &mut config.storage_mode else {
                panic!("strict config");
            };
            schema
                .columns
                .push(ColumnDef::nullable("base", ColumnType::Int64));
            let (expr, depends_on) =
                nodedb_query::expr_parse::parse_generated_expr("base * 2").expect("expression");
            config.enforcement.generated_columns = vec![GeneratedColumnSpec {
                name: "code".into(),
                expr,
                depends_on,
            }];
        }
        let documents = [("a".to_owned(), doc_format::encode_to_msgpack(&body))];
        let response = policy_insert_page(
            &mut core,
            &documents,
            &[Surrogate(4401)],
            &code_write_policy(expected),
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert!(stored(&core, Surrogate(4401)).is_some());
    }
}

#[test]
fn batch_insert_policy_denial_rolls_back_every_row_and_event() {
    use crate::event::bus::create_event_bus_with_capacity;
    for policy in [
        code_write_policy(7),
        nodedb_types::RlsWriteCheck::PendingInjection,
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = strict_unique_core(dir.path(), true);
        let (mut producers, mut consumers) = create_event_bus_with_capacity(1, 16);
        core.set_event_producer(producers.pop().expect("producer"));
        let mut events = consumers.pop().expect("events");
        let documents = [
            (
                "a".to_owned(),
                doc_format::encode_to_msgpack(&serde_json::json!({"id":"a","code":7})),
            ),
            (
                "b".to_owned(),
                doc_format::encode_to_msgpack(&serde_json::json!({"id":"b","code":8})),
            ),
        ];
        let response = policy_insert_page(
            &mut core,
            &documents,
            &[Surrogate(4401), Surrogate(4402)],
            &policy,
        );
        assert_eq!(response.status, Status::Error);
        assert!(response.write_set.is_empty());
        assert!(stored(&core, Surrogate(4401)).is_none());
        assert!(stored(&core, Surrogate(4402)).is_none());
        assert!(events.try_recv().is_none());
        assert!(!core.is_fail_stopped());
        let healthy = [(
            "c".to_owned(),
            doc_format::encode_to_msgpack(&serde_json::json!({"id":"c","code":9})),
        )];
        let response = policy_insert_page(
            &mut core,
            &healthy,
            &[Surrogate(4403)],
            &code_write_policy(9),
        );
        assert_eq!(response.status, Status::Ok, "{:?}", response.error_code);
        assert!(stored(&core, Surrogate(4403)).is_some());
    }
}
