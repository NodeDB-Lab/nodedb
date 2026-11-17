// SPDX-License-Identifier: BUSL-1.1

//! A volatile strict DEFAULT bound on the proposer installs identical bytes
//! on every replica and every replay.

use std::collections::HashMap;

use nodedb_physical::physical_plan::StorageMode;
use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};
use nodedb_types::{Surrogate, Value};

use super::PointPutParams;
use crate::control::write_resolve::volatile_defaults::{bind_body, compile_volatile_defaults};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::typeguard::GuardScope;
use crate::data::executor::enforcement::unique::UniqueJudge;
use crate::engine::document::store::{CollectionConfig, StorageKey};
use crate::types::{DatabaseId, TenantId};

const COLL: &str = "bound_defaults";

/// No relation the evaluator looks up: the DEFAULTs here are generators.
struct NoCatalog;

impl nodedb_sql::catalog::SqlCatalog for NoCatalog {
    fn get_collection(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _name: &str,
    ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
    {
        Ok(None)
    }
}

/// `created DEFAULT NOW()`, `token DEFAULT UUID_V4()`, `status DEFAULT 'new'`.
fn schema() -> StrictSchema {
    StrictSchema::new(vec![
        ColumnDef::required("_rowid", ColumnType::Int64),
        ColumnDef::nullable("created", ColumnType::Timestamp).with_default("NOW()"),
        ColumnDef::nullable("token", ColumnType::String).with_default("UUID_V4()"),
        ColumnDef::nullable("status", ColumnType::String).with_default("'new'"),
    ])
    .expect("schema")
}

/// One replica: its own core and store, holding the strict collection.
fn replica(dir: &std::path::Path) -> CoreLoop {
    let (mut core, _tx, _rx) = make_core_with_dir(dir);
    let config =
        CollectionConfig::new(COLL).with_storage_mode(StorageMode::Strict { schema: schema() });
    core.doc_configs
        .insert((DatabaseId::DEFAULT, TenantId::new(1), COLL.into()), config);
    core.apply_scope.replicated_ordinal = Some(100);
    core
}

fn params(value: &[u8]) -> PointPutParams<'_> {
    PointPutParams {
        database_id: 0,
        tid: 1,
        collection: COLL,
        storage_key: StorageKey::for_surrogate(Surrogate::new(1)),
        surrogate: Surrogate::new(1),
        value,
        index_text: false,
        user_roles: &[],
        enforce: false,
        unique: UniqueJudge::Unit,
        wal_lsn: None,
        resolved_targets: &[],
        guards: GuardScope::Document,
    }
}

/// The bytes `core` installs for the record body `value`, or the error the
/// apply refuses it with.
fn installed(core: &mut CoreLoop, value: &[u8]) -> crate::Result<Vec<u8>> {
    let txn = core.sparse.begin_write().expect("txn");
    let prepared = core.prepare_point_put(&txn, params(value))?;
    Ok(prepared.installed().stored.clone())
}

/// A logged apply on `core`: a replica applying the record, or a replay.
fn logged(core: &mut CoreLoop, value: &[u8]) -> crate::Result<Vec<u8>> {
    core.apply_scope.journaled = true;
    let result = installed(core, value);
    core.apply_scope.journaled = false;
    result
}

/// The stored bytes of row 1, `None` when nothing is stored.
fn stored_row(core: &CoreLoop) -> Option<Vec<u8>> {
    let txn = core.sparse.begin_write().expect("txn");
    core.sparse
        .get_in_txn(
            &txn,
            0,
            1,
            COLL,
            &StorageKey::for_surrogate(Surrogate::new(1)),
        )
        .expect("read row")
}

/// Replay one redo put of `value` on `core`. Returns whether it applied.
fn replay(core: &mut CoreLoop, value: &[u8]) -> bool {
    core.apply_document_put(
        crate::data::executor::wal_replay_redo_document_apply::ReplayDocPut {
            database_id: 0,
            tenant_id: 1,
            collection: COLL,
            surrogate: 1,
            record_lsn: 7,
        },
        value,
    )
}

fn empty_body() -> Vec<u8> {
    nodedb_types::value_to_msgpack(&Value::Object(HashMap::new())).expect("encode body")
}

/// The proposer's binding, as `bind_volatile_defaults` runs it.
fn bound_body() -> Vec<u8> {
    let defaults = compile_volatile_defaults(&schema()).expect("compile");
    let mut body = empty_body();
    assert!(bind_body(&mut body, &defaults, &NoCatalog).expect("bind"));
    body
}

/// The proposer binds `NOW()` and `UUID_V4()` once. Two replicas, and a
/// second apply on one of them, install identical bytes. The literal
/// DEFAULT stays for the apply to fill, and fills the same everywhere.
#[test]
fn a_bound_volatile_default_installs_identical_bytes_everywhere() {
    let defaults = compile_volatile_defaults(&schema()).expect("compile");
    let mut columns: Vec<&str> = defaults.iter().map(|default| default.column()).collect();
    columns.sort_unstable();
    assert_eq!(columns, ["created", "token"]);

    let body = bound_body();
    let Value::Object(fields) = doc_format::decode_document_value(&body).expect("bound body")
    else {
        panic!("bound body is an object");
    };
    assert!(fields.contains_key("created") && fields.contains_key("token"));
    assert!(
        !fields.contains_key("status"),
        "a literal DEFAULT binds at apply"
    );
    let mut again = body.clone();
    assert!(!bind_body(&mut again, &defaults, &NoCatalog).expect("rebind"));
    assert_eq!(again, body, "a bound body binds nothing a second time");

    let (first_dir, second_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (mut first, mut second) = (replica(first_dir.path()), replica(second_dir.path()));
    let on_first = logged(&mut first, &body).expect("bound body applies");
    assert_eq!(
        logged(&mut second, &body).expect("applies"),
        on_first,
        "replicas agree"
    );
    assert_eq!(
        logged(&mut first, &body).expect("applies"),
        on_first,
        "a replay agrees"
    );

    // Unbound, each fresh evaluation draws its own token: the hazard the
    // binding and the logged-apply refusal remove.
    let unbound = empty_body();
    assert_ne!(
        installed(&mut first, &unbound).expect("fresh"),
        installed(&mut second, &unbound).expect("fresh")
    );
}

/// A logged apply of an unbound volatile DEFAULT is an invariant break that
/// names the collection and the column, and it evaluates nothing.
#[test]
fn a_logged_apply_refuses_an_unbound_volatile_default() {
    let dir = tempfile::tempdir().unwrap();
    let mut core = replica(dir.path());
    let Err(crate::Error::Internal { detail }) = logged(&mut core, &empty_body()) else {
        panic!("a logged apply must refuse the unbound body");
    };
    assert!(detail.contains(COLL), "{detail}");
    assert!(
        detail.contains("created") || detail.contains("token"),
        "{detail}"
    );
}

/// A replayed record with an unbound volatile DEFAULT applies nothing. A
/// bound one installs the same bytes on every replaying node.
#[test]
fn a_replayed_record_installs_only_bound_values() {
    let (first_dir, second_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (mut first, mut second) = (replica(first_dir.path()), replica(second_dir.path()));

    assert!(
        !replay(&mut first, &empty_body()),
        "an unbound record is refused"
    );
    assert!(
        stored_row(&first).is_none(),
        "a refused record writes nothing"
    );

    let body = bound_body();
    assert!(replay(&mut first, &body));
    assert!(replay(&mut second, &body));
    let installed = stored_row(&first).expect("row stored");
    assert_eq!(stored_row(&second), Some(installed));
}
