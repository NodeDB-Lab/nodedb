// SPDX-License-Identifier: BUSL-1.1

//! CRDT rows of the vShard departure purge tests.
//!
//! The fixture writes its CRDT row through the native protocol, into a
//! collection homed on a departed vShard. A movable-list insert
//! (`OpCode::CrdtListInsert`) needs its document to exist, so the fixture
//! first creates the document with a `crdt_apply` delta over the same native
//! connection, as the other cluster CRDT tests create theirs. The gateway
//! routes each op to the collection's vShard and proposes it through the data
//! group's Raft log.

use std::collections::HashMap;

use nodedb::types::TenantDataSnapshot;
use nodedb_client::NodeDb;
use nodedb_types::value::Value;

use super::vshard_departure_fixture::first_column;
use crate::common::cluster_harness::TestClusterNode;

/// The CRDT document the fixture writes.
pub const CRDT_DOC: &str = "r1x";
const CRDT_LIST: &str = "items";

/// A Loro delta that creates document `CRDT_DOC` of `collection`: the
/// collection's root map holds the row as a map container with one field.
/// Hex-encoded, the form `crdt_apply` takes.
fn create_doc_delta_hex(collection: &str) -> String {
    let doc = loro::LoroDoc::new();
    let row = doc
        .get_map(collection)
        .insert_container(CRDT_DOC, loro::LoroMap::new())
        .expect("row container");
    row.insert("name", "departed").expect("field");
    doc.commit();
    hex::encode(
        doc.export(loro::ExportMode::Snapshot)
            .expect("export loro snapshot"),
    )
}

/// Write one CRDT row into `collection` through `node`'s native listener:
/// create the document, then insert a block into its movable list.
pub async fn write_crdt_row(node: &TestClusterNode, collection: &str) {
    let client = node.native_client();
    let create = format!(
        "SELECT crdt_apply('{collection}', '{CRDT_DOC}', '{}')",
        create_doc_delta_hex(collection)
    );
    client
        .execute_sql(&create, &[])
        .await
        .unwrap_or_else(|e| panic!("native crdt_apply into {collection}: {e}"));
    let mut block = HashMap::new();
    block.insert("title".to_string(), Value::String("departed".to_string()));
    client
        .list_insert(collection, CRDT_DOC, CRDT_LIST, 0, &Value::Object(block))
        .await
        .unwrap_or_else(|e| panic!("native CRDT list insert into {collection}: {e}"));
}

/// Whether `snap` holds CRDT state of `collection`.
///
/// Any exported document of the collection counts, empty rows included: a
/// Loro export carries the full op history, so a document whose rows were
/// deleted still holds every value the departed row ever had.
pub fn holds_crdt_state(snap: &TenantDataSnapshot, collection: &str) -> bool {
    snap.crdt_state
        .iter()
        .any(|(_, _, key, _)| key.contains(collection))
}

/// Assert `node` serves the CRDT row of `collection`.
pub async fn assert_crdt_served(node: &TestClusterNode, collection: &str, stage: &str) {
    let state = first_column(
        node,
        &format!("SELECT crdt_state('{collection}', '{CRDT_DOC}')"),
    )
    .await;
    assert!(
        state.len() == 1 && !state[0].is_empty(),
        "{stage}: the CRDT row of {collection} serves: {state:?}"
    );
}
