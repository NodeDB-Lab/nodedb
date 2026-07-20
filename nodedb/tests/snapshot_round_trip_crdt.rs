// SPDX-License-Identifier: BUSL-1.1

//! Production Raft-snapshot builder→applier round-trip for the **CRDT** section.
//!
//! CRDT is one Loro doc per tenant; Loro 1.13 has no per-container export, so
//! the whole tenant doc is shipped to any group owning ≥1 of its collections.
//! This drives the REAL snapshot SEND/RECEIVE path end to end in one process:
//! craft a real Loro delta, apply it on a SOURCE node over pgwire, build the
//! group snapshot, assert the builder captured + filtered the CRDT section, then
//! apply on a fresh TARGET and read the row back.

mod common;

use common::pgwire_harness::TestServer;

use nodedb::control::cluster::snapshot_applier::DataPlaneSnapshotApplier;
use nodedb::control::cluster::snapshot_builder::DataPlaneSnapshotBuilder;
use nodedb_cluster::SnapshotApplier;
use nodedb_cluster::SnapshotBuilder;
use nodedb_cluster::routing::vshard_for_collection;
use nodedb_types::id::DatabaseId;

mod snapshot_rt_common;
use snapshot_rt_common::{DATA_GROUP_ID, single_node_routing};

/// Builder→applier round-trip for the **CRDT** snapshot section.
///
/// CRDT is one Loro doc per tenant; Loro 1.13 has no per-container export, so
/// the whole tenant doc is shipped to any group owning ≥1 of its collections.
/// This test crafts a real Loro delta (collection `crdt_coll`, row `doc1`,
/// field `name=alice`) exactly as `CrdtState` models it (collection = root map,
/// row = `insert_container`, fields on the row map), applies it on the SOURCE
/// via `crdt_apply` (which hex-decodes the delta then merges it into the tenant
/// doc), builds the group snapshot, asserts the builder captured + filtered the
/// CRDT section, then applies on a fresh TARGET and reads the row back.
#[tokio::test]
async fn snapshot_round_trip_crdt() {
    const COLL: &str = "crdt_coll";
    const DOC: &str = "doc1";

    // ── Sanity: the collection's vShard belongs to the data group we build. ───
    let vshard = vshard_for_collection(DatabaseId::DEFAULT, COLL);
    assert!(
        single_node_routing()
            .vshards_for_group(DATA_GROUP_ID)
            .contains(&vshard),
        "collection vShard {vshard} must belong to data group {DATA_GROUP_ID}"
    );

    // ── Craft a real Loro delta matching the `CrdtState` model. ───────────────
    // Collection = root map keyed by name; row = a Map container under it; the
    // row's fields are inserted on that map. `collection_names()` derives from
    // `get_deep_value()`, so this yields exactly `["crdt_coll"]`.
    let delta_hex = {
        let doc = loro::LoroDoc::new();
        let coll = doc.get_map(COLL);
        let row = coll
            .insert_container(DOC, loro::LoroMap::new())
            .expect("row container");
        row.insert("name", "alice").expect("field");
        doc.commit();
        let delta = doc
            .export(loro::ExportMode::Snapshot)
            .expect("export loro snapshot");
        hex::encode(delta)
    };

    // ── SOURCE node: create the collection, then apply the CRDT delta. ────────
    // The builder enumerates tenants from the system catalog, so the collection
    // must be registered (as it always is in production: CRDT collections are
    // created before any delta is applied) for the tenant to be snapshotted.
    let source = TestServer::start_with_routing(single_node_routing()).await;
    source
        .exec(&format!("CREATE COLLECTION {COLL}"))
        .await
        .expect("CREATE COLLECTION on source");
    source
        .exec(&format!(
            "SELECT crdt_apply('{COLL}', '{DOC}', '{delta_hex}')"
        ))
        .await
        .expect("crdt_apply on source");

    // (sanity) the row is readable on the SOURCE → the write landed.
    let src_read = source
        .query_text(&format!("SELECT crdt_state('{COLL}', '{DOC}')"))
        .await
        .expect("crdt_state on source");
    assert!(
        !src_read.is_empty(),
        "source must read back the just-applied CRDT row"
    );

    // ── Build the group snapshot via the PRODUCTION builder. ──────────────────
    let builder = DataPlaneSnapshotBuilder::new(source.shared.clone());
    let bytes = builder
        .build_group_snapshot(DATA_GROUP_ID, 0, 0)
        .await
        .expect("build_group_snapshot");
    assert!(
        !bytes.is_empty(),
        "production builder must produce a non-empty group snapshot"
    );

    // Decode the snapshot and assert the builder carried the CRDT doc
    // (group-filtered), tagged with its collection. This is the direct proof of
    // the builder fix, independent of the apply path below.
    let decoded: nodedb::types::TenantDataSnapshot =
        zerompk::from_msgpack(&bytes).expect("decode group snapshot");
    assert_eq!(
        decoded.crdt_state.len(),
        1,
        "builder must carry the in-group tenant CRDT doc; got {}",
        decoded.crdt_state.len()
    );
    assert!(
        decoded.crdt_state[0].2 == COLL,
        "carried CRDT entry must be tagged with collection {COLL}; got {:?}",
        decoded.crdt_state[0].1
    );

    // ── TARGET node: fresh server, NO routing. The applier needs none (the
    // bytes are already group-filtered), and a CRDT point read on a plain node
    // stays on the local dispatch path. ──────────────────────────────────────
    let target = TestServer::start().await;

    // ── Apply via the PRODUCTION applier. ─────────────────────────────────────
    let applier = DataPlaneSnapshotApplier::new(target.shared.clone());
    applier
        .apply_snapshot(DATA_GROUP_ID, &bytes)
        .await
        .expect("apply_snapshot");

    // ── Verify on the TARGET: the CRDT row round-tripped. ─────────────────────
    // `crdt_state` returns the row only if it exists in the restored tenant doc;
    // a non-empty result proves the full build→ship→apply→read round-trip.
    let tgt_read = target
        .query_text(&format!("SELECT crdt_state('{COLL}', '{DOC}')"))
        .await
        .expect("crdt_state on target");
    assert!(
        !tgt_read.is_empty(),
        "target must read back the CRDT row from the snapshot-installed tenant doc"
    );
}
