// SPDX-License-Identifier: Apache-2.0

use loro::LoroValue;

use super::CrdtState;

const OLD_PEER: u64 = 7;
const NEW_PEER: u64 = 99;

fn checked_rekey(source: &CrdtState, collection: &str) -> super::Rekeyed {
    let value = source.doc.get_deep_value();
    let state_frontiers = source.doc.state_frontiers();
    let oplog_frontiers = source.doc.oplog_frontiers();
    let version = source.oplog_version_vector();
    let rekeyed = source.rekey(collection, NEW_PEER).unwrap();
    assert_eq!(source.doc.get_deep_value(), value);
    assert_eq!(source.doc.state_frontiers(), state_frontiers);
    assert_eq!(source.doc.oplog_frontiers(), oplog_frontiers);
    assert_eq!(source.oplog_version_vector(), version);
    assert_eq!(rekeyed.state.doc.get_deep_value(), value);
    assert_eq!(rekeyed.state.peer_id(), NEW_PEER);
    assert_eq!(rekeyed.state.oplog_version_vector().get(&OLD_PEER), None);
    rekeyed
}

#[test]
fn rootless_document_rekey_leaves_both_documents_rootless() {
    let source = CrdtState::new(OLD_PEER).unwrap();
    let rekeyed = checked_rekey(&source, "users");
    assert!(source.collection_names().is_empty());
    assert!(rekeyed.state.collection_names().is_empty());
    assert!(rekeyed.rows.is_empty());
    assert_eq!(rekeyed.state.local_op_counter(), 0);
}

#[test]
fn existing_empty_collection_root_survives_rekey_without_row_deltas() {
    let source = CrdtState::new(OLD_PEER).unwrap();
    source.doc.get_map("users");
    let rekeyed = checked_rekey(&source, "users");
    assert_eq!(rekeyed.state.collection_names(), ["users"]);
    assert!(rekeyed.rows.is_empty());
    assert_eq!(rekeyed.state.local_op_counter(), 0);
}

#[test]
fn deleting_the_last_row_preserves_the_empty_collection_root_during_rekey() {
    let source = CrdtState::new(OLD_PEER).unwrap();
    source
        .upsert("users", "a", &[("v", LoroValue::I64(1))])
        .unwrap();
    source.delete("users", "a").unwrap();
    let rekeyed = checked_rekey(&source, "users");
    assert_eq!(rekeyed.state.collection_names(), ["users"]);
    assert!(rekeyed.rows.is_empty());
    assert_eq!(rekeyed.state.local_op_counter(), 0);
}

#[test]
fn rekeyed_state_carries_scalar_rows() {
    let state = CrdtState::new(OLD_PEER).expect("state");
    state
        .upsert(
            "users",
            "u1",
            &[
                ("name", LoroValue::String("alice".into())),
                ("age", LoroValue::I64(30)),
            ],
        )
        .expect("upsert");

    let rekeyed = state.rekey("users", NEW_PEER).expect("rekey");

    assert_eq!(rekeyed.state.peer_id(), NEW_PEER);
    assert_eq!(
        rekeyed.state.read_field("users", "u1", "name"),
        Some(LoroValue::String("alice".into()))
    );
    assert_eq!(
        rekeyed.state.read_field("users", "u1", "age"),
        Some(LoroValue::I64(30))
    );
}

#[test]
fn rekeyed_state_preserves_nested_containers() {
    let state = CrdtState::new(OLD_PEER).expect("state");
    state
        .upsert("pages", "p1", &[("title", LoroValue::String("v1".into()))])
        .expect("upsert");
    state
        .list_insert_fields(
            "pages",
            "p1",
            "blocks",
            0,
            &[("id".into(), LoroValue::String("blk-0".into()))],
        )
        .expect("block");

    let rekeyed = state.rekey("pages", NEW_PEER).expect("rekey").state;

    // A scalar re-upsert would have flattened the block list into a plain
    // value, leaving a row that can never take another list operation.
    assert_eq!(
        rekeyed
            .list_length("pages", "p1", "blocks")
            .expect("length"),
        1,
        "the nested block list must survive as a live container"
    );
    rekeyed
        .list_insert_fields(
            "pages",
            "p1",
            "blocks",
            1,
            &[("id".into(), LoroValue::String("blk-1".into()))],
        )
        .expect("the rebuilt list must still accept list operations");
    assert_eq!(
        rekeyed
            .list_length("pages", "p1", "blocks")
            .expect("length"),
        2
    );
}

#[test]
fn rekeyed_state_shares_no_history_with_the_abandoned_peer() {
    let state = CrdtState::new(OLD_PEER).expect("state");
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("alice".into()))],
        )
        .expect("upsert");

    let rekeyed = state.rekey("users", NEW_PEER).expect("rekey").state;

    // An operation still attributed to the abandoned peer is exactly what
    // the owning replica refuses and what the merge discards as a replay.
    let vv = rekeyed.oplog_version_vector();
    assert_eq!(
        vv.get(&OLD_PEER).copied().unwrap_or(0),
        0,
        "no operation may remain attributed to the abandoned peer"
    );
    assert!(
        vv.get(&NEW_PEER).copied().unwrap_or(0) > 0,
        "the new peer must own the rebuilt operations"
    );
}

#[test]
fn rekey_leaves_the_source_state_intact() {
    let state = CrdtState::new(OLD_PEER).expect("state");
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("alice".into()))],
        )
        .expect("upsert");

    let _rekeyed = state.rekey("users", NEW_PEER).expect("rekey");

    assert_eq!(state.peer_id(), OLD_PEER);
    assert_eq!(
        state.read_field("users", "u1", "name"),
        Some(LoroValue::String("alice".into())),
        "a rekey must not mutate the state it copies from"
    );
}

#[test]
fn each_row_gets_its_own_applicable_delta() {
    let state = CrdtState::new(OLD_PEER).expect("state");
    for row in ["u1", "u2", "u3"] {
        state
            .upsert("users", row, &[("name", LoroValue::String(row.into()))])
            .expect("upsert");
    }

    let rekeyed = state.rekey("users", NEW_PEER).expect("rekey");

    assert_eq!(rekeyed.rows.len(), 3, "one span per row");

    // Every row must be carried by exactly one span, and the spans must
    // together reconstruct the collection at the receiver. A span that
    // covered two rows would make Origin commit them as one, and a row
    // covered by none would be silently absent after the rotation.
    let receiver = CrdtState::new(1234).expect("receiver");
    for row in &rekeyed.rows {
        let bytes = rekeyed
            .state
            .export_local_range(row.from_counter, row.to_counter)
            .expect("range export");
        assert!(!bytes.is_empty(), "row {} exported nothing", row.row_id);

        receiver.import(&bytes).expect("row delta must apply");
        assert_eq!(
            receiver.read_field("users", &row.row_id, "name"),
            Some(LoroValue::String(row.row_id.as_str().into())),
            "row {} did not arrive from its own span",
            row.row_id
        );
    }

    assert_eq!(
        receiver.row_ids("users").len(),
        3,
        "the spans must reconstruct the whole collection"
    );
}

#[test]
fn an_unreproducible_root_is_refused() {
    let state = CrdtState::new(OLD_PEER).expect("state");
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("alice".into()))],
        )
        .expect("upsert");
    state
        .upsert("other", "o1", &[("name", LoroValue::String("bob".into()))])
        .expect("upsert");

    let Err(err) = state.rekey("users", NEW_PEER) else {
        panic!("a root outside the rekeyed collection must not be silently dropped");
    };
    assert!(
        err.to_string().contains("other"),
        "the refusal must name the root it could not re-author, got: {err}"
    );
}
