use loro::LoroValue;

use super::core::CrdtState;

#[test]
fn upsert_and_check_existence() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "user-1",
            &[
                ("name", LoroValue::String("Alice".into())),
                ("email", LoroValue::String("alice@example.com".into())),
            ],
        )
        .unwrap();

    assert!(state.row_exists("users", "user-1"));
    assert!(!state.row_exists("users", "user-2"));
}

#[test]
fn delete_row() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "user-1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();

    assert!(state.row_exists("users", "user-1"));
    state.delete("users", "user-1").unwrap();
    assert!(!state.row_exists("users", "user-1"));
}

#[test]
fn row_ids_listing() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert("users", "a", &[("x", LoroValue::I64(1))])
        .unwrap();
    state
        .upsert("users", "b", &[("x", LoroValue::I64(2))])
        .unwrap();

    let mut ids = state.row_ids("users");
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn field_value_uniqueness_check() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "u1",
            &[("email", LoroValue::String("alice@example.com".into()))],
        )
        .unwrap();

    assert!(state.field_value_exists(
        "users",
        "email",
        &LoroValue::String("alice@example.com".into())
    ));
    assert!(!state.field_value_exists(
        "users",
        "email",
        &LoroValue::String("bob@example.com".into())
    ));
}

#[test]
fn compact_history_preserves_state() {
    let mut state = CrdtState::new(1).unwrap();
    // Create some state with history.
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();
    state
        .upsert("users", "u2", &[("name", LoroValue::String("Bob".into()))])
        .unwrap();
    // Update to create more history.
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice Updated".into()))],
        )
        .unwrap();

    // Compact.
    state.compact_history().unwrap();

    // State should be preserved after compaction.
    assert!(state.row_exists("users", "u1"));
    assert!(state.row_exists("users", "u2"));

    // New operations should still work.
    state
        .upsert(
            "users",
            "u3",
            &[("name", LoroValue::String("Carol".into()))],
        )
        .unwrap();
    assert!(state.row_exists("users", "u3"));
}

#[test]
fn estimated_memory_grows_with_data() {
    let state = CrdtState::new(1).unwrap();
    let before = state.estimated_memory_bytes();

    for i in 0..100 {
        state
            .upsert(
                "items",
                &format!("item-{i}"),
                &[("value", LoroValue::I64(i))],
            )
            .unwrap();
    }

    let after = state.estimated_memory_bytes();
    assert!(
        after > before,
        "memory should grow: before={before}, after={after}"
    );
}

#[test]
fn snapshot_roundtrip() {
    let state1 = CrdtState::new(1).unwrap();
    state1
        .upsert("users", "u1", &[("name", LoroValue::String("Bob".into()))])
        .unwrap();

    let snapshot = state1.export_snapshot().unwrap();

    let state2 = CrdtState::new(2).unwrap();
    state2.import(&snapshot).unwrap();

    assert!(state2.row_exists("users", "u1"));
}
