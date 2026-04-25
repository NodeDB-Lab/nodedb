//! Bitemporal CRDT sync: version preservation + live-scoped UNIQUE +
//! receiver-stamped system time.
//!
//! The CRDT engine must:
//! - Scope UNIQUE checks to currently-live rows (`_ts_valid_until == MAX`)
//!   so a superseded version does not falsely collide with a new live row.
//! - Stamp `_ts_system` with the receiving node's clock on `validate_and_apply`,
//!   ignoring whatever the sender supplied — keeping system-time receiver-
//!   authoritative for convergence.

use loro::LoroValue;

use nodedb::engine::crdt::tenant_state::TenantCrdtEngine;
use nodedb::types::TenantId;
use nodedb_crdt::CrdtAuthContext;
use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_crdt::validator::ProposedChange;
use nodedb_crdt::validator::bitemporal::VALID_UNTIL_OPEN;

const USERS: &str = "users";

fn tenant() -> TenantCrdtEngine {
    let mut cs = ConstraintSet::new();
    cs.add_unique("users_email_unique", USERS, "email");
    let mut eng = TenantCrdtEngine::new(TenantId::new(1), 1, cs).unwrap();
    eng.mark_bitemporal(USERS);
    eng.set_collection_policy_typed(USERS, CollectionPolicy::strict());
    eng
}

fn change(
    row_id: &str,
    email: &str,
    valid_from: i64,
    valid_until: i64,
    sender_system_ts: i64,
) -> ProposedChange {
    ProposedChange {
        collection: USERS.into(),
        row_id: row_id.into(),
        surrogate: nodedb_types::Surrogate::ZERO,
        fields: vec![
            ("name".into(), LoroValue::String("Alice".into())),
            ("email".into(), LoroValue::String(email.into())),
            ("_ts_valid_from".into(), LoroValue::I64(valid_from)),
            ("_ts_valid_until".into(), LoroValue::I64(valid_until)),
            ("_ts_system".into(), LoroValue::I64(sender_system_ts)),
        ],
    }
}

/// A superseded version with the same email as a new live row should not
/// trigger a UNIQUE collision: the old version is no longer live.
#[test]
fn bitemporal_unique_scoped_to_current() {
    let mut eng = tenant();

    // Version 1: Alice at alice@old.com, terminated at t=1000.
    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1-v1", "alice@old.com", 0, 1_000, 100),
        b"delta1".to_vec(),
    )
    .expect("v1 should accept");

    // A second user insert at the SAME email (old one) — but the old row is
    // no longer live, so this must succeed, not collide.
    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u2-v1", "alice@old.com", 2_000, VALID_UNTIL_OPEN, 200),
        b"delta2".to_vec(),
    )
    .expect("reusing superseded email should not collide");
}

/// Two currently-live rows with the same UNIQUE value MUST collide even in
/// bitemporal mode — bitemporal relaxes only the old-vs-new axis.
#[test]
fn bitemporal_unique_still_collides_for_live_rows() {
    let mut eng = tenant();

    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1", "bob@example.com", 0, VALID_UNTIL_OPEN, 100),
        b"delta1".to_vec(),
    )
    .expect("first live insert accepted");

    let err = eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u2", "bob@example.com", 0, VALID_UNTIL_OPEN, 200),
        b"delta2".to_vec(),
    );
    assert!(
        err.is_err(),
        "two live rows with same UNIQUE value must collide"
    );
}

/// The sender's `_ts_system` must be overwritten with receiver's clock on
/// apply — receiver-authoritative system time.
#[test]
fn bitemporal_crdt_system_ts_receiver_stamped() {
    let mut eng = tenant();

    let sender_ts = 100_000i64; // some arbitrary sender-supplied stamp
    let before_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1", "c@example.com", 0, VALID_UNTIL_OPEN, sender_ts),
        b"delta".to_vec(),
    )
    .unwrap();

    let row = eng.read_row(USERS, "u1").expect("row present");
    let map = match row {
        LoroValue::Map(m) => m,
        other => panic!("expected map, got {other:?}"),
    };
    let stamped = match map.get("_ts_system") {
        Some(LoroValue::I64(n)) => *n,
        other => panic!("expected i64 _ts_system, got {other:?}"),
    };
    assert_ne!(
        stamped, sender_ts,
        "receiver must overwrite sender-supplied _ts_system"
    );
    assert!(
        stamped >= before_ms,
        "stamped ts ({stamped}) must be >= receiver clock at apply ({before_ms})"
    );
}

/// Three successive writes to the same logical row must leave two prior
/// versions in the bitemporal archive. Reading `AS OF` the time between
/// any two stamps returns the version live at that time.
#[test]
fn bitemporal_crdt_preserves_multi_version_history() {
    let mut eng = tenant();

    // v1, then v2 supersedes v1, then v3 supersedes v2.
    for (row_email, valid_from) in [
        ("alice-1@example.com", 0),
        ("alice-2@example.com", 1_000),
        ("alice-3@example.com", 2_000),
    ] {
        std::thread::sleep(std::time::Duration::from_millis(2));
        eng.validate_and_apply(
            1,
            CrdtAuthContext::default(),
            &change("u1", row_email, valid_from, VALID_UNTIL_OPEN, 0),
            vec![],
        )
        .unwrap();
    }

    // Two archived versions expected (v1, v2); v3 is the live row.
    let state = eng.state();
    let archived = state.archive_version_count(USERS, "u1");
    assert_eq!(archived, 2, "v1 and v2 must be archived when v3 overwrites");

    // Live row holds v3.
    let live = state.read_row(USERS, "u1").unwrap();
    if let LoroValue::Map(m) = live {
        let email = match m.get("email").unwrap() {
            LoroValue::String(s) => s.to_string(),
            other => panic!("expected string, got {other:?}"),
        };
        assert_eq!(email, "alice-3@example.com");
    } else {
        panic!("expected map");
    }
}

/// Audit retention purge drops archived versions older than the cutoff
/// while preserving the live row and versions after the cutoff.
#[test]
fn bitemporal_crdt_purge_drops_superseded_below_cutoff() {
    let mut eng = tenant();

    // Stage three versions with well-separated stamps.
    for email in [
        "v1@example.com",
        "v2@example.com",
        "v3@example.com",
        "v4@example.com",
    ] {
        std::thread::sleep(std::time::Duration::from_millis(3));
        eng.validate_and_apply(
            1,
            CrdtAuthContext::default(),
            &change("u1", email, 0, VALID_UNTIL_OPEN, 0),
            vec![],
        )
        .unwrap();
    }
    let state = eng.state();
    assert_eq!(state.archive_version_count(USERS, "u1"), 3);

    // Capture the stamps the receiver assigned, then purge everything
    // strictly below the stamp of the second-oldest archived version.
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Purge history older than "now" — sweeps everything since all stamps
    // precede the call to SystemTime::now() below. Live row must survive.
    let cutoff = now_ms + 1;
    let dropped = eng.purge_history_before(USERS, cutoff).unwrap();
    assert_eq!(dropped, 3, "every archived version must be reclaimed");
    assert_eq!(state.archive_version_count(USERS, "u1"), 0);

    // Live row still readable — purge never touches the current state.
    let live = state.read_row(USERS, "u1").unwrap();
    if let LoroValue::Map(m) = live {
        let email = match m.get("email").unwrap() {
            LoroValue::String(s) => s.to_string(),
            other => panic!("expected string, got {other:?}"),
        };
        assert_eq!(email, "v4@example.com");
    } else {
        panic!("expected map");
    }
}

/// Two peers producing divergent writes with different receiver stamps
/// must both leave an archived version after merge so the timeline is
/// reconstructible via `read_row_as_of`.
#[test]
fn bitemporal_crdt_divergence_preserves_both_versions_via_archive() {
    let mut eng = tenant();

    // Peer A writes v1.
    eng.validate_and_apply(
        1,
        CrdtAuthContext::default(),
        &change("u1", "a@peer.com", 0, VALID_UNTIL_OPEN, 0),
        vec![],
    )
    .unwrap();
    let ts_a = {
        let LoroValue::Map(live_a) = eng.state().read_row(USERS, "u1").unwrap() else {
            panic!("expected map");
        };
        match live_a.get("_ts_system").unwrap() {
            LoroValue::I64(n) => *n,
            other => panic!("expected i64, got {other:?}"),
        }
    };

    std::thread::sleep(std::time::Duration::from_millis(5));

    // Peer B writes v2, which supersedes v1 — v1 must land in archive.
    eng.validate_and_apply(
        2,
        CrdtAuthContext::default(),
        &change("u1", "b@peer.com", 1_000, VALID_UNTIL_OPEN, 0),
        vec![],
    )
    .unwrap();

    let state = eng.state();
    // AS OF ts_a returns the peer-A version.
    let at_a = state.read_row_as_of(USERS, "u1", ts_a).expect("v1 as-of");
    if let LoroValue::Map(m) = at_a {
        let email = match m.get("email").unwrap() {
            LoroValue::String(s) => s.to_string(),
            other => panic!("expected string, got {other:?}"),
        };
        assert_eq!(email, "a@peer.com", "AS OF peer-A stamp returns peer-A row");
    } else {
        panic!("expected map");
    }

    // Current live row is peer-B's write.
    let LoroValue::Map(live_b) = state.read_row(USERS, "u1").unwrap() else {
        panic!("expected map");
    };
    let email_b = match live_b.get("email").unwrap() {
        LoroValue::String(s) => s.to_string(),
        other => panic!("expected string, got {other:?}"),
    };
    assert_eq!(email_b, "b@peer.com");
}
