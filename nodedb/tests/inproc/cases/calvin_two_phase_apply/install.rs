// SPDX-License-Identifier: BUSL-1.1

//! A staged write becomes visible only at its stamped install.

use std::time::{Duration, Instant};

use nodedb::bridge::envelope::{Request, StageVote, Status};
use nodedb::types::*;
use nodedb_physical::physical_plan::PhysicalPlan;

use super::support::*;

/// A valid (empty read-set) staged write is NOT visible until the stamped
/// install, and the install makes it visible: the stage/install atomicity seam.
#[test]
fn the_stamped_install_makes_a_staged_calvin_write_visible() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Stage a write with an empty read-set → vote is valid (commit).
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(6, 0, vec![kv_put("flushcoll", b"fk", b"fv")], Vec::new()),
        0,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed");
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::Commit),
        "empty read-set is vacuously current → commit vote"
    );

    // Not yet applied: the staged write is invisible before the install.
    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("flushcoll", b"fk"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "staged write must NOT be visible before the install; got {before:?}"
    );

    // The stamped install writes the resolved redo record to base.
    let install_plan = resolved_install(&mut core, &mut tx, &mut rx, 6, 0);
    let installed = send(
        &mut core,
        &mut tx,
        &mut rx,
        install_plan,
        0,
        Some(Lsn::new(60)),
    );
    assert_eq!(
        installed.status,
        Status::Ok,
        "install must succeed: {installed:?}"
    );

    // Now visible.
    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("flushcoll", b"fk"),
        0,
        None,
    );
    assert_eq!(
        after.status,
        Status::Ok,
        "read after the install must succeed: {after:?}"
    );
    assert!(
        !after.payload.is_empty(),
        "the installed write must be visible after the install"
    );
}

/// Calvin sub-operations are already ordered: every replica must run them.
/// A stage and a stamped install whose envelope deadline has already passed
/// still execute, and the write becomes visible. They never answer
/// `DeadlineExceeded`.
#[test]
fn already_ordered_stage_and_install_run_past_their_deadline() {
    let (mut core, mut tx, mut rx, _dir) = make_core();
    let already_ordered = |plan: PhysicalPlan| Request {
        deadline: Instant::now() - Duration::from_secs(1),
        admission: nodedb::bridge::envelope::Admission::Exempt(
            nodedb::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
        ..make_request(plan, 0, None)
    };

    let staged = send_request(
        &mut core,
        &mut tx,
        &mut rx,
        already_ordered(stage_static(
            9,
            0,
            vec![kv_put("latecoll", b"lk", b"lv")],
            Vec::new(),
        )),
    );
    assert_eq!(staged.status, Status::Ok, "late stage must run: {staged:?}");
    assert_eq!(staged.stage_vote, Some(StageVote::Commit));

    let install_plan = resolved_install(&mut core, &mut tx, &mut rx, 9, 0);
    let installed = send_request(
        &mut core,
        &mut tx,
        &mut rx,
        Request {
            wal_lsn: Some(Lsn::new(90)),
            ..already_ordered(install_plan)
        },
    );
    assert_eq!(
        installed.status,
        Status::Ok,
        "late install must run: {installed:?}"
    );

    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("latecoll", b"lk"),
        0,
        None,
    );
    assert_eq!(
        after.status,
        Status::Ok,
        "read after the install: {after:?}"
    );
    assert!(
        !after.payload.is_empty(),
        "the late install must make the staged write visible"
    );
}
