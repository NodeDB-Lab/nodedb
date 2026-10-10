// SPDX-License-Identifier: BUSL-1.1

//! The stage vote judges the slice's read-set against the local write
//! versions: a current read commits, a stale one aborts and drops.

use nodedb::bridge::envelope::{StageVote, Status};
use nodedb::types::*;
use nodedb_physical::physical_plan::{MetaOp, PhysicalPlan};
use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

use super::support::*;

/// An invalid vote (a stale versioned read against a newer local write) STAGES
/// but never applies; the drop discards it and base stays unchanged.
#[test]
fn drop_discards_invalid_staged_calvin_write() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to `dropcoll` at LSN 100 on its home vShard, so
    // its collection version there is the version of LSN 100.
    let seed = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: home_vshard("dropcoll"),
            collection: "dropcoll",
            plans: vec![kv_put("dropcoll", b"seed", b"v")],
            lsn: 100,
        },
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    // The read entry's collection must home to the staged request's vShard for
    // the read-set check to consider it.
    let read_vshard = home_vshard("dropcoll");

    // A read of `dropcoll` observed at the version of LSN 50 — stale against
    // the seed's write at LSN 100 → the read-set is no longer current → abort
    // vote.
    let stale_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "dropcoll".to_string(),
        key: ReadKeyIdent::Predicate,
        read_version: local_version(50),
        home_vshard: None,
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            7,
            0,
            vec![kv_put("targetcoll", b"tk", b"tv")],
            vec![stale_read],
        ),
        read_vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "stage must succeed even on abort vote"
    );
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::SerializationConflict),
        "stale read-set must produce an abort vote"
    );

    // Staged, not applied: the target write is invisible.
    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("targetcoll", b"tk"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "aborted staged write must NOT be visible; got {before:?}"
    );

    // Drop discards the staged plans and fires nothing. The drop must target
    // the SAME vShard the stage keyed under (as production dispatches it), so it
    // actually pops this participant's staged slice.
    let dropped = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: 7,
            position: 0,
        }),
        read_vshard,
        None,
    );
    assert_eq!(dropped.status, Status::Ok, "drop must succeed: {dropped:?}");

    // Still invisible after the drop — base was never mutated.
    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("targetcoll", b"tk"),
        0,
        None,
    );
    assert!(
        after.payload.is_empty() || after.status == Status::Error,
        "dropped write must never be visible; got {after:?}"
    );
}

/// A `Point` read (not just a `Predicate` read) at or after the recorded write
/// version is current → commit vote, and the stamped install applies the
/// staged write.
#[test]
fn point_read_at_write_version_commits_and_the_install_applies() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to key `pk` in `pointcoll` at LSN 10.
    let seed = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: home_vshard("pointcoll"),
            collection: "pointcoll",
            plans: vec![kv_put("pointcoll", b"pk", b"v1")],
            lsn: 10,
        },
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    let point_vshard = home_vshard("pointcoll");

    // A Point read of the exact same key observed at the write's version is
    // still current: no write happened AFTER the read.
    let current_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "pointcoll".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"pk".as_slice()))),
        read_version: local_version(10),
        home_vshard: None,
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            8,
            0,
            vec![kv_put("committarget", b"ck", b"cv")],
            vec![current_read],
        ),
        point_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::Commit),
        "a read at or after the last write's version must be current -> commit vote"
    );

    let install_plan = resolved_install(&mut core, &mut tx, &mut rx, 8, point_vshard);
    let installed = send(
        &mut core,
        &mut tx,
        &mut rx,
        install_plan,
        point_vshard,
        Some(Lsn::new(80)),
    );
    assert_eq!(
        installed.status,
        Status::Ok,
        "install must succeed: {installed:?}"
    );

    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("committarget", b"ck"),
        0,
        None,
    );
    assert_eq!(
        after.status,
        Status::Ok,
        "read after the install must succeed"
    );
    assert!(
        !after.payload.is_empty(),
        "the installed write must be visible after the install"
    );
}

/// A `Point` read of a key STALE against a later write to that same key
/// (read at LSN 5 vs. a committed write at LSN 10) aborts the stage vote, and the
/// drop discards the staged write with no base mutation — mirrors
/// `drop_discards_invalid_staged_calvin_write` but for a Point key (not a
/// collection-scoped Predicate).
#[test]
fn stale_point_read_of_kv_key_aborts_stage_and_drop_discards() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    // Seed a committed write to key `pk` in `stalecoll` at LSN 10.
    let seed = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: home_vshard("stalecoll"),
            collection: "stalecoll",
            plans: vec![kv_put("stalecoll", b"pk", b"v1")],
            lsn: 10,
        },
    );
    assert_eq!(seed.status, Status::Ok, "seed write must commit: {seed:?}");

    let stale_vshard = home_vshard("stalecoll");

    // A Point read of the exact same key observed at LSN 5 — stale against the
    // write at LSN 10 → the read-set is no longer current → abort vote.
    let stale_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "stalecoll".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"pk".as_slice()))),
        read_version: local_version(5),
        home_vshard: None,
    };

    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static(
            9,
            0,
            vec![kv_put("aborttarget", b"ak", b"av")],
            vec![stale_read],
        ),
        stale_vshard,
        None,
    );
    assert_eq!(
        staged.status,
        Status::Ok,
        "stage must succeed even on abort vote"
    );
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::SerializationConflict),
        "a stale Point read (write after the read) must produce an abort vote"
    );

    let before = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("aborttarget", b"ak"),
        0,
        None,
    );
    assert!(
        before.payload.is_empty() || before.status == Status::Error,
        "aborted staged write must NOT be visible; got {before:?}"
    );

    let dropped = send(
        &mut core,
        &mut tx,
        &mut rx,
        PhysicalPlan::Meta(MetaOp::CalvinDrop {
            epoch: 9,
            position: 0,
        }),
        stale_vshard,
        None,
    );
    assert_eq!(dropped.status, Status::Ok, "drop must succeed: {dropped:?}");

    let after = send(
        &mut core,
        &mut tx,
        &mut rx,
        kv_get("aborttarget", b"ak"),
        0,
        None,
    );
    assert!(
        after.payload.is_empty() || after.status == Status::Error,
        "dropped write must never be visible; got {after:?}"
    );
}
