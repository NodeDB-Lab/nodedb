// SPDX-License-Identifier: BUSL-1.1

//! A phantom insert into a key or collection a slice read as absent
//! aborts the stage vote. An unrelated insert does not.

use nodedb::bridge::envelope::{StageVote, Status};
use nodedb::types::*;
use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};
use nodedb_types::QualifiedCollection;
use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};

use super::support::*;

/// Phantom-insert proof for the KV engine: a Point read observed a key as
/// ABSENT at a snapshot below every seed. A concurrent INSERT of that EXACT
/// key commits above the snapshot. The KV read-key identity is the raw key
/// bytes, the identity the insert writes under. The key's stored version
/// ordinal sits above the snapshot, so validation aborts the stage.
#[test]
fn absent_kv_key_phantom_insert_causes_abort() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let phantom_vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "phantomkv")
        .vshard()
        .as_u32();

    // The key was absent when read at the snapshot: no write is seeded yet.
    let absent_read = VersionedReadEntry {
        engine: EngineTag::Kv,
        collection: "phantomkv".to_string(),
        key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(b"newkey".as_slice()))),
        home_vshard: None,
    };

    // Concurrently, the exact same key is inserted and commits above the
    // snapshot.
    let insert = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: phantom_vshard,
            collection: "phantomkv",
            plans: vec![PhysicalPlan::Kv(KvOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "phantomkv"),
                key: b"newkey".to_vec(),
                value: b"v".to_vec(),
                ttl_ms: 0,
                surrogate: nodedb_test_support::kv_rows::kv_row_surrogate(b"newkey".as_ref()),
                returning: None,
                rls_filters: Vec::new(),
            })],
            lsn: 8,
        },
    );
    assert_eq!(
        insert.status,
        Status::Ok,
        "phantom insert must commit: {insert:?}"
    );

    // Validating the stale absent-read must abort: the inserted key's version
    // ordinal sits above the read's snapshot.
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static_at(
            10,
            0,
            vec![kv_put("phantomtarget", b"tk", b"tv")],
            vec![absent_read],
            BEFORE_SEED,
        ),
        phantom_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::SerializationConflict),
        "a phantom insert into a key observed absent must abort the stage vote"
    );
}

/// Absent-DOCUMENT phantom safety: a document `PointGet` on an absent document
/// cannot record `Point(KeyRepr::Surrogate(s))`. The placeholder surrogate `s`
/// a miss carries never coincides with the surrogate a later INSERT of that
/// `document_id` receives, so a per-key check never catches the phantom. The
/// capture layer therefore degrades an absent document read to `Predicate`.
/// A concurrent INSERT into that collection stores a version ordinal above
/// the read's snapshot. Validation of the predicate reads the collection's
/// newest ordinal, judges the read stale, and the stage VOTES ABORT.
#[test]
fn absent_document_phantom_insert_is_caught() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let doc_vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "phantomdocs")
        .vshard()
        .as_u32();

    // The document was absent when read: capture degraded the miss to a
    // collection-scoped predicate on "phantomdocs" at the snapshot.
    let absent_doc_read = VersionedReadEntry {
        engine: EngineTag::Document,
        collection: "phantomdocs".to_string(),
        key: ReadKeyIdent::Predicate,
        home_vshard: None,
    };

    // Concurrently, a document with the same document_id the read targeted is
    // inserted under a fresh surrogate (42), and commits above the snapshot.
    // Its version ordinal is what the predicate read validates against.
    const NEWLY_ALLOCATED_SURROGATE: u32 = 42;
    let insert = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: doc_vshard,
            collection: "phantomdocs",
            plans: vec![doc_insert(
                "phantomdocs",
                "the-doc-id",
                NEWLY_ALLOCATED_SURROGATE,
            )],
            lsn: 8,
        },
    );
    assert_eq!(
        insert.status,
        Status::Ok,
        "phantom insert must commit: {insert:?}"
    );

    // Validate the stale absent-read: the newest ordinal of phantomdocs sits
    // above the read's snapshot, so the stage must abort.
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static_at(
            11,
            0,
            vec![kv_put("docphantomtarget", b"tk", b"tv")],
            vec![absent_doc_read],
            BEFORE_SEED,
        ),
        doc_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::SerializationConflict),
        "a phantom insert into a collection observed absent must abort the stage \
         vote via the collection's newest ordinal"
    );
}

/// No-over-abort companion to `absent_document_phantom_insert_is_caught`: the
/// predicate degrade must NOT abort every absent-read transaction. An absent
/// document read (predicate on "phantomdocs") stays valid when no insert
/// lands in THAT collection. A concurrent insert into an UNRELATED collection
/// stores versions of that collection only. Phantomdocs holds no version above
/// the snapshot, so the stage VOTES COMMIT.
#[test]
fn absent_document_read_without_matching_insert_still_commits() {
    let (mut core, mut tx, mut rx, _dir) = make_core();

    let doc_vshard = nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, "phantomdocs")
        .vshard()
        .as_u32();

    let absent_doc_read = VersionedReadEntry {
        engine: EngineTag::Document,
        collection: "phantomdocs".to_string(),
        key: ReadKeyIdent::Predicate,
        home_vshard: None,
    };

    // A concurrent insert into a DIFFERENT collection commits above the
    // snapshot. Phantomdocs is untouched.
    const UNRELATED_SURROGATE: u32 = 42;
    let insert = commit_calvin(
        &mut core,
        &mut tx,
        &mut rx,
        CalvinSeed {
            epoch: 1,
            vshard: doc_vshard,
            collection: "othercoll",
            plans: vec![doc_insert("othercoll", "other-id", UNRELATED_SURROGATE)],
            lsn: 8,
        },
    );
    assert_eq!(
        insert.status,
        Status::Ok,
        "unrelated insert must commit: {insert:?}"
    );

    // The predicate read on phantomdocs is still current: no version of it
    // sits above the snapshot, so the stage must commit.
    let staged = send(
        &mut core,
        &mut tx,
        &mut rx,
        stage_static_at(
            11,
            0,
            vec![kv_put("docphantomtarget", b"tk", b"tv")],
            vec![absent_doc_read],
            BEFORE_SEED,
        ),
        doc_vshard,
        None,
    );
    assert_eq!(staged.status, Status::Ok, "stage must succeed: {staged:?}");
    assert_eq!(
        staged.stage_vote,
        Some(StageVote::Commit),
        "an absent-document read must NOT over-abort when no insert lands in its \
         collection"
    );
}
