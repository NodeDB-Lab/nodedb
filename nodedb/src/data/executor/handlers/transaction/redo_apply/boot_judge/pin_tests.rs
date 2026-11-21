// SPDX-License-Identifier: BUSL-1.1

//! Boot judges and installs a committed record under the pins its WAL copy
//! carries, whatever config the booting core holds.
//!
//! Each test appends committed records of the `codes` collection with
//! pins, and no core installs them. Boot then settles, judges, and replays
//! them as restart does.

use nodedb_physical::physical_plan::{
    CalvinInstall, CalvinReplySpec, DocIndexFence, DocIndexOwner, DocumentOp, MetaOp, PhysicalPlan,
    RedoOrigin, RedoPins,
};
use nodedb_types::{DatabaseId, QualifiedCollection, StorageKey, Surrogate, Value};

use crate::data::executor::core_loop::CoreLoop;
use crate::wal::{BootVerdictOutcome, RedoSubRecord};

use super::super::install_pin_tests::{field, guarded_wire, register_as, schemaless_wire};
use super::super::judged_tests::{codes_pins, config_key, drop_current_unique, pin_of, pins, wire};
use super::super::stream_validate::tests::{TID, record};
use super::super::test_commit::doc_put_sub_record;
use super::super::unique_handover_tests::{COLL, put, unique_core};
use super::tests::{
    MakeCore, Node, append, append_index_step, boot_and_close_with, marker_pinned, node,
    outcome_of, stored, whole_pinned,
};

const REFUSED: BootVerdictOutcome = BootVerdictOutcome::Refused {
    final_refusal: true,
};

/// What a `MakeCore` returns: the core and the ends of its rings.
type Ends = (CoreLoop, Box<dyn std::any::Any>, Box<dyn std::any::Any>);

/// A boot core whose current `codes` config has no UNIQUE index.
fn no_unique_core(dir: &std::path::Path) -> Ends {
    let (mut core, req, resp) = unique_core(dir);
    drop_current_unique(&mut core);
    (core, req, resp)
}

/// Give the current `codes` config of `core` the digest of `wire(unique)`.
fn adopt_digest(core: &mut CoreLoop, unique: bool) {
    core.doc_configs
        .get_mut(&config_key())
        .expect("codes config")
        .digest = pin_of(COLL, &wire(unique)).digest;
}

/// A boot core with a UNIQUE index, whose config digest matches the pin of
/// a UNIQUE `codes`.
fn unique_digest_core(dir: &std::path::Path) -> Ends {
    let (mut core, req, resp) = unique_core(dir);
    adopt_digest(&mut core, true);
    (core, req, resp)
}

/// A boot core with no UNIQUE index, whose config digest matches the pin
/// of a `codes` with none.
fn plain_digest_core(dir: &std::path::Path) -> Ends {
    let (mut core, req, resp) = no_unique_core(dir);
    adopt_digest(&mut core, false);
    (core, req, resp)
}

/// A boot core whose current `codes` config carries a later typeguard
/// VALUE `lowered`.
fn guarded_core(dir: &std::path::Path) -> Ends {
    let (mut core, req, resp) = unique_core(dir);
    let wire = guarded_wire();
    register_as(&mut core, COLL, &wire, wire.digest().expect("digest"));
    (core, req, resp)
}

/// Rows 1 and 2 both claim code `A`: row 1 is committed under `first`, row
/// 2 as a whole record or a stream marker under `second`. Returns the
/// node and the LSN of row 2.
fn commit_pair(first: &RedoPins, second: &RedoPins, streamed: bool) -> (Node, u64) {
    let mut node = node();
    append(&node, &whole_pinned(vec![put(1, "A")], None, first));
    let plan = if streamed {
        marker_pinned(&mut node, vec![put(2, "A")], second)
    } else {
        whole_pinned(vec![put(2, "A")], None, second)
    };
    let lsn = append(&node, &plan);
    (node, lsn)
}

/// The record of row 2 passes: both rows are stored.
fn assert_row_two_passes(node: &Node, lsn: u64, make_core: MakeCore) {
    let booted = boot_and_close_with(node, make_core);
    assert_eq!(
        outcome_of(&booted.verdicts, lsn),
        Some(BootVerdictOutcome::Passed)
    );
    assert!(stored(&booted.core, 1), "row 1 is stored");
    assert!(stored(&booted.core, 2), "row 2 is stored");
}

/// A record pinned to a `codes` with no UNIQUE index passes on a core that
/// holds a UNIQUE index since added.
#[test]
fn a_record_committed_before_a_unique_was_added_passes_at_boot() {
    let (node, lsn) = commit_pair(&RedoPins::default(), &codes_pins(false), false);
    assert_row_two_passes(&node, lsn, unique_core);
}

/// The stream-marker form of the passing case.
#[test]
fn a_stream_marker_committed_before_a_unique_was_added_passes_at_boot() {
    let (node, lsn) = commit_pair(&RedoPins::default(), &codes_pins(false), true);
    assert_row_two_passes(&node, lsn, unique_core);
}

/// A put of row `surrogate` with `code` and `status` fields.
fn put_status(surrogate: u32, code: &str, status: &str) -> RedoSubRecord {
    let object = [("code", code), ("status", status)]
        .into_iter()
        .map(|(name, value)| (name.to_string(), Value::String(value.to_string())))
        .collect();
    let body = nodedb_types::value_to_msgpack(&Value::Object(object)).expect("encode body");
    doc_put_sub_record(COLL, &format!("r{surrogate}"), &body, surrogate)
}

/// The stored row `surrogate` of `codes`, decoded.
fn row(core: &CoreLoop, surrogate: u32) -> Value {
    let bytes = core
        .sparse
        .get(
            0,
            TID,
            COLL,
            &StorageKey::for_surrogate(Surrogate::new(surrogate)),
        )
        .expect("read row")
        .expect("the row is stored");
    crate::data::executor::doc_format::decode_document_value(&bytes).expect("decode row")
}

/// Boot installs row 1 under `pins` on a core with the later typeguard, and
/// returns the stored row.
fn booted_row(pins: &RedoPins) -> Value {
    let node = node();
    let lsn = append(
        &node,
        &whole_pinned(vec![put_status(1, "A", "OPEN")], None, pins),
    );
    let booted = boot_and_close_with(&node, guarded_core);
    assert_eq!(
        outcome_of(&booted.verdicts, lsn),
        Some(BootVerdictOutcome::Passed)
    );
    row(&booted.core, 1)
}

/// A record that passed at boot stores the row its pins derive, without a
/// typeguard VALUE the core's config added later. With no pins the core's
/// config derives the row, so the case bites.
#[test]
fn a_pinned_record_that_passed_at_boot_stores_the_pinned_row() {
    let pinned = pins(vec![pin_of(COLL, &schemaless_wire())]);
    let row = booted_row(&pinned);
    assert_eq!(field(&row, "status"), Some(&Value::String("OPEN".into())));
    assert_eq!(field(&row, "lowered"), None);

    let unpinned = booted_row(&RedoPins::default());
    assert_eq!(
        field(&unpinned, "lowered"),
        Some(&Value::String("open".into()))
    );
}

/// The committed redo of a Calvin slice of `ops` under `pins`, as the
/// data-group apply dispatches it. Its record carries the slice's stamp,
/// and its plan carries the Calvin install.
fn calvin_pinned(ops: Vec<RedoSubRecord>, pins: &RedoPins) -> PhysicalPlan {
    let mut slice = record(ops);
    slice.calvin_stamp = Some(crate::wal::CalvinStamp {
        epoch: 1,
        position: 0,
        vshard_id: 0,
        mirror_database: None,
    });
    PhysicalPlan::Meta(MetaOp::ApplyTransactionRedo {
        redo: slice.to_bytes().expect("encode redo"),
        collections: vec![COLL.to_string()],
        sum_targets: Vec::new(),
        origin: RedoOrigin::Commit,
        calvin: Some(CalvinInstall {
            epoch: 1,
            position: 0,
            epoch_system_ms: 1,
            reply: CalvinReplySpec::Frozen,
            user_write: true,
            mirror: false,
        }),
        reads: Vec::new(),
        snapshot_ordinal: None,
        pins: pins.clone(),
    })
}

/// Boot replays a Calvin slice of row 1 under `pins` on a core with the
/// later typeguard, and returns the stored row. The slice's stage vote
/// validated it, so boot reaches no verdict on it.
fn booted_calvin_row(pins: &RedoPins) -> Value {
    let node = node();
    let lsn = append(
        &node,
        &calvin_pinned(vec![put_status(1, "A", "OPEN")], pins),
    );
    let booted = boot_and_close_with(&node, guarded_core);
    assert_eq!(
        outcome_of(&booted.verdicts, lsn),
        None,
        "boot judges no Calvin slice"
    );
    row(&booted.core, 1)
}

/// A committed Calvin slice replays at boot into the row its pins derive,
/// without a typeguard VALUE the core's config added later. With no pins
/// the core's config derives the row, so the case bites.
#[test]
fn a_pinned_calvin_slice_replays_the_pinned_row_at_boot() {
    let pinned = pins(vec![pin_of(COLL, &schemaless_wire())]);
    let row = booted_calvin_row(&pinned);
    assert_eq!(field(&row, "status"), Some(&Value::String("OPEN".into())));
    assert_eq!(field(&row, "lowered"), None);

    let unpinned = booted_calvin_row(&RedoPins::default());
    assert_eq!(
        field(&unpinned, "lowered"),
        Some(&Value::String("open".into()))
    );
}

/// The verdict on row 2 of a pinned pair, and whether row 2 is stored,
/// after boot on the core `make_core` builds.
fn pair_verdict(pins: &RedoPins, make_core: MakeCore) -> (Option<BootVerdictOutcome>, bool) {
    let (node, lsn) = commit_pair(pins, pins, false);
    let booted = boot_and_close_with(&node, make_core);
    (outcome_of(&booted.verdicts, lsn), stored(&booted.core, 2))
}

/// The fence of descriptor version `version` of the first incarnation.
fn fence(version: u64) -> DocIndexFence {
    DocIndexFence {
        incarnation: nodedb_types::Hlc::new(1, 0),
        descriptor_version: version,
    }
}

fn code_owner() -> DocIndexOwner {
    DocIndexOwner::Named("code".into())
}

fn codes() -> QualifiedCollection {
    QualifiedCollection::new(DatabaseId::DEFAULT, COLL)
}

/// The data-log build of the UNIQUE `code` index.
fn build_code() -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::BackfillIndex {
        collection: codes(),
        path: "code".into(),
        is_array: false,
        unique: true,
        case_insensitive: false,
        predicate: None,
        owner: code_owner(),
        fence: fence(1),
    })
}

/// The data-log retire of the `code` index, as DROP INDEX proposes it.
fn retire_code() -> PhysicalPlan {
    PhysicalPlan::Document(DocumentOp::DropIndex {
        collection: codes(),
        field: "code".into(),
        owner: code_owner(),
        fence: fence(2),
    })
}

/// The build of `code`, row 1 `A`, the retire of `code`, then row 2 `A`,
/// both rows pinned with a `Ready` UNIQUE `code`. Row 2 is a whole record
/// or a stream marker. Returns the node and the LSN of row 2.
fn commit_across_a_retire(streamed: bool) -> (Node, u64) {
    let pinned = codes_pins(true);
    let mut node = node();
    append_index_step(&node, &build_code());
    append(&node, &whole_pinned(vec![put(1, "A")], None, &pinned));
    append_index_step(&node, &retire_code());
    let plan = if streamed {
        marker_pinned(&mut node, vec![put(2, "A")], &pinned)
    } else {
        whole_pinned(vec![put(2, "A")], None, &pinned)
    };
    let lsn = append(&node, &plan);
    (node, lsn)
}

/// Row 2 lands after this core applied the retire of its pinned `Ready`
/// UNIQUE index. Boot refuses it as final, the `StalePinnedSchema`
/// refusal, and does not store it. Without the refusal it passes: the
/// retire left no entry to probe.
#[test]
fn a_record_pinned_to_a_retired_unique_is_a_final_refusal_at_boot() {
    for streamed in [false, true] {
        let (node, lsn) = commit_across_a_retire(streamed);
        let booted = boot_and_close_with(&node, unique_core);
        assert_eq!(
            outcome_of(&booted.verdicts, lsn),
            Some(REFUSED),
            "streamed {streamed}"
        );
        assert!(stored(&booted.core, 1), "row 1 is stored");
        assert!(!stored(&booted.core, 2), "streamed {streamed}: row 2");
    }
}

/// Rows 1 and 2 both claim `A` under a UNIQUE pin. The booting core's
/// current config has no UNIQUE index, and its maintained set holds the
/// unique owner of `code`. Boot refuses row 2 as a duplicate, as a whole
/// record and as a stream marker.
#[test]
fn a_duplicate_under_a_unique_pin_is_refused_at_boot() {
    for streamed in [false, true] {
        let pinned = codes_pins(true);
        let (node, lsn) = commit_pair(&pinned, &pinned, streamed);
        let booted = boot_and_close_with(&node, no_unique_core);
        assert_eq!(
            outcome_of(&booted.verdicts, lsn),
            Some(REFUSED),
            "streamed {streamed}"
        );
        assert!(stored(&booted.core, 1), "row 1 is stored");
        assert!(!stored(&booted.core, 2), "streamed {streamed}: row 2");
    }
}

/// A core whose config digest equals the pin's digest reads its current
/// config, and reaches the verdict a core that decodes the pin reaches.
#[test]
fn a_pinned_record_whose_digest_matches_the_core_takes_the_core_config() {
    for unique in [true, false] {
        let pinned = codes_pins(unique);
        let (fast, decoded): (MakeCore, MakeCore) = if unique {
            (unique_digest_core, unique_core)
        } else {
            (plain_digest_core, unique_core)
        };
        let by_digest = pair_verdict(&pinned, fast);
        let by_decode = pair_verdict(&pinned, decoded);
        assert_eq!(by_digest, by_decode, "unique {unique}");
        let expected = if unique {
            (Some(REFUSED), false)
        } else {
            (Some(BootVerdictOutcome::Passed), true)
        };
        assert_eq!(by_digest, expected, "unique {unique}");
    }
}
