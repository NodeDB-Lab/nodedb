// SPDX-License-Identifier: BUSL-1.1

//! A Calvin slice resolved in one part, as a test installs it.

use nodedb::wal::RedoRecord;
use nodedb::wal::redo::frame::lists_record;
use nodedb_physical::physical_plan::{CalvinReplySpec, CalvinResolvedPart, MetaOp, PhysicalPlan};

/// The resolve of the slice at `(epoch, position)` in one part.
pub fn whole_calvin_resolve(epoch: u64, position: u32) -> PhysicalPlan {
    PhysicalPlan::Meta(MetaOp::CalvinResolve {
        epoch,
        position,
        part: 0,
        max_part_bytes: u64::MAX,
    })
}

/// The encoded redo record and the staged reply a one-part resolve answered
/// with.
pub fn decode_whole_calvin_resolve(payload: &[u8]) -> (Vec<u8>, CalvinReplySpec) {
    let answer: CalvinResolvedPart =
        zerompk::from_msgpack(payload).expect("decode the resolved part");
    assert!(answer.part.done, "a one-part resolve answers one part");
    let record: RedoRecord = lists_record(&answer.part.bytes).expect("decode the resolved lists");
    let reply = answer.reply.expect("part 0 carries the staged reply");
    (record.to_bytes().expect("encode the redo record"), reply)
}
