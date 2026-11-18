// SPDX-License-Identifier: BUSL-1.1

//! The streamed validate of a chunked redo: every check the whole-record
//! path runs before its install, over content read back one bounded batch
//! at a time.
//!
//! The walk reads the stream's chunk records through this core's io_uring
//! cursor and cuts them into batches of whole framed records. It checks the
//! header first, the reads as they arrive, and each op as it arrives, and
//! runs the arms over each batch's ops. Row sources, row changes, publishes
//! and identities decode, so damaged bytes refuse as the whole-record decode
//! refuses them. The walk keeps what the install reads after its arms ran:
//! the row sources, the publishes, the KV key images read before any write,
//! the node-label deltas and the collections the arms note by key. All but
//! the collections sit in spools, checked after each batch, which spill
//! into the install's space past the threshold.
//!
//! A decode error anywhere in the stream outranks every check, so the walk
//! never stops at a check's error: it reads to the end, and stops early only
//! at a decode or read error. Work whose rank cannot replace the kept error
//! is skipped. The verdict is the error the whole-record path refuses with.

use nodedb_types::calvin::VersionedReadEntry;

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::spill::cleanup::SpillKind;
use crate::data::io::uring_wal_cursor::UringWalCursor;
use crate::wal::redo::frame::{
    FrameBatcher, RedoContentHeader, RedoFrameTag, RedoFrames, decode_frame,
};
use crate::wal::{RedoPublish, RedoRowChange, RedoRowSource, RedoStreamReader, RedoSubRecord};

use super::super::batched::{ActiveArms, InstallSpace};

use super::super::passes::{ValidateBatches, final_refusal};
use super::super::streamed::unreadable;
use super::collection::post_state_failed;
use super::post_state::PostState;
use super::verdict::Rank;
use super::walk::{StreamHeader, StreamValidateInput, StreamValidated, StreamWalk, ValidateSpill};

/// Content that does not decode. Every replica holds the same bytes, so the
/// refusal is final.
fn undecodable(error: crate::Error) -> ErrorCode {
    final_refusal(error.into())
}

impl CoreLoop {
    /// Validate the content of the stream `input` names, before its
    /// install. `Ok` holds what the install reads, and the post-state until
    /// the install ran. Nothing is written to any engine.
    pub(in crate::data::executor::handlers::transaction::redo_apply) fn validate_redo_stream(
        &mut self,
        input: &StreamValidateInput<'_>,
    ) -> Result<StreamValidated, ErrorCode> {
        let reference = input.reference;
        let reader = RedoStreamReader::<UringWalCursor>::open(&self.wal_source, reference.clone())
            .map_err(|error| unreadable(reference, error))?;
        let mut batcher = FrameBatcher::new(reader, input.limits.batch_bytes);
        let dir = SpillKind::Txn
            .core_dir(&self.data_dir, self.core_id)
            .join(format!("redo-validate-{}", input.lsn.as_u64()));
        let space = InstallSpace::new(
            &self.data_dir,
            self.core_id,
            input.lsn,
            input.limits.spill_threshold,
        );
        let mut walk = StreamWalk::new(
            PostState::new(dir, input.limits.spill_threshold),
            space,
            self.kv_read_now_ms(),
        );
        loop {
            let batch = match batcher.next_batch() {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(error) => return Err(unreadable(reference, error)),
            };
            self.validate_stream_batch(&mut walk, input, &batch)?;
            self.validate_stream_arms(&mut walk, input);
            if walk.verdict.admits(&Rank::Local)
                && let Err(error) = walk.held.spill_past_threshold(walk.space.space_mut(), 0)
            {
                walk.verdict.offer(Rank::Local, post_state_failed(error));
            }
        }
        walk.order.finish().map_err(undecodable)?;
        self.flush_stream_reads(&mut walk, input);
        self.finish_stream_collections(&mut walk, input);
        let mut active_arms = ActiveArms::default();
        if walk.verdict.admits(&Rank::Arms)
            && let Some(batches) = walk.batches.take()
        {
            let ops = usize::try_from(walk.ops).unwrap_or(usize::MAX);
            match batches.finish(ops) {
                Ok(arms) => active_arms = arms,
                Err(refusal) => walk.verdict.offer(Rank::Arms, refusal.into_code()),
            }
        }
        let StreamWalk {
            verdict,
            post,
            header,
            keyed,
            held,
            space,
            ts_loads,
            array_loads,
            ..
        } = walk;
        verdict.into_result()?;
        let header = header.ok_or(ErrorCode::RejectedPrevalidation {
            reason: "the committed redo stream holds no content header".into(),
        })?;
        Ok(StreamValidated {
            header,
            active_arms,
            keyed,
            held,
            space,
            ts_loads,
            array_loads,
            spill: ValidateSpill::new(post),
        })
    }

    /// Walk the records of one batch. `Err` only for content that does not
    /// decode.
    fn validate_stream_batch(
        &mut self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
        batch: &[u8],
    ) -> Result<(), ErrorCode> {
        for frame in RedoFrames::new(batch) {
            let (tag, body) = frame.map_err(undecodable)?;
            walk.order.admit(tag).map_err(undecodable)?;
            if tag != RedoFrameTag::Read {
                self.flush_stream_reads(walk, input);
            }
            match tag {
                RedoFrameTag::Header => {
                    let header: RedoContentHeader = decode_frame(tag, body).map_err(undecodable)?;
                    walk.batches = Some(ValidateBatches::open(&header.sum_targets, input.origin));
                    walk.header = Some(StreamHeader {
                        version: header.version,
                        user_roles: header.user_roles,
                        sum_targets: header.sum_targets,
                    });
                    self.check_stream_window(walk, input);
                }
                RedoFrameTag::Read => {
                    let read: VersionedReadEntry = decode_frame(tag, body).map_err(undecodable)?;
                    self.push_stream_read(walk, input, read);
                }
                RedoFrameTag::Op => {
                    let sub: RedoSubRecord = decode_frame(tag, body).map_err(undecodable)?;
                    self.check_stream_op(walk, input, sub);
                }
                RedoFrameTag::RowSource => {
                    let source: RedoRowSource = decode_frame(tag, body).map_err(undecodable)?;
                    walk.held.row_sources.add(source);
                }
                RedoFrameTag::RowChange => {
                    decode_frame::<RedoRowChange>(tag, body).map_err(undecodable)?;
                }
                RedoFrameTag::Publish => {
                    let mut publish: RedoPublish = decode_frame(tag, body).map_err(undecodable)?;
                    // Every message names the record's place in its
                    // replicated log.
                    if let Some(position) = input.reference.publish_position {
                        publish.position = Some(position);
                    }
                    walk.held.publishes.push(publish);
                }
                // The Control Plane binds every identity before the install.
                RedoFrameTag::Identity => {}
            }
        }
        Ok(())
    }
}
