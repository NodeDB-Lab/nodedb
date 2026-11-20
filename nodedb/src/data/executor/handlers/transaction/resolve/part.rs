// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::ResolveTxnPart`: a session commit reads its resolved redo in
//! parts.
//!
//! Part `0` checks the transaction's guards and resolves it into parts as the
//! serializers produce each record. One part stays in memory. More parts go
//! to the transaction's resolve spill file, one row per part. It then opens
//! the transaction's cursor over the parts and reports the byte length of
//! every part, the sub-record count and the rejected timeseries lines. Every check of the resolve runs there,
//! before the commit proposes any part. A later part reads the cursor.

use nodedb_physical::physical_plan::{
    PhysicalPlan, RejectedLines, ResolvedTxnOpen, ResolvedTxnPart,
};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay_gauge::txn_spill_dir;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

use super::cursor::{RESOLVE_FILE, remove_resolve_path};
use super::sink::{FinishedSink, RedoSink, SinkOutput};

/// One `ResolveTxnPart` request.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct PartRequest {
    pub(in crate::data::executor) txn_id: TxnId,
    pub(in crate::data::executor) part: u32,
    pub(in crate::data::executor) max_part_bytes: u64,
}

impl CoreLoop {
    /// Answer one `ResolveTxnPart` request with a [`ResolvedTxnPart`].
    pub(in crate::data::executor) fn execute_resolve_txn_part(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        request: PartRequest,
        plans: &[PhysicalPlan],
    ) -> Response {
        let answer = if request.part == 0 {
            self.open_resolve_cursor(task, tid, request, plans)
        } else {
            self.read_resolve_part(request.txn_id, request.part)
        };
        let encoded = answer.and_then(|part| {
            zerompk::to_msgpack_vec(&part).map_err(|e| {
                ErrorCode::from(crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("resolve part encode: {e}"),
                })
            })
        });
        match encoded {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(code) => self.response_error(task, code),
        }
    }

    /// Check the guards, then resolve the transaction into parts and read
    /// part `0`. A refused guard asks for a retry.
    fn open_resolve_cursor(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        request: PartRequest,
        plans: &[PhysicalPlan],
    ) -> Result<ResolvedTxnPart, ErrorCode> {
        // A repeated part 0 resolves again, so a failed resolve leaves no
        // cursor of an earlier one.
        if let Some(earlier) = self.resolve_cursors.close(request.txn_id) {
            remove_resolve_path(&earlier)?;
        }
        if !self.session_guards_hold(task, tid, plans)? {
            return Err(ErrorCode::OllpRetryRequired);
        }
        self.resolve_into_cursor(task, tid, request, plans)
    }

    /// Part `request.part` of `txn_id`'s open cursor.
    pub(in crate::data::executor) fn read_resolve_part(
        &mut self,
        txn_id: TxnId,
        part: u32,
    ) -> Result<ResolvedTxnPart, ErrorCode> {
        let now = self.hlc.peek();
        self.resolve_cursors
            .read(txn_id, part, now)
            .map(|(bytes, done)| ResolvedTxnPart {
                index: part,
                bytes,
                done,
                opened: None,
            })
            .map_err(ErrorCode::from)
    }

    /// Resolve `plans`, staged under `request.txn_id`, into parts, open the
    /// cursor over them, and read part `0`. Any earlier cursor of the
    /// transaction is closed first.
    ///
    /// A resolve of one part keeps it in memory and creates no file. A
    /// resolve of more parts writes all of them to the transaction's resolve
    /// spill file. The resolve holds one part at a time, and every check runs
    /// before the answer. An error leaves no cursor, and it removes the
    /// spill file when the resolve created one.
    pub(in crate::data::executor) fn resolve_into_cursor(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        request: PartRequest,
        plans: &[PhysicalPlan],
    ) -> Result<ResolvedTxnPart, ErrorCode> {
        let txn_id = request.txn_id;
        if let Some(earlier) = self.resolve_cursors.close(txn_id) {
            remove_resolve_path(&earlier)?;
        }
        // The sink creates the directory and the file only for a resolve of
        // more than one part.
        let path = txn_spill_dir(&self.data_dir, self.core_id, txn_id).join(RESOLVE_FILE);
        let max_part_bytes = usize::try_from(request.max_part_bytes).unwrap_or(usize::MAX);
        let mut sink = RedoSink::spill(&path, max_part_bytes);
        let finished = self
            .resolve_txn_into(task, tid, txn_id, plans, &mut sink)
            .and_then(|()| sink.finish());
        let FinishedSink {
            output,
            op_count,
            rejected,
        } = match finished {
            Ok(finished) => finished,
            Err(error) => {
                // A resolve that stopped before its second part created no
                // file, and removing an absent file succeeds. The removal
                // files its own report when it fails, and the resolve answers
                // its own error.
                if remove_resolve_path(&path).is_err() {
                    tracing::error!(
                        path = %path.display(),
                        "a failed resolve left its resolve spill file in place"
                    );
                }
                return Err(error.into());
            }
        };
        let (source, len, parts) = match output {
            SinkOutput::Spill { source, len, parts } => (source, len, parts),
            #[cfg(test)]
            SinkOutput::Memory(_) => {
                return Err(crate::Error::Internal {
                    detail: format!("{txn_id}: a spill resolve sink finished in memory"),
                }
                .into());
            }
        };
        let now = self.hlc.peek();
        self.resolve_cursors.open(txn_id, source, parts, now)?;
        let (bytes, done) = self.resolve_cursors.read(txn_id, 0, now)?;
        Ok(ResolvedTxnPart {
            index: 0,
            bytes,
            done,
            opened: Some(ResolvedTxnOpen {
                len,
                op_count,
                ts_rejected: rejected
                    .into_iter()
                    .map(|(collection, lines)| RejectedLines { collection, lines })
                    .collect(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::{KvOp, MetaOp};
    use nodedb_types::{QualifiedCollection, Surrogate};

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{Priority, Request, Status};
    use crate::data::executor::handlers::transaction::overlay_gauge::txn_spill_dir;
    use crate::data::executor::handlers::transaction::stage_write::kv_row_identity;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use crate::wal::redo::frame::{RedoFrameTag, RedoFrames, decode_frame};
    use crate::wal::{RedoRecord, RedoRowChange, RedoRowSource, RedoSubRecord};

    const TID: u64 = 1;
    const ROWS: u32 = 200;

    fn make_core() -> (CoreLoop, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            crate::data::executor::core_loop::CoreOpenParams::unencrypted(
                0,
                req_rx,
                resp_tx,
                dir.path(),
                Arc::new(nodedb_types::OrdinalClock::new()),
                crate::data::executor::core_loop::test_governor(),
            ),
        )
        .expect("CoreLoop::open");
        (core, dir)
    }

    fn make_task() -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(TID),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            commit_hlc: None,
            system_ordinal: None,
            config_pins: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    fn kv_write_plan() -> PhysicalPlan {
        PhysicalPlan::Kv(KvOp::Put {
            declared_columns: nodedb_physical::physical_plan::KvDeclaredColumns::none(),
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "cache"),
            key: Vec::new(),
            value: Vec::new(),
            ttl_ms: 0,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            provenance: None,
        })
    }

    /// Stage `ROWS` KV rows under a zero hot-tier threshold, so the overlay
    /// spills to disk.
    fn stage_spilled_kv(core: &mut CoreLoop, txn: TxnId) {
        core.query_tuning.max_txn_overlay_bytes = 0;
        let coll = (DatabaseId::DEFAULT, TenantId::new(TID), "cache".to_string());
        for n in 0..ROWS {
            let key = format!("k{n:04}");
            core.txn_overlay_mut(txn)
                .insert_put(
                    coll.clone(),
                    n + 1,
                    &kv_row_identity(key.as_bytes()),
                    vec![n as u8; 48],
                )
                .expect("stage a row");
        }
        let dir = txn_spill_dir(&core.data_dir, core.core_id, txn);
        assert!(dir.is_dir(), "the overlay spilled into {}", dir.display());
    }

    fn read_part(
        core: &mut CoreLoop,
        txn: TxnId,
        part: u32,
        max_part_bytes: u64,
    ) -> Result<ResolvedTxnPart, ErrorCode> {
        let plans = if part == 0 {
            vec![kv_write_plan()]
        } else {
            Vec::new()
        };
        let resp = core.execute_resolve_txn_part(
            &make_task(),
            TID,
            PartRequest {
                txn_id: txn,
                part,
                max_part_bytes,
            },
            &plans,
        );
        if resp.status != Status::Ok {
            return Err(resp
                .error_code
                .as_deref()
                .cloned()
                .unwrap_or(ErrorCode::Internal {
                    detail: "no error code".into(),
                }));
        }
        Ok(zerompk::from_msgpack(resp.payload.as_bytes()).expect("decode part"))
    }

    /// The lists the framed bytes hold.
    fn decode_lists(bytes: &[u8]) -> (Vec<RedoRowSource>, Vec<RedoSubRecord>, Vec<RedoRowChange>) {
        let (mut sources, mut ops, mut changes) = (Vec::new(), Vec::new(), Vec::new());
        for frame in RedoFrames::new(bytes) {
            let (tag, body) = frame.expect("frame");
            match tag {
                RedoFrameTag::RowSource => sources.push(decode_frame(tag, body).expect("source")),
                RedoFrameTag::Op => ops.push(decode_frame(tag, body).expect("op")),
                RedoFrameTag::RowChange => changes.push(decode_frame(tag, body).expect("change")),
                other => panic!("a resolve frames no {other:?} record"),
            }
        }
        (sources, ops, changes)
    }

    /// The parts of a spilled overlay's resolve join to the lists a whole
    /// resolve of the same overlay holds.
    #[test]
    fn a_cursor_resolve_of_a_spilled_overlay_matches_a_whole_resolve() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(81);
        stage_spilled_kv(&mut core, txn);

        let whole = core.execute_resolve_txn(&make_task(), TID, txn, &[kv_write_plan()]);
        assert_eq!(whole.status, Status::Ok, "{whole:?}");
        let whole = RedoRecord::from_bytes(whole.payload.as_bytes()).expect("whole redo");
        assert_eq!(whole.ops.len(), ROWS as usize);

        let first = read_part(&mut core, txn, 0, 1024).expect("part 0");
        let opened = first.opened.clone().expect("part 0 reports the resolve");
        assert_eq!(opened.op_count, ROWS as u64);
        assert!(!first.done, "a 1 KiB part bound splits the redo");
        let mut joined = first.bytes.clone();
        let mut part = first;
        let mut count = 1u32;
        while !part.done {
            part = read_part(&mut core, txn, count, 1024).expect("next part");
            assert_eq!(part.index, count);
            assert!(part.opened.is_none());
            assert!(
                part.bytes.len() <= 1024,
                "a part of {} bytes",
                part.bytes.len()
            );
            joined.extend_from_slice(&part.bytes);
            count += 1;
        }
        assert!(count > 5, "{count} parts");
        assert_eq!(joined.len() as u64, opened.len);
        let (sources, ops, changes) = decode_lists(&joined);
        assert_eq!(ops, whole.ops);
        assert_eq!(changes, whole.row_changes);
        assert_eq!(sources, whole.row_sources);
        assert!(
            !core.resolve_cursors.is_open(txn),
            "the last part closes the cursor"
        );
    }

    /// A rollback drops the overlay and the open cursor with it. A later
    /// part is refused.
    #[test]
    fn a_rollback_drops_the_resolve_cursor() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(82);
        stage_spilled_kv(&mut core, txn);
        let first = read_part(&mut core, txn, 0, 1024).expect("part 0");
        assert!(!first.done);
        assert!(core.resolve_cursors.is_open(txn));

        // The release `DropTxnOverlay` runs on rollback.
        core.drop_overlay_entry(txn).expect("drop overlay");
        assert!(!core.resolve_cursors.is_open(txn));
        assert!(read_part(&mut core, txn, 1, 1024).is_err());
    }

    /// A repeated part 0 resolves again from the start.
    #[test]
    fn a_repeated_part_zero_reopens_the_cursor() {
        let (mut core, _dir) = make_core();
        let txn = TxnId::new(83);
        stage_spilled_kv(&mut core, txn);
        let first = read_part(&mut core, txn, 0, 1024).expect("part 0");
        let _ = read_part(&mut core, txn, 1, 1024).expect("part 1");
        let again = read_part(&mut core, txn, 0, 1024).expect("part 0 again");
        assert_eq!(again.bytes, first.bytes);
        assert!(
            read_part(&mut core, txn, 2, 1024).is_err(),
            "part 1 is next"
        );
        assert!(read_part(&mut core, txn, 1, 1024).is_ok());
    }
}
