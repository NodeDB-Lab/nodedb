// SPDX-License-Identifier: BUSL-1.1

//! The sync idempotency-gate checkpoint load path: decode the published state
//! file whole and install both gate maps before WAL replay merges on top.

use tracing::info;

use super::format::{SYNC_HWM_CKPT_FORMAT_VERSION, SyncHwmCheckpointFile};
use super::paths::{sync_hwm_ckpt_dir, sync_hwm_ckpt_state_path};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::Lsn;

impl CoreLoop {
    /// Load the sync gate from disk on startup, BEFORE WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/sync-hwm-ckpt/core-{core_id}/`) — the gate maps are per-core
    /// state and were never shared.
    ///
    /// Replay then MERGES the `SyncSeqAdvance` records above this state into the
    /// restored maps with the same max-wins rule that built them, so no floor is
    /// needed: a record already folded in re-folds to the same value. The maps
    /// this installs are exactly what a truncated WAL can no longer rebuild.
    pub fn load_sync_hwm_checkpoint(&mut self) -> crate::Result<()> {
        let ckpt_dir = sync_hwm_ckpt_dir(&self.data_dir, self.core_id);
        let path = sync_hwm_ckpt_state_path(&ckpt_dir);
        if !path.exists() {
            return Ok(());
        }

        // A present-but-corrupt checkpoint is fail-stop, not skip-and-replay:
        // the WAL below this generation's durable LSN may already be gone, so
        // silently restoring nothing here would boot with a gate this build
        // can never recover.
        let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
            CheckpointDecodeError::ReadFile {
                path: path.clone(),
                source,
            }
        })?;
        let file = zerompk::from_msgpack::<SyncHwmCheckpointFile>(&bytes).map_err(|source| {
            CheckpointDecodeError::MsgpackDecode {
                path: path.clone(),
                source,
            }
        })?;
        if file.format_version != SYNC_HWM_CKPT_FORMAT_VERSION {
            return Err(CheckpointDecodeError::FormatVersion {
                path: path.clone(),
                found: file.format_version,
                expected: SYNC_HWM_CKPT_FORMAT_VERSION,
            }
            .into());
        }

        let streams = file.hwm.len();
        let producers = file.epoch_floor.len();
        for (producer_id, stream_id, seq) in file.hwm {
            self.sync_hwm.insert((producer_id, stream_id), seq);
        }
        for (producer_id, epoch) in file.epoch_floor {
            self.producer_epoch_floor.insert(producer_id, epoch);
        }
        self.floors.sync_hwm_durable_lsn = Lsn::new(file.durable_through_lsn);

        info!(
            core = self.core_id,
            streams,
            producers,
            durable_through_lsn = file.durable_through_lsn,
            "sync HWM checkpoint restored"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_types::OrdinalClock;
    use nodedb_types::sync::wire::SyncProvenance;
    use tempfile::TempDir;

    use super::*;
    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::data::executor::sync_gate::SyncAdmit;

    fn make_prov(producer_id: u64, epoch: u64, stream_id: u64, seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id,
            epoch,
            stream_id,
            seq,
        }
    }

    /// A core rooted at `dir`, so two cores in one test share a data dir the way
    /// a restart does: the second reads exactly what the first wrote.
    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        let hlc = Arc::new(OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in these tests
        CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir,
            hlc,
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("CoreLoop::open")
    }

    /// The whole point of this checkpoint: after a restart whose WAL no longer
    /// carries the producer's `SyncSeqAdvance` records, a frame the producer
    /// re-sends must STILL be rejected as a duplicate rather than applied twice.
    ///
    /// Drives the real write path, then the real load path on a SECOND core over
    /// the same data dir — the second core's gate starts empty, exactly as it
    /// does after truncation, so only the restore can make these assertions hold.
    #[test]
    fn restored_gate_still_rejects_a_duplicate() {
        let dir = TempDir::new().expect("tempdir");

        // A core that has applied and acknowledged frame seq 1 from producer 1
        // at epoch 3, on two streams.
        let mut before = open_core_at(dir.path());
        let applied = make_prov(1, 3, 5, 1);
        assert_eq!(before.sync_admit(&applied), SyncAdmit::Apply);
        before.sync_commit(&applied);
        let other_stream = make_prov(1, 3, 6, 1);
        assert_eq!(before.sync_admit(&other_stream), SyncAdmit::Apply);
        before.sync_commit(&other_stream);
        before.advance_watermark(Lsn::new(900));
        before
            .floors
            .applied_prefix
            .observe_outcome_floor(Lsn::new(900));

        let reported = before
            .checkpoint_sync_hwm()
            .expect("flush to a writable dir must succeed");
        assert_eq!(
            reported,
            Lsn::new(900),
            "the flush must report exactly the LSN it made durable — the manager \
             deletes WAL segments below whatever this returns"
        );

        // Released before the next core opens: a core owns its data dir's redb
        // exclusively, so a restart is modelled by dropping this one first.
        drop(before);

        // The restart WITHOUT the restore, first — proving the assertions below
        // are load-bearing and not merely true of any fresh core.
        let mut unrestored = open_core_at(dir.path());
        assert!(
            unrestored.sync_hwm.is_empty(),
            "a fresh core's gate must start empty, or this test proves nothing"
        );
        assert_eq!(
            unrestored.sync_admit(&make_prov(1, 3, 5, 1)),
            SyncAdmit::Apply,
            "before the restore the already-applied frame IS re-admitted — this is \
             the duplicate the checkpoint exists to prevent"
        );
        drop(unrestored);

        let mut after = open_core_at(dir.path());
        after
            .load_sync_hwm_checkpoint()
            .expect("valid checkpoint must load");

        assert_eq!(
            after.sync_admit(&make_prov(1, 3, 5, 1)),
            SyncAdmit::Duplicate,
            "the frame at the restored high-watermark must not be applied again"
        );
        assert_eq!(
            after.sync_admit(&make_prov(1, 3, 6, 1)),
            SyncAdmit::Duplicate,
            "every stream's high-watermark must restore, not just the first"
        );
        assert_eq!(
            after.sync_admit(&make_prov(1, 3, 5, 2)),
            SyncAdmit::Apply,
            "the next frame in sequence must still be admitted"
        );
        assert_eq!(
            after.sync_admit(&make_prov(1, 2, 5, 2)),
            SyncAdmit::Fenced,
            "the restored epoch floor must still fence a stale producer generation"
        );
        assert_eq!(
            after.floors.sync_hwm_durable_lsn,
            Lsn::new(900),
            "the restored durable LSN is what a failed flush clamps to; losing it \
             would pin truncation at zero for the rest of the process"
        );
    }

    /// Restoring must not resurrect a high-watermark the WAL has since moved
    /// past: replay merges max-wins over the restored maps, so a later record
    /// still wins and an earlier one changes nothing.
    #[test]
    fn replay_merges_over_the_restored_gate_max_wins() {
        let dir = TempDir::new().expect("tempdir");

        let mut before = open_core_at(dir.path());
        before.sync_commit(&make_prov(1, 3, 5, 42));
        before.checkpoint_sync_hwm().expect("flush");
        drop(before); // a core owns its data dir's redb exclusively

        let mut after = open_core_at(dir.path());
        after
            .load_sync_hwm_checkpoint()
            .expect("valid checkpoint must load");

        // A replayed record above the restored state advances it.
        let mut maps = crate::wal::replay::SyncHwmReplayMaps::default();
        maps.sync_hwm.insert((1, 5), 50);
        maps.producer_epoch_floor.insert(1, 3);
        after.install_sync_hwm_maps(maps);
        assert_eq!(after.sync_hwm_value(1, 5), 50, "a later record must win");

        // A replayed record below it must NOT drag it backwards — that would
        // re-open the duplicate window the restore just closed.
        let mut stale = crate::wal::replay::SyncHwmReplayMaps::default();
        stale.sync_hwm.insert((1, 5), 10);
        after.install_sync_hwm_maps(stale);
        assert_eq!(
            after.sync_hwm_value(1, 5),
            50,
            "an earlier record must never lower the high-watermark"
        );
    }

    /// An absent checkpoint must leave the gate untouched and claim nothing, so
    /// a first boot falls back to a full WAL replay rather than a zeroed LSN
    /// that looks like a real durability claim.
    #[test]
    fn absent_checkpoint_restores_nothing() {
        let dir = TempDir::new().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_sync_hwm_checkpoint()
            .expect("an absent checkpoint is a legitimate no-op, not an error");
        assert!(core.sync_hwm.is_empty());
        assert!(core.producer_epoch_floor.is_empty());
        assert_eq!(core.floors.sync_hwm_durable_lsn, Lsn::ZERO);
    }

    /// A file from a future format must be refused, not misparsed: a
    /// high-watermark read too high would silently discard new sync writes as
    /// already-seen. It must now fail-stop the boot rather than silently
    /// restore nothing, because the WAL below this generation's durable LSN
    /// may already be gone.
    #[test]
    fn unknown_version_is_fail_stop() {
        let dir = TempDir::new().expect("tempdir");
        let ckpt_dir = sync_hwm_ckpt_dir(dir.path(), 0);
        std::fs::create_dir_all(&ckpt_dir).expect("mkdir");
        let file = SyncHwmCheckpointFile {
            format_version: SYNC_HWM_CKPT_FORMAT_VERSION + 1,
            durable_through_lsn: 5,
            hwm: vec![(1, 5, 42)],
            epoch_floor: vec![(1, 3)],
        };
        let bytes = zerompk::to_msgpack_vec(&file).expect("encode");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "STATE", &bytes).expect("write");

        let mut core = open_core_at(dir.path());
        assert!(
            core.load_sync_hwm_checkpoint().is_err(),
            "a file this build cannot read must abort the load, not restore nothing"
        );
    }

    /// A corrupt (non-MessagePack) checkpoint body must also fail-stop the
    /// boot — the file exists and the frame-level checksum passes, but the
    /// payload does not decode.
    #[test]
    fn corrupt_msgpack_body_is_fail_stop() {
        let dir = TempDir::new().expect("tempdir");
        let ckpt_dir = sync_hwm_ckpt_dir(dir.path(), 0);
        std::fs::create_dir_all(&ckpt_dir).expect("mkdir");
        nodedb_wal::segment::write_checkpoint_framed(&ckpt_dir, "STATE", b"not valid msgpack")
            .expect("write");

        let mut core = open_core_at(dir.path());
        assert!(
            core.load_sync_hwm_checkpoint().is_err(),
            "an undecodable checkpoint body must abort the load, not restore nothing"
        );
    }
}
