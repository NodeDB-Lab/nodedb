// SPDX-License-Identifier: BUSL-1.1

//! Carry a group's open streams in its Raft snapshot.
//!
//! A follower that installs a snapshot cut between a stream's chunks and its
//! final entry never applies those chunks. The snapshot carries every open
//! stream of the group, so the final entry finds the same stream on every
//! replica.

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, RedoChunkPlacement};
use crate::wal::{CarriedRedoStream, RedoChunkHeader};

use super::error::RedoChunkError;
use super::store::RedoChunkStore;
use super::stream::OpenStream;

impl RedoChunkStore {
    /// Every open stream of `group_id`, for the group's snapshot. The caller
    /// holds the group's apply fence.
    pub fn capture_group(&self, group_id: u64) -> Vec<CarriedRedoStream> {
        self.lock()
            .streams
            .values()
            .filter(|open| open.group_id == group_id)
            .map(|open| CarriedRedoStream {
                stream: open.stream,
                term: open.term,
                len: open.len,
                chunks: open.chunks.clone(),
            })
            .collect()
    }

    /// Replace the open and parked streams of `group_id` with `carried`,
    /// from the group's installed snapshot. The carried chunks are durable
    /// in this node's WAL before they count, so a restart rebuilds them.
    /// Runs after the install marker: boot drops the group's earlier streams
    /// there.
    pub async fn install_group(
        &self,
        group_id: u64,
        carried: Vec<CarriedRedoStream>,
    ) -> Result<(), RedoChunkError> {
        let (replaced, released) = {
            let mut state = self.lock();
            let streams = std::mem::take(&mut state.streams);
            let (replaced, kept): (Vec<_>, Vec<_>) = streams
                .into_iter()
                .partition(|(_, open)| open.group_id == group_id);
            state.streams = kept.into_iter().collect();
            // The snapshot covers every final entry a parked stream waits
            // for, so no re-apply needs its records.
            let parked = std::mem::take(&mut state.parked);
            let (released, kept): (Vec<_>, Vec<_>) = parked
                .into_iter()
                .partition(|parked| parked.group_id == group_id);
            state.parked = kept;
            self.note_open(&state);
            (replaced, released)
        };
        // The floor holds release outside the store lock.
        drop(replaced);
        drop(released);
        let mut installed = Vec::with_capacity(carried.len());
        let mut last = None;
        for stream in carried {
            let placement = RedoChunkPlacement {
                tenant_id: TenantId::new(0),
                vshard_id: VShardId::new(stream.stream.vshard()),
                database_id: DatabaseId::DEFAULT,
            };
            let mut first: Option<Lsn> = None;
            for (index, bytes) in (0u32..).zip(&stream.chunks) {
                let header = RedoChunkHeader {
                    stream: stream.stream,
                    group_id,
                    term: stream.term,
                    len: stream.len,
                    index,
                };
                let lsns = self
                    .wal
                    .append_redo_chunk(NO_APPLY_KEY, placement, header, bytes)
                    .map_err(|e| RedoChunkError::storage(stream.stream, e))?;
                first.get_or_insert(lsns.first);
                last = Some((stream.stream, lsns.last));
            }
            if let Some(first) = first {
                installed.push((stream, first));
            }
        }
        if let Some((stream, lsn)) = last {
            self.wal
                .wait_durable(lsn)
                .await
                .map_err(|e| RedoChunkError::storage(stream, e))?;
        }
        let mut state = self.lock();
        for (carried, first) in installed {
            let held = carried.chunks.iter().map(|chunk| chunk.len() as u64).sum();
            let hold = self.wal.floor_holds().hold(first);
            state.streams.insert(
                carried.stream,
                OpenStream {
                    stream: carried.stream,
                    group_id,
                    term: carried.term,
                    len: carried.len,
                    chunks: carried.chunks,
                    held,
                    hold,
                },
            );
        }
        self.note_open(&state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::store::tests::{chunk, session, wal};
    use super::super::store::{RedoChunkLimits, RedoChunkStore};

    fn limits() -> RedoChunkLimits {
        RedoChunkLimits {
            max_entry_bytes: 64 * 1024,
            max_open_bytes: 1 << 20,
        }
    }

    #[tokio::test]
    async fn a_carried_stream_replaces_the_followers_streams_and_survives_a_rebuild() {
        let leader_dir = tempfile::tempdir().expect("tempdir");
        let leader = RedoChunkStore::new(wal(&leader_dir), limits());
        leader
            .apply_chunk(chunk(session(1), 0, 5, b"abc"))
            .await
            .expect("chunk");
        leader
            .apply_chunk(chunk(session(1), 1, 5, b"de"))
            .await
            .expect("chunk");
        let carried = leader.capture_group(1);
        assert_eq!(carried.len(), 1);

        let follower_dir = tempfile::tempdir().expect("tempdir");
        let follower_wal = wal(&follower_dir);
        let follower = RedoChunkStore::new(std::sync::Arc::clone(&follower_wal), limits());
        follower
            .apply_chunk(chunk(session(9), 0, 2, b"zz"))
            .await
            .expect("stale chunk");
        follower_wal
            .appender(0)
            .append_snapshot_installed(1)
            .expect("install marker");
        follower.install_group(1, carried).await.expect("install");
        assert!(follower.take_for_final(&session(9)).is_none());

        let rebuilt = RedoChunkStore::new(std::sync::Arc::clone(&follower_wal), limits());
        rebuilt
            .rebuild(&follower_wal.replay().expect("replay"))
            .expect("rebuild");
        assert!(rebuilt.take_for_final(&session(9)).is_none());
        let open = rebuilt.take_for_final(&session(1)).expect("carried stream");
        assert_eq!(open.assemble(2, 5).expect("assembles"), b"abcde".to_vec());
        let live = follower
            .take_for_final(&session(1))
            .expect("installed stream");
        assert_eq!(live.assemble(2, 5).expect("assembles"), b"abcde".to_vec());
    }
}
