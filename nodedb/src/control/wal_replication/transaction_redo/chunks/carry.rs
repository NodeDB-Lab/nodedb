// SPDX-License-Identifier: BUSL-1.1

//! Carry a group's open streams in its Raft snapshot.
//!
//! A follower that installs a snapshot cut between a stream's chunks and its
//! final entry never applies those chunks. The snapshot carries every open
//! stream of the group, so the final entry finds the same stream on every
//! replica. The store holds only where each chunk sits, so the capture reads
//! the chunks' bytes back from this node's WAL on a blocking thread.

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::{NO_APPLY_KEY, RedoChunkPlacement};
use crate::wal::{CarriedRedoStream, RedoChunkHeader, RedoChunkRef, RedoStreamRef, chunk_crc};

use super::error::RedoChunkError;
use super::store::RedoChunkStore;
use super::stream::OpenStream;

impl RedoChunkStore {
    /// Every open stream of `group_id`, for the group's snapshot. The caller
    /// holds the group's apply fence, so the references read here are the
    /// streams at the cut. Each stream's floor hold keeps its records while
    /// the bytes are read back.
    pub async fn capture_group(
        &self,
        group_id: u64,
    ) -> Result<Vec<CarriedRedoStream>, RedoChunkError> {
        let open: Vec<(u64, RedoStreamRef)> = self
            .lock()
            .streams
            .values()
            .filter(|open| open.group_id == group_id)
            .map(|open| (open.term, open.reference()))
            .collect();
        // The stream the read's own failure names: the group's first.
        let Some(first) = open.first().map(|(_, reference)| reference.stream) else {
            return Ok(Vec::new());
        };
        let source = self.wal.read_source();
        tokio::task::spawn_blocking(move || {
            open.into_iter()
                .map(|(term, reference)| {
                    let chunks =
                        crate::wal::manager::stream_read::read_redo_stream(&source, &reference)
                            .map_err(|e| RedoChunkError::storage(reference.stream, e))?;
                    Ok(CarriedRedoStream {
                        stream: reference.stream,
                        term,
                        len: reference.len,
                        chunks,
                    })
                })
                .collect()
        })
        .await
        .map_err(|e| {
            RedoChunkError::storage(
                first,
                crate::Error::Internal {
                    detail: format!("the redo stream capture thread stopped: {e}"),
                },
            )
        })?
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
            let mut refs = Vec::with_capacity(stream.chunks.len());
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
                refs.push(RedoChunkRef {
                    index,
                    first_lsn: lsns.first.as_u64(),
                    last_lsn: lsns.last.as_u64(),
                    len: bytes.len() as u64,
                    crc: chunk_crc(bytes),
                });
            }
            if let Some(first) = first {
                installed.push((stream, refs, first));
            }
        }
        if let Some((stream, lsn)) = last {
            self.wal
                .wait_durable(lsn)
                .await
                .map_err(|e| RedoChunkError::storage(stream, e))?;
        }
        let mut state = self.lock();
        for (carried, refs, first) in installed {
            let held = refs.iter().map(|chunk| chunk.len).sum();
            let hold = self.wal.floor_holds().hold(first);
            state.streams.insert(
                carried.stream,
                OpenStream {
                    stream: carried.stream,
                    group_id,
                    term: carried.term,
                    len: carried.len,
                    chunks: refs,
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
    use super::super::store::tests::{chunk, read_back, session, wal};
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
        let carried = leader.capture_group(1).await.expect("capture");
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
        assert_eq!(
            read_back(&follower_wal, &open, 2, 5).expect("whole"),
            b"abcde".to_vec()
        );
        let live = follower
            .take_for_final(&session(1))
            .expect("installed stream");
        assert_eq!(
            read_back(&follower_wal, &live, 2, 5).expect("whole"),
            b"abcde".to_vec()
        );
    }
}
