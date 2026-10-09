// SPDX-License-Identifier: BUSL-1.1

//! The node's open chunked redo streams.
//!
//! Every replica decides each chunk and final entry from the log alone: the
//! chunk order, the stream term and the declared lengths. No replica refuses
//! an entry for its own memory. The group leader bounds memory instead: its
//! write gate admits a stream's first chunk only while this node's open and
//! admitted stream bytes, plus the new stream's length, stay within
//! `tuning.calvin.max_open_redo_bytes`. A node then holds at most that cap
//! per data group it hosts.
//!
//! A stream closes when its final entry takes it, when its proposer abandons
//! it, when its group applies an entry of a later term, when its Calvin
//! position applies through another copy, or when this node stops hosting
//! its group.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use nodedb_types::config::tuning::CalvinTuning;
use tokio::sync::Notify;

use crate::control::wal_replication::encode::RedoEntryTarget;
use crate::types::Lsn;
use crate::wal::manager::{RedoChunkPlacement, WalFloorHold};
use crate::wal::{RedoChunkHeader, RedoStreamId, WalManager};

use super::error::RedoChunkError;
use super::stream::OpenStream;

/// The size bounds of chunked redo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedoChunkLimits {
    /// Largest encoded redo entry a proposer puts in a data-group log.
    pub max_entry_bytes: usize,
    /// Most bytes of open streams the leader admits on its node.
    pub max_open_bytes: u64,
}

impl RedoChunkLimits {
    pub fn from_tuning(tuning: &CalvinTuning) -> Self {
        Self {
            max_entry_bytes: tuning.max_redo_entry_bytes,
            max_open_bytes: tuning.max_open_redo_bytes,
        }
    }
}

/// A first chunk the leader admitted that has not applied yet.
#[derive(Debug, Clone, Copy)]
pub(super) struct Pending {
    pub(super) len: u64,
    /// The proposer's deadline. Past it the admission counts no more.
    pub(super) until: Instant,
}

/// The floor of a stream whose final entry failed on this node.
pub(super) struct ParkedStream {
    pub(super) group_id: u64,
    /// Keeps the floor until the stream drops.
    pub(super) _hold: WalFloorHold,
}

#[derive(Default)]
pub(super) struct StoreState {
    pub(super) streams: BTreeMap<RedoStreamId, OpenStream>,
    pub(super) pending: HashMap<RedoStreamId, Pending>,
    /// Streams whose final entry failed on this node. The next boot rebuilds
    /// them from the records their floors keep.
    pub(super) parked: Vec<ParkedStream>,
    /// Session streams whose abandon proposal failed, and the target the
    /// abandon entry goes to. The abandoner retries each while it is open.
    pub(super) abandons: BTreeMap<RedoStreamId, RedoEntryTarget>,
}

/// One committed chunk entry, as the apply loop hands it over.
#[derive(Debug, Clone)]
pub struct ChunkApply {
    pub group_id: u64,
    /// The Raft term of the chunk entry.
    pub term: u64,
    /// The chunk entry's idempotency key.
    pub apply_key: u64,
    pub placement: RedoChunkPlacement,
    pub stream: RedoStreamId,
    pub index: u32,
    /// The byte length of the whole stream.
    pub len: u64,
    pub bytes: Vec<u8>,
}

/// The node's open chunked redo streams.
pub struct RedoChunkStore {
    pub(super) wal: Arc<WalManager>,
    limits: RedoChunkLimits,
    pub(super) state: Mutex<StoreState>,
    /// How many streams are open: the per-entry term check skips the lock
    /// when none is.
    open_count: AtomicUsize,
    /// Wakes the abandoner when a failed abandon is queued.
    pub(super) abandon_wake: Arc<Notify>,
    /// Whether an abandoner task runs for this store.
    pub(super) abandoner: AtomicBool,
    /// The data groups whose replica here owes a snapshot install (see
    /// [`super::owed`]). A leaf lock, apart from `state`.
    pub(super) owed: Mutex<BTreeSet<u64>>,
}

impl RedoChunkStore {
    pub fn new(wal: Arc<WalManager>, limits: RedoChunkLimits) -> Self {
        Self {
            wal,
            limits,
            state: Mutex::new(StoreState::default()),
            open_count: AtomicUsize::new(0),
            abandon_wake: Arc::new(Notify::new()),
            abandoner: AtomicBool::new(false),
            owed: Mutex::new(BTreeSet::new()),
        }
    }

    pub fn limits(&self) -> RedoChunkLimits {
        self.limits
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record how many streams `state` holds open.
    pub(super) fn note_open(&self, state: &StoreState) {
        self.open_count
            .store(state.streams.len(), Ordering::Release);
    }

    /// How many streams are open.
    pub fn open_streams(&self) -> usize {
        self.open_count.load(Ordering::Acquire)
    }

    /// The declared bytes of every open stream.
    pub fn open_bytes(&self) -> u64 {
        self.lock().streams.values().map(|open| open.len).sum()
    }

    /// Admit the first chunk of `stream`, declaring `len` bytes, on this
    /// group leader. The admission counts until the chunk applies here or
    /// `until` passes. A re-proposal of an admitted or open stream passes.
    pub fn admit_stream(
        &self,
        stream: RedoStreamId,
        len: u64,
        until: Instant,
    ) -> Result<(), RedoChunkError> {
        let now = Instant::now();
        let mut state = self.lock();
        state.pending.retain(|_, pending| pending.until > now);
        if state.streams.contains_key(&stream) {
            return Ok(());
        }
        if let Some(pending) = state.pending.get_mut(&stream) {
            pending.until = pending.until.max(until);
            return Ok(());
        }
        let open = state.streams.values().map(|open| open.len).sum::<u64>()
            + state
                .pending
                .values()
                .map(|pending| pending.len)
                .sum::<u64>();
        let cap = self.limits.max_open_bytes;
        if open.saturating_add(len) > cap {
            return Err(RedoChunkError::StoreFull {
                stream,
                open,
                len,
                cap,
            });
        }
        state.pending.insert(stream, Pending { len, until });
        Ok(())
    }

    /// Apply one chunk: check it against its stream, append its records
    /// keyed with the entry's key, wait until they are durable, then hold it.
    pub async fn apply_chunk(&self, chunk: ChunkApply) -> Result<(), RedoChunkError> {
        self.check_chunk(&chunk)?;
        let header = RedoChunkHeader {
            stream: chunk.stream,
            group_id: chunk.group_id,
            term: chunk.term,
            len: chunk.len,
            index: chunk.index,
        };
        let lsns = self
            .wal
            .append_redo_chunk(chunk.apply_key, chunk.placement, header, &chunk.bytes)
            .map_err(|e| RedoChunkError::storage(chunk.stream, e))?;
        self.wal
            .wait_durable(lsns.last)
            .await
            .map_err(|e| RedoChunkError::storage(chunk.stream, e))?;
        self.hold_chunk(chunk, lsns.first);
        Ok(())
    }

    /// Refuse a chunk the stream cannot take next.
    fn check_chunk(&self, chunk: &ChunkApply) -> Result<(), RedoChunkError> {
        let stream = chunk.stream;
        let bytes = chunk.bytes.len() as u64;
        let state = self.lock();
        let Some(open) = state.streams.get(&stream) else {
            if chunk.index != 0 {
                return Err(RedoChunkError::NotOpen { stream });
            }
            if bytes > chunk.len {
                return Err(RedoChunkError::Overflow {
                    stream,
                    held: bytes,
                    len: chunk.len,
                });
            }
            return Ok(());
        };
        let expected = u32::try_from(open.chunks.len()).unwrap_or(u32::MAX);
        if chunk.index != expected {
            return Err(RedoChunkError::OutOfOrder {
                stream,
                index: chunk.index,
                expected,
            });
        }
        if open.group_id != chunk.group_id || open.term != chunk.term {
            return Err(RedoChunkError::TermChanged {
                stream,
                term: chunk.term,
                open_term: open.term,
            });
        }
        if open.len != chunk.len {
            return Err(RedoChunkError::LengthChanged {
                stream,
                declared: chunk.len,
                len: open.len,
            });
        }
        if open.held.saturating_add(bytes) > open.len {
            return Err(RedoChunkError::Overflow {
                stream,
                held: open.held.saturating_add(bytes),
                len: open.len,
            });
        }
        Ok(())
    }

    /// Hold a durable chunk whose first record is at `first`.
    fn hold_chunk(&self, chunk: ChunkApply, first: Lsn) {
        let mut state = self.lock();
        let held = chunk.bytes.len() as u64;
        match state.streams.get_mut(&chunk.stream) {
            Some(open) => {
                open.held += held;
                open.chunks.push(chunk.bytes);
            }
            None => {
                state.pending.remove(&chunk.stream);
                let hold = self.wal.floor_holds().hold(first);
                state.streams.insert(
                    chunk.stream,
                    OpenStream {
                        stream: chunk.stream,
                        group_id: chunk.group_id,
                        term: chunk.term,
                        len: chunk.len,
                        chunks: vec![chunk.bytes],
                        held,
                        hold,
                    },
                );
            }
        }
        self.note_open(&state);
    }

    /// Take `stream` for its final entry. The final entry decides it from
    /// the log, so it leaves the store whatever the final entry's outcome.
    pub fn take_for_final(&self, stream: &RedoStreamId) -> Option<OpenStream> {
        let mut state = self.lock();
        let taken = state.streams.remove(stream);
        self.note_open(&state);
        taken
    }

    /// Keep the records of a taken stream whose final entry failed on this
    /// node, so the next boot rebuilds the stream for the entry's re-apply.
    pub fn park(&self, open: OpenStream) {
        let group_id = open.group_id;
        let hold = open.into_hold();
        self.lock().parked.push(ParkedStream {
            group_id,
            _hold: hold,
        });
    }

    /// Drop `stream` for its abandon entry, and make the drop durable under
    /// the entry's key.
    pub async fn abandon(
        &self,
        stream: RedoStreamId,
        apply_key: u64,
        placement: RedoChunkPlacement,
    ) -> Result<(), RedoChunkError> {
        {
            let mut state = self.lock();
            state.streams.remove(&stream);
            state.pending.remove(&stream);
            self.note_open(&state);
        }
        let lsn = self
            .wal
            .append_redo_stream_closed(apply_key, placement, stream)
            .map_err(|e| RedoChunkError::storage(stream, e))?;
        self.wal
            .wait_durable(lsn)
            .await
            .map_err(|e| RedoChunkError::storage(stream, e))
    }

    /// Drop every stream of `group_id` opened in a term before `term`: the
    /// group applies an entry of `term`, so no earlier term adds an entry.
    pub fn end_terms_before(&self, group_id: u64, term: u64) {
        if self.open_streams() == 0 {
            return;
        }
        let mut state = self.lock();
        state
            .streams
            .retain(|_, open| open.group_id != group_id || open.term >= term);
        self.note_open(&state);
    }

    /// Drop every stream of the Calvin position `(epoch, position)` of
    /// `vshard`. The position applied through one copy of its redo, so no
    /// other attempt's stream installs. The Calvin apply path calls it once
    /// the position's install is durable.
    pub fn drop_calvin_position(&self, vshard: u32, epoch: u64, position: u32) {
        if self.open_streams() == 0 {
            return;
        }
        let mut state = self.lock();
        state.streams.retain(|stream, _| {
            !matches!(
                stream,
                RedoStreamId::Calvin { vshard: v, epoch: e, position: p, .. }
                    if *v == vshard && *e == epoch && *p == position
            )
        });
        self.note_open(&state);
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::types::{DatabaseId, TenantId, VShardId};

    pub(in super::super) fn wal(dir: &tempfile::TempDir) -> Arc<WalManager> {
        Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("open wal"))
    }

    pub(in super::super) fn session(key: u64) -> RedoStreamId {
        RedoStreamId::Session {
            vshard: 2,
            idempotency_key: key,
        }
    }

    pub(in super::super) fn chunk(
        stream: RedoStreamId,
        index: u32,
        len: u64,
        bytes: &[u8],
    ) -> ChunkApply {
        ChunkApply {
            group_id: 1,
            term: 3,
            apply_key: 100 + u64::from(index),
            placement: RedoChunkPlacement {
                tenant_id: TenantId::new(1),
                vshard_id: VShardId::new(2),
                database_id: DatabaseId::DEFAULT,
            },
            stream,
            index,
            len,
            bytes: bytes.to_vec(),
        }
    }

    fn limits(max_open_bytes: u64) -> RedoChunkLimits {
        RedoChunkLimits {
            max_entry_bytes: 64 * 1024,
            max_open_bytes,
        }
    }

    #[tokio::test]
    async fn chunks_reassemble_in_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(wal(&dir), limits(1 << 20));
        let stream = session(7);
        for (index, part) in [&b"abc"[..], b"def", b"g"].into_iter().enumerate() {
            store
                .apply_chunk(chunk(stream, index as u32, 7, part))
                .await
                .expect("chunk applies");
        }
        assert_eq!(store.open_streams(), 1);
        assert_eq!(store.open_bytes(), 7);
        let open = store.take_for_final(&stream).expect("open stream");
        assert_eq!(open.assemble(3, 7).expect("assembles"), b"abcdefg".to_vec());
        assert_eq!(store.open_streams(), 0);
        // The floor holds the stream's first record until the stream drops.
        assert!(store.wal.floor_holds().lowest().is_some());
        drop(open);
        assert_eq!(store.wal.floor_holds().lowest(), None);
    }

    #[tokio::test]
    async fn a_missing_chunk_refuses_the_final_entry() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(wal(&dir), limits(1 << 20));
        let stream = session(8);
        store
            .apply_chunk(chunk(stream, 0, 6, b"abc"))
            .await
            .expect("first chunk");
        let skipped = store.apply_chunk(chunk(stream, 2, 6, b"ghi")).await;
        assert!(matches!(
            skipped,
            Err(RedoChunkError::OutOfOrder {
                index: 2,
                expected: 1,
                ..
            })
        ));
        let open = store.take_for_final(&stream).expect("open stream");
        let refused = open.assemble(2, 6);
        assert!(matches!(refused, Err(RedoChunkError::Mismatch { .. })));
        let error = crate::Error::from(refused.expect_err("refused"));
        assert!(matches!(
            error,
            crate::Error::DataPlane(crate::bridge::envelope::ErrorCode::RetryableRefusal { .. })
        ));
        // A later chunk of the taken stream finds no stream.
        assert!(matches!(
            store.apply_chunk(chunk(stream, 1, 6, b"def")).await,
            Err(RedoChunkError::NotOpen { .. })
        ));
    }

    #[tokio::test]
    async fn a_new_stream_past_the_open_byte_cap_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(wal(&dir), limits(10));
        let until = Instant::now() + std::time::Duration::from_secs(60);
        store
            .admit_stream(session(1), 6, until)
            .expect("first stream fits");
        // A re-proposal of an admitted stream passes.
        store
            .admit_stream(session(1), 6, until)
            .expect("re-proposal passes");
        let refused = store.admit_stream(session(2), 5, until);
        assert!(matches!(
            refused,
            Err(RedoChunkError::StoreFull {
                open: 6,
                len: 5,
                cap: 10,
                ..
            })
        ));
        // The admitted stream applies, then finishes, and frees its bytes.
        store
            .apply_chunk(chunk(session(1), 0, 6, b"abcdef"))
            .await
            .expect("chunk");
        assert!(store.admit_stream(session(2), 5, until).is_err());
        drop(store.take_for_final(&session(1)));
        store
            .admit_stream(session(2), 5, until)
            .expect("room after the first stream closed");
        // An admission past its deadline counts no more.
        let other_dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(wal(&other_dir), limits(10));
        store
            .admit_stream(session(3), 10, Instant::now())
            .expect("fits");
        store
            .admit_stream(session(4), 10, until)
            .expect("the expired admission frees its bytes");
    }

    #[tokio::test]
    async fn a_later_term_drops_the_groups_earlier_streams() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(wal(&dir), limits(1 << 20));
        store
            .apply_chunk(chunk(session(1), 0, 4, b"ab"))
            .await
            .expect("chunk");
        let mut other_group = chunk(session(2), 0, 4, b"ab");
        other_group.group_id = 9;
        store.apply_chunk(other_group).await.expect("chunk");
        store.end_terms_before(1, 3);
        assert_eq!(store.open_streams(), 2);
        store.end_terms_before(1, 4);
        assert_eq!(store.open_streams(), 1);
        assert!(store.take_for_final(&session(2)).is_some());
    }

    #[tokio::test]
    async fn an_applied_calvin_position_drops_every_attempt() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedoChunkStore::new(wal(&dir), limits(1 << 20));
        for attempt in 0..2 {
            let stream = RedoStreamId::Calvin {
                vshard: 2,
                epoch: 5,
                position: 1,
                attempt,
            };
            store
                .apply_chunk(chunk(stream, 0, 2, b"ab"))
                .await
                .expect("chunk");
        }
        store.drop_calvin_position(2, 5, 1);
        assert_eq!(store.open_streams(), 0);
    }
}
