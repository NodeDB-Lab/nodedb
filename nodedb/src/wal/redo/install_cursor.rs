// SPDX-License-Identifier: BUSL-1.1

//! The durable position of a batched install of a stream marker.
//!
//! A batched install commits each batch durably, and the same commit stores
//! the install's cursor. A cursor in a core's sparse store means an install
//! of the record at its LSN made effects durable and did not end: its final
//! write set capture or its rollback end removes the cursor in the commit
//! that ends it. Boot reads the cursors before replay. Restart replay then
//! completes each record forward from op 0, and boot drops the cursors once
//! every arm replayed. A write set that boot replay stores for a folded
//! record keeps the cursor beside it until then.

use std::collections::BTreeMap;

use super::chunk::RedoStreamId;
use super::replay_arm::RedoArm;

/// Where one batched install stands.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct InstallCursor {
    /// The LSN of the marker's `TransactionRedo` WAL record.
    pub lsn: u64,
    /// The stream the marker names.
    pub stream: RedoStreamId,
    pub state: CursorState,
}

/// The last durable step of one batched install. Boot reads only that a
/// cursor exists and the stream it names: the arm, the batch and the op
/// count are diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(crate) enum CursorState {
    /// Batch `batch` of `arm` is durable, with every earlier batch of the
    /// arm and every batch of each earlier arm. `ops_through` counts the
    /// stream's ops through the batch's end.
    Installing {
        arm: RedoArm,
        batch: u32,
        ops_through: u64,
    },
    /// The rollback reversed every batch after batch `batch` of `arm`, and
    /// the reversal is durable. The state is the forward install through
    /// that batch.
    RollingBack { arm: RedoArm, batch: u32 },
}

impl InstallCursor {
    /// The stored form.
    pub(crate) fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo install cursor at lsn {} encode: {e}", self.lsn),
        })
    }

    /// Decode the cursor stored under `lsn`.
    pub(crate) fn from_bytes(lsn: u64, bytes: &[u8]) -> crate::Result<Self> {
        let cursor: Self = zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("the redo install cursor stored at lsn {lsn} does not decode: {e}"),
        })?;
        if cursor.lsn != lsn {
            return Err(crate::Error::Storage {
                engine: "sparse".into(),
                detail: format!(
                    "the redo install cursor stored at lsn {lsn} names lsn {}",
                    cursor.lsn
                ),
            });
        }
        Ok(cursor)
    }
}

/// The cursors a core's store held at boot, by LSN.
#[derive(Debug, Default)]
pub(crate) struct InstallCursors {
    by_lsn: BTreeMap<u64, InstallCursor>,
}

impl InstallCursors {
    pub(crate) fn new(cursors: impl IntoIterator<Item = InstallCursor>) -> Self {
        Self {
            by_lsn: cursors
                .into_iter()
                .map(|cursor| (cursor.lsn, cursor))
                .collect(),
        }
    }

    /// Whether an install of the record at `lsn` ended part way.
    pub(crate) fn contains(&self, lsn: u64) -> bool {
        self.by_lsn.contains_key(&lsn)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.by_lsn.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &InstallCursor> {
        self.by_lsn.values()
    }

    pub(crate) fn lsns(&self) -> Vec<u64> {
        self.by_lsn.keys().copied().collect()
    }
}

/// A batched install's cursor that boot cannot settle.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RedoInstallError {
    /// The install of the marker at `lsn` ended part way, and the marker's
    /// chunks lie below the oldest retained WAL segment. Replay cannot
    /// complete the record.
    #[error(
        "the batched install of the stream marker at lsn {lsn} ended part way, and its chunks \
         are not in the WAL: replay cannot complete the record"
    )]
    ChunksMissing { lsn: u64 },
    /// A cursor names a record the WAL does not hold.
    #[error(
        "a batched install cursor names the stream marker at lsn {lsn}, and the WAL holds no \
         such marker: the install's durable effects have no record"
    )]
    CursorWithoutRecord { lsn: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream() -> RedoStreamId {
        RedoStreamId::Session {
            vshard: 3,
            idempotency_key: 41,
        }
    }

    #[test]
    fn a_cursor_round_trips_in_each_state() {
        for state in [
            CursorState::Installing {
                arm: RedoArm::Document,
                batch: 4,
                ops_through: 77,
            },
            CursorState::RollingBack {
                arm: RedoArm::GraphNodeLabelRedo,
                batch: 0,
            },
        ] {
            let cursor = InstallCursor {
                lsn: 900,
                stream: stream(),
                state,
            };
            let bytes = cursor.to_bytes().expect("encode");
            assert_eq!(
                InstallCursor::from_bytes(900, &bytes).expect("decode"),
                cursor
            );
        }
    }

    #[test]
    fn a_cursor_stored_under_another_lsn_is_refused() {
        let cursor = InstallCursor {
            lsn: 900,
            stream: stream(),
            state: CursorState::RollingBack {
                arm: RedoArm::Kv,
                batch: 1,
            },
        };
        let bytes = cursor.to_bytes().expect("encode");
        assert!(InstallCursor::from_bytes(901, &bytes).is_err());
        assert!(InstallCursor::from_bytes(900, b"\x01").is_err());
    }
}
