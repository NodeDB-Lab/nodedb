// SPDX-License-Identifier: BUSL-1.1

//! Replicated positions of applied writes, and the WAL payload that keeps a
//! Raft entry's position durable on each replica.

/// Where a committed data-group entry sits in its group's Raft log. Every
/// replica that applies the entry sees the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplicatedPosition {
    /// The epoch at which the entry's vShard moved to `group_id`.
    pub epoch: u64,
    pub group_id: u64,
    pub log_index: u64,
}

/// Byte length of an encoded [`ChangePositionMarker`].
pub const MARKER_LEN: usize = 32;

/// Payload of a `ChangePosition` WAL record: the entry's proposal key, and
/// the entry's position. The records the entry's apply writes carry the same
/// key in their headers, which is how recovery links them to the position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangePositionMarker {
    pub apply_key: u64,
    pub position: ReplicatedPosition,
}

impl ChangePositionMarker {
    pub fn to_bytes(&self) -> [u8; MARKER_LEN] {
        let mut out = [0u8; MARKER_LEN];
        out[0..8].copy_from_slice(&self.apply_key.to_le_bytes());
        out[8..16].copy_from_slice(&self.position.group_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.position.log_index.to_le_bytes());
        out[24..32].copy_from_slice(&self.position.epoch.to_le_bytes());
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MarkerDecodeError> {
        let bytes: &[u8; MARKER_LEN] = bytes
            .try_into()
            .map_err(|_| MarkerDecodeError { len: bytes.len() })?;
        let word = |at: usize| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[at..at + 8]);
            u64::from_le_bytes(buf)
        };
        Ok(Self {
            apply_key: word(0),
            position: ReplicatedPosition {
                group_id: word(8),
                log_index: word(16),
                epoch: word(24),
            },
        })
    }
}

/// A `ChangePosition` payload of the wrong length.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("ChangePosition payload is {len} bytes, expected {MARKER_LEN}")]
pub struct MarkerDecodeError {
    pub len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_round_trips() {
        let marker = ChangePositionMarker {
            apply_key: 0xDEAD_BEEF,
            position: ReplicatedPosition {
                epoch: 3,
                group_id: 7,
                log_index: 1_234,
            },
        };
        assert_eq!(
            ChangePositionMarker::from_bytes(&marker.to_bytes()),
            Ok(marker)
        );
    }

    #[test]
    fn a_short_payload_is_refused() {
        assert_eq!(
            ChangePositionMarker::from_bytes(&[0u8; 24]),
            Err(MarkerDecodeError { len: 24 })
        );
    }
}
