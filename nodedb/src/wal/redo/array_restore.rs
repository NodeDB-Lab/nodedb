// SPDX-License-Identifier: BUSL-1.1

//! Payload of restored array cells: a `RecordType::ArrayPut` sub-record of a
//! RESTORE batch's redo.
//!
//! A backup holds every version of an array cell with the system time the
//! source stamped. The sub-record carries every version with its stamp, so
//! every replica and every replay installs each version at exactly that
//! stamp, and a read at any cut answers as the source answered.
//!
//! A RESTORE is a write in the Calvin sequence. When the destination holds a
//! version or a delete of a coordinate above the backup's newest, the resolve
//! adds the backup's newest state once more, stamped above both, and the
//! sub-record carries that stamp too. The destination's later history stays
//! readable below it.
//!
//! The payload is the pair `("array_restore", ArrayRestoreRedo)`. A plain
//! `ArrayPut` payload opens with its format-version byte, never with a
//! two-element array, so the two shapes never decode as each other.

use nodedb_array::types::ArrayId;
use serde::{Deserialize, Serialize};

use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};

/// The discriminator a restored array's payload opens with.
pub const ARRAY_RESTORE: &str = "array_restore";

/// Every restored version of some cells of one array.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ArrayRestoreRedo {
    /// The destination array.
    pub array_id: ArrayId,
    /// The vShard the batch restores into. Every restored tile version is
    /// placed on it, on every replica and every replay.
    pub vshard_id: u32,
    /// Every live version. Each installs at its `system_from_ms` under the
    /// destination's surrogate of its coordinate.
    pub puts: Vec<ArrayPutCell>,
    /// Every tombstone and erasure. Each installs at its `system_from_ms`.
    pub deletes: Vec<ArrayDeleteCell>,
}

impl ArrayRestoreRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(ARRAY_RESTORE, self)).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal array restore of '{}': {e}", self.array_id.name),
        })
    }

    /// Decode a sub-record payload. `None` when it is no restored array.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == ARRAY_RESTORE).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::{Surrogate, TenantId};

    use super::*;
    use crate::engine::array::wal::{ArrayPutPayload, encode_put_with_version};

    fn sample() -> ArrayRestoreRedo {
        ArrayRestoreRedo {
            array_id: ArrayId::new(TenantId::new(1), "grid"),
            vshard_id: 12,
            puts: vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(1)],
                attrs: vec![CellValue::Int64(7)],
                surrogate: Surrogate::new(3),
                system_from_ms: 100,
                valid_from_ms: 0,
                valid_until_ms: i64::MAX,
            }],
            deletes: vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(2)],
                system_from_ms: 200,
                erasure: false,
            }],
        }
    }

    #[test]
    fn a_restored_array_round_trips() {
        let bytes = sample().encode().expect("encode");
        assert_eq!(ArrayRestoreRedo::decode(&bytes), Some(sample()));
    }

    #[test]
    fn a_plain_array_put_is_no_restored_array() {
        let restored = sample();
        let put = encode_put_with_version(&ArrayPutPayload {
            array_id: restored.array_id,
            vshard_id: restored.vshard_id,
            cells: restored.puts,
            provenance: None,
        })
        .expect("encode");
        assert!(ArrayRestoreRedo::decode(&put).is_none());
    }
}
