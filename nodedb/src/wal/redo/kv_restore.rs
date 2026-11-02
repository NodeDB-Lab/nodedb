// SPDX-License-Identifier: BUSL-1.1

//! Payload of a restored KV key: a `RecordType::Put` sub-record of a RESTORE
//! batch's redo.
//!
//! A backup holds every version of a KV key with the ordinals the source
//! stamped: the live row with the ordinal its value was set at, and every
//! ended version with its closed-open interval. The sub-record carries all of
//! them, so every replica and every replay installs each version at exactly
//! its ordinal, and a read at any cut answers as the source answered.
//!
//! A RESTORE is a write in the Calvin sequence. When the destination holds a
//! version or a delete of the key above the backup's newest, the resolve
//! names an ordinal above both in `reassert_at`, and the install sets the
//! backup's current state again there. The destination's later history
//! stays readable below it.
//!
//! The payload is the pair `("kv_restore", KvRestoreRedo)`. No other
//! document, KV or graph shape decodes a two-element array that opens with
//! this discriminator.

use serde::{Deserialize, Serialize};

/// The discriminator a restored KV key's payload opens with.
pub const KV_RESTORE: &str = "kv_restore";

/// The live row of a restored key.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct KvRestoreLive {
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms, `0` for no TTL.
    pub expire_at_ms: u64,
    /// Ordinal of the write that set the value.
    pub sys_from: i64,
}

/// One ended version of a restored key, visible at every ordinal `S` with
/// `sys_from <= S < sys_to`.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct KvRestoreVersion {
    pub sys_from: i64,
    pub sys_to: i64,
    pub value: Vec<u8>,
    /// Absolute expiry instant in ms the version held, `0` for none.
    pub expire_at_ms: u64,
}

/// Every version of one restored KV key.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct KvRestoreRedo {
    /// The collection, as the destination KV engine stores it.
    pub collection: String,
    pub key: Vec<u8>,
    /// The key's identity in the destination database. Every version
    /// installs under it.
    pub surrogate: u32,
    /// The live row. `None` when the key ends deleted.
    pub live: Option<KvRestoreLive>,
    /// Every ended version, oldest first.
    pub history: Vec<KvRestoreVersion>,
    /// The ordinal the backup's current state is set again at. The resolve
    /// sets it when the destination holds a version or a delete of the key
    /// above the backup's newest one: the restore then ends that state at
    /// this ordinal. `None` when the backup's newest version is the key's
    /// newest.
    pub reassert_at: Option<i64>,
    /// A clone copy of a source key: the live row installs only where the
    /// key holds no version, live or ended, so a write into the clone always
    /// wins over a copy. A clone copy publishes no change event: the key read
    /// the same version through the source before it.
    pub clone_copy: bool,
}

impl KvRestoreRedo {
    /// Encode the sub-record payload.
    pub fn encode(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(&(KV_RESTORE, self)).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal kv restore: {e}"),
        })
    }

    /// Decode a sub-record payload. `None` when it is no restored KV key.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        let (disc, redo) = zerompk::from_msgpack::<(String, Self)>(payload).ok()?;
        (disc == KV_RESTORE).then_some(redo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> KvRestoreRedo {
        KvRestoreRedo {
            collection: "sessions".into(),
            key: b"k".to_vec(),
            surrogate: 7,
            live: Some(KvRestoreLive {
                value: b"v2".to_vec(),
                expire_at_ms: 0,
                sys_from: 20,
            }),
            history: vec![KvRestoreVersion {
                sys_from: 10,
                sys_to: 20,
                value: b"v1".to_vec(),
                expire_at_ms: 0,
            }],
            reassert_at: Some(30),
            clone_copy: false,
        }
    }

    #[test]
    fn a_restored_key_round_trips() {
        let bytes = sample().encode().expect("encode");
        assert_eq!(KvRestoreRedo::decode(&bytes), Some(sample()));
    }

    #[test]
    fn a_restored_key_decodes_as_no_document_or_kv_put() {
        let bytes = sample().encode().expect("encode");
        assert!(crate::wal::DocumentPutRedo::decode(&bytes).is_none());
        assert!(crate::wal::DocumentDeleteRedo::decode(&bytes).is_none());
        let kv_put = zerompk::to_msgpack_vec(&(
            "kv_put",
            "c",
            b"k".to_vec(),
            b"v".to_vec(),
            0u64,
            None::<u64>,
            1u32,
        ))
        .expect("encode kv put");
        assert!(KvRestoreRedo::decode(&kv_put).is_none());
    }
}
