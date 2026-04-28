//! Serialize / deserialize helpers for [`MetadataEntry`].
//!
//! As of wire_version v2, entries are wrapped in a [`crate::wire_version::Versioned`]
//! envelope so future variant additions (e.g. `MigrationCheckpoint`) can be
//! detected and rejected cleanly on older nodes rather than silently
//! misinterpreted.
//!
//! Existing serialized bytes (raw MessagePack without envelope) are decoded
//! via the v1 fallback path in [`crate::wire_version::decode_versioned`] —
//! the round-trip behavior for all current variants is preserved exactly.

use crate::error::ClusterError;
use crate::metadata_group::entry::MetadataEntry;
use crate::wire_version::{decode_versioned, encode_versioned};

/// Encode a [`MetadataEntry`] into a v2 versioned wire envelope.
pub fn encode_entry(entry: &MetadataEntry) -> Result<Vec<u8>, ClusterError> {
    encode_versioned(entry).map_err(|e| ClusterError::Codec {
        detail: format!("metadata encode: {e}"),
    })
}

/// Decode a [`MetadataEntry`] from bytes.
///
/// Accepts both v2 versioned envelopes and raw v1 (pre-versioning) bytes;
/// rejects envelopes with unsupported future version numbers.
pub fn decode_entry(data: &[u8]) -> Result<MetadataEntry, ClusterError> {
    decode_versioned(data).map_err(|e| ClusterError::Codec {
        detail: format!("metadata decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::entry::{MetadataEntry, TopologyChange};

    #[test]
    fn metadata_entry_versioned_roundtrip() {
        let entry = MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 42,
            addr: "127.0.0.1:7001".to_string(),
        });
        let bytes = encode_entry(&entry).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn metadata_entry_v1_raw_bytes_decodable() {
        // Simulate a legacy peer: encode raw (no envelope).
        let entry = MetadataEntry::SurrogateAlloc { hwm: 1024 };
        let raw_bytes = zerompk::to_msgpack_vec(&entry).unwrap();

        // decode_entry must accept v1 raw bytes via the fallback path.
        let decoded = decode_entry(&raw_bytes).unwrap();
        assert_eq!(entry, decoded);
    }
}
