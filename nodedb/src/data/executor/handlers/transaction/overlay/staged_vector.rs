// SPDX-License-Identifier: BUSL-1.1

//! The staged body of one vector-primary row.
//!
//! A vector-primary row is one surrogate-keyed row whose vector lives in
//! HNSW and whose payload lives in the sparse-store sidecar. Staging keeps
//! both in a single [`Staged::Put`](super::Staged::Put) body so an
//! in-transaction search can rank the vector and an in-transaction point
//! read or scan can render the sidecar. The sidecar bytes are the exact
//! bytes `write_vector_direct_row` stores at COMMIT, so a read of the
//! staged row and a read after COMMIT decode through the same converter.

/// The staged body of a vector-primary `Staged::Put`.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StagedVectorRow {
    /// FP32 vector the row's HNSW node carries at COMMIT.
    pub vector: Vec<f32>,
    /// `zerompk` TAGGED payload sidecar, as the sparse store holds it.
    pub sidecar: Vec<u8>,
}

impl StagedVectorRow {
    /// Encode for the overlay.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("staged vector-primary row encode: {e}"),
        })
    }

    /// Decode a staged body. A body that does not decode is a broken
    /// overlay invariant, never an absent row.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("staged vector-primary row decode: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_vector_and_sidecar() {
        let row = StagedVectorRow {
            vector: vec![0.5, -1.0, 2.0],
            sidecar: vec![0x80],
        };
        let bytes = row.to_bytes().expect("encode");
        assert_eq!(StagedVectorRow::from_bytes(&bytes).expect("decode"), row);
    }

    #[test]
    fn garbage_is_an_error_not_an_empty_row() {
        assert!(StagedVectorRow::from_bytes(&[0xc1, 0xff]).is_err());
    }
}
