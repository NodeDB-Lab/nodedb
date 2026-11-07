// SPDX-License-Identifier: BUSL-1.1

//! What one bootstrap transfer carries: a source database at one cut.
//!
//! The transfer is plain msgpack inside the link's mTLS. No KEK is involved, so
//! the two clusters share no backup key. The receiver checks its CRC32C before
//! it decodes the image.

use nodedb_types::MirrorGroupPosition;

use crate::Error;

/// A source database at one cut.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct MirrorBootstrapImage {
    /// The source database's id, which the mirror keeps.
    pub database: u64,
    /// The cut's HLC wall time on the source, in nanoseconds.
    pub cut_hlc_ns: u64,
    /// The newest system-time ordinal the capture holds in full.
    pub cap_ordinal: i64,
    /// Where each source group resumes: every data group's capturing barrier,
    /// and the metadata index the catalog image covers.
    pub positions: Vec<MirrorGroupPosition>,
    /// The database's catalog, one encoded `CatalogEntry` per object.
    pub catalog: Vec<Vec<u8>>,
    /// `(tenant_id, TenantDataSnapshot msgpack)`.
    pub tenants: Vec<(u64, Vec<u8>)>,
}

impl MirrorBootstrapImage {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        zerompk::to_msgpack_vec(self).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("mirror bootstrap: encode the capture: {e}"),
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        zerompk::from_msgpack(bytes).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("mirror bootstrap: decode the capture: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_image_survives_its_encoding() {
        let image = MirrorBootstrapImage {
            database: 7,
            cut_hlc_ns: 11,
            cap_ordinal: 13,
            positions: vec![MirrorGroupPosition {
                group_id: 2,
                log_index: 17,
            }],
            catalog: vec![vec![1, 2]],
            tenants: vec![(1, vec![3])],
        };
        let bytes = image.encode().expect("encode");
        assert_eq!(MirrorBootstrapImage::decode(&bytes).expect("decode"), image);
    }

    #[test]
    fn bytes_that_are_no_image_are_refused() {
        assert!(MirrorBootstrapImage::decode(b"not an image").is_err());
    }
}
