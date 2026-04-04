//! Fieldnorm storage: SmallFloat-encoded document lengths per collection.
//!
//! Stores a compact `Vec<u8>` array indexed by u32 doc_id. Each byte is
//! a SmallFloat-encoded document length. Persisted as metadata blob via
//! the backend's `read_meta`/`write_meta`.

use crate::backend::FtsBackend;
use crate::codec::smallfloat;
use crate::index::FtsIndex;

impl<B: FtsBackend> FtsIndex<B> {
    /// Get the fieldnorm (SmallFloat-encoded doc length) for a doc.
    ///
    /// Returns the decoded approximate u32 length, or `None` if not stored.
    pub fn read_fieldnorm(&self, collection: &str, doc_id: u32) -> Result<Option<u32>, B::Error> {
        let meta_key = format!("{collection}:fieldnorms");
        let data = self.backend.read_meta(&meta_key)?;
        match data {
            Some(bytes) if (doc_id as usize) < bytes.len() => {
                Ok(Some(smallfloat::decode(bytes[doc_id as usize])))
            }
            _ => Ok(None),
        }
    }

    /// Write a fieldnorm byte for a doc_id. Grows the array if needed.
    pub fn write_fieldnorm(
        &self,
        collection: &str,
        doc_id: u32,
        doc_length: u32,
    ) -> Result<(), B::Error> {
        let meta_key = format!("{collection}:fieldnorms");
        let mut data = self.backend.read_meta(&meta_key)?.unwrap_or_default();

        let idx = doc_id as usize;
        if idx >= data.len() {
            data.resize(idx + 1, 0);
        }
        data[idx] = smallfloat::encode(doc_length);

        self.backend.write_meta(&meta_key, &data)
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::codec::smallfloat;
    use crate::index::FtsIndex;

    #[test]
    fn fieldnorm_roundtrip() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.write_fieldnorm("col", 0, 100).unwrap();
        idx.write_fieldnorm("col", 5, 50).unwrap();

        let norm0 = idx.read_fieldnorm("col", 0).unwrap().unwrap();
        let norm5 = idx.read_fieldnorm("col", 5).unwrap().unwrap();

        // SmallFloat roundtrip within tolerance.
        assert!(norm0 <= 100);
        assert!(norm5 <= 50);
        assert_eq!(norm0, smallfloat::decode(smallfloat::encode(100)));
        assert_eq!(norm5, smallfloat::decode(smallfloat::encode(50)));
    }

    #[test]
    fn fieldnorm_missing_doc() {
        let idx = FtsIndex::new(MemoryBackend::new());
        assert_eq!(idx.read_fieldnorm("col", 99).unwrap(), None);
    }

    #[test]
    fn fieldnorm_overwrite() {
        let idx = FtsIndex::new(MemoryBackend::new());
        idx.write_fieldnorm("col", 0, 100).unwrap();
        idx.write_fieldnorm("col", 0, 200).unwrap();

        let norm = idx.read_fieldnorm("col", 0).unwrap().unwrap();
        assert_eq!(norm, smallfloat::decode(smallfloat::encode(200)));
    }
}
