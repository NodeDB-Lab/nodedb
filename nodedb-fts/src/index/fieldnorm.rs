// SPDX-License-Identifier: Apache-2.0

//! Fieldnorm storage: SmallFloat-encoded document lengths per collection.
//!
//! Stores a compact `Vec<u8>` array indexed by surrogate. Each byte is
//! a SmallFloat-encoded document length. Persisted as metadata blob via
//! the backend's `read_meta`/`write_meta`.

use nodedb_types::Surrogate;

use crate::backend::FtsBackend;
use crate::codec::smallfloat;
use crate::index::FtsIndex;
use crate::scope::IndexScope;

impl<B: FtsBackend> FtsIndex<B> {
    /// Get the fieldnorm (SmallFloat-encoded doc length) for a doc.
    ///
    /// Returns the decoded approximate u32 length, or `None` if not stored.
    pub fn read_fieldnorm<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        doc_id: Surrogate,
    ) -> Result<Option<u32>, B::Error> {
        let data = self
            .backend
            .read_meta(database_id, tid, index.into(), "fieldnorms")?;
        match data {
            Some(bytes) if (doc_id.0 as usize) < bytes.len() => {
                Ok(Some(smallfloat::decode(bytes[doc_id.0 as usize])))
            }
            _ => Ok(None),
        }
    }

    /// Write a fieldnorm byte for a surrogate. Grows the array if needed.
    pub fn write_fieldnorm<'a>(
        &self,
        database_id: u64,
        tid: u64,
        index: impl Into<IndexScope<'a>>,
        doc_id: Surrogate,
        doc_length: u32,
    ) -> Result<(), B::Error> {
        let index = index.into();
        let mut data = self
            .backend
            .read_meta(database_id, tid, index, "fieldnorms")?
            .unwrap_or_default();

        let idx = doc_id.0 as usize;
        if idx >= data.len() {
            data.resize(idx + 1, 0);
        }
        data[idx] = smallfloat::encode(doc_length);

        self.backend
            .write_meta(database_id, tid, index, "fieldnorms", &data)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use crate::backend::memory::MemoryBackend;
    use crate::codec::smallfloat;
    use crate::index::FtsIndex;
    use crate::test_support::test_governor;

    const DB: u64 = 0;
    const T: u64 = 1;

    #[test]
    fn fieldnorm_roundtrip() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.write_fieldnorm(DB, T, "col", Surrogate(0), 100)
            .unwrap();
        idx.write_fieldnorm(DB, T, "col", Surrogate(5), 50).unwrap();

        let norm0 = idx
            .read_fieldnorm(DB, T, "col", Surrogate(0))
            .unwrap()
            .unwrap();
        let norm5 = idx
            .read_fieldnorm(DB, T, "col", Surrogate(5))
            .unwrap()
            .unwrap();

        assert!(norm0 <= 100);
        assert!(norm5 <= 50);
        assert_eq!(norm0, smallfloat::decode(smallfloat::encode(100)));
        assert_eq!(norm5, smallfloat::decode(smallfloat::encode(50)));
    }

    #[test]
    fn fieldnorm_missing_doc() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        assert_eq!(
            idx.read_fieldnorm(DB, T, "col", Surrogate(99)).unwrap(),
            None
        );
    }

    #[test]
    fn fieldnorm_overwrite() {
        let idx = FtsIndex::new(MemoryBackend::new(), test_governor());
        idx.write_fieldnorm(DB, T, "col", Surrogate(0), 100)
            .unwrap();
        idx.write_fieldnorm(DB, T, "col", Surrogate(0), 200)
            .unwrap();

        let norm = idx
            .read_fieldnorm(DB, T, "col", Surrogate(0))
            .unwrap()
            .unwrap();
        assert_eq!(norm, smallfloat::decode(smallfloat::encode(200)));
    }
}
