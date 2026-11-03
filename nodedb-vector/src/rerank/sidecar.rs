// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use super::codec::{CodecName, PreparedQuery, RerankCodec};
use super::types::RerankError;

const SIDECAR_MAGIC: [u8; 4] = *b"NDCC";
const SIDECAR_VERSION: u8 = 1;

/// Per-collection encoded-vector storage keyed by surrogate id, paired with
/// the trained codec. Encoded vectors live alongside (not inside) the HNSW
/// index — HNSW keeps full-precision vectors for graph traversal; the sidecar
/// is consulted only during base-layer rerank.
pub struct CodecSidecar {
    codec: Arc<dyn RerankCodec>,
    encoded: HashMap<u32, Vec<u8>>,
}

impl CodecSidecar {
    pub fn new(codec: Arc<dyn RerankCodec>) -> Self {
        Self {
            codec,
            encoded: HashMap::new(),
        }
    }

    pub fn codec_name(&self) -> CodecName {
        self.codec.name()
    }

    /// Encode a vector and insert it under `id`. Overwrites any existing entry.
    pub fn encode_and_insert(&mut self, id: u32, vector: &[f32]) -> Result<(), RerankError> {
        let bytes = self.codec.encode(vector)?;
        self.encoded.insert(id, bytes);
        Ok(())
    }

    pub fn remove(&mut self, id: u32) {
        self.encoded.remove(&id);
    }

    pub fn get(&self, id: u32) -> Option<&[u8]> {
        self.encoded.get(&id).map(|v| v.as_slice())
    }

    pub fn len(&self) -> usize {
        self.encoded.len()
    }

    pub fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    /// Serialize the sidecar (codec state + all encoded vectors) to bytes.
    ///
    /// Format: `[NDCC (4 bytes)][version: u8 = 1][msgpack payload]`
    pub fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
        let codec_bytes = self.codec.to_bytes()?;
        let codec_name_byte = codec_name_to_u8(self.codec.name());

        #[derive(
            serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
        )]
        struct Payload {
            codec_name: u8,
            codec_bytes: Vec<u8>,
            encoded: Vec<(u32, Vec<u8>)>,
        }

        let payload = Payload {
            codec_name: codec_name_byte,
            codec_bytes,
            encoded: self.encoded.iter().map(|(k, v)| (*k, v.clone())).collect(),
        };

        let body = zerompk::to_msgpack_vec(&payload)
            .map_err(|e| RerankError::BadInput(format!("sidecar serialize: {e}")))?;
        let mut buf = Vec::with_capacity(5 + body.len());
        buf.extend_from_slice(&SIDECAR_MAGIC);
        buf.push(SIDECAR_VERSION);
        buf.extend_from_slice(&body);
        Ok(buf)
    }

    /// Deserialize a sidecar from bytes produced by [`Self::to_bytes`].
    pub fn from_bytes(bytes: &[u8], memory: nodedb_mem::ScopedMemory) -> Result<Self, RerankError> {
        if bytes.len() < 5 {
            return Err(RerankError::BadInput(
                "sidecar from_bytes: too short".into(),
            ));
        }
        if bytes[..4] != SIDECAR_MAGIC {
            return Err(RerankError::BadInput(
                "sidecar from_bytes: bad magic".into(),
            ));
        }
        let version = bytes[4];
        if version != SIDECAR_VERSION {
            return Err(RerankError::BadInput(format!(
                "sidecar from_bytes: unknown version {version}"
            )));
        }

        #[derive(
            serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
        )]
        struct Payload {
            codec_name: u8,
            codec_bytes: Vec<u8>,
            encoded: Vec<(u32, Vec<u8>)>,
        }

        let payload: Payload = zerompk::from_msgpack(&bytes[5..])
            .map_err(|e| RerankError::BadInput(format!("sidecar deserialize: {e}")))?;

        let codec_name = codec_name_from_u8(payload.codec_name).ok_or_else(|| {
            RerankError::BadInput(format!(
                "sidecar from_bytes: unknown codec_name byte {}",
                payload.codec_name
            ))
        })?;
        let codec =
            super::codec::rerank_codec_from_bytes(codec_name, &payload.codec_bytes, memory)?;
        let encoded = payload.encoded.into_iter().collect();
        Ok(CodecSidecar { codec, encoded })
    }

    pub fn prepare_query(&self, query: &[f32]) -> Result<PreparedQuery, RerankError> {
        self.codec.prepare_query(query)
    }

    /// Compute distance from a prepared query to the encoded vector at `id`.
    /// Returns `Ok(None)` when the id isn't in the sidecar (lost / not yet
    /// encoded); returns the distance otherwise.
    pub fn distance_prepared(
        &self,
        prepared: &PreparedQuery,
        id: u32,
    ) -> Result<Option<f32>, RerankError> {
        match self.encoded.get(&id) {
            None => Ok(None),
            Some(bytes) => self.codec.distance_prepared(prepared, bytes).map(Some),
        }
    }
}

fn codec_name_to_u8(name: CodecName) -> u8 {
    match name {
        CodecName::Sq8 => 0,
        CodecName::Pq => 1,
        CodecName::Binary => 2,
        CodecName::RaBitQ => 3,
        CodecName::Bbq => 4,
        CodecName::Ternary => 5,
        CodecName::Opq => 6,
    }
}

fn codec_name_from_u8(b: u8) -> Option<CodecName> {
    match b {
        0 => Some(CodecName::Sq8),
        1 => Some(CodecName::Pq),
        2 => Some(CodecName::Binary),
        3 => Some(CodecName::RaBitQ),
        4 => Some(CodecName::Bbq),
        5 => Some(CodecName::Ternary),
        6 => Some(CodecName::Opq),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rerank::codec::CodecName;
    use crate::test_support::test_memory;

    struct StubCodec;

    impl RerankCodec for StubCodec {
        fn encode(&self, v: &[f32]) -> Result<Vec<u8>, RerankError> {
            Ok(v.iter().flat_map(|x| x.to_le_bytes()).collect())
        }

        fn prepare_query(&self, q: &[f32]) -> Result<PreparedQuery, RerankError> {
            Ok(PreparedQuery::Raw(q.to_vec()))
        }

        fn distance_prepared(
            &self,
            prepared: &PreparedQuery,
            encoded: &[u8],
        ) -> Result<f32, RerankError> {
            let query = match prepared {
                PreparedQuery::Raw(v) => v,
                _ => {
                    return Err(RerankError::BadInput(
                        "StubCodec expects Raw prepared query".into(),
                    ));
                }
            };
            let encoded_floats: Vec<f32> = encoded
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            if query.len() != encoded_floats.len() {
                return Err(RerankError::BadInput("dimension mismatch".into()));
            }
            let dist = query
                .iter()
                .zip(encoded_floats.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum::<f32>()
                .sqrt();
            Ok(dist)
        }

        fn name(&self) -> CodecName {
            CodecName::Binary
        }

        fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
            Err(RerankError::BadInput(
                "StubCodec does not support serialization".into(),
            ))
        }
    }

    fn make_sidecar() -> CodecSidecar {
        CodecSidecar::new(Arc::new(StubCodec))
    }

    #[test]
    fn insert_and_get() {
        let mut s = make_sidecar();
        assert!(s.is_empty());
        s.encode_and_insert(1, &[1.0, 2.0]).unwrap();
        s.encode_and_insert(2, &[3.0, 4.0]).unwrap();
        s.encode_and_insert(3, &[5.0, 6.0]).unwrap();
        assert_eq!(s.len(), 3);

        let expected_1: Vec<u8> = [1.0f32, 2.0f32]
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();
        assert_eq!(s.get(1), Some(expected_1.as_slice()));
    }

    #[test]
    fn remove_returns_none() {
        let mut s = make_sidecar();
        s.encode_and_insert(10, &[1.0]).unwrap();
        s.remove(10);
        assert_eq!(s.get(10), None);
        let prepared = s.prepare_query(&[1.0]).unwrap();
        assert_eq!(s.distance_prepared(&prepared, 10).unwrap(), None);
    }

    #[test]
    fn distance_prepared_correct() {
        let mut s = make_sidecar();
        s.encode_and_insert(5, &[0.0, 0.0]).unwrap();
        let prepared = s.prepare_query(&[3.0, 4.0]).unwrap();
        let dist = s.distance_prepared(&prepared, 5).unwrap().unwrap();
        assert!((dist - 5.0).abs() < 1e-5, "expected L2=5.0, got {dist}");
    }

    #[test]
    fn codec_name_passthrough() {
        let s = make_sidecar();
        assert_eq!(s.codec_name(), CodecName::Binary);
        assert_eq!(s.codec_name().as_str(), "binary");
    }

    #[test]
    fn len_and_is_empty() {
        let mut s = make_sidecar();
        assert!(s.is_empty());
        s.encode_and_insert(1, &[1.0]).unwrap();
        assert!(!s.is_empty());
        assert_eq!(s.len(), 1);
        s.remove(1);
        assert!(s.is_empty());
    }

    // ── Sidecar serialization tests ────────────────────────────────────────────

    fn det_vec(i: usize, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| ((i * 31 + j) % 100) as f32 / 100.0)
            .collect()
    }

    #[test]
    fn sidecar_roundtrip_sq8() {
        use crate::rerank::codecs::Sq8Rerank;
        let dim = 16;
        let mut codec = Sq8Rerank::new(dim);
        let samples: Vec<Vec<f32>> = (0..20).map(|i| det_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = samples.iter().map(|v| v.as_slice()).collect();
        codec.train(&refs).unwrap();

        let mut s = CodecSidecar::new(Arc::new(codec));
        for i in 0..5u32 {
            s.encode_and_insert(i, &det_vec(i as usize, dim)).unwrap();
        }
        let bytes = s.to_bytes().expect("to_bytes");
        let s2 = CodecSidecar::from_bytes(&bytes, test_memory()).expect("from_bytes");
        assert_eq!(s2.codec_name(), CodecName::Sq8);
        for i in 0..5u32 {
            assert_eq!(s.get(i), s2.get(i), "encoded bytes differ for id {i}");
        }
    }

    #[test]
    fn sidecar_roundtrip_binary() {
        use crate::rerank::codecs::BinaryRerank;
        let dim = 16;
        let mut s = CodecSidecar::new(Arc::new(BinaryRerank::new(dim)));
        for i in 0..5u32 {
            s.encode_and_insert(i, &det_vec(i as usize, dim)).unwrap();
        }
        let bytes = s.to_bytes().expect("to_bytes");
        let s2 = CodecSidecar::from_bytes(&bytes, test_memory()).expect("from_bytes");
        assert_eq!(s2.codec_name(), CodecName::Binary);
        for i in 0..5u32 {
            assert_eq!(s.get(i), s2.get(i), "encoded bytes differ for id {i}");
        }
    }

    #[test]
    fn sidecar_roundtrip_pq() {
        use crate::rerank::codecs::PqRerank;
        let dim = 16;
        let m = 4;
        let k = 8;
        let mut codec = PqRerank::new(dim, m, k, test_memory());
        let samples: Vec<Vec<f32>> = (0..32).map(|i| det_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = samples.iter().map(|v| v.as_slice()).collect();
        codec.train(&refs).unwrap();

        let mut s = CodecSidecar::new(Arc::new(codec));
        for i in 0..5u32 {
            s.encode_and_insert(i, &det_vec(i as usize, dim)).unwrap();
        }
        let bytes = s.to_bytes().expect("to_bytes");
        let s2 = CodecSidecar::from_bytes(&bytes, test_memory()).expect("from_bytes");
        assert_eq!(s2.codec_name(), CodecName::Pq);
        for i in 0..5u32 {
            assert_eq!(s.get(i), s2.get(i), "encoded bytes differ for id {i}");
        }
    }

    #[test]
    fn sidecar_roundtrip_rabitq() {
        use crate::rerank::codecs::RaBitQRerank;
        let dim = 16;
        let mut codec = RaBitQRerank::new(dim, 0xDEADBEEF_C0FFEE42);
        let samples: Vec<Vec<f32>> = (0..20).map(|i| det_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = samples.iter().map(|v| v.as_slice()).collect();
        codec.train(&refs).unwrap();

        let mut s = CodecSidecar::new(Arc::new(codec));
        for i in 0..5u32 {
            s.encode_and_insert(i, &det_vec(i as usize, dim)).unwrap();
        }
        let bytes = s.to_bytes().expect("to_bytes");
        let s2 = CodecSidecar::from_bytes(&bytes, test_memory()).expect("from_bytes");
        assert_eq!(s2.codec_name(), CodecName::RaBitQ);
        for i in 0..5u32 {
            assert_eq!(s.get(i), s2.get(i), "encoded bytes differ for id {i}");
        }
    }

    #[test]
    fn sidecar_roundtrip_bbq() {
        use crate::rerank::codecs::BbqRerank;
        let dim = 16;
        let mut codec = BbqRerank::new(dim, 4);
        let samples: Vec<Vec<f32>> = (0..20).map(|i| det_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = samples.iter().map(|v| v.as_slice()).collect();
        codec.train(&refs).unwrap();

        let mut s = CodecSidecar::new(Arc::new(codec));
        for i in 0..5u32 {
            s.encode_and_insert(i, &det_vec(i as usize, dim)).unwrap();
        }
        let bytes = s.to_bytes().expect("to_bytes");
        let s2 = CodecSidecar::from_bytes(&bytes, test_memory()).expect("from_bytes");
        assert_eq!(s2.codec_name(), CodecName::Bbq);
        for i in 0..5u32 {
            assert_eq!(s.get(i), s2.get(i), "encoded bytes differ for id {i}");
        }
    }

    #[test]
    fn sidecar_bad_magic_returns_error() {
        use crate::rerank::codecs::BinaryRerank;
        let s = CodecSidecar::new(Arc::new(BinaryRerank::new(4)));
        let mut bytes = s.to_bytes().unwrap();
        bytes[0] = b'X';
        assert!(CodecSidecar::from_bytes(&bytes, test_memory()).is_err());
    }

    #[test]
    fn sidecar_bad_version_returns_error() {
        use crate::rerank::codecs::BinaryRerank;
        let s = CodecSidecar::new(Arc::new(BinaryRerank::new(4)));
        let mut bytes = s.to_bytes().unwrap();
        bytes[4] = 99;
        assert!(CodecSidecar::from_bytes(&bytes, test_memory()).is_err());
    }

    #[test]
    fn sidecar_distance_matches_after_roundtrip() {
        use crate::rerank::codecs::Sq8Rerank;
        let dim = 16;
        let mut codec = Sq8Rerank::new(dim);
        let samples: Vec<Vec<f32>> = (0..20).map(|i| det_vec(i, dim)).collect();
        let refs: Vec<&[f32]> = samples.iter().map(|v| v.as_slice()).collect();
        codec.train(&refs).unwrap();

        let mut s = CodecSidecar::new(Arc::new(codec));
        s.encode_and_insert(1, &det_vec(3, dim)).unwrap();
        let query_vec = det_vec(7, dim);
        let prepared_orig = s.prepare_query(&query_vec).unwrap();
        let d_orig = s.distance_prepared(&prepared_orig, 1).unwrap().unwrap();

        let bytes = s.to_bytes().unwrap();
        let s2 = CodecSidecar::from_bytes(&bytes, test_memory()).unwrap();
        let prepared_rest = s2.prepare_query(&query_vec).unwrap();
        let d_rest = s2.distance_prepared(&prepared_rest, 1).unwrap().unwrap();

        assert!(
            (d_orig - d_rest).abs() < 1e-5,
            "distance diverged: {d_orig} vs {d_rest}"
        );
    }
}
