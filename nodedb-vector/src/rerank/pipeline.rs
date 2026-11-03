// SPDX-License-Identifier: Apache-2.0

use std::borrow::Cow;

use nodedb_types::vector_ann::VectorAnnOptions;
use nodedb_types::vector_distance::DistanceMetric;

use super::gating::codec_name_for_quant;
use super::sidecar::CodecSidecar;
use super::types::{Candidate, Ranked, RerankError};

/// Shared rerank pipeline. Lite calls this after its index-level coarse search.
///
/// Callers size the candidate set with [`super::rerank_pool`]`(k, opts.oversample)` before
/// pre-fetching from HNSW;
/// this function receives whatever candidates were fetched and reranks by exact distance.
///
/// When `opts.quantization` is `None` (or `VectorQuantization::None`), the FP32 path is used:
/// `fetch_vector` is called once per candidate and must return the stored full-precision vector.
/// Returning `None` for any id is a hard inconsistency error.
///
/// `fetch_vector` returns a `Cow` so that an index storing vectors in a narrower dtype
/// (F16/BF16) can decode to owned f32 on the fly, while an F32 index still borrows.
///
/// When `opts.quantization` is `Some(_)`, a `CodecSidecar` must be provided. The sidecar
/// encodes query and stored vectors; `fetch_vector` is not called in this path.
///
/// When `opts.query_dim = Some(d)`, the FP32 path applies Matryoshka truncated-distance
/// reranking using only the first `d` components. `d` must satisfy `0 < d <= query.len()`.
/// `query_dim` combined with `quantization` is not supported — return `BadInput` if both set.
///
/// `target_recall`, `oversample`, and `meta_token_budget` are accepted via `opts` but not
/// honored here — callers handle those before calling this function.
pub fn rerank<'v, F>(
    candidates: Vec<Candidate>,
    query: &[f32],
    metric: DistanceMetric,
    k: usize,
    opts: &VectorAnnOptions,
    sidecar: Option<&CodecSidecar>,
    mut fetch_vector: F,
) -> Result<Vec<Ranked>, RerankError>
where
    F: FnMut(u32) -> Option<Cow<'v, [f32]>>,
{
    if k == 0 {
        return Err(RerankError::BadInput("k must be > 0".into()));
    }
    if query.is_empty() {
        return Err(RerankError::BadInput("query is empty".into()));
    }

    // Determine requested codec (if any) from opts.
    let requested_codec = opts.quantization.and_then(codec_name_for_quant);

    // Part C: query_dim + quantization combination is not supported.
    if opts.query_dim.is_some() && requested_codec.is_some() {
        return Err(RerankError::BadInput(
            "rerank: query_dim (Matryoshka truncation) is not yet supported in combination \
             with quantization codecs — use one or the other"
                .into(),
        ));
    }

    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    // Codec path.
    if let Some(requested) = requested_codec {
        let sc = sidecar.ok_or_else(|| {
            RerankError::BadInput(
                "rerank: opts.quantization requested but no codec sidecar provided".into(),
            )
        })?;

        let actual = sc.codec_name();
        if actual != requested {
            return Err(RerankError::BadInput(format!(
                "rerank: requested codec {requested:?} does not match sidecar codec {actual:?}"
            )));
        }

        let prepared = sc.prepare_query(query)?;

        let mut scored: Vec<Ranked> = Vec::with_capacity(candidates.len());
        for c in candidates {
            match sc.distance_prepared(&prepared, c.id)? {
                None => {
                    return Err(RerankError::BadInput(format!(
                        "rerank: candidate id {} not present in sidecar (index/sidecar drift)",
                        c.id
                    )));
                }
                Some(d) => {
                    scored.push(Ranked {
                        id: c.id,
                        distance: d,
                    });
                }
            }
        }

        scored.sort_unstable_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);

        // Suppress unused-closure warning — fetch_vector is not used in codec path.
        let _ = &mut fetch_vector;
        return Ok(scored);
    }

    // FP32 path: validate query_dim before touching candidates.
    let effective_dim: usize = match opts.query_dim {
        Some(d) => {
            let d = d as usize;
            if d == 0 || d > query.len() {
                return Err(RerankError::BadInput(format!(
                    "query_dim={d} is out of range; query has {} dimensions \
                     (must be 0 < query_dim <= query.len())",
                    query.len(),
                )));
            }
            d
        }
        None => query.len(),
    };

    // Truncate query once; candidates are sliced inline using the same length.
    let query_slice = crate::matryoshka::truncate(query, effective_dim);

    let mut scored: Vec<Ranked> = Vec::with_capacity(candidates.len());
    let query_dim = query.len();

    for c in candidates {
        let vec = fetch_vector(c.id).ok_or_else(|| {
            RerankError::BadInput(format!(
                "rerank: fetch_vector returned None for id {}",
                c.id
            ))
        })?;
        if vec.len() != query_dim {
            return Err(RerankError::BadInput(format!(
                "candidate id={} has dim {} but query has dim {}",
                c.id,
                vec.len(),
                query_dim,
            )));
        }
        let vec_slice = crate::matryoshka::truncate(&vec, effective_dim);
        let d = crate::distance::distance(query_slice, vec_slice, metric);
        scored.push(Ranked {
            id: c.id,
            distance: d,
        });
    }

    scored.sort_unstable_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(k);

    Ok(scored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use nodedb_types::vector_ann::{VectorAnnOptions, VectorQuantization};

    use crate::rerank::codec::{CodecName, PreparedQuery, RerankCodec};
    use crate::rerank::sidecar::CodecSidecar;
    use crate::rerank::types::RerankError;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn opts() -> VectorAnnOptions {
        VectorAnnOptions::default()
    }

    fn opts_with_dim(d: u32) -> VectorAnnOptions {
        VectorAnnOptions {
            query_dim: Some(d),
            ..Default::default()
        }
    }

    fn opts_with_quant(q: VectorQuantization) -> VectorAnnOptions {
        VectorAnnOptions {
            quantization: Some(q),
            ..Default::default()
        }
    }

    fn make(id: u32) -> Candidate {
        Candidate {
            id,
            index_distance: 0.0,
        }
    }

    fn store(pairs: &[(u32, Vec<f32>)]) -> HashMap<u32, Vec<f32>> {
        pairs.iter().cloned().collect()
    }

    fn fetch<'a>(store: &'a HashMap<u32, Vec<f32>>) -> impl FnMut(u32) -> Option<Cow<'a, [f32]>> {
        move |id| store.get(&id).map(|v| Cow::Borrowed(v.as_slice()))
    }

    /// Stub codec that encodes as raw LE f32 bytes and computes L2 distance.
    /// Reports `CodecName::Binary` so tests can request it via `VectorQuantization::Binary`.
    struct StubCodec {
        name: CodecName,
    }

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
            let floats: Vec<f32> = encoded
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            if query.len() != floats.len() {
                return Err(RerankError::BadInput("dimension mismatch".into()));
            }
            let d = query
                .iter()
                .zip(floats.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum::<f32>()
                .sqrt();
            Ok(d)
        }

        fn name(&self) -> CodecName {
            self.name
        }

        fn to_bytes(&self) -> Result<Vec<u8>, RerankError> {
            Err(RerankError::BadInput(
                "StubCodec does not support serialization".into(),
            ))
        }
    }

    fn make_sidecar(name: CodecName) -> CodecSidecar {
        CodecSidecar::new(Arc::new(StubCodec { name }))
    }

    // ── existing FP32 tests (updated to pass None sidecar) ───────────────────

    #[test]
    fn happy_path_top2() {
        let s = store(&[
            (1, vec![1.0, 0.0]),
            (2, vec![0.1, 0.0]),
            (3, vec![0.5, 0.0]),
            (4, vec![2.0, 0.0]),
            (5, vec![0.3, 0.0]),
        ]);
        let candidates = vec![make(1), make(2), make(3), make(4), make(5)];
        let query = [0.0, 0.0];
        let result = rerank(
            candidates,
            &query,
            DistanceMetric::L2,
            2,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        // closest: id=2 (0.01), then id=5 (0.09)
        assert_eq!(result[0].id, 2);
        assert_eq!(result[1].id, 5);
    }

    #[test]
    fn empty_candidates_returns_empty() {
        let s: HashMap<u32, Vec<f32>> = HashMap::new();
        let result = rerank(
            vec![],
            &[1.0, 2.0],
            DistanceMetric::L2,
            3,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn dim_mismatch_returns_bad_input() {
        let s = store(&[(7, vec![1.0, 2.0, 3.0])]);
        let err = rerank(
            vec![make(7)],
            &[1.0, 2.0],
            DistanceMetric::L2,
            1,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("7"), "expected id in message: {msg}");
        assert!(
            msg.contains("3"),
            "expected candidate dim in message: {msg}"
        );
        assert!(msg.contains("2"), "expected query dim in message: {msg}");
    }

    #[test]
    fn k_zero_returns_bad_input() {
        let s = store(&[(1, vec![1.0])]);
        let err = rerank(
            vec![make(1)],
            &[0.0],
            DistanceMetric::L2,
            0,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap_err();
        assert!(err.to_string().contains("k must be > 0"));
    }

    #[test]
    fn k_exceeds_candidates_returns_all() {
        let s = store(&[
            (1, vec![1.0, 0.0]),
            (2, vec![2.0, 0.0]),
            (3, vec![3.0, 0.0]),
        ]);
        let candidates = vec![make(1), make(2), make(3)];
        let result = rerank(
            candidates,
            &[0.0, 0.0],
            DistanceMetric::L2,
            10,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap();
        assert_eq!(result.len(), 3);
    }

    // ── query_dim (Matryoshka truncated-distance reranking) ───────────────────

    #[test]
    fn query_dim_truncated_ranking_differs_from_full() {
        let s = store(&[(1, vec![0.1, 0.1]), (2, vec![0.0, 9.0])]);
        let query = [0.0_f32, 1.0];

        let full = rerank(
            vec![make(1), make(2)],
            &query,
            DistanceMetric::L2,
            1,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap();
        assert_eq!(full[0].id, 1, "full-dim should rank id=1 first");

        let trunc = rerank(
            vec![make(1), make(2)],
            &query,
            DistanceMetric::L2,
            1,
            &opts_with_dim(1),
            None,
            fetch(&s),
        )
        .unwrap();
        assert_eq!(trunc[0].id, 2, "truncated-dim=1 should rank id=2 first");
    }

    #[test]
    fn query_dim_zero_returns_bad_input() {
        let s = store(&[(1, vec![1.0, 2.0])]);
        let err = rerank(
            vec![make(1)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts_with_dim(0),
            None,
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("query_dim=0"),
            "error should name query_dim=0: {msg}"
        );
    }

    #[test]
    fn query_dim_exceeds_query_len_returns_bad_input() {
        let s = store(&[(1, vec![1.0, 2.0])]);
        let err = rerank(
            vec![make(1)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts_with_dim(5),
            None,
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("query_dim=5"),
            "error should name query_dim=5: {msg}"
        );
        assert!(
            msg.contains('2'),
            "error should mention query len (2): {msg}"
        );
    }

    #[test]
    fn query_dim_equal_to_query_len_matches_full_dim() {
        let s = store(&[
            (1, vec![1.0, 0.0, 0.0]),
            (2, vec![0.5, 0.0, 0.0]),
            (3, vec![3.0, 0.0, 0.0]),
        ]);
        let query = [0.0_f32, 0.0, 0.0];

        let full = rerank(
            vec![make(1), make(2), make(3)],
            &query,
            DistanceMetric::L2,
            3,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap();
        let trunc = rerank(
            vec![make(1), make(2), make(3)],
            &query,
            DistanceMetric::L2,
            3,
            &opts_with_dim(3),
            None,
            fetch(&s),
        )
        .unwrap();

        let full_ids: Vec<u32> = full.iter().map(|r| r.id).collect();
        let trunc_ids: Vec<u32> = trunc.iter().map(|r| r.id).collect();
        assert_eq!(
            full_ids, trunc_ids,
            "query_dim == query.len() should produce identical ranking"
        );
    }

    #[test]
    fn fetch_returns_none_is_bad_input() {
        let s: HashMap<u32, Vec<f32>> = HashMap::new();
        let err = rerank(
            vec![make(99)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts(),
            None,
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("99"),
            "error should name the missing id (99): {msg}"
        );
        assert!(
            matches!(err, RerankError::BadInput(_)),
            "expected BadInput, got: {err}"
        );
    }

    // ── Part F: codec path tests ──────────────────────────────────────────────

    /// Part F.1: codec path uses sidecar distances rather than FP32 fetch_vector.
    #[test]
    fn codec_path_uses_sidecar() {
        // StubCodec uses Binary name, so request VectorQuantization::Binary.
        let mut sc = make_sidecar(CodecName::Binary);
        // Insert 3 vectors: distances from [0,0] are 1.0, 2.0, 3.0.
        sc.encode_and_insert(1, &[1.0, 0.0]).unwrap();
        sc.encode_and_insert(2, &[0.0, 2.0]).unwrap();
        sc.encode_and_insert(3, &[3.0, 0.0]).unwrap();

        let candidates = vec![make(1), make(2), make(3)];
        let opts = opts_with_quant(VectorQuantization::Binary);

        // fetch_vector should never be called in codec path — pass a closure
        // that panics to confirm.
        let result = rerank(
            candidates,
            &[0.0, 0.0],
            DistanceMetric::L2,
            3,
            &opts,
            Some(&sc),
            |_id| panic!("fetch_vector must not be called in codec path"),
        )
        .unwrap();

        assert_eq!(result.len(), 3);
        // Distances: id=1 → 1.0, id=2 → 2.0, id=3 → 3.0
        assert_eq!(result[0].id, 1);
        assert_eq!(result[1].id, 2);
        assert_eq!(result[2].id, 3);
        assert!((result[0].distance - 1.0).abs() < 1e-5);
    }

    /// Part F.2: opts requests codec but sidecar is None → BadInput.
    #[test]
    fn codec_requested_but_no_sidecar_returns_bad_input() {
        let s: HashMap<u32, Vec<f32>> = HashMap::new();
        let opts = opts_with_quant(VectorQuantization::Binary);
        let err = rerank(
            vec![make(1)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts,
            None,
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("no codec sidecar provided"),
            "expected sidecar-missing message: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }

    /// Part F.3: sidecar codec name (Binary) does not match requested (Sq8) → BadInput.
    #[test]
    fn codec_name_mismatch_returns_bad_input() {
        let mut sc = make_sidecar(CodecName::Binary); // sidecar is Binary
        sc.encode_and_insert(1, &[1.0, 0.0]).unwrap();

        let opts = opts_with_quant(VectorQuantization::Sq8); // request Sq8
        let s: HashMap<u32, Vec<f32>> = HashMap::new();
        let err = rerank(
            vec![make(1)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts,
            Some(&sc),
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Sq8") || msg.contains("sq8"),
            "expected requested codec in message: {msg}"
        );
        assert!(
            msg.contains("Binary") || msg.contains("binary"),
            "expected actual codec in message: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }

    /// Part F.4: both query_dim and quantization set → BadInput.
    #[test]
    fn codec_with_query_dim_returns_bad_input() {
        let mut sc = make_sidecar(CodecName::Binary);
        sc.encode_and_insert(1, &[1.0, 0.0]).unwrap();

        let opts = VectorAnnOptions {
            query_dim: Some(1),
            quantization: Some(VectorQuantization::Binary),
            ..Default::default()
        };
        let s: HashMap<u32, Vec<f32>> = HashMap::new();
        let err = rerank(
            vec![make(1)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts,
            Some(&sc),
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("query_dim") && msg.contains("quantization"),
            "expected both terms in message: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }

    /// Part F.5: quantization=None with sidecar Some → FP32 path, sidecar ignored.
    #[test]
    fn fp32_path_with_some_sidecar_argument() {
        let sc = make_sidecar(CodecName::Binary);
        // FP32 store has real vectors.
        let s = store(&[(1, vec![1.0, 0.0]), (2, vec![0.1, 0.0])]);
        // opts has no quantization — FP32 path should run.
        let result = rerank(
            vec![make(1), make(2)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            2,
            &opts(), // no quantization
            Some(&sc),
            fetch(&s),
        )
        .unwrap();
        // FP32: id=2 is closer (dist=0.1) than id=1 (dist=1.0).
        assert_eq!(result[0].id, 2);
        assert_eq!(result[1].id, 1);
    }

    /// Part F.6: candidate id not in sidecar → BadInput (index/sidecar drift).
    #[test]
    fn codec_path_missing_id_in_sidecar_returns_bad_input() {
        let sc = make_sidecar(CodecName::Binary);
        // Sidecar is empty — id 99 is not present.
        let opts = opts_with_quant(VectorQuantization::Binary);
        let s: HashMap<u32, Vec<f32>> = HashMap::new();
        let err = rerank(
            vec![make(99)],
            &[0.0, 0.0],
            DistanceMetric::L2,
            1,
            &opts,
            Some(&sc),
            fetch(&s),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("99"), "expected id 99 in message: {msg}");
        assert!(
            msg.contains("sidecar drift") || msg.contains("not present in sidecar"),
            "expected drift message: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }
}
