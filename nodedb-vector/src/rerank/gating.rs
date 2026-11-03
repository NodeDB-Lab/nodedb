// SPDX-License-Identifier: Apache-2.0

//! Option-combination gating for `rerank`.
//!
//! Validates a [`VectorAnnOptions`] request against the index shape and returns
//! the [`CodecName`] the search should use (or `None` for FP32-only), surfacing
//! unsupported combinations as [`RerankError::BadInput`] with precise messages.

use nodedb_types::vector_ann::{VectorAnnOptions, VectorQuantization};

use super::codec::CodecName;
use super::types::RerankError;

/// Shape of the underlying vector index, used by [`validate_options`] to decide
/// which options are coherent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexShape {
    SingleVector,
    MultiVector,
}

/// Validate the option combo against the index shape and the collection's
/// configured quantization. Returns the [`CodecName`] the search should use
/// for rerank (`None` when the request is FP32-only), or
/// [`RerankError::BadInput`] when the combination is invalid.
///
/// # Quantization contract
///
/// The codec is fixed at collection-creation time. `collection_quant` is the
/// quantization that was declared via DDL; `opts.quantization` is the optional
/// search-time override.
///
/// - If `opts.quantization` is `None`: honor `collection_quant` — map it to a
///   `CodecName` via `codec_name_for_quant`.
/// - If `opts.quantization` is `Some(q)` and `q == collection_quant`: proceed
///   (same as old behavior — the caller is being explicit about what the index
///   already uses).
/// - If `opts.quantization` is `Some(q)` and `q != collection_quant`: return
///   `RerankError::BadInput` naming both the requested codec and the collection's
///   configured codec. Silent fallback is never allowed.
/// - `Some(VectorQuantization::None)` against any non-`None` `collection_quant`
///   is also a contradiction and returns `BadInput`.
pub fn validate_options(
    opts: &VectorAnnOptions,
    index_shape: IndexShape,
    collection_quant: VectorQuantization,
) -> Result<Option<CodecName>, RerankError> {
    validate_meta_token_budget(opts, index_shape)?;
    validate_quantization_with_collection(opts, collection_quant)
}

fn validate_meta_token_budget(
    opts: &VectorAnnOptions,
    index_shape: IndexShape,
) -> Result<(), RerankError> {
    if opts.meta_token_budget.is_none() {
        return Ok(());
    }
    match index_shape {
        IndexShape::SingleVector => Err(RerankError::BadInput(
            "meta_token_budget requires a multi-vector (MetaEmbed) index; \
             the target collection is single-vector. \
             Multi-vector indexes are not yet available in this deployment."
                .to_owned(),
        )),
        IndexShape::MultiVector => Err(RerankError::BadInput(
            "meta_token_budget routing not yet implemented; \
             multi-vector indexes exist but PLAID/MaxSim dispatch is not wired."
                .to_owned(),
        )),
    }
}

/// Map a `VectorQuantization` variant to its `CodecName`. `None` means the
/// FP32 path: `VectorQuantization::None` carries no codec.
pub(crate) fn codec_name_for_quant(q: VectorQuantization) -> Option<CodecName> {
    match q {
        VectorQuantization::None => None,
        VectorQuantization::Sq8 => Some(CodecName::Sq8),
        VectorQuantization::Pq => Some(CodecName::Pq),
        VectorQuantization::Binary => Some(CodecName::Binary),
        VectorQuantization::RaBitQ => Some(CodecName::RaBitQ),
        VectorQuantization::Bbq => Some(CodecName::Bbq),
        VectorQuantization::Ternary => Some(CodecName::Ternary),
        VectorQuantization::Opq => Some(CodecName::Opq),
    }
}

/// Validate the search-time quantization against the collection's configured
/// codec, and surface a precise `BadInput` on any mismatch. No silent fallback.
fn validate_quantization_with_collection(
    opts: &VectorAnnOptions,
    collection_quant: VectorQuantization,
) -> Result<Option<CodecName>, RerankError> {
    match opts.quantization {
        // Caller did not specify a codec: honor whatever the collection was
        // built with.
        None => Ok(codec_name_for_quant(collection_quant)),

        // Caller explicitly requested "no quantization" (FP32 path).
        Some(VectorQuantization::None) => {
            if collection_quant != VectorQuantization::None {
                return Err(RerankError::BadInput(format!(
                    "search-time quantization 'None' does not match collection's configured \
                     quantization '{collection_quant:?}'; the codec is fixed at \
                     collection-creation time"
                )));
            }
            Ok(None)
        }

        // Caller specified a concrete codec.
        Some(requested) => {
            if requested != collection_quant {
                return Err(RerankError::BadInput(format!(
                    "search-time quantization '{requested:?}' does not match collection's \
                     configured quantization '{collection_quant:?}'; the codec is fixed at \
                     collection-creation time"
                )));
            }
            Ok(codec_name_for_quant(collection_quant))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts_with_quant(q: Option<VectorQuantization>) -> VectorAnnOptions {
        VectorAnnOptions {
            quantization: q,
            ..Default::default()
        }
    }

    fn opts_with_budget(budget: u8, q: Option<VectorQuantization>) -> VectorAnnOptions {
        VectorAnnOptions {
            meta_token_budget: Some(budget),
            quantization: q,
            ..Default::default()
        }
    }

    // ── Existing tests updated to pass collection_quant ───────────────────────

    #[test]
    fn none_quantization_returns_none() {
        let result = validate_options(
            &opts_with_quant(None),
            IndexShape::SingleVector,
            VectorQuantization::None,
        );
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn explicit_none_quantization_returns_none() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::None)),
            IndexShape::SingleVector,
            VectorQuantization::None,
        );
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn sq8_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Sq8)),
            IndexShape::SingleVector,
            VectorQuantization::Sq8,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Sq8));
    }

    #[test]
    fn pq_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Pq)),
            IndexShape::SingleVector,
            VectorQuantization::Pq,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Pq));
    }

    #[test]
    fn binary_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Binary)),
            IndexShape::SingleVector,
            VectorQuantization::Binary,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Binary));
    }

    #[test]
    fn rabitq_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::RaBitQ)),
            IndexShape::SingleVector,
            VectorQuantization::RaBitQ,
        );
        assert_eq!(result.unwrap(), Some(CodecName::RaBitQ));
    }

    #[test]
    fn bbq_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Bbq)),
            IndexShape::SingleVector,
            VectorQuantization::Bbq,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Bbq));
    }

    #[test]
    fn ternary_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Ternary)),
            IndexShape::SingleVector,
            VectorQuantization::Ternary,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Ternary));
    }

    #[test]
    fn opq_returns_codec() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Opq)),
            IndexShape::SingleVector,
            VectorQuantization::Opq,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Opq));
    }

    #[test]
    fn meta_token_budget_single_vec_returns_bad_input() {
        let err = validate_options(
            &opts_with_budget(8, None),
            IndexShape::SingleVector,
            VectorQuantization::None,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("single-vector"),
            "expected 'single-vector' in: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }

    #[test]
    fn meta_token_budget_multi_vec_returns_bad_input() {
        let err = validate_options(
            &opts_with_budget(8, None),
            IndexShape::MultiVector,
            VectorQuantization::None,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("PLAID") || msg.contains("MaxSim"),
            "expected 'PLAID' or 'MaxSim' in: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }

    #[test]
    fn meta_token_budget_none_passes_with_sq8() {
        let result = validate_options(
            &opts_with_quant(Some(VectorQuantization::Sq8)),
            IndexShape::SingleVector,
            VectorQuantization::Sq8,
        );
        assert_eq!(result.unwrap(), Some(CodecName::Sq8));
    }

    // ── New mismatch / collection-default tests ───────────────────────────────

    #[test]
    fn quantization_mismatch_returns_bad_input() {
        let opts = opts_with_quant(Some(VectorQuantization::Sq8));
        let err =
            validate_options(&opts, IndexShape::SingleVector, VectorQuantization::Pq).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Sq8") && msg.contains("Pq"),
            "message must name both requested and configured codec: {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }

    #[test]
    fn quantization_matches_collection_passes() {
        let opts = opts_with_quant(Some(VectorQuantization::RaBitQ));
        let result = validate_options(&opts, IndexShape::SingleVector, VectorQuantization::RaBitQ);
        assert_eq!(result.unwrap(), Some(CodecName::RaBitQ));
    }

    #[test]
    fn quantization_none_with_collection_codec_uses_collection_codec() {
        // Caller didn't specify; collection was built with Sq8 → return Sq8.
        let opts = opts_with_quant(None);
        let result = validate_options(&opts, IndexShape::SingleVector, VectorQuantization::Sq8);
        assert_eq!(result.unwrap(), Some(CodecName::Sq8));
    }

    #[test]
    fn quantization_none_with_collection_none_returns_none() {
        // Both unset → FP32-only path.
        let opts = opts_with_quant(None);
        let result = validate_options(&opts, IndexShape::SingleVector, VectorQuantization::None);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn explicit_none_against_sq8_collection_returns_bad_input() {
        // Requesting "no codec" against a collection configured with Sq8 is contradictory.
        let opts = opts_with_quant(Some(VectorQuantization::None));
        let err =
            validate_options(&opts, IndexShape::SingleVector, VectorQuantization::Sq8).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("None") && msg.contains("Sq8"),
            "message must name both requested 'None' and collection's 'Sq8': {msg}"
        );
        assert!(matches!(err, RerankError::BadInput(_)));
    }
}
