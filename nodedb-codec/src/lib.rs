// SPDX-License-Identifier: Apache-2.0

// Library code propagates errors; tests may unwrap.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Compression codecs for NodeDB timeseries columnar storage.
//!
//! Provides per-column codec selection with **cascading compression**:
//! type-aware encoding (ALP, FastLanes, FSST, Pcodec) followed by a terminal
//! byte compressor (lz4_flex for hot/warm, rANS for cold/S3).
//!
//! Cascading chains (hot/warm — lz4 terminal):
//! - `AlpFastLanesLz4`:   f64 metrics → ALP → FastLanes → lz4
//! - `DeltaFastLanesLz4`: i64 timestamps/counters → Delta → FastLanes → lz4
//! - `FastLanesLz4`:      i64 raw integers → FastLanes → lz4
//! - `FsstLz4`:           strings/logs → FSST → lz4
//! - `PcodecLz4`:         complex numerics → Pcodec → lz4
//! - `AlpRdLz4`:          true doubles → ALP-RD → lz4
//!
//! Cold/S3 tier chains (rANS terminal):
//! - `AlpFastLanesRans`, `DeltaFastLanesRans`, `FsstRans`
//!
//! Shared by Origin and Lite. Compiles to WASM.

pub mod alp;
pub mod alp_rd;
mod bounds;
pub mod codec_types;
pub mod crdt_compress;
pub mod delta;
pub mod detect;
pub mod double_delta;
pub mod error;
pub mod fastlanes;
pub mod fsst;
pub mod gorilla;
pub mod gorilla_f32;
pub mod lz4;
pub mod pcodec;
pub mod pipeline;
pub mod rans;
pub mod raw;
pub mod spherical;
pub mod vector_quant;
pub mod zstd_codec;

/// Number of values to sample for codec auto-detection and exponent selection.
/// Used by ALP, ALP-RD, and the codec detector.
pub const CODEC_SAMPLE_SIZE: usize = 1024;

pub use codec_types::{
    ColumnCodec, ColumnStatistics, ColumnTypeHint, ResolvedColumnCodec, parse_codec_name,
};
pub use crdt_compress::CrdtOp;
pub use delta::{DeltaDecoder, DeltaEncoder};
pub use detect::detect_codec;
pub use double_delta::{DoubleDeltaDecoder, DoubleDeltaEncoder};
pub use error::CodecError;
pub use gorilla::{GorillaDecoder, GorillaEncoder};
pub use lz4::{Lz4Decoder, Lz4Encoder};
pub use pipeline::{
    decode_bytes_pipeline, decode_f64_pipeline, decode_i64_pipeline, encode_bytes_pipeline,
    encode_f64_pipeline, encode_i64_pipeline,
};
pub use raw::{RawDecoder, RawEncoder};
pub use zstd_codec::{ZstdDecoder, ZstdEncoder};

#[cfg(test)]
mod tests {
    use super::*;

    /// Frozen canonical codec name surface. Locks the lowercase, snake_case
    /// forms before any user DDL exposes them. Adding a codec means appending
    /// here and to `as_str()`; renaming any existing entry is a wire break.
    #[test]
    fn canonical_codec_names_frozen() {
        let canonical: &[(ColumnCodec, &str)] = &[
            (ColumnCodec::Auto, "auto"),
            (ColumnCodec::AlpFastLanesLz4, "alp_fastlanes_lz4"),
            (ColumnCodec::AlpRdLz4, "alp_rd_lz4"),
            (ColumnCodec::PcodecLz4, "pcodec_lz4"),
            (ColumnCodec::DeltaFastLanesLz4, "delta_fastlanes_lz4"),
            (ColumnCodec::FastLanesLz4, "fastlanes_lz4"),
            (ColumnCodec::FsstLz4, "fsst_lz4"),
            (ColumnCodec::AlpFastLanesRans, "alp_fastlanes_rans"),
            (ColumnCodec::DeltaFastLanesRans, "delta_fastlanes_rans"),
            (ColumnCodec::FsstRans, "fsst_rans"),
            (ColumnCodec::Gorilla, "gorilla"),
            (ColumnCodec::DoubleDelta, "double_delta"),
            (ColumnCodec::Delta, "delta"),
            (ColumnCodec::Lz4, "lz4"),
            (ColumnCodec::Zstd, "zstd"),
            (ColumnCodec::Raw, "raw"),
        ];
        for (codec, expected) in canonical {
            assert_eq!(codec.as_str(), *expected, "codec name drift: {codec:?}");
            assert!(
                expected
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "codec name '{expected}' is not lowercase snake_case"
            );
        }
    }

    // ── ResolvedColumnCodec tests ──────────────────────────────────────────────

    /// Discriminants of ResolvedColumnCodec must exactly match those of the
    /// corresponding ColumnCodec variants so on-disk byte values are unchanged.
    #[test]
    fn resolved_codec_discriminants_match_column_codec() {
        let pairs: &[(ResolvedColumnCodec, ColumnCodec)] = &[
            (
                ResolvedColumnCodec::AlpFastLanesLz4,
                ColumnCodec::AlpFastLanesLz4,
            ),
            (ResolvedColumnCodec::AlpRdLz4, ColumnCodec::AlpRdLz4),
            (ResolvedColumnCodec::PcodecLz4, ColumnCodec::PcodecLz4),
            (
                ResolvedColumnCodec::DeltaFastLanesLz4,
                ColumnCodec::DeltaFastLanesLz4,
            ),
            (ResolvedColumnCodec::FastLanesLz4, ColumnCodec::FastLanesLz4),
            (ResolvedColumnCodec::FsstLz4, ColumnCodec::FsstLz4),
            (
                ResolvedColumnCodec::AlpFastLanesRans,
                ColumnCodec::AlpFastLanesRans,
            ),
            (
                ResolvedColumnCodec::DeltaFastLanesRans,
                ColumnCodec::DeltaFastLanesRans,
            ),
            (ResolvedColumnCodec::FsstRans, ColumnCodec::FsstRans),
            (ResolvedColumnCodec::Gorilla, ColumnCodec::Gorilla),
            (ResolvedColumnCodec::DoubleDelta, ColumnCodec::DoubleDelta),
            (ResolvedColumnCodec::Delta, ColumnCodec::Delta),
            (ResolvedColumnCodec::Lz4, ColumnCodec::Lz4),
            (ResolvedColumnCodec::Zstd, ColumnCodec::Zstd),
            (ResolvedColumnCodec::Raw, ColumnCodec::Raw),
        ];

        for &(resolved, column) in pairs {
            let resolved_bytes = zerompk::to_msgpack_vec(&resolved).unwrap();
            let column_bytes = zerompk::to_msgpack_vec(&column).unwrap();
            assert_eq!(
                resolved_bytes, column_bytes,
                "discriminant mismatch for {resolved} vs {column}"
            );

            assert_eq!(
                resolved.into_column_codec(),
                column,
                "into_column_codec mismatch for {resolved}"
            );
        }
    }

    /// Auto resolves to an error; all concrete variants resolve successfully.
    #[test]
    fn try_resolve_auto_returns_error() {
        assert!(
            matches!(
                ColumnCodec::Auto.try_resolve(),
                Err(crate::error::CodecError::UnresolvedAuto)
            ),
            "Auto.try_resolve() must return UnresolvedAuto error"
        );
    }

    #[test]
    fn try_resolve_concrete_succeeds() {
        let concretes = [
            ColumnCodec::AlpFastLanesLz4,
            ColumnCodec::Gorilla,
            ColumnCodec::Delta,
            ColumnCodec::Raw,
            ColumnCodec::Lz4,
        ];
        for codec in concretes {
            assert!(
                codec.try_resolve().is_ok(),
                "{codec} should resolve successfully"
            );
        }
    }

    #[test]
    fn resolved_codec_serde_roundtrip() {
        for codec in [
            ResolvedColumnCodec::AlpFastLanesLz4,
            ResolvedColumnCodec::AlpRdLz4,
            ResolvedColumnCodec::PcodecLz4,
            ResolvedColumnCodec::DeltaFastLanesLz4,
            ResolvedColumnCodec::FastLanesLz4,
            ResolvedColumnCodec::FsstLz4,
            ResolvedColumnCodec::AlpFastLanesRans,
            ResolvedColumnCodec::DeltaFastLanesRans,
            ResolvedColumnCodec::FsstRans,
            ResolvedColumnCodec::Gorilla,
            ResolvedColumnCodec::DoubleDelta,
            ResolvedColumnCodec::Delta,
            ResolvedColumnCodec::Lz4,
            ResolvedColumnCodec::Zstd,
            ResolvedColumnCodec::Raw,
        ] {
            let json = sonic_rs::to_string(&codec).unwrap();
            let back: ResolvedColumnCodec = sonic_rs::from_str(&json).unwrap();
            assert_eq!(codec, back, "serde roundtrip failed for {codec}");
        }
    }

    #[test]
    fn column_codec_serde_roundtrip() {
        for codec in [
            ColumnCodec::Auto,
            ColumnCodec::AlpFastLanesLz4,
            ColumnCodec::AlpRdLz4,
            ColumnCodec::PcodecLz4,
            ColumnCodec::DeltaFastLanesLz4,
            ColumnCodec::FastLanesLz4,
            ColumnCodec::FsstLz4,
            ColumnCodec::AlpFastLanesRans,
            ColumnCodec::DeltaFastLanesRans,
            ColumnCodec::FsstRans,
            ColumnCodec::Gorilla,
            ColumnCodec::DoubleDelta,
            ColumnCodec::Delta,
            ColumnCodec::Lz4,
            ColumnCodec::Zstd,
            ColumnCodec::Raw,
        ] {
            let json = sonic_rs::to_string(&codec).unwrap();
            let back: ColumnCodec = sonic_rs::from_str(&json).unwrap();
            assert_eq!(codec, back, "serde roundtrip failed for {codec}");
        }
    }

    #[test]
    fn column_statistics_i64() {
        let values = vec![10i64, 20, 30, 40, 50];
        let stats = ColumnStatistics::from_i64(&values, ResolvedColumnCodec::Delta, 12);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.min, Some(10.0));
        assert_eq!(stats.max, Some(50.0));
        assert_eq!(stats.sum, Some(150.0));
        assert_eq!(stats.uncompressed_bytes, 40);
        assert_eq!(stats.compressed_bytes, 12);
    }

    #[test]
    fn column_statistics_f64() {
        let values = vec![1.5f64, 2.5, 3.5];
        let stats = ColumnStatistics::from_f64(&values, ResolvedColumnCodec::Gorilla, 8);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, Some(1.5));
        assert_eq!(stats.max, Some(3.5));
        assert_eq!(stats.sum, Some(7.5));
    }

    #[test]
    fn column_statistics_symbols() {
        let values = vec![0u32, 1, 2, 0, 1];
        let stats = ColumnStatistics::from_symbols(&values, 3, ResolvedColumnCodec::Raw, 20);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.cardinality, Some(3));
        assert!(stats.min.is_none());
    }

    #[test]
    fn compression_ratio_calculation() {
        let stats = ColumnStatistics {
            codec: ResolvedColumnCodec::Delta,
            count: 100,
            min: None,
            max: None,
            sum: None,
            cardinality: None,
            compressed_bytes: 200,
            uncompressed_bytes: 800,
        };
        assert!((stats.compression_ratio() - 4.0).abs() < f64::EPSILON);
    }

    // ── parse_codec_name snapshot tests ───────────────────────────────────────

    #[test]
    fn parse_codec_name_all_canonical_round_trip() {
        let cases: &[(&str, ColumnCodec)] = &[
            ("auto", ColumnCodec::Auto),
            ("alp_fastlanes_lz4", ColumnCodec::AlpFastLanesLz4),
            ("alp_rd_lz4", ColumnCodec::AlpRdLz4),
            ("pcodec_lz4", ColumnCodec::PcodecLz4),
            ("delta_fastlanes_lz4", ColumnCodec::DeltaFastLanesLz4),
            ("fastlanes_lz4", ColumnCodec::FastLanesLz4),
            ("fsst_lz4", ColumnCodec::FsstLz4),
            ("alp_fastlanes_rans", ColumnCodec::AlpFastLanesRans),
            ("delta_fastlanes_rans", ColumnCodec::DeltaFastLanesRans),
            ("fsst_rans", ColumnCodec::FsstRans),
            ("gorilla", ColumnCodec::Gorilla),
            ("double_delta", ColumnCodec::DoubleDelta),
            ("delta", ColumnCodec::Delta),
            ("lz4", ColumnCodec::Lz4),
            ("zstd", ColumnCodec::Zstd),
            ("raw", ColumnCodec::Raw),
        ];
        for &(name, expected) in cases {
            let parsed = parse_codec_name(name)
                .unwrap_or_else(|e| panic!("parse_codec_name({name:?}) failed: {e}"));
            assert_eq!(parsed, expected, "parse mismatch for {name:?}");
            assert_eq!(
                parsed.as_str(),
                name,
                "as_str() round-trip mismatch for {name:?}"
            );
        }
        assert_eq!(
            cases.len(),
            16,
            "variant count changed — update parse_codec_name"
        );
    }

    #[test]
    fn parse_codec_name_rejects_non_canonical() {
        let bad: &[&str] = &[
            "LZ4",
            "Lz4",
            "GORILLA",
            "Gorilla",
            "FastLanes",
            "fast_lanes",
            "fast-lanes",
            "FSST",
            "alp-fastlanes-lz4",
            "ALP_FASTLANES_LZ4",
            "Delta_FastLanes_LZ4",
            "ZSTD",
            "RAW",
            "",
            " lz4",
            "lz4 ",
            "unknown",
            "pcodec",
        ];
        for &name in bad {
            let result = parse_codec_name(name);
            assert!(
                result.is_err(),
                "parse_codec_name({name:?}) should have been rejected but returned Ok"
            );
            let err = result.unwrap_err();
            assert!(
                matches!(err, crate::error::CodecError::UnknownCodec { .. }),
                "wrong error variant for {name:?}: {err}"
            );
        }
    }

    #[test]
    fn parse_codec_name_error_message_content() {
        let err = parse_codec_name("BadCodec").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("BadCodec"),
            "error message should contain the bad name: {msg}"
        );
        assert!(
            msg.contains("lz4"),
            "error message should list at least one valid name: {msg}"
        );
    }
}
