// SPDX-License-Identifier: Apache-2.0

pub mod breadth;
pub mod codec;
pub mod codecs;
pub mod gating;
pub mod pipeline;
pub mod recall;
pub mod sidecar;
pub mod types;

pub use breadth::{DEFAULT_OVERSAMPLE, SearchBreadth, rerank_pool};
pub use codec::{CodecName, PreparedQuery, RerankCodec};
pub use codecs::{BinaryRerank, OpqRerank, Sq8Rerank, TernaryRerank};
pub use gating::{IndexShape, validate_options};
pub use pipeline::rerank;
pub use recall::recall_scale;
pub use sidecar::CodecSidecar;
pub use types::{Candidate, Ranked, RerankError};
