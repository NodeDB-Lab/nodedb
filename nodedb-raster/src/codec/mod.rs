// SPDX-License-Identifier: Apache-2.0

//! TIFF chunk codecs and predictors, all pure Rust.

pub mod compression;
pub mod deflate;
pub mod error;
pub mod jpeg;
pub mod lerc;
pub mod lerc_blob;
pub mod lzw;
pub mod packbits;
pub mod predictor;
pub mod zstd;

pub use compression::{Compression, LercWrap};
pub use error::CodecError;
pub use predictor::Predictor;
