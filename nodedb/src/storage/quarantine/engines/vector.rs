// SPDX-License-Identifier: BUSL-1.1

//! Quarantine wrapper for vector NDVS segment reads.
//!
//! `MmapVectorSegment::open_with_policy` returns `VectorError`. Structural
//! corruption (magic, version, size mismatch, CRC32C mismatch) surfaces as
//! `VectorError::SegmentIo` wrapping an `io::Error` of kind `InvalidData`.
//! Every other variant is not corruption and does not quarantine. Examples
//! are budget exhaustion and a plain I/O error such as a missing file.

use std::path::Path;
use std::sync::Arc;

use nodedb_mem::ScopedMemory;
use nodedb_vector::error::VectorError;
use nodedb_vector::mmap_segment::{MmapVectorSegment, VectorSegmentDropPolicy};

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::key::{CollectionScope, SegmentKey};
use crate::storage::quarantine::registry::QuarantineRegistry;

/// Arguments for [`open_vector_segment_with_quarantine`].
pub struct VectorQuarantineOpen<'a> {
    pub registry: &'a Arc<QuarantineRegistry>,
    pub path: &'a Path,
    pub policy: VectorSegmentDropPolicy,
    pub scope: &'a CollectionScope,
    pub segment_id: &'a str,
    pub memory: &'a ScopedMemory,
}

/// Attempt to open a vector NDVS segment, routing CRC-class corruption
/// through the quarantine registry. The segment file is never renamed.
pub fn open_vector_segment_with_quarantine(
    args: VectorQuarantineOpen,
) -> Result<MmapVectorSegment, VectorOrQuarantine> {
    let VectorQuarantineOpen {
        registry,
        path,
        policy,
        scope,
        segment_id,
        memory,
    } = args;
    let key = SegmentKey::Vector {
        scope: scope.clone(),
        segment_id: segment_id.to_string(),
    };

    match MmapVectorSegment::open_with_policy(path, memory, policy) {
        Ok(seg) => {
            registry.record_success(&key);
            Ok(seg)
        }
        Err(VectorError::SegmentIo(e)) if e.kind() == std::io::ErrorKind::InvalidData => {
            registry
                .record_failure(key, &e.to_string())
                .map_err(VectorOrQuarantine::Quarantined)?;
            Err(VectorOrQuarantine::Vector(VectorError::SegmentIo(e)))
        }
        Err(e) => Err(VectorOrQuarantine::Vector(e)),
    }
}

/// Error type returned by `open_vector_segment_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum VectorOrQuarantine {
    #[error(transparent)]
    Vector(#[from] VectorError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}
