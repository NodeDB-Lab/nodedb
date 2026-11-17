// SPDX-License-Identifier: BUSL-1.1

//! Read a chunked redo's stream back from this WAL.
//!
//! The reads here run on the calling thread with blocking file reads. A
//! Control Plane or Event Plane caller runs them where it already reads the
//! WAL. A Data Plane core reads through its own io_uring cursor instead (see
//! `crate::data::io::uring_wal_cursor`).

use super::core::WalManager;
use crate::wal::redo::frame::ChunkSource;
use crate::wal::redo::{RedoStreamReader, RedoStreamRef, WalReadSource, read_stream_content};

impl WalManager {
    /// The directory and key ring a reader outside the manager opens.
    pub fn read_source(&self) -> WalReadSource {
        WalReadSource {
            dir: self.wal_dir.clone(),
            ring: self.encryption_ring.clone(),
        }
    }

    /// The whole content of `reference` read from disk, `None` when its
    /// chunks lie below the oldest retained segment.
    pub fn fetch_stream_content(
        &self,
        reference: &RedoStreamRef,
    ) -> crate::Result<Option<Vec<u8>>> {
        read_stream_content::<nodedb_wal::LazyWalReader>(&self.read_source(), reference)
    }
}

/// Every chunk of `reference` in the WAL `source` names, read back with
/// blocking file reads and checked against the reference.
pub fn read_redo_stream(
    source: &WalReadSource,
    reference: &RedoStreamRef,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut reader =
        RedoStreamReader::<nodedb_wal::LazyWalReader>::open(source, reference.clone())?;
    let mut chunks = Vec::with_capacity(reference.chunks.len());
    while let Some(chunk) = reader.next_chunk()? {
        chunks.push(chunk);
    }
    Ok(chunks)
}
