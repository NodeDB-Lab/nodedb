// SPDX-License-Identifier: BUSL-1.1

//! Cross-cluster snapshot transfer for mirror bootstrap.
//!
//! A new mirror database takes one consistent capture of its source before it
//! tails the source's logs. This module provides:
//!
//! - [`CrossClusterSnapshotEnvelope`]: the wire envelope of one chunk. It
//!   carries `source_cluster_id`, `source_database_id`, `snapshot_lsn` and
//!   `total_crc32c`, so the receiver can check provenance and detect bit-rot.
//!
//! - [`MirrorBootstrapReceiver`]: the mirror-side chunk accumulator. It writes
//!   chunks to a `.partial` file, keeps a running CRC32C, and checks it against
//!   `total_crc32c` on the final chunk. It reports
//!   `MirrorStatus::Bootstrapping { bytes_done, bytes_total }` through a
//!   callback every `PROGRESS_REPORT_CHUNK_BYTES` (~1 MiB). The transfer ends
//!   short of `Following`: the caller re-issues the file's rows first.
//!
//! # Resume semantics
//!
//! A transfer interrupted before the re-issue starts over from offset 0, which
//! discards any existing `.partial` file. This avoids re-hashing a partial file
//! to rebuild a running CRC, at the cost of one extra transfer.

mod envelope;
mod receiver;

pub use envelope::{
    BootstrapChunkOutcome, CrossClusterSnapshotEnvelope, PROGRESS_REPORT_CHUNK_BYTES,
    ProgressCallback,
};
pub use receiver::MirrorBootstrapReceiver;
