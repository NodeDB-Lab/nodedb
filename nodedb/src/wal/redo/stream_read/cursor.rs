// SPDX-License-Identifier: BUSL-1.1

//! The record cursor a stream reader walks one WAL segment with.
//!
//! The Control Plane reads through [`nodedb_wal::LazyWalReader`] on a
//! blocking thread. A Data Plane core reads through its io_uring cursor
//! (`crate::data::io::uring_wal_cursor`), so a read never blocks the core
//! on a `std::fs` call.

use std::path::Path;

use nodedb_wal::RecordHeader;
use nodedb_wal::crypto::KeyRing;

/// A forward cursor over the records of one segment. After a header, the
/// caller reads or skips its payload before the next header.
pub trait WalRecordCursor: Sized {
    /// Open the segment at `path`, past any preamble.
    fn open(path: &Path, ring: Option<&KeyRing>) -> crate::Result<Self>;

    /// The next record's header, `None` where the segment's records end.
    fn next_header(&mut self) -> crate::Result<Option<RecordHeader>>;

    /// The payload of `header` as plaintext, checked against its CRC.
    fn read_payload(&mut self, header: &RecordHeader) -> crate::Result<Vec<u8>>;

    /// Step over the payload of `header`.
    fn skip_payload(&mut self, header: &RecordHeader) -> crate::Result<()>;
}

impl WalRecordCursor for nodedb_wal::LazyWalReader {
    fn open(path: &Path, ring: Option<&KeyRing>) -> crate::Result<Self> {
        nodedb_wal::LazyWalReader::open(path, ring).map_err(crate::Error::Wal)
    }

    fn next_header(&mut self) -> crate::Result<Option<RecordHeader>> {
        nodedb_wal::LazyWalReader::next_header(self).map_err(crate::Error::Wal)
    }

    fn read_payload(&mut self, header: &RecordHeader) -> crate::Result<Vec<u8>> {
        nodedb_wal::LazyWalReader::read_payload(self, header).map_err(crate::Error::Wal)
    }

    fn skip_payload(&mut self, header: &RecordHeader) -> crate::Result<()> {
        nodedb_wal::LazyWalReader::skip_payload(self, header).map_err(crate::Error::Wal)
    }
}
