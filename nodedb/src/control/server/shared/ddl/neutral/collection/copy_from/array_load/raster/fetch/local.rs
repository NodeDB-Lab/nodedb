// SPDX-License-Identifier: BUSL-1.1

//! `LocalFile`: positional reads of a server file through the handle every
//! open-time check ran on. The caller checks the superuser privilege and
//! the path before it opens the file.

use std::os::unix::fs::FileExt as _;
use std::sync::Arc;

use nodedb_raster::ByteRange;

use super::error::FetchError;
use super::traits::{RangeFetch, fetch_bounded};

/// One server file, read with `pread` on Tokio's blocking pool.
pub struct LocalFile {
    file: Arc<std::fs::File>,
    name: String,
}

impl LocalFile {
    /// Read `file`, named `name` in messages.
    pub fn new(file: std::fs::File, name: impl Into<String>) -> Self {
        Self {
            file: Arc::new(file),
            name: name.into(),
        }
    }

    /// The bytes of `range`, fewer at the end of the file.
    pub async fn fetch_one(&self, range: ByteRange) -> Result<Option<Vec<u8>>, FetchError> {
        let len = usize::try_from(range.len()).map_err(|_| FetchError::Protocol {
            source_name: self.name.clone(),
            what: format!(
                "byte range {}..{} exceeds the address space",
                range.start, range.end
            ),
        })?;
        let file = Arc::clone(&self.file);
        let read = tokio::task::spawn_blocking(move || pread_range(&file, range.start, len))
            .await
            .map_err(|e| FetchError::Task(e.to_string()))?;
        read.map(Some).map_err(|error| FetchError::Io {
            source_name: self.name.clone(),
            error,
        })
    }
}

/// Read `len` bytes at `start`, fewer at the end of the file.
fn pread_range(file: &std::fs::File, start: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let at = start.saturating_add(filled as u64);
        match file.read_at(&mut buf[filled..], at) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    buf.truncate(filled);
    Ok(buf)
}

impl RangeFetch for LocalFile {
    async fn fetch(&self, ranges: &[ByteRange]) -> Result<Vec<Option<Vec<u8>>>, FetchError> {
        fetch_bounded(ranges, |range| self.fetch_one(range)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_raster::ObjectId;

    #[tokio::test]
    async fn reads_clip_at_the_end_of_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("a.bin");
        std::fs::write(&path, [1u8, 2, 3, 4, 5]).expect("write");
        let local = LocalFile::new(std::fs::File::open(&path).expect("open"), "a.bin");
        let r = |start, end| ByteRange::new(ObjectId(0), start, end);
        let got = local
            .fetch(&[r(1, 3), r(3, 9), r(7, 9)])
            .await
            .expect("reads");
        assert_eq!(got, vec![Some(vec![2, 3]), Some(vec![4, 5]), Some(vec![])]);
    }
}
