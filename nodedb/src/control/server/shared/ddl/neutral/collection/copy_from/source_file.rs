// SPDX-License-Identifier: BUSL-1.1

//! The server file a `COPY ... FROM '<path>'` reads, opened once.
//!
//! The open is non-blocking, so a FIFO or a device never stalls it. Every
//! check then runs on the open handle: the file type, the size cap, and
//! each read. No second path lookup can resolve to another file.

use nodedb_mem::{MemError, ReservationToken, ScopedMemory};
use tokio::io::AsyncReadExt;

use crate::control::server::shared::ddl::result::DdlError;

/// Maximum file size accepted for COPY FROM (16 GiB).
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// A whole file in memory and the reservation that accounts for it. The
/// bytes release their reservation when this drops.
#[derive(Debug)]
pub(crate) struct ReservedFile {
    pub(crate) bytes: Vec<u8>,
    _reservation: ReservationToken,
}

/// An open regular file under the size cap.
pub(crate) struct CopySourceFile {
    file: tokio::fs::File,
    path: String,
    /// The size `fstat` reported at open.
    len: u64,
    /// Bytes `read_some` returned so far.
    consumed: u64,
}

impl CopySourceFile {
    /// Open `path` and check it through the handle. A missing or unreadable
    /// path and a non-regular file are `58030`. A file over the cap is
    /// `54000`.
    pub(crate) async fn open(path: &str) -> Result<Self, DdlError> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)
            .await
            .map_err(|e| DdlError::new("58030", format!("COPY: cannot open '{path}': {e}")))?;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| DdlError::new("58030", format!("COPY: cannot stat '{path}': {e}")))?;
        if !metadata.is_file() {
            return Err(DdlError::new(
                "58030",
                format!(
                    "COPY: '{path}' is not a regular file; \
                     COPY reads regular files only, never a FIFO, device, or directory"
                ),
            ));
        }
        let len = metadata.len();
        if len > MAX_FILE_BYTES {
            return Err(too_large(path, len));
        }
        // A read of a regular file never blocks, so `O_NONBLOCK` changes
        // nothing past the open.
        Ok(Self {
            file,
            path: path.to_owned(),
            len,
            consumed: 0,
        })
    }

    /// The open handle as a blocking file, for a decoder that runs on a
    /// blocking thread. The handle is the one every open-time check ran on.
    pub(crate) async fn into_std(self) -> std::fs::File {
        self.file.into_std().await
    }

    /// Read the next bytes into `buf`, `0` at the end of the file. A file
    /// that grew past the cap after the open is `54000`.
    pub(crate) async fn read_some(&mut self, buf: &mut [u8]) -> Result<usize, DdlError> {
        let read = self.file.read(buf).await;
        let n = read.map_err(|e| self.read_error(e))?;
        self.consumed = self.consumed.saturating_add(n as u64);
        if self.consumed > MAX_FILE_BYTES {
            return Err(too_large(&self.path, self.consumed));
        }
        Ok(n)
    }

    /// Read the whole file through the handle, after `memory` reserves its
    /// size. A refused reservation is `53200`. A file that grew after the
    /// open is `54000`, since its bytes exceed the reservation.
    pub(crate) async fn read_all(
        &mut self,
        memory: &ScopedMemory,
    ) -> Result<ReservedFile, DdlError> {
        // One spare byte lets the read find the end without growing the buffer.
        // A read that fills the spare byte means the file grew.
        let capacity = usize::try_from(self.len.saturating_add(1)).unwrap_or(usize::MAX);
        let reservation = memory
            .reserve(capacity)
            .map_err(|refusal| over_memory(&self.path, self.len, &refusal))?;
        let mut bytes = Vec::with_capacity(capacity);
        let read = (&mut self.file)
            .take(self.len.saturating_add(1))
            .read_to_end(&mut bytes)
            .await;
        read.map_err(|e| self.read_error(e))?;
        if bytes.len() as u64 > self.len {
            return Err(grew(&self.path, self.len));
        }
        Ok(ReservedFile {
            bytes,
            _reservation: reservation,
        })
    }

    /// The `58030` error of a failed read.
    fn read_error(&self, e: std::io::Error) -> DdlError {
        DdlError::new("58030", format!("COPY: cannot read '{}': {e}", self.path))
    }
}

fn too_large(path: &str, len: u64) -> DdlError {
    DdlError::new(
        "54000",
        format!(
            "COPY: file '{path}' is {len} bytes, exceeds limit of {MAX_FILE_BYTES} bytes; \
             split it and run one COPY per part"
        ),
    )
}

/// The `53200` refusal of a file the memory governor will not hold.
fn over_memory(path: &str, len: u64, refusal: &MemError) -> DdlError {
    DdlError::new(
        "53200",
        format!(
            "COPY: file '{path}' is {len} bytes and must fit in memory to load: {refusal}; \
             split it and run one COPY per part, or raise the memory limit"
        ),
    )
}

/// The `54000` refusal of a file that grew after the open.
fn grew(path: &str, len: u64) -> DdlError {
    DdlError::new(
        "54000",
        format!(
            "COPY: file '{path}' grew past its {len} bytes at open; \
             run the COPY again once the file stops growing"
        ),
    )
}

#[cfg(test)]
mod tests {
    use nodedb_mem::EngineId;
    use std::io::Write;

    use super::super::row_memory::test_support::memory_of;
    use super::*;

    const BUDGET: usize = 4096;

    fn memory() -> ScopedMemory {
        memory_of(BUDGET)
    }

    fn file_of(len: usize) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("create a temp file");
        file.write_all(&vec![b'x'; len]).expect("write the file");
        file.flush().expect("flush the file");
        file
    }

    async fn open(file: &tempfile::NamedTempFile) -> CopySourceFile {
        let path = file.path().to_str().expect("a UTF-8 temp path");
        CopySourceFile::open(path).await.expect("open the file")
    }

    #[tokio::test]
    async fn a_read_holds_a_reservation_until_the_bytes_drop() {
        let memory = memory();
        let file = file_of(1000);
        let read = open(&file).await.read_all(&memory).await.expect("read");
        assert_eq!(read.bytes.len(), 1000);
        let budget = memory.governor().budget(EngineId::Query);
        assert_eq!(budget.allocated(), 1001);
        drop(read);
        assert_eq!(budget.allocated(), 0);
    }

    #[tokio::test]
    async fn a_file_over_the_budget_is_refused_before_the_read() {
        let memory = memory();
        let file = file_of(BUDGET + 1);
        let error = open(&file)
            .await
            .read_all(&memory)
            .await
            .expect_err("over the budget");
        assert_eq!(error.sqlstate, "53200");
        let size = format!("{} bytes", BUDGET + 1);
        let limit = format!("limit: {BUDGET} bytes");
        assert!(error.message.contains(&size), "{}", error.message);
        assert!(error.message.contains(&limit), "{}", error.message);
        assert!(error.message.contains("split it"), "{}", error.message);
        assert_eq!(memory.governor().budget(EngineId::Query).allocated(), 0);
    }

    #[tokio::test]
    async fn two_reads_share_one_bound() {
        let memory = memory();
        let file = file_of(BUDGET / 2 + 1);
        let first = open(&file).await.read_all(&memory).await.expect("first");
        let error = open(&file)
            .await
            .read_all(&memory)
            .await
            .expect_err("the second is over the shared budget");
        assert_eq!(error.sqlstate, "53200");
        drop(first);
        assert!(open(&file).await.read_all(&memory).await.is_ok());
    }

    #[tokio::test]
    async fn a_file_that_grew_after_the_open_is_refused() {
        let memory = memory();
        let mut file = file_of(100);
        let mut source = open(&file).await;
        file.write_all(&[b'y'; 100]).expect("grow the file");
        file.flush().expect("flush the file");
        let error = source.read_all(&memory).await.expect_err("the file grew");
        assert_eq!(error.sqlstate, "54000");
        assert!(error.message.contains("grew"), "{}", error.message);
    }

    #[test]
    fn a_file_over_the_size_cap_names_the_next_action() {
        let error = too_large("/data/x.csv", MAX_FILE_BYTES + 1);
        assert_eq!(error.sqlstate, "54000");
        assert!(error.message.contains("split it"), "{}", error.message);
    }
}
