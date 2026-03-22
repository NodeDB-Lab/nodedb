//! OPFS `StorageBackend` for redb — persistent storage in the browser.
//!
//! Translates redb's `StorageBackend` trait (flat byte-range I/O) to
//! the browser's Origin Private File System `FileSystemSyncAccessHandle`.
//!
//! **Important:** OPFS `SyncAccessHandle` is only available inside a
//! Web Worker. The main UI thread cannot use this backend — it must
//! communicate with the Worker via `postMessage`.
//!
//! Usage:
//! ```js
//! // In a Web Worker:
//! const db = await NodeDbLiteWasm.openPersistent("mydb", 1n);
//! ```

use std::io;
use std::sync::Mutex;

use js_sys::Uint8Array;
use web_sys::FileSystemSyncAccessHandle;

/// OPFS storage backend for redb.
///
/// Wraps a `FileSystemSyncAccessHandle` from the browser's OPFS.
/// All operations are synchronous (required by redb and by OPFS SyncAccessHandle).
#[derive(Debug)]
pub struct OpfsBackend {
    handle: Mutex<FileSystemSyncAccessHandle>,
}

// SAFETY: WASM is single-threaded. The Mutex is purely for trait compliance.
unsafe impl Send for OpfsBackend {}
unsafe impl Sync for OpfsBackend {}

impl OpfsBackend {
    /// Create a backend from an OPFS `FileSystemSyncAccessHandle`.
    ///
    /// The handle must be obtained via the OPFS API in a Web Worker:
    /// ```js
    /// const root = await navigator.storage.getDirectory();
    /// const fileHandle = await root.getFileHandle("mydb.redb", { create: true });
    /// const syncHandle = await fileHandle.createSyncAccessHandle();
    /// ```
    pub fn new(handle: FileSystemSyncAccessHandle) -> Self {
        Self {
            handle: Mutex::new(handle),
        }
    }
}

impl redb::StorageBackend for OpfsBackend {
    fn len(&self) -> Result<u64, io::Error> {
        let handle = self
            .handle
            .lock()
            .map_err(|_| io::Error::other("OPFS handle lock poisoned"))?;
        let size = handle
            .get_size()
            .map_err(|e| io::Error::other(format!("OPFS getSize failed: {e:?}")))?;
        Ok(size as u64)
    }

    fn read(&self, offset: u64, len: usize) -> Result<Vec<u8>, io::Error> {
        let handle = self
            .handle
            .lock()
            .map_err(|_| io::Error::other("OPFS handle lock poisoned"))?;

        let buffer = Uint8Array::new_with_length(len as u32);
        let opts = web_sys::FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);

        let bytes_read = handle
            .read_with_buffer_source_and_options(&buffer, &opts)
            .map_err(|e| io::Error::other(format!("OPFS read failed: {e:?}")))?
            as usize;

        let mut result = vec![0u8; bytes_read];
        buffer.slice(0, bytes_read as u32).copy_to(&mut result);
        Ok(result)
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        let handle = self
            .handle
            .lock()
            .map_err(|_| io::Error::other("OPFS handle lock poisoned"))?;

        handle
            .truncate_with_u32(len as u32)
            .map_err(|e| io::Error::other(format!("OPFS truncate failed: {e:?}")))
    }

    fn sync_data(&self, _eventual: bool) -> Result<(), io::Error> {
        let handle = self
            .handle
            .lock()
            .map_err(|_| io::Error::other("OPFS handle lock poisoned"))?;

        handle
            .flush()
            .map_err(|e| io::Error::other(format!("OPFS flush failed: {e:?}")))
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let handle = self
            .handle
            .lock()
            .map_err(|_| io::Error::other("OPFS handle lock poisoned"))?;

        let buffer = Uint8Array::from(data);
        let opts = web_sys::FileSystemReadWriteOptions::new();
        opts.set_at(offset as f64);

        handle
            .write_with_buffer_source_and_options(&buffer, &opts)
            .map_err(|e| io::Error::other(format!("OPFS write failed: {e:?}")))?;

        Ok(())
    }
}
