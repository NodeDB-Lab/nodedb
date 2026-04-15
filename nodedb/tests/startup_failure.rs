//! Integration test: nodedb binary exits non-zero when startup fails.
//!
//! The test spawns the real `nodedb` binary (built in the test profile) with
//! a corrupted WAL segment in the data directory. The binary must detect the
//! corruption and exit with a non-zero status within 5 seconds.
//!
//! WAL segment naming: `wal-{lsn:020}.seg` under `<data_dir>/wal/`.

use std::fs;
use std::time::Duration;

/// The WAL segment filename for LSN 0 (the first segment a fresh node writes).
const SEGMENT_NAME: &str = "wal-00000000000000000000.seg";

/// Corrupt WAL content that looks like a valid page header but has a bad CRC.
/// The WAL reader validates CRC32C on every page, so this should cause an error.
const CORRUPT_CONTENT: &[u8] = b"NDBS\x00\x01\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00JUNK_CORRUPT_WAL_PAYLOAD_TO_FORCE_FAILURE";

#[test]
fn nodedb_exits_nonzero_on_corrupted_wal() {
    // Locate the nodedb binary. In nextest / cargo test the binary is compiled
    // alongside the test artifacts; `CARGO_BIN_EXE_nodedb` is set by cargo.
    let bin = env!("CARGO_BIN_EXE_nodedb");

    // Build a temporary data directory with a corrupt WAL segment.
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let wal_dir = data_dir.join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    fs::write(wal_dir.join(SEGMENT_NAME), CORRUPT_CONTENT).expect("write corrupt segment");

    // Spawn the nodedb binary pointing at the corrupted data directory.
    let mut child = std::process::Command::new(bin)
        .env("NODEDB_DATA_DIR", &data_dir)
        // Silence logs so the test output is clean.
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to spawn nodedb binary");

    // Wait up to 5 seconds for the binary to exit.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let status = loop {
        match child.try_wait().expect("try_wait failed") {
            Some(s) => break s,
            None => {
                if std::time::Instant::now() >= deadline {
                    child.kill().ok();
                    panic!("nodedb did not exit within 5s after corrupt WAL");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    };

    assert!(
        !status.success(),
        "nodedb exited with success (0) despite corrupted WAL — expected non-zero exit"
    );
}
