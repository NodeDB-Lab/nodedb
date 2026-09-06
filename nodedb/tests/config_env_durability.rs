// SPDX-License-Identifier: BUSL-1.1

//! Startup rejection of malformed WAL and checkpoint overrides.
//!
//! These variables decide write durability and WAL trimming. A fallback here
//! costs the most in this class. The boot looks configured. The difference
//! surfaces as a lost write after a crash, or as an unbounded WAL.

mod support;

use nodedb::ServerConfig;
use nodedb::config::server::apply_env_overrides;
use support::env_guard::{EnvGuard, assert_rejected};

/// Direct I/O ships on, and only an explicit opt-out turns it off. A
/// malformed value is neither an opt-out nor an opt-in. It is an operator who
/// believes they set the flag and did not.
#[test]
fn malformed_wal_direct_io_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_WAL_DIRECT_IO", "nonsense");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_WAL_DIRECT_IO",
        "nonsense",
    );
}

#[test]
fn malformed_wal_write_buffer_size_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_WAL_WRITE_BUFFER_SIZE", "1MB!");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_WAL_WRITE_BUFFER_SIZE",
        "1MB!",
    );
}

/// A buffer under the 64 KiB floor is a value the process refuses to honour.
/// Refusing it while starting anyway hands the operator the throughput profile
/// of the default, attributed to the size they set.
#[test]
fn below_minimum_wal_write_buffer_size_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_WAL_WRITE_BUFFER_SIZE", "4KiB");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_WAL_WRITE_BUFFER_SIZE",
        "4KiB",
    );
}

#[test]
fn malformed_checkpoint_interval_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_CHECKPOINT_INTERVAL_SECS", "5m");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_CHECKPOINT_INTERVAL_SECS",
        "5m",
    );
}

#[test]
fn zero_checkpoint_interval_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_CHECKPOINT_INTERVAL_SECS", "0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_CHECKPOINT_INTERVAL_SECS",
        "0",
    );
}

#[test]
fn malformed_wal_segment_target_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_WAL_SEGMENT_TARGET_MB", "64MiB");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_WAL_SEGMENT_TARGET_MB",
        "64MiB",
    );
}

#[test]
fn zero_wal_segment_target_fails_startup() {
    let _guard = EnvGuard::set("NODEDB_WAL_SEGMENT_TARGET_MB", "0");
    let mut cfg = ServerConfig::default();
    assert_rejected(
        apply_env_overrides(&mut cfg),
        "NODEDB_WAL_SEGMENT_TARGET_MB",
        "0",
    );
}
