// SPDX-License-Identifier: BUSL-1.1

//! `NODEDB_WAL_DIRECT_IO` / `NODEDB_WAL_WRITE_BUFFER_SIZE` overrides.

use crate::config::server::ServerConfig;

use super::super::memory_size::parse_memory_size;
use super::super::parse::parse_bool_lenient;
use super::super::table::EnvRow;
use crate::config::server::domain::MIN_WAL_WRITE_BUFFER_BYTES;

fn apply_direct_io(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    config.tuning.wal.direct_io = parse_bool_lenient(raw)?;
    Ok(())
}

fn apply_write_buffer_size(config: &mut ServerConfig, raw: &str) -> Result<(), &'static str> {
    let bytes = parse_memory_size(raw).map_err(|_| "a memory size of at least 64KiB")?;
    if bytes < MIN_WAL_WRITE_BUFFER_BYTES {
        return Err("a memory size of at least 64KiB");
    }
    config.tuning.wal.write_buffer_size = bytes;
    Ok(())
}

pub(in super::super) const ROWS: &[EnvRow] = &[
    EnvRow {
        name: "NODEDB_WAL_DIRECT_IO",
        apply: apply_direct_io,
        redact: false,
    },
    EnvRow {
        name: "NODEDB_WAL_WRITE_BUFFER_SIZE",
        apply: apply_write_buffer_size,
        redact: false,
    },
];

#[cfg(test)]
mod tests {
    use super::super::super::apply_env_overrides;
    use super::*;

    /// Direct I/O is the shipped default. Only an explicit opt-out turns it
    /// off.
    #[test]
    fn env_wal_direct_io_override() {
        unsafe { std::env::set_var("NODEDB_WAL_DIRECT_IO", "false") };
        let mut cfg = ServerConfig::default();
        apply_env_overrides(&mut cfg).expect("a valid direct-io toggle must apply");
        assert!(!cfg.tuning.wal.direct_io);
        unsafe { std::env::remove_var("NODEDB_WAL_DIRECT_IO") };
    }
}
