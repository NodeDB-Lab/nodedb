// SPDX-License-Identifier: BUSL-1.1

//! Out-of-process pgwire test harness: spawns the real `nodedb` binary as a
//! subprocess and drives it over a connected `tokio_postgres::Client`.
//! Unlike `nodedb-test-support::pgwire_harness`, this depends on neither the
//! `nodedb` library nor `nodedb-test-support`, so every file under
//! `tests/wire/cases/` compiles into one test binary.

mod config_toml;
mod connect;
mod lifecycle;
mod process;
mod query;
mod resp_user;
mod types;

pub mod insert_returning_engines;
pub mod raw_pgwire;

#[path = "../../support/mod.rs"]
mod support;
pub use support::resp_client;

// Mirrors the `TestServer` API surface `pgwire_harness` exposes, so porting a
// test file is an import swap.
pub(crate) use config_toml::TEST_BACKUP_KEK;
pub use types::TestServer;
