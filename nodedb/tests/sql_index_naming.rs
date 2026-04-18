//! Integration tests for `CREATE INDEX` on document collections:
//! naming, registration, uniqueness enforcement, planner visibility,
//! partial indexes, backfill, and EXPLAIN plan-shape.
//!
//! Split across `tests/sql_index/` files to stay under the 500-line
//! hard limit from `CLAUDE.md`. This file is the test-binary entry
//! point; each sub-file is included via `#[path = ...]` so they share
//! one compiled test crate (and one pgwire-harness boot per test).

mod common;

#[path = "sql_index/helpers.rs"]
mod helpers;

#[path = "sql_index/naming.rs"]
mod naming;

#[path = "sql_index/planner.rs"]
mod planner;

#[path = "sql_index/backfill.rs"]
mod backfill;

#[path = "sql_index/partial.rs"]
mod partial;
