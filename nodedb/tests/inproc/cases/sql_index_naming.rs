// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `CREATE INDEX` on document collections:
//! naming, registration, uniqueness enforcement, planner visibility,
//! partial indexes, backfill, and EXPLAIN plan-shape.

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

#[path = "sql_index/update_reindex.rs"]
mod update_reindex;

#[path = "sql_index/sorted.rs"]
mod sorted;
