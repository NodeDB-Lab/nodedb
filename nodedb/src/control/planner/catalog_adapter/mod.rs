// SPDX-License-Identifier: BUSL-1.1

//! Implements `nodedb_sql::SqlCatalog` for Origin.
//!
//! The adapter acquires a descriptor lease at plan time. The
//! lease is what binds an in-flight query to the descriptor
//! version it was planned against: while the lease is held, no
//! DDL can bump the descriptor (drain blocks until the lease
//! releases or expires). This is the mechanism that closes the
//! planner-side race between "read descriptor" and "execute plan".
//!
//! Lease ownership is per-node, not per-query. Every call to
//! `get_collection` goes through `force-refresh the lease` via
//! the `lease::acquire_lease` fast path: if a valid lease
//! already exists, returns instantly with zero raft round-trips.
//! The first query on a cold collection pays one raft round-trip
//! to acquire; subsequent queries within the lease window read
//! from the in-memory cache. The renewal loop keeps held leases
//! alive indefinitely.
//!
//! **Drain interaction**: if the descriptor is being drained at
//! the version we read, `acquire_descriptor_lease` returns
//! `Err::RetryableSchemaChanged`. We surface that as
//! `SqlCatalogError::RetryableSchemaChanged`, which the statement
//! entry points catch and retry (up to the retry budget) together
//! with the post-planning lease acquisition, which can observe the
//! same drain. On any other lease-acquire failure we log and
//! proceed with the descriptor we read — lease acquisition is
//! best-effort; the planner's primary job is still to produce
//! a plan, and a transient lease glitch should not break user
//! queries.

mod adapter;
mod sequence_access;
mod sql_catalog_impl;
mod type_convert;

pub use adapter::OriginCatalog;
