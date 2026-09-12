// SPDX-License-Identifier: BUSL-1.1

//! INSERT conflict semantics for columnar-family engines.
//!
//! A declared `PRIMARY KEY` means uniqueness, on every engine and every
//! column. A duplicate raises 23505 whatever the column is named. An
//! explicit `UPSERT` or `ON CONFLICT DO UPDATE` still merges into the
//! existing row instead of refusing it. A collection with no declared
//! primary key mints a fresh identity per row and never dedups.
//!
//! Spatial extends columnar and inherits the same semantics. Timeseries is
//! a different profile (append-only, time-keyed) and is not covered here.

mod columnar;
mod declared_key;
mod spatial;
