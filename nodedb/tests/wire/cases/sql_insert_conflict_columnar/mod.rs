// SPDX-License-Identifier: BUSL-1.1

//! INSERT conflict semantics for columnar-family engines.
//!
//! Columnar storage is OLAP-shaped (append-only segments, zonemap pruning),
//! but `PRIMARY KEY` appears in the ANSI SQL surface and must mean the same
//! thing across every engine NodeDB ships. The resolution is to treat PK on
//! a columnar collection as both a sort key (enforced at segment flush) and
//! a logical uniqueness constraint enforced via a sparse PK index plus
//! positional deletes: duplicate INSERTs tombstone the prior row rather
//! than raising 23505, and readers skip tombstoned row-ids. A `PRIMARY KEY`
//! declared on any column other than `id` or `document_id` refuses a
//! duplicate with 23505 instead.
//!
//! Spatial extends columnar and inherits the same semantics. Timeseries is
//! a different profile (append-only, time-keyed) and is not covered here.

mod columnar;
mod declared_key;
mod spatial;
