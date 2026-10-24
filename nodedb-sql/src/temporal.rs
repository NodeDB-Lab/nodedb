// SPDX-License-Identifier: Apache-2.0

//! Bitemporal scope types threaded through the SQL planner.
//!
//! A `TemporalScope` captures the user-facing `FOR SYSTEM_TIME AS OF <ms>` /
//! `FOR VALID_TIME CONTAINS <ms>` / `FOR VALID_TIME FROM <ms> TO <ms>`
//! qualifiers. It is extracted from raw SQL by
//! [`crate::parser::preprocess::temporal`] before sqlparser-rs sees the
//! statement, threaded through `plan_sql` into each `SqlPlan::Scan`, and
//! finally honored by the engine's Data Plane handler.
//!
//! All timestamps are **milliseconds since Unix epoch**. The edge store and
//! the document versioned store key versions by system-time ordinal, so a
//! system-time cut converts via `nodedb_types::ms_to_ordinal_upper`. A
//! `TemporalScope::default()` value is equivalent to "current state"
//! (no temporal qualifier) — every scan site must construct one even when
//! the query is not temporal.

use nodedb_types::SystemTimeScope;

/// Valid-time qualifier attached to a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidTime {
    /// No `FOR VALID_TIME` clause — every version qualifies.
    #[default]
    Any,
    /// `FOR VALID_TIME CONTAINS <ms>` — version must satisfy
    /// `valid_from_ms <= ms < valid_until_ms`.
    At(i64),
    /// `FOR VALID_TIME FROM <lo_ms> TO <hi_ms>` — version's valid
    /// interval must overlap `[lo_ms, hi_ms)`.
    Range(i64, i64),
}

/// Combined bitemporal qualifier for a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TemporalScope {
    /// System-time selection. `Current` = latest committed version;
    /// `AsOf(ms)` = point-in-time; `AllVersions` = every system-time version
    /// (`AS OF SYSTEM TIME NULL`, audit-log semantics).
    pub system_time: SystemTimeScope,
    /// Valid-time qualifier.
    pub valid_time: ValidTime,
}

impl TemporalScope {
    /// True when either temporal axis is qualified.
    pub fn is_temporal(&self) -> bool {
        self.system_time.is_temporal() || !matches!(self.valid_time, ValidTime::Any)
    }

    /// Refuse a temporal scope on `collection` unless it is bitemporal.
    pub fn require_bitemporal(&self, collection: &str, bitemporal: bool) -> crate::Result<()> {
        if self.is_temporal() && !bitemporal {
            return Err(crate::SqlError::Unsupported {
                detail: format!(
                    "FOR SYSTEM_TIME / FOR VALID_TIME requires a bitemporal collection; \
                     '{collection}' was not created WITH bitemporal = true"
                ),
            });
        }
        Ok(())
    }
}
