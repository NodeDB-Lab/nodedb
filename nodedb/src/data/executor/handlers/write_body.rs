// SPDX-License-Identifier: BUSL-1.1

//! The body one write stores: typeguards, then generated columns.
//!
//! Every write path derives its stored body through
//! [`materialize_write_body`], so a typeguard and a generated column are
//! applied the same way on every write form: SQL and native inserts, puts,
//! batches, UPSERT, UPDATE, MERGE, transaction staging and redo replay.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::enforcement::typeguard::{GuardScope, enforce_type_guards};
use crate::engine::document::store::CollectionConfig;

use super::generated::evaluate_generated_columns;

/// Derive the body one write stores.
///
/// Typeguards run first, so a generated column can read a field a guard
/// injected. A CRDT collection's guards are skipped here: every replica merges
/// its state, so the server injects nothing into it. The CRDT write handlers
/// validate its merged row against the guards instead.
pub fn materialize_write_body(
    config: &CollectionConfig,
    doc: &mut serde_json::Value,
    scope: GuardScope<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), ErrorCode> {
    if !config.crdt_enabled {
        enforce_type_guards(&config.type_guards, doc, scope, crs)?;
    }
    evaluate_generated_columns(doc, &config.enforcement.generated_columns, crs)
}

/// Whether a write to `config` rewrites its body: it carries typeguards or
/// generated columns.
pub fn rewrites_body(config: &CollectionConfig) -> bool {
    (!config.crdt_enabled && !config.type_guards.is_empty())
        || !config.enforcement.generated_columns.is_empty()
}

/// The field names an UPDATE assigns.
pub fn assigned_names<V>(updates: &[(String, V)]) -> Vec<String> {
    updates.iter().map(|(name, _)| name.clone()).collect()
}
