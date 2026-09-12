// SPDX-License-Identifier: BUSL-1.1

//! Period lock enforcement: reject writes to rows in closed/locked fiscal periods.
//!
//! On each write (INSERT, UPDATE, DELETE), extracts the period column value from
//! the document, looks up the period status in the reference collection, and
//! rejects the write if the status is not in the allowed set.

use sonic_rs;

use crate::bridge::envelope::ErrorCode;
use crate::engine::sparse::btree::SparseEngine;
use nodedb_physical::physical_plan::{PeriodLockConfig, ResolvedSumTarget, resolved_sum_surrogate};
use nodedb_types::StorageKey;

/// Check whether a write is allowed given the period lock configuration.
///
/// `doc_bytes` is the document being written (INSERT/UPDATE) or the existing
/// document (DELETE). The period column value is extracted from `doc_bytes`
/// and resolved to the reference collection's row through `resolved_targets`.
///
/// A declared primary-key VALUE resolves to a row's surrogate through the
/// Control Plane's pk → surrogate catalog, which is off-limits to the Data
/// Plane. The Control Plane resolves `config.ref_pk`'s value at plan time and
/// carries the reference row's surrogate in `resolved_targets`, keyed by
/// `(config.ref_table, period value)` — the same slot and lookup every other
/// cross-collection resolution in this codebase uses.
///
/// Returns `Ok(())` if the write is allowed, or `Err(PeriodLocked)` if the
/// period is closed, locked, or names no row the Control Plane could resolve.
pub fn check_period_lock(
    sparse: &SparseEngine,
    database_id: u64,
    tid: u64,
    collection: &str,
    doc_bytes: &[u8],
    config: &PeriodLockConfig,
    resolved_targets: &[ResolvedSumTarget],
) -> Result<(), ErrorCode> {
    // Extract the period column value from the document.
    let period_value = extract_period_value(doc_bytes, &config.period_column);
    let Some(period_key) = period_value else {
        // Period column not present in document — skip check (schemaless collections
        // may have rows without the period column).
        return Ok(());
    };

    // Resolve the period value to the reference row's surrogate, exactly as
    // the Control Plane resolved it at plan time. A period value with no
    // binding in `resolved_targets` names no reference row and is treated as
    // an unknown period below.
    let Some(surrogate) = resolved_sum_surrogate(resolved_targets, &config.ref_table, &period_key)
    else {
        return Err(ErrorCode::PeriodLocked {
            collection: collection.to_string(),
        });
    };
    let ref_bytes = match sparse.get(
        database_id,
        tid,
        &config.ref_table,
        &StorageKey::for_surrogate(surrogate),
    ) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            // Period key not found in reference table — reject (unknown period).
            return Err(ErrorCode::PeriodLocked {
                collection: collection.to_string(),
            });
        }
        Err(e) => {
            return Err(ErrorCode::Internal {
                detail: format!("period lock: failed to read {}: {e}", config.ref_table),
            });
        }
    };

    // Extract the status value from the reference document.
    let Some(status) = extract_field_string(&ref_bytes, &config.status_column) else {
        // The reference row exists but does not carry the configured
        // `status_column` — a misconfigured column name, refused as a
        // config error rather than admitted or treated as locked.
        return Err(ErrorCode::PeriodLockMisconfigured {
            collection: collection.to_string(),
            ref_table: config.ref_table.clone(),
            status_column: config.status_column.clone(),
            row_identity: StorageKey::for_surrogate(surrogate)
                .to_identity()
                .to_string(),
        });
    };

    // Check if the status is in the allowed set.
    if config
        .allowed_statuses
        .iter()
        .any(|s| s.eq_ignore_ascii_case(&status))
    {
        Ok(())
    } else {
        Err(ErrorCode::PeriodLocked {
            collection: collection.to_string(),
        })
    }
}

/// Extract a string field value from a MessagePack or JSON document.
fn extract_period_value(doc_bytes: &[u8], field_name: &str) -> Option<String> {
    extract_field_string(doc_bytes, field_name)
}

/// Extract a string field from a MessagePack or JSON document.
///
/// Uses first-byte detection: MessagePack maps start with 0x80-0x8F (fixmap),
/// 0xDE (map16), 0xDF (map32). Everything else is tried as JSON.
fn extract_field_string(bytes: &[u8], field_name: &str) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let first = bytes[0];
    let is_msgpack = (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF;
    if is_msgpack && let Ok(val) = nodedb_types::json_from_msgpack(bytes) {
        return val.get(field_name)?.as_str().map(String::from);
    }
    if let Ok(val) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return val.get(field_name)?.as_str().map(String::from);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Surrogate;

    fn make_config(allowed: &[&str]) -> PeriodLockConfig {
        PeriodLockConfig {
            period_column: "fiscal_period".into(),
            ref_table: "fiscal_periods".into(),
            ref_pk: "period_key".into(),
            status_column: "status".into(),
            allowed_statuses: allowed.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn extract_field_from_json() {
        let doc = serde_json::json!({"fiscal_period": "2026-03", "amount": 100});
        let bytes = serde_json::to_vec(&doc).unwrap();
        assert_eq!(
            extract_field_string(&bytes, "fiscal_period"),
            Some("2026-03".into())
        );
    }

    #[test]
    fn extract_field_from_msgpack() {
        let doc = serde_json::json!({"fiscal_period": "2026-04", "status": "OPEN"});
        let bytes = nodedb_types::json_to_msgpack(&doc).unwrap();
        assert_eq!(
            extract_field_string(&bytes, "fiscal_period"),
            Some("2026-04".into())
        );
        assert_eq!(extract_field_string(&bytes, "status"), Some("OPEN".into()));
    }

    #[test]
    fn missing_field_returns_none() {
        let doc = serde_json::json!({"amount": 100});
        let bytes = serde_json::to_vec(&doc).unwrap();
        assert_eq!(extract_field_string(&bytes, "fiscal_period"), None);
    }

    #[test]
    fn allowed_status_check() {
        let config = make_config(&["OPEN", "ADJUSTING"]);
        assert!(
            config
                .allowed_statuses
                .iter()
                .any(|s| s.eq_ignore_ascii_case("OPEN"))
        );
        assert!(
            config
                .allowed_statuses
                .iter()
                .any(|s| s.eq_ignore_ascii_case("adjusting"))
        );
        assert!(
            !config
                .allowed_statuses
                .iter()
                .any(|s| s.eq_ignore_ascii_case("CLOSED"))
        );
    }

    const DB: u64 = 0;
    const TID: u64 = 1;
    const REF_TABLE: &str = "fiscal_periods";
    const REF_ROW: Surrogate = Surrogate(9001);

    fn open_sparse(dir: &std::path::Path) -> SparseEngine {
        SparseEngine::open(&dir.join("sparse.redb")).expect("open sparse engine")
    }

    /// Seed the reference row through the surrogate a resolved target names —
    /// never a bare string key. A bare-string seed is exactly the shape that
    /// let the pre-fix lookup APPEAR to work while never matching a real row.
    fn seed_ref_row(sparse: &SparseEngine, status: &str) {
        let doc = serde_json::json!({"period_key": "2024-Q1", "status": status});
        sparse
            .put(
                DB,
                TID,
                REF_TABLE,
                &StorageKey::for_surrogate(REF_ROW),
                &nodedb_types::json_to_msgpack(&doc).unwrap(),
            )
            .expect("seed reference row");
    }

    fn resolved(period_key: &str) -> Vec<ResolvedSumTarget> {
        vec![ResolvedSumTarget::new(REF_TABLE, period_key, REF_ROW)]
    }

    fn entry(period: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({"fiscal_period": period})).unwrap()
    }

    #[test]
    fn a_resolved_open_period_admits_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sparse = open_sparse(dir.path());
        seed_ref_row(&sparse, "OPEN");
        let config = make_config(&["OPEN", "ADJUSTING"]);

        assert!(
            check_period_lock(
                &sparse,
                DB,
                TID,
                "journal_entries",
                &entry("2024-Q1"),
                &config,
                &resolved("2024-Q1"),
            )
            .is_ok()
        );
    }

    #[test]
    fn a_resolved_closed_period_refuses_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sparse = open_sparse(dir.path());
        seed_ref_row(&sparse, "CLOSED");
        let config = make_config(&["OPEN", "ADJUSTING"]);

        let err = check_period_lock(
            &sparse,
            DB,
            TID,
            "journal_entries",
            &entry("2024-Q1"),
            &config,
            &resolved("2024-Q1"),
        )
        .expect_err("a closed period must refuse the write");
        assert!(matches!(err, ErrorCode::PeriodLocked { .. }));
    }

    /// A period value with no entry in `resolved_targets` names no reference
    /// row — an unknown period, refused exactly like a closed one.
    #[test]
    fn an_unresolved_period_refuses_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sparse = open_sparse(dir.path());
        seed_ref_row(&sparse, "OPEN");
        let config = make_config(&["OPEN", "ADJUSTING"]);

        let err = check_period_lock(
            &sparse,
            DB,
            TID,
            "journal_entries",
            &entry("2024-Q1"),
            &config,
            &[],
        )
        .expect_err("an unresolved period must refuse the write");
        assert!(matches!(err, ErrorCode::PeriodLocked { .. }));
    }

    /// A reference row that exists but is missing the configured
    /// `status_column` names a misconfigured column, not a locked period —
    /// a typo in `status_column` must not silently refuse every write.
    #[test]
    fn a_reference_row_missing_the_status_column_is_a_config_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sparse = open_sparse(dir.path());
        let doc = serde_json::json!({"period_key": "2024-Q1"});
        sparse
            .put(
                DB,
                TID,
                REF_TABLE,
                &StorageKey::for_surrogate(REF_ROW),
                &nodedb_types::json_to_msgpack(&doc).unwrap(),
            )
            .expect("seed reference row without a status column");
        let config = make_config(&["OPEN", "ADJUSTING"]);

        let err = check_period_lock(
            &sparse,
            DB,
            TID,
            "journal_entries",
            &entry("2024-Q1"),
            &config,
            &resolved("2024-Q1"),
        )
        .expect_err("a missing status column must refuse as a config error");
        match err {
            ErrorCode::PeriodLockMisconfigured {
                collection,
                ref_table,
                status_column,
                ..
            } => {
                assert_eq!(collection, "journal_entries");
                assert_eq!(ref_table, REF_TABLE);
                assert_eq!(status_column, "status");
            }
            other => panic!("expected PeriodLockMisconfigured, got {other:?}"),
        }
    }

    /// A document with no period column at all is not subject to the lock —
    /// a schemaless collection may carry rows the lock never gates.
    #[test]
    fn a_missing_period_column_admits_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let sparse = open_sparse(dir.path());
        let config = make_config(&["OPEN"]);
        let doc = nodedb_types::json_to_msgpack(&serde_json::json!({"amount": 100})).unwrap();

        assert!(check_period_lock(&sparse, DB, TID, "journal_entries", &doc, &config, &[]).is_ok());
    }
}
