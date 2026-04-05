//! Period lock enforcement: reject writes to rows in closed/locked fiscal periods.
//!
//! On each write (INSERT, UPDATE, DELETE), extracts the period column value from
//! the document, looks up the period status in the reference collection, and
//! rejects the write if the status is not in the allowed set.

use crate::bridge::envelope::ErrorCode;
use crate::bridge::physical_plan::PeriodLockConfig;
use crate::engine::sparse::btree::SparseEngine;

/// Check whether a write is allowed given the period lock configuration.
///
/// `doc_bytes` is the document being written (INSERT/UPDATE) or the existing
/// document (DELETE). The period column value is extracted and looked up in
/// the reference collection.
///
/// Returns `Ok(())` if the write is allowed, or `Err(PeriodLocked)` if the
/// period is closed/locked.
pub fn check_period_lock(
    sparse: &SparseEngine,
    tid: u32,
    collection: &str,
    doc_bytes: &[u8],
    config: &PeriodLockConfig,
) -> Result<(), ErrorCode> {
    // Extract the period column value from the document.
    let period_value = extract_period_value(doc_bytes, &config.period_column);
    let Some(period_key) = period_value else {
        // Period column not present in document — skip check (schemaless collections
        // may have rows without the period column).
        return Ok(());
    };

    // Look up the period status in the reference collection.
    let ref_doc = sparse
        .get(tid, &config.ref_table, &period_key)
        .map_err(|e| ErrorCode::Internal {
            detail: format!("period lock: failed to read {}: {e}", config.ref_table),
        })?;

    let Some(ref_bytes) = ref_doc else {
        // Period key not found in reference table — reject (unknown period).
        return Err(ErrorCode::PeriodLocked {
            collection: collection.to_string(),
        });
    };

    // Extract the status value from the reference document.
    let status = extract_field_string(&ref_bytes, &config.status_column);
    let Some(status) = status else {
        // Status column not found — treat as locked (defensive).
        return Err(ErrorCode::PeriodLocked {
            collection: collection.to_string(),
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
    if let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return val.get(field_name)?.as_str().map(String::from);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
