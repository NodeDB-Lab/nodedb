//! `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)`
//!
//! Validates the SHA-256 hash chain integrity of audit entries over a sequence range.
//! Reads from the durable audit WAL (or in-memory cache as fallback).
//!
//! **Note:** For complete chain verification, pass `from_seq=1`. Partial ranges
//! validate chain integrity *within* the range but do not verify continuity from genesis.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{sqlstate_error, text_field};
use super::helpers::clean_arg;

/// `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)`
///
/// Reads audit entries from the durable audit WAL (or in-memory cache),
/// verifies the hash chain over the specified sequence range, and returns
/// `{valid: true/false, entries: N, broken_at_seq: N}`.
pub async fn verify_audit_chain(
    state: &SharedState,
    _identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    let args = super::helpers::extract_function_args(sql, "VERIFY_AUDIT_CHAIN")?;

    let from_seq: u64 = if !args.is_empty() {
        clean_arg(args[0])
            .parse()
            .map_err(|_| sqlstate_error("22023", "from_seq must be an integer"))?
    } else {
        1
    };
    let to_seq: u64 = if args.len() >= 2 {
        clean_arg(args[1])
            .parse()
            .map_err(|_| sqlstate_error("22023", "to_seq must be an integer"))?
    } else {
        u64::MAX
    };

    // Try the durable audit WAL first.
    let audit_entries = state
        .wal
        .recover_audit_entries()
        .map_err(|e| sqlstate_error("XX000", &format!("audit WAL recovery failed: {e}")))?;

    let mut valid = true;
    let mut checked = 0u64;
    let mut broken_at: Option<u64> = None;
    let mut prev_hash = String::new();

    if !audit_entries.is_empty() {
        // Verify from durable WAL entries.
        for (_data_lsn, audit_bytes) in &audit_entries {
            let entry: crate::control::security::audit::AuditEntry =
                match rmp_serde::from_slice(audit_bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        tracing::warn!(error = %e, "skipping malformed audit WAL entry");
                        continue;
                    }
                };

            if entry.seq < from_seq {
                prev_hash = compute_audit_hash(&entry);
                continue;
            }
            if entry.seq > to_seq {
                break;
            }

            if entry.prev_hash != prev_hash {
                valid = false;
                broken_at = Some(entry.seq);
                break;
            }

            prev_hash = compute_audit_hash(&entry);
            checked += 1;
        }
    } else {
        // Fall back to in-memory audit log.
        // Recover from poisoned mutex — audit log corruption doesn't justify panic.
        let log = match state.audit.lock() {
            Ok(l) => l,
            Err(poisoned) => {
                tracing::warn!("audit log mutex poisoned, recovering");
                poisoned.into_inner()
            }
        };
        for entry in log.all() {
            if entry.seq < from_seq {
                prev_hash = compute_audit_hash(entry);
                continue;
            }
            if entry.seq > to_seq {
                break;
            }

            if entry.prev_hash != prev_hash {
                valid = false;
                broken_at = Some(entry.seq);
                break;
            }

            prev_hash = compute_audit_hash(entry);
            checked += 1;
        }
    }

    let result = serde_json::json!({
        "valid": valid,
        "entries_checked": checked,
        "from_seq": from_seq,
        "to_seq": if to_seq == u64::MAX { checked + from_seq - 1 } else { to_seq },
        "broken_at_seq": broken_at,
        "last_hash": prev_hash,
    });

    let schema = Arc::new(vec![text_field("result")]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder
        .encode_field(&result.to_string())
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(vec![Ok(encoder.take_row())]),
    ))])
}

/// Compute SHA-256 hash of an audit entry (same algorithm as audit.rs::hash_entry).
fn compute_audit_hash(entry: &crate::control::security::audit::AuditEntry) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(entry.prev_hash.as_bytes());
    hasher.update(entry.seq.to_le_bytes());
    hasher.update(entry.timestamp_us.to_le_bytes());
    hasher.update(format!("{:?}", entry.event).as_bytes());
    hasher.update(entry.auth_user_id.as_bytes());
    hasher.update(entry.auth_user_name.as_bytes());
    hasher.update(entry.session_id.as_bytes());
    hasher.update(entry.source.as_bytes());
    hasher.update(entry.detail.as_bytes());
    format!("{:x}", hasher.finalize())
}
