// SPDX-License-Identifier: BUSL-1.1

//! `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)`
//!
//! Validates the SHA-256 hash chain integrity of audit entries over a sequence range.
//! Reads the whole audit log: persisted catalog rows plus the in-memory buffer, merged by seq.
//! Entries before `from_seq` still feed the link of the first entry in the range.
//!
//! Superuser only, for the same reason `SHOW AUDIT LOG` is: the audit log is a
//! single node-wide record covering every tenant on the node.

use nodedb_sql::parser::query_function::VerifyAuditChainArgs;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{err, single_result};

/// `SELECT VERIFY_AUDIT_CHAIN(from_seq, to_seq)`
///
/// Verifies the hash chain over the specified sequence range and returns
/// `{valid: true/false, entries_checked: N, broken_at_seq: N}`.
///
/// **Superuser only.** `AuditEntry` does carry a `tenant_id`, but the chain
/// itself cannot be narrowed by it: entries from every tenant share one
/// monotonic `seq` and one `last_hash`, so each entry's `prev_hash` links to
/// whichever entry preceded it globally — commonly another tenant's. Dropping
/// the other tenants' entries breaks the very links being checked and
/// reports a false `valid: false` for any interleaved log. Since the answer is
/// unavoidably node-wide, it is gated the way the other node-wide audit
/// readers are (`SHOW AUDIT LOG`, `SHOW AUDIT WHERE`, `EXPORT AUDIT`): to a
/// superuser, whose remit already spans every tenant.
///
/// `database_id` is accepted for call-site uniformity with the other query
/// functions; the audit chain is not scoped per database either.
pub async fn verify_audit_chain(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _database_id: DatabaseId,
    args: &VerifyAuditChainArgs,
) -> Result<Vec<DdlResult>, DdlError> {
    if !identity.is_superuser {
        return Err(err(
            "42501",
            "permission denied: only superuser can verify the audit chain",
        ));
    }

    let from_seq: u64 = args
        .from_seq
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(|_| err("22023", "from_seq must be an integer"))?
        .unwrap_or(1);
    let to_seq: u64 = args
        .to_seq
        .as_deref()
        .map(str::parse)
        .transpose()
        .map_err(|_| err("22023", "to_seq must be an integer"))?
        .unwrap_or(u64::MAX);

    let (report, last_in_range) = state
        .verify_audit_chain(from_seq, to_seq)
        .map_err(|e| DdlError::from_error_in_context("audit log read failed", &e))?;
    let last_checked_seq = last_in_range.unwrap_or(from_seq.saturating_sub(1));

    let result = serde_json::json!({
        "valid": report.broken_at.is_none(),
        "entries_checked": report.checked,
        "from_seq": from_seq,
        "to_seq": if to_seq == u64::MAX { last_checked_seq } else { to_seq },
        "broken_at_seq": report.broken_at,
        "last_hash": report.last_hash,
    });

    Ok(single_result(&result.to_string()))
}
