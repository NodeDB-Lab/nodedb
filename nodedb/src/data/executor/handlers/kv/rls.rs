// SPDX-License-Identifier: BUSL-1.1

//! Data Plane write-policy gate for KV rows.
//!
//! Every KV write whose image is produced where it is persisted — the merged
//! body of an `ON CONFLICT DO UPDATE`, the row a `DELETE` removes, the row a
//! TTL mutation leaves untouched, a field merge, an atomic's computed value,
//! both sides of a transfer — funnels through here so the policy is decided
//! against the exact bytes about to be written.
//!
//! Two properties this indirection exists to hold:
//!
//! - **One decode rule for the whole engine.** KV never stores Binary Tuples,
//!   so the stored body is always MessagePack and the shared gate is always
//!   called with no strict schema. Spelling that out once stops a caller from
//!   guessing.
//! - **An opaque scalar fails closed.** A single-column `value` write stores a
//!   bare scalar with no field for the predicate to name, so the shared gate
//!   rejects it — the same evaluation that decides a field-addressed row, not a
//!   carve-out.

use crate::data::executor::handlers::rls_write_gate;

/// Decide one KV row body against the compiled write policy.
///
/// [`nodedb_types::WriteGateDecision::AdmitAll`] means no write policy
/// restricts this identity here, and the row is admitted without decoding
/// anything.
pub(in crate::data::executor) fn admit_kv_row(
    rls_write_check: &nodedb_types::RlsWriteCheck,
    body: &[u8],
    key: &[u8],
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    if matches!(
        rls_write_check.decision(),
        nodedb_types::WriteGateDecision::AdmitAll
    ) {
        return Ok(());
    }
    // The key is shown for diagnostics only; a non-UTF-8 key is lossily
    // rendered rather than failing a security decision on its encoding. A
    // KV key is never a document storage key, so it is always the row's
    // own identity, taken verbatim.
    let key_display = String::from_utf8_lossy(key);
    let identity = crate::engine::document::store::RowIdentity::from_user_key(key_display.as_ref());
    rls_write_gate::admit_stored_row(rls_write_check, body, &identity, None, tid, collection)
}
