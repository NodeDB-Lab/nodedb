use loro::LoroValue;
use sonic_rs;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::policy::CollectionPolicy;
use nodedb_crdt::pre_validate::{self, PreValidationResult};
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{ProposedChange, Validator};

use crate::types::TenantId;

/// Per-tenant CRDT engine state.
///
/// Manages the loro-backed CRDT state, constraint validation, and dead-letter
/// queue for a single tenant. Lives on the Data Plane (one per tenant per core).
pub struct TenantCrdtEngine {
    tenant_id: TenantId,

    /// Leader's committed CRDT state for this tenant.
    state: CrdtState,

    /// Constraint validator with DLQ and policy registry.
    pub(crate) validator: Validator,
}

impl TenantCrdtEngine {
    /// Create a new engine for a tenant with the given peer ID and constraints.
    pub fn new(
        tenant_id: TenantId,
        peer_id: u64,
        constraints: ConstraintSet,
    ) -> crate::Result<Self> {
        Ok(Self {
            tenant_id,
            state: CrdtState::new(peer_id).map_err(crate::Error::Crdt)?,
            validator: Validator::new(constraints, 1000),
        })
    }

    /// Get the peer ID for this CRDT engine.
    pub fn peer_id(&self) -> u64 {
        self.state.peer_id()
    }

    /// Export the full CRDT state as binary bytes (for snapshot transfer).
    pub fn export_snapshot_bytes(&self) -> crate::Result<Vec<u8>> {
        self.state.export_snapshot().map_err(crate::Error::Crdt)
    }

    /// Read a document's CRDT state, returning the raw snapshot bytes.
    pub fn read_snapshot(&self, collection: &str, row_id: &str) -> crate::Result<Option<Vec<u8>>> {
        if self.state.row_exists(collection, row_id) {
            Ok(Some(
                self.state.export_snapshot().map_err(crate::Error::Crdt)?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Read a single row's fields as a `LoroValue`.
    ///
    /// Returns the deep value of the row (all nested containers resolved),
    /// or `None` if the row does not exist.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        self.state.read_row(collection, row_id)
    }

    /// Pre-validate a proposed change (fast-reject before Raft).
    pub fn pre_validate(&self, change: &ProposedChange) -> PreValidationResult {
        pre_validate::pre_validate(&self.validator, &self.state, change)
    }

    /// Apply a validated delta from Raft commit.
    ///
    /// Import a full CRDT snapshot (for snapshot restore).
    pub fn import_snapshot_bytes(&self, bytes: &[u8]) -> crate::Result<()> {
        self.state.import(bytes).map_err(crate::Error::Crdt)
    }

    /// This is called AFTER Raft consensus — the delta has been committed
    /// to the Raft log and now needs to be applied to the local state.
    pub fn apply_committed_delta(&self, delta: &[u8]) -> crate::Result<()> {
        self.state.import(delta).map_err(crate::Error::Crdt)
    }

    /// Validate and attempt to apply a delta from a peer.
    ///
    /// If constraints are violated, the delta is routed to the DLQ.
    /// Returns `Ok(())` on success, or the constraint violation error.
    pub fn validate_and_apply(
        &mut self,
        peer_id: u64,
        auth: nodedb_crdt::CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> crate::Result<()> {
        self.validator
            .validate_or_reject(&self.state, peer_id, auth, change, delta_bytes)
            .map_err(crate::Error::Crdt)?;

        // Validation passed — apply to state.
        // In production this would apply the delta bytes, but for now
        // we upsert the fields directly since we have the ProposedChange.
        let fields: Vec<(&str, LoroValue)> = change
            .fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();

        self.state
            .upsert(&change.collection, &change.row_id, &fields)
            .map_err(crate::Error::Crdt)
    }

    /// Number of entries in the dead-letter queue.
    pub fn dlq_len(&self) -> usize {
        self.validator.dlq().len()
    }

    /// Check if a row exists in a collection.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.state.row_exists(collection, row_id)
    }

    // ─── Version History ─────────────────────────────────────────────

    /// Get the current version vector as a JSON string.
    pub fn version_vector_json(&self) -> crate::Result<String> {
        let vv = self.state.oplog_version_vector();
        let map = vv_to_json_map(&vv);
        sonic_rs::to_string(&map).map_err(|e| crate::Error::Internal {
            detail: format!("version vector serialization: {e}"),
        })
    }

    /// Read a document at a historical version, returning JSON bytes.
    pub fn read_at_version_json(
        &self,
        collection: &str,
        document_id: &str,
        version_json: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        let vv = json_to_vv(version_json)?;
        match self.state.read_at_version(collection, document_id, &vv) {
            Ok(Some(val)) => {
                let json = crate::engine::document::crdt_store::loro_value_to_json(&val);
                sonic_rs::to_vec(&json)
                    .map(Some)
                    .map_err(|e| crate::Error::Internal {
                        detail: format!("JSON serialization: {e}"),
                    })
            }
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Crdt(e)),
        }
    }

    /// Export delta from a version to current, returning raw Loro bytes.
    pub fn export_delta(&self, from_version_json: &str) -> crate::Result<Vec<u8>> {
        let vv = json_to_vv(from_version_json)?;
        self.state
            .export_updates_since(&vv)
            .map_err(crate::Error::Crdt)
    }

    /// Restore a document to a historical version (forward mutation).
    pub fn restore_to_version(
        &self,
        collection: &str,
        document_id: &str,
        target_version_json: &str,
    ) -> crate::Result<Vec<u8>> {
        let vv = json_to_vv(target_version_json)?;
        self.state
            .restore_to_version(collection, document_id, &vv)
            .map_err(crate::Error::Crdt)
    }

    /// Compact history at a specific version.
    pub fn compact_at_version(&mut self, target_version_json: &str) -> crate::Result<()> {
        let vv = json_to_vv(target_version_json)?;
        self.state
            .compact_at_version(&vv)
            .map_err(crate::Error::Crdt)
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Set conflict resolution policy for a collection from JSON.
    ///
    /// Called when the Data Plane receives a `SetCollectionPolicy` physical plan
    /// from the `ALTER COLLECTION ... SET ON CONFLICT ...` DDL.
    pub fn set_collection_policy(
        &mut self,
        collection: &str,
        policy_json: &str,
    ) -> crate::Result<()> {
        let policy: CollectionPolicy =
            sonic_rs::from_str(policy_json).map_err(|e| crate::Error::BadRequest {
                detail: format!("invalid collection policy JSON: {e}"),
            })?;
        Self::validate_policy(&policy)?;
        self.validator.policies_mut().set(collection, policy);
        Ok(())
    }

    /// Validate business rules on a collection policy before accepting it.
    fn validate_policy(policy: &CollectionPolicy) -> crate::Result<()> {
        Self::validate_conflict_policy(&policy.unique, "unique")?;
        Self::validate_conflict_policy(&policy.foreign_key, "foreign_key")?;
        Self::validate_conflict_policy(&policy.not_null, "not_null")?;
        Self::validate_conflict_policy(&policy.check, "check")?;
        Ok(())
    }

    fn validate_conflict_policy(
        policy: &nodedb_crdt::policy::ConflictPolicy,
        field_name: &str,
    ) -> crate::Result<()> {
        use nodedb_crdt::policy::ConflictPolicy;
        match policy {
            ConflictPolicy::CascadeDefer {
                max_retries,
                ttl_secs,
            } => {
                if *max_retries == 0 {
                    return Err(crate::Error::BadRequest {
                        detail: format!("{field_name}: max_retries must be > 0"),
                    });
                }
                if *ttl_secs == 0 {
                    return Err(crate::Error::BadRequest {
                        detail: format!("{field_name}: ttl_secs must be > 0"),
                    });
                }
            }
            ConflictPolicy::Custom {
                webhook_url,
                timeout_secs,
            } => {
                if webhook_url.is_empty() {
                    return Err(crate::Error::BadRequest {
                        detail: format!("{field_name}: webhook_url must not be empty"),
                    });
                }
                if !webhook_url.starts_with("http://") && !webhook_url.starts_with("https://") {
                    return Err(crate::Error::BadRequest {
                        detail: format!("{field_name}: webhook_url must be an HTTP(S) URL"),
                    });
                }
                if *timeout_secs == 0 {
                    return Err(crate::Error::BadRequest {
                        detail: format!("{field_name}: timeout_secs must be > 0"),
                    });
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Convert a Loro VersionVector to a JSON-friendly map: `{peer_id_hex: counter}`.
fn vv_to_json_map(vv: &loro::VersionVector) -> std::collections::HashMap<String, i64> {
    let mut map = std::collections::HashMap::new();
    for (peer, counter) in vv.iter() {
        map.insert(format!("{peer:016x}"), *counter as i64);
    }
    map
}

/// Parse a JSON version vector string into a Loro VersionVector.
fn json_to_vv(json: &str) -> crate::Result<loro::VersionVector> {
    let map: std::collections::HashMap<String, i64> =
        sonic_rs::from_str(json).map_err(|e| crate::Error::BadRequest {
            detail: format!("invalid version vector JSON: {e}"),
        })?;
    let mut vv = loro::VersionVector::default();
    for (peer_hex, counter) in &map {
        let peer = u64::from_str_radix(peer_hex.trim_start_matches("0x"), 16).map_err(|e| {
            crate::Error::BadRequest {
                detail: format!("invalid peer_id hex '{peer_hex}': {e}"),
            }
        })?;
        vv.insert(peer, *counter as i32);
    }
    Ok(vv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_constraints() -> ConstraintSet {
        let mut cs = ConstraintSet::new();
        cs.add_unique("users_email_unique", "users", "email");
        cs.add_not_null("users_name_nn", "users", "name");
        cs
    }

    #[test]
    fn valid_write_applies() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };

        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
            )
            .unwrap();

        assert!(engine.row_exists("users", "u1"));
        assert_eq!(engine.dlq_len(), 0);
    }

    #[test]
    fn constraint_violation_routes_to_dlq() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
        // Use strict policy so violations escalate to DLQ instead of auto-resolving.
        engine
            .validator
            .policies_mut()
            .set("users", CollectionPolicy::strict());

        // Missing "name" field violates NOT NULL.
        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        let err = engine
            .validate_and_apply(
                42,
                nodedb_crdt::CrdtAuthContext::default(),
                &change,
                b"delta".to_vec(),
            )
            .unwrap_err();

        assert!(matches!(err, crate::Error::Crdt(_)));
        assert_eq!(engine.dlq_len(), 1);
    }

    #[test]
    fn pre_validate_fast_rejects() {
        let engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();

        let change = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![("email".into(), LoroValue::String("a@b.com".into()))],
        };

        match engine.pre_validate(&change) {
            PreValidationResult::FastReject { constraint, .. } => {
                assert_eq!(constraint, "users_name_nn");
            }
            _ => panic!("expected fast reject"),
        }
    }

    #[test]
    fn unique_violation_after_first_write() {
        let mut engine = TenantCrdtEngine::new(TenantId::new(1), 0, test_constraints()).unwrap();
        // Strict mode: UNIQUE violations escalate to DLQ.
        engine
            .validator
            .policies_mut()
            .set("users", CollectionPolicy::strict());

        let first = ProposedChange {
            collection: "users".into(),
            row_id: "u1".into(),
            fields: vec![
                ("name".into(), LoroValue::String("Alice".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };
        engine
            .validate_and_apply(
                1,
                nodedb_crdt::CrdtAuthContext::default(),
                &first,
                b"d1".to_vec(),
            )
            .unwrap();

        // Second write with same email should fail.
        let second = ProposedChange {
            collection: "users".into(),
            row_id: "u2".into(),
            fields: vec![
                ("name".into(), LoroValue::String("Bob".into())),
                (
                    "email".into(),
                    LoroValue::String("alice@example.com".into()),
                ),
            ],
        };
        assert!(
            engine
                .validate_and_apply(
                    2,
                    nodedb_crdt::CrdtAuthContext::default(),
                    &second,
                    b"d2".to_vec()
                )
                .is_err()
        );
        assert_eq!(engine.dlq_len(), 1);
    }
}
