// SPDX-License-Identifier: BUSL-1.1

//! The policy-store and identity inputs every injection arm keys on.
//!
//! Each engine module receives an [`RlsCtx`] and resolves one outcome per
//! plan variant: a read injects into a filter slot, refuses (no filter
//! slot), or no-ops (no user rows). A write admits its post-image, ships
//! the predicate for the Data Plane to evaluate, or refuses — never a
//! silent no-op, indistinguishable from no policy at all.

use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::{PolicyType, RlsPolicyStore};

use super::filters::{get_rls, get_rls_write, merge_filters};

/// Policy registry plus the requester's tenant and authenticated identity.
/// Superuser bypass and fail-closed `$auth.*` handling live inside
/// `combined_read_predicate_with_auth`, reached via [`get_rls`].
pub(super) struct RlsCtx<'a> {
    pub(super) store: &'a RlsPolicyStore,
    pub(super) tenant_id: u64,
    pub(super) auth: &'a AuthContext,
    /// Database bare op-carried collection names (e.g.
    /// `AlgoParams.collection`) must be qualified against before a lookup.
    pub(super) database_id: nodedb_types::DatabaseId,
}

impl RlsCtx<'_> {
    /// The concrete read filters for `collection`; empty when no policy
    /// restricts this identity.
    pub(super) fn read_filters(
        &self,
        collection: &nodedb_types::QualifiedCollection,
    ) -> crate::Result<Vec<u8>> {
        get_rls(self.store, self.tenant_id, collection.as_str(), self.auth)
    }

    /// AND the collection's read policy into a scan-style pushdown slot.
    pub(super) fn merge_into(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        filters: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let rls = self.read_filters(collection)?;
        if !rls.is_empty() {
            merge_filters(filters, &rls)?;
        }
        Ok(())
    }

    /// Store the collection's read policy in a dedicated post-fetch slot.
    pub(super) fn set_post_filters(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        rls_filters: &mut Vec<u8>,
    ) -> crate::Result<()> {
        let rls = self.read_filters(collection)?;
        if !rls.is_empty() {
            *rls_filters = rls;
        }
        Ok(())
    }

    /// Refuse while a read policy restricts this identity on `collection`.
    /// `why` completes "…is not supported with this operation: {why}".
    pub(super) fn refuse_if_policy(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        why: &str,
    ) -> crate::Result<()> {
        if collection.as_str().is_empty() || self.read_filters(collection)?.is_empty() {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS policy on '{collection}' is not supported with this operation: {why}"
            ),
        })
    }

    /// Refuse when this identity holds any read policy in the tenant. Used
    /// only where the plan names no collection, so the narrow question
    /// can't be asked. Mirrors the redaction pass's unscoped-MATCH fallback.
    pub(super) fn refuse_if_any_policy(&self, why: &str) -> crate::Result<()> {
        if self.auth.is_superuser() || !self.identity_has_any_read_policy() {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS is not supported with this operation while a read policy applies to this \
                 identity and the plan names no collection: {why}"
            ),
        })
    }

    /// Admit a write whose post-image the plan already carries as
    /// MessagePack. A rejected row fails the whole statement — a silently
    /// dropped row would report a write that never happened.
    pub(super) fn admit_write_image(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        image: &[u8],
    ) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection.as_str(), self.auth)?;
        crate::control::security::rls::admit_compiled_write_image(
            &check,
            image,
            self.tenant_id,
            collection.as_str(),
        )
    }

    /// Admit a document write, injecting the row's storage key as `id`.
    ///
    /// A schemaless row with no declared `id` column carries its identity
    /// only in that key, never in `image`. The read paths inject the same
    /// string via `sparse_row_to_doc`, so a policy naming `id` judges the
    /// write against the value a later read returns. `inject_str_field` is
    /// a no-op when `image` already carries `id`.
    pub(super) fn admit_document_write_image(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        row_key: &str,
        image: &[u8],
    ) -> crate::Result<()> {
        let with_id = nodedb_query::msgpack_scan::inject_str_field(image, "id", row_key);
        self.admit_write_image(collection, &with_id)
    }

    /// Admit a write whose post-image is a JSON object (a graph edge's
    /// `PROPERTIES`). Non-object bytes, including an empty `PROPERTIES`,
    /// deny rather than admit by omission.
    pub(super) fn admit_write_json_image(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        image: &[u8],
    ) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection.as_str(), self.auth)?;
        if check.is_empty() {
            return Ok(());
        }
        let decoded = sonic_rs::from_slice::<serde_json::Value>(image).ok();
        let Some(object @ serde_json::Value::Object(_)) = decoded else {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(self.tenant_id),
                resource: format!(
                    "RLS write policy on '{collection}': the write carries no decodable property \
                     object, so the policy could not be evaluated against it"
                ),
            });
        };
        crate::control::security::rls::admit_compiled_write_image(
            &check,
            &nodedb_types::json_to_msgpack_or_empty(&object),
            self.tenant_id,
            collection.as_str(),
        )
    }

    /// Admit a write whose post-image is a zerompk `HashMap<String, Value>`,
    /// TAGGED (`[4, "…"]`, not a bare string) — rewritten as plain
    /// MessagePack first, or comparing tags directly would deny every row.
    pub(super) fn admit_write_value_map_image(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        payload: &[u8],
    ) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection.as_str(), self.auth)?;
        if check.is_empty() {
            return Ok(());
        }
        let decoded = zerompk::from_msgpack::<std::collections::HashMap<String, nodedb_types::Value>>(
            payload,
        );
        let Ok(fields) = decoded else {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(self.tenant_id),
                resource: format!(
                    "RLS write policy on '{collection}': the write carries no decodable field \
                     image, so the policy could not be evaluated against it"
                ),
            });
        };
        let image = nodedb_types::Value::Object(
            fields
                .into_iter()
                .map(|(field, value)| (field.to_ascii_lowercase(), value))
                .collect(),
        );
        let bytes =
            nodedb_types::value_to_msgpack(&image).map_err(|error| crate::Error::PlanError {
                detail: format!("RLS write admission could not re-encode the row image: {error}"),
            })?;
        crate::control::security::rls::admit_compiled_write_image(
            &check,
            &bytes,
            self.tenant_id,
            collection.as_str(),
        )
    }

    /// Admit every row of a MessagePack batch; the first violation fails
    /// the whole statement before dispatch. Records the verdict in the
    /// check slot — left at `PendingInjection` it reads as "never ran".
    pub(super) fn admit_write_batch(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        payload: &[u8],
        rls_write_check: &mut nodedb_types::RlsWriteCheck,
    ) -> crate::Result<()> {
        let check = get_rls_write(self.store, self.tenant_id, collection.as_str(), self.auth)?;
        if check.is_empty() {
            *rls_write_check = nodedb_types::RlsWriteCheck::NoPolicyApplies;
            return Ok(());
        }
        let rows = match nodedb_types::value_from_msgpack(payload) {
            Ok(nodedb_types::Value::Array(rows)) => rows,
            Ok(row @ nodedb_types::Value::Object(_)) => vec![row],
            _ => {
                return Err(crate::Error::RejectedAuthz {
                    tenant_id: crate::types::TenantId::new(self.tenant_id),
                    resource: format!(
                        "RLS write policy on '{collection}': the row batch did not decode, so the \
                         policy could not be evaluated against it"
                    ),
                });
            }
        };
        for row in &rows {
            let image =
                nodedb_types::value_to_msgpack(row).map_err(|error| crate::Error::PlanError {
                    detail: format!("RLS write admission could not re-encode a row: {error}"),
                })?;
            crate::control::security::rls::admit_compiled_write_image(
                &check,
                &image,
                self.tenant_id,
                collection.as_str(),
            )?;
        }
        // Decided here, with a live identity, against the exact images carried.
        *rls_write_check = nodedb_types::RlsWriteCheck::decided_earlier_in_request();
        Ok(())
    }

    /// Compile the write policy into a plan's write-gate slot, for a write
    /// whose image exists only where it's persisted (update's post-image,
    /// delete's removed row) — evaluated there instead of refusing outright.
    pub(super) fn set_write_check(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        rls_write_check: &mut nodedb_types::RlsWriteCheck,
    ) -> crate::Result<()> {
        *rls_write_check = nodedb_types::RlsWriteCheck::from_injected(get_rls_write(
            self.store,
            self.tenant_id,
            collection.as_str(),
            self.auth,
        )?);
        Ok(())
    }

    /// Refuse the write while a write policy restricts this identity on
    /// `collection`. `why` completes "…cannot be enforced for this operation:
    /// {why}".
    pub(super) fn refuse_if_write_policy(
        &self,
        collection: &nodedb_types::QualifiedCollection,
        why: &str,
    ) -> crate::Result<()> {
        if collection.as_str().is_empty()
            || get_rls_write(self.store, self.tenant_id, collection.as_str(), self.auth)?.is_empty()
        {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "RLS write policy on '{collection}' cannot be enforced for this operation: {why}"
            ),
        })
    }

    /// Refuse while this identity holds any write policy in the tenant.
    /// Used only where the write names no collection, so the narrow
    /// question can't be asked.
    pub(super) fn refuse_if_any_write_policy(&self, why: &str) -> crate::Result<()> {
        if self.auth.is_superuser() || !self.store.tenant_has_write_policy(self.tenant_id) {
            return Ok(());
        }
        Err(crate::Error::PlanError {
            detail: format!(
                "an RLS write policy applies to this identity and the plan names no collection, so \
                 it cannot be enforced for this operation: {why}"
            ),
        })
    }

    /// Whether any enabled, non-vacuous read policy exists in this tenant
    /// (a policy with no compiled predicate is ignored, like elsewhere).
    fn identity_has_any_read_policy(&self) -> bool {
        self.store
            .all_policies_for_tenant(self.tenant_id)
            .iter()
            .any(|policy| {
                policy.enabled
                    && policy.compiled_predicate.is_some()
                    && matches!(policy.policy_type, PolicyType::Read | PolicyType::All)
            })
    }
}
