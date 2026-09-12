// SPDX-License-Identifier: BUSL-1.1

//! The authorization step for hand-built physical plans in this tree.
//!
//! The SQL-function handlers here bypass the planner: they take a collection
//! name out of the caller's own arguments, construct a [`PhysicalPlan`] by
//! hand, and hand it to `dispatch_utils::dispatch_to_data_plane`, which accepts
//! a trusted internal plan and therefore runs neither the RBAC check nor RLS
//! injection. Everything the planner-driven read path applies between planning
//! and dispatch has to be applied here too, and applied identically, or the
//! function reads arbitrary rows under the caller's own SQL.
//!
//! [`CollectionReadGate`] is that step in one place: the RBAC check, the RLS
//! predicate merged into the hand-built plan, and the redaction inputs for
//! whatever the handler returns. A handler resolves its collection, opens a
//! gate on it, injects into every plan it builds, and redacts (or refuses) on
//! the way out.
//!
//! # Inject, do not refuse
//!
//! A plan that carries a `filters` / `rls_filters` slot can express the policy,
//! so [`CollectionReadGate::inject_rls`] puts it there and the query keeps
//! working with fewer rows. [`CollectionReadGate::refuse_if_read_policy`] is
//! for the plans that carry no such slot at all (`EstimateCount`): those cannot
//! express a row filter, so a policy on the collection makes the answer
//! unrepresentable rather than smaller, and they fail closed.
//!
//! A hand-built WRITE has no separate gate here at all. Its verdict — admit the
//! image, ship the compiled predicate for the Data Plane to decide, or refuse —
//! belongs to the injection pass alone, so a handler that builds the same plan
//! variant a planned statement builds cannot reach a different one.

use nodedb_types::DatabaseId;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::planner::rls_injection::inject_rls_for_single_plan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::{QueryRedaction, redact_decoded_value};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::super::result::DdlError;

/// SQLSTATE for a denied RBAC check.
const INSUFFICIENT_PRIVILEGE: &str = "42501";
/// SQLSTATE for a policy this delivery shape cannot express, or a feature
/// the collection's engine does not carry.
const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// SQLSTATE for a collection the catalog does not hold.
const UNDEFINED_TABLE: &str = "42P01";

fn gate_err(sqlstate: &str, message: impl Into<String>) -> DdlError {
    DdlError::new(sqlstate, message)
}

/// The per-request authorization context a hand-built read runs under.
///
/// Holds the same [`RequestAuthScope`] the planner-driven path resolves, so a
/// row is redacted for exactly the roles it was authorized and RLS-filtered
/// for — the three cannot drift to different principals.
pub struct CollectionReadGate<'a> {
    state: &'a SharedState,
    scope: RequestAuthScope<'a>,
}

impl<'a> CollectionReadGate<'a> {
    /// Open a gate and authorize `collection` for reading in one step — the
    /// shape every single-collection handler needs.
    pub fn open(
        state: &'a SharedState,
        identity: &'a AuthenticatedIdentity,
        database_id: DatabaseId,
        collection: &str,
    ) -> Result<Self, DdlError> {
        let gate = Self::for_request(state, identity, database_id);
        gate.authorize(collection)?;
        Ok(gate)
    }

    /// Open a gate that has authorized nothing yet.
    ///
    /// For handlers whose collection set is only known after a catalog lookup
    /// (a materialized sum's source collection) or spans several collections
    /// (`TREE_SUM` without an explicit collection). Every one of those
    /// collections still has to pass [`Self::authorize`] before it is read.
    pub fn for_request(
        state: &'a SharedState,
        identity: &'a AuthenticatedIdentity,
        database_id: DatabaseId,
    ) -> Self {
        Self {
            state,
            scope: RequestAuthScope::for_database(identity, state.auth_stores(), database_id),
        }
    }

    /// The tenant this gate is scoped to.
    pub fn tenant_id(&self) -> TenantId {
        self.scope.tenant_id()
    }

    /// The identity this gate authorizes for.
    pub fn identity(&self) -> &AuthenticatedIdentity {
        self.scope.identity()
    }

    /// The resolved `AuthContext` this gate authorizes, filters, and redacts
    /// for — handed out so a handler's own refusal (a graph read's redaction
    /// refusal, say) runs against the same principal rather than resolving a
    /// second scope that could describe a different one.
    pub fn auth(&self) -> &AuthContext {
        self.scope.auth()
    }

    /// Fail closed unless the caller holds `Read` on `collection`.
    pub fn authorize(&self, collection: &str) -> Result<(), DdlError> {
        self.authorize_permission(collection, Permission::Read)
    }

    /// Fail closed unless the caller holds `permission` on `collection`.
    ///
    /// A read-modify-write function (`KV_INCR`, `KV_CAS`, `KV_GETSET`) needs
    /// both halves and calls this twice rather than settling for the read
    /// grant alone.
    pub fn authorize_permission(
        &self,
        collection: &str,
        permission: Permission,
    ) -> Result<(), DdlError> {
        let audit = crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(
            &self.state.audit,
        ));
        crate::control::server::shared::authorization::authorize_collection(
            self.scope.identity(),
            self.scope.database_id(),
            collection,
            permission,
            &self.state.permissions,
            &self.state.roles,
            &audit,
        )
        .map_err(|error| {
            gate_err(
                INSUFFICIENT_PRIVILEGE,
                format!("permission denied: {}", error.resource()),
            )
        })
    }

    /// Merge the caller's RLS read predicate into a hand-built plan.
    ///
    /// Delegates to the same [`inject_rls_for_single_plan`] the planner-driven
    /// read path uses, so a hand-built `Scan` / `PointGet` receives byte-identical
    /// filters to the ones a `SELECT` against the same collection would get.
    pub fn inject_rls(&self, plan: &mut PhysicalPlan) -> Result<(), DdlError> {
        inject_rls_for_single_plan(
            self.tenant_id().as_u64(),
            self.scope.database_id(),
            plan,
            &self.state.rls,
            self.scope.auth(),
        )
        .map_err(|error| {
            let sqlstate = match &error {
                crate::Error::RejectedAuthz { .. } => INSUFFICIENT_PRIVILEGE,
                _ => FEATURE_NOT_SUPPORTED,
            };
            gate_err(sqlstate, error.to_string())
        })
    }

    /// Fail closed unless `collection` is a document collection.
    ///
    /// For handlers that hand-build a `DocumentOp::Scan` over a caller-named
    /// collection: the document scan reads the sparse store only, so a KV or
    /// columnar-family collection answers with no rows. `what` names the
    /// feature in the refusal.
    pub fn require_document_engine(&self, collection: &str, what: &str) -> Result<(), DdlError> {
        let stored = self
            .state
            .credentials
            .catalog()
            .get_collection(
                self.scope.database_id(),
                self.tenant_id().as_u64(),
                collection,
            )
            .map_err(|e| gate_err("XX000", e.to_string()))?;
        match stored {
            None => Err(gate_err(
                UNDEFINED_TABLE,
                format!("{what}: collection '{collection}' does not exist"),
            )),
            Some(stored) if stored.collection_type.is_document() => Ok(()),
            Some(stored) => Err(gate_err(
                FEATURE_NOT_SUPPORTED,
                format!(
                    "{what} reads document collections; '{collection}' is a {} collection",
                    stored.collection_type.as_str()
                ),
            )),
        }
    }

    /// Fail closed when a read policy exists on `collection`.
    ///
    /// For plans with no filter slot to inject into, where a policy cannot be
    /// honored and returning the unfiltered answer would leak it.
    pub fn refuse_if_read_policy(&self, collection: &str, what: &str) -> Result<(), DdlError> {
        let unrestricted = self
            .state
            .rls
            .combined_read_predicate_with_auth(
                self.tenant_id().as_u64(),
                collection,
                self.scope.auth(),
            )
            .is_some_and(|filters| filters.is_empty());
        if unrestricted {
            return Ok(());
        }
        Err(gate_err(
            FEATURE_NOT_SUPPORTED,
            format!(
                "RLS policies on '{collection}' are not supported with {what}: it has no row \
                 filter the policy could be applied through"
            ),
        ))
    }

    /// The redaction inputs for rows delivered from `collections`.
    pub fn redaction_for<'c, I>(&self, collections: I) -> QueryRedaction
    where
        I: IntoIterator<Item = &'c str>,
    {
        QueryRedaction::for_collections(
            self.tenant_id(),
            self.scope.auth(),
            collections
                .into_iter()
                .map(|collection| (String::new(), collection.to_string()))
                .collect(),
        )
    }

    /// Apply column redaction to a decoded result value in place.
    pub fn redact(&self, redaction: &QueryRedaction, value: &mut serde_json::Value) {
        redact_decoded_value(Some(redaction), &self.state.redaction, value);
    }

    /// Fail closed when a redaction rule covers `field` on `collection`.
    ///
    /// For handlers that return a value *computed* from a stored column rather
    /// than the column itself: masking the computed value would report a number
    /// no row holds, and returning it unmasked would disclose the very column
    /// the rule hides. Neither is an answer, so the function refuses.
    pub fn refuse_if_field_redacted(
        &self,
        collection: &str,
        field: &str,
        what: &str,
    ) -> Result<(), DdlError> {
        if self
            .redaction_for([collection])
            .field_has_rule(&self.state.redaction, field)
        {
            return Err(gate_err(
                FEATURE_NOT_SUPPORTED,
                format!(
                    "column '{field}' on '{collection}' carries a redaction rule for this role, \
                     and {what} is computed from it"
                ),
            ));
        }
        Ok(())
    }

    /// Fail closed when any redaction rule covers `collection`.
    ///
    /// For handlers that derive their answer from whole row bodies (a hash
    /// chain over the document, a sum over an arbitrary expression), where the
    /// covered columns cannot be narrowed to one field.
    pub fn refuse_if_any_redaction(&self, collection: &str, what: &str) -> Result<(), DdlError> {
        if self
            .redaction_for([collection])
            .has_any_rule(&self.state.redaction)
        {
            return Err(gate_err(
                FEATURE_NOT_SUPPORTED,
                format!(
                    "'{collection}' carries a redaction rule for this role, and {what} is \
                     computed from the columns it hides"
                ),
            ));
        }
        Ok(())
    }
}
