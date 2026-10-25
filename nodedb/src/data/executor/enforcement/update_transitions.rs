// SPDX-License-Identifier: BUSL-1.1

//! The check every path that rewrites an existing row runs: the collection's
//! state constraints and transition checks, judged on the stored image and
//! the post-image.
//!
//! Point UPDATE, bulk UPDATE, UPDATE ... FROM, every put that replaces a row
//! (UPSERT, MERGE, staged writes at commit) and CRDT admission call this one
//! function, so a transition rule binds every form of update the same way.

use nodedb_physical::physical_plan::EnforcementOptions;
use nodedb_physical::physical_plan::document::{StateTransitionDef, TransitionCheckDef};

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

use super::{state_transition, transition_check};

/// The rules a collection declares on how a row may change.
#[derive(Clone, Copy)]
pub struct UpdateTransitionRules<'a> {
    pub state_constraints: &'a [StateTransitionDef],
    pub transition_checks: &'a [TransitionCheckDef],
}

impl<'a> UpdateTransitionRules<'a> {
    /// The rules a registered collection's enforcement carries.
    pub fn of(enforcement: &'a EnforcementOptions) -> Self {
        Self {
            state_constraints: &enforcement.state_constraints,
            transition_checks: &enforcement.transition_checks,
        }
    }

    /// Whether any rule is declared.
    pub fn is_empty(&self) -> bool {
        self.state_constraints.is_empty() && self.transition_checks.is_empty()
    }
}

/// Judge `old_doc` → `new_doc` against `rules`. `user_roles` are the roles of
/// the identity that writes, which a role-guarded transition requires.
pub fn check_update_transitions(
    collection: &str,
    rules: UpdateTransitionRules<'_>,
    old_doc: &serde_json::Value,
    new_doc: &serde_json::Value,
    user_roles: &[String],
    crs: &nodedb_crs::CrsContext<'_>,
) -> Result<(), ErrorCode> {
    if !rules.state_constraints.is_empty() {
        state_transition::check_state_transitions(
            collection,
            rules.state_constraints,
            old_doc,
            new_doc,
            user_roles,
        )?;
    }
    if !rules.transition_checks.is_empty() {
        transition_check::check_transition_predicates(
            collection,
            rules.transition_checks,
            old_doc,
            new_doc,
            crs,
        )?;
    }
    Ok(())
}

impl CoreLoop {
    /// [`check_update_transitions`] on two stored bodies, each decoded in the
    /// collection's storage mode. A collection with no rule decodes nothing.
    /// An image that does not decode fails the write, since skipping the
    /// check would admit the transition the collection forbids.
    pub(in crate::data::executor) fn check_stored_update_transitions(
        &self,
        config_key: &(DatabaseId, TenantId, String),
        collection: &str,
        old_bytes: &[u8],
        new_bytes: &[u8],
        user_roles: &[String],
    ) -> Result<(), ErrorCode> {
        let Some(config) = self.doc_configs.get(config_key) else {
            return Ok(());
        };
        let rules = UpdateTransitionRules::of(&config.enforcement);
        if rules.is_empty() {
            return Ok(());
        }
        let old_doc = self
            .decode_stored_document(config, old_bytes)
            .map_err(ErrorCode::from)?;
        let new_doc = self
            .decode_stored_document(config, new_bytes)
            .map_err(ErrorCode::from)?;
        check_update_transitions(
            collection,
            rules,
            &old_doc,
            &new_doc,
            user_roles,
            &self.crs_context(),
        )
    }
}
