// SPDX-License-Identifier: BUSL-1.1

//! Stateless PUT admission: the checks a write must pass before it is written.
//!
//! Separate from the write itself because these are the only steps in the put
//! that are allowed to REFUSE a row, and none of them may leave a trace when
//! they do. They read the collection's declared rules and the pre-image, decide
//! yes or no, and touch nothing — which is what makes it safe to run them
//! inside a transaction the caller owns and will commit. Keeping them out of
//! the write path means a new rule is added here, where "no side effects" is
//! the file's stated contract, rather than somewhere a later reader has to
//! prove it.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::{
    append_only, period_lock, state_transition, transition_check,
};
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::ResolvedSumTarget;

use super::types::map_enforcement_error;

/// The write being admitted, and the pre-image it is judged against.
pub(in crate::data::executor::handlers::point) struct PutEnforcement<'a> {
    pub(in crate::data::executor::handlers::point) config_key: &'a (DatabaseId, TenantId, String),
    pub(in crate::data::executor::handlers::point) database_id: u64,
    pub(in crate::data::executor::handlers::point) tid: u64,
    pub(in crate::data::executor::handlers::point) collection: &'a str,
    /// The incoming body in MessagePack form for both storage modes (a strict
    /// collection encodes its Binary Tuple separately).
    pub(in crate::data::executor::handlers::point) value: &'a [u8],
    /// The row as currently stored, when one exists.
    pub(in crate::data::executor::handlers::point) old_value: &'a Option<Vec<u8>>,
    pub(in crate::data::executor::handlers::point) user_roles: &'a [String],
    /// `(target collection, join-key value)` → target row surrogate, resolved
    /// on the Control Plane at plan time. A period-lock check reads its
    /// reference row's surrogate off this slice, keyed by
    /// `(config.ref_table, period value)`.
    pub(in crate::data::executor::handlers::point) resolved_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Run stateless PUT enforcement, unified across the autocommit
    /// (`apply_point_put`) and transactional (`tx_point_put`) paths.
    /// These checks have no persistent side effect, so a violation here
    /// simply aborts before the write — safe even though the caller
    /// owns a single redb write transaction.
    ///
    /// A no-op for an unregistered collection, and skipped entirely by
    /// CRDT-sync materialization (which passes `enforce == false`): those
    /// deltas already passed admission on their origin replica at Raft commit
    /// time.
    pub(in crate::data::executor::handlers::point) fn check_stateless_put_enforcement(
        &self,
        enforce: bool,
        p: PutEnforcement<'_>,
    ) -> crate::Result<()> {
        let PutEnforcement {
            config_key,
            database_id,
            tid,
            collection,
            value,
            old_value,
            user_roles,
            resolved_targets,
        } = p;
        if !enforce {
            return Ok(());
        }
        let Some(config) = self.doc_configs.get(config_key) else {
            return Ok(());
        };

        append_only::check_point_put(collection, &config.enforcement, old_value)
            .map_err(map_enforcement_error)?;
        if let Some(ref pl) = config.enforcement.period_lock {
            period_lock::check_period_lock(
                &self.sparse,
                database_id,
                tid,
                collection,
                value,
                pl,
                resolved_targets,
            )
            .map_err(map_enforcement_error)?;
        }
        // Both images must be readable whenever a transition rule is
        // configured: skipping a configured check because an image would
        // not decode admits exactly the write the collection's own rules
        // forbid, and says nothing about having done so. The prior image
        // goes through the storage-mode-aware decoder because a strict
        // collection stores it as a Binary Tuple, which the schemaless
        // decoder cannot read at all.
        if let Some(old_bytes) = old_value.as_ref()
            && (!config.enforcement.state_constraints.is_empty()
                || !config.enforcement.transition_checks.is_empty())
        {
            let old_doc = self.decode_stored_document(config, old_bytes)?;
            let new_doc = doc_format::decode_document(value)?;
            if !config.enforcement.state_constraints.is_empty() {
                state_transition::check_state_transitions(
                    collection,
                    &config.enforcement.state_constraints,
                    &old_doc,
                    &new_doc,
                    user_roles,
                )
                .map_err(map_enforcement_error)?;
            }
            if !config.enforcement.transition_checks.is_empty() {
                transition_check::check_transition_predicates(
                    collection,
                    &config.enforcement.transition_checks,
                    &old_doc,
                    &new_doc,
                )
                .map_err(map_enforcement_error)?;
            }
        }
        Ok(())
    }
}
