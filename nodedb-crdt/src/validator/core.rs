// SPDX-License-Identifier: Apache-2.0

//! Validator struct, constructors, and accessors.

use std::collections::HashMap;

use crate::constraint::{Constraint, ConstraintSet};
use crate::dead_letter::DeadLetterQueue;
use crate::deferred::DeferredQueue;
use crate::policy::PolicyRegistry;
use crate::signing::DeltaSigner;

/// The constraint validator.
///
/// Validates proposed changes against a set of constraints and the current
/// committed state. Violations are resolved via declarative policies:
/// - AUTO_RESOLVED — policy handles it (e.g., LAST_WRITER_WINS)
/// - DEFERRED — queued for exponential backoff retry (CASCADE_DEFER)
/// - WEBHOOK_REQUIRED — caller must POST to webhook for decision
/// - ESCALATE — route to dead-letter queue (fallback)
pub struct Validator {
    pub(super) constraints: ConstraintSet,
    pub(super) dlq: DeadLetterQueue,
    pub(super) policies: PolicyRegistry,
    pub(super) deferred: DeferredQueue,
    /// Monotonic suffix counter: (collection, field) -> next suffix number
    pub(super) suffix_counter: HashMap<(String, String), u64>,
    /// Optional delta signature verifier. When set, signed deltas are
    /// verified before constraint validation.
    pub(super) delta_verifier: Option<DeltaSigner>,
}

impl Validator {
    /// Create a new validator with default (ephemeral) policies.
    pub fn new(constraints: ConstraintSet, dlq_capacity: usize) -> Self {
        Self::new_with_policies(constraints, dlq_capacity, PolicyRegistry::new(), 1000)
    }

    /// Create a new validator with custom policies and deferred queue.
    pub fn new_with_policies(
        constraints: ConstraintSet,
        dlq_capacity: usize,
        policies: PolicyRegistry,
        deferred_capacity: usize,
    ) -> Self {
        Self {
            constraints,
            dlq: DeadLetterQueue::new(dlq_capacity),
            policies,
            deferred: DeferredQueue::new(deferred_capacity),
            suffix_counter: HashMap::new(),
            delta_verifier: None,
        }
    }

    /// Replace the constraint set scoped to `collection`. Constraints for
    /// other collections are unaffected.
    pub fn set_collection_constraints(&mut self, collection: &str, new: Vec<Constraint>) {
        self.constraints.set_for_collection(collection, new);
    }

    /// Remove every constraint scoped to `collection`.
    pub fn clear_collection_constraints(&mut self, collection: &str) {
        self.constraints.clear_for_collection(collection);
    }

    /// Read the constraints currently scoped to `collection`.
    pub fn constraints_for(&self, collection: &str) -> Vec<&Constraint> {
        self.constraints.for_collection(collection)
    }

    /// Access the dead-letter queue.
    pub fn dlq(&self) -> &DeadLetterQueue {
        &self.dlq
    }

    /// Mutable access to the DLQ (for dequeue/retry).
    pub fn dlq_mut(&mut self) -> &mut DeadLetterQueue {
        &mut self.dlq
    }

    /// Access the policy registry.
    pub fn policies(&self) -> &PolicyRegistry {
        &self.policies
    }

    /// Mutable access to the policy registry.
    pub fn policies_mut(&mut self) -> &mut PolicyRegistry {
        &mut self.policies
    }

    /// Access the deferred queue.
    pub fn deferred(&self) -> &DeferredQueue {
        &self.deferred
    }

    /// Mutable access to the deferred queue.
    pub fn deferred_mut(&mut self) -> &mut DeferredQueue {
        &mut self.deferred
    }

    /// Set the delta signature verifier. Every non-zero signature is verified;
    /// signed input fails closed when no verifier is installed.
    pub fn set_delta_verifier(&mut self, verifier: DeltaSigner) {
        self.delta_verifier = Some(verifier);
    }

    /// Access the delta verifier.
    pub fn delta_verifier(&self) -> Option<&DeltaSigner> {
        self.delta_verifier.as_ref()
    }

    /// Mutable access to the delta verifier.
    pub fn delta_verifier_mut(&mut self) -> Option<&mut DeltaSigner> {
        self.delta_verifier.as_mut()
    }
}
