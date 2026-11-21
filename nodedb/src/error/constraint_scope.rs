// SPDX-License-Identifier: BUSL-1.1

//! Why a CRDT constraint install refused a constraint.
//!
//! Carried by [`Error::ConstraintScope`](super::types::Error::ConstraintScope).
//! A constraint names collections by their stored names. The install key is
//! the stored name the set installs under. A constraint that names another
//! form never meets the validator's lookups, so the install refuses it.

use std::fmt;

/// The constraint an install refused, and the name it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintScopeRefusal {
    /// The constraint's `collection` differs from the install key.
    CollectionMismatch {
        /// The refused constraint's name.
        constraint: String,
        /// The collection the constraint names.
        collection: String,
    },
    /// A foreign key references a name that is no stored collection of the
    /// install key's database.
    ForeignReferent {
        /// The refused constraint's name.
        constraint: String,
        /// The collection the foreign key references.
        ref_collection: String,
    },
}

impl fmt::Display for ConstraintScopeRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CollectionMismatch {
                constraint,
                collection,
            } => write!(
                f,
                "constraint '{constraint}' names collection '{collection}'"
            ),
            Self::ForeignReferent {
                constraint,
                ref_collection,
            } => write!(
                f,
                "foreign key '{constraint}' references '{ref_collection}', \
                 which is no stored collection of the same database"
            ),
        }
    }
}
