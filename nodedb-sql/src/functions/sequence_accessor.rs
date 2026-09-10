// SPDX-License-Identifier: Apache-2.0

//! The sequence-accessor function names, named once for every gate that
//! treats them apart from other scalars.

/// The accessors the planner routes to `SqlCatalog` sequence state.
pub const SEQUENCE_ACCESSORS: [&str; 3] = ["nextval", "currval", "setval"];

/// Whether `name` calls a sequence accessor. Comparison ignores ASCII case.
pub fn is_sequence_accessor(name: &str) -> bool {
    SEQUENCE_ACCESSORS
        .iter()
        .any(|accessor| name.eq_ignore_ascii_case(accessor))
}
