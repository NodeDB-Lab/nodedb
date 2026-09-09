// SPDX-License-Identifier: Apache-2.0

//! Shared builders for built-in function registrations.

use nodedb_types::columnar::ColumnType;

use super::super::registry::{ArgTypeSpec, FunctionCategory, FunctionMeta, SearchTrigger, Version};

pub(super) const V0_1_0: Version = Version::new(0, 1, 0);

pub(super) fn no_trigger() -> SearchTrigger {
    SearchTrigger::None
}

pub(super) fn m(
    name: &'static str,
    cat: FunctionCategory,
    min: usize,
    max: usize,
    trigger: SearchTrigger,
    return_type: Option<ColumnType>,
    arg_types: &'static [ArgTypeSpec],
) -> FunctionMeta {
    FunctionMeta {
        name,
        category: cat,
        min_args: min,
        max_args: max,
        search_trigger: trigger,
        return_type,
        arg_types,
        since: V0_1_0,
        volatility: nodedb_types::Volatility::Immutable,
    }
}
