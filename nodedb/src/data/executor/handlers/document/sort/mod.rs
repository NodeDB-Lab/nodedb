// SPDX-License-Identifier: BUSL-1.1

//! Document sort: key evaluation, in-memory sort, and external spill/merge.

pub(in crate::data::executor) mod compare;
pub(in crate::data::executor) mod external;
pub(in crate::data::executor) mod in_memory;

pub(in crate::data::executor) use compare::decimal_sort_keys;
pub(in crate::data::executor) use in_memory::{sort_decoded_rows, sort_rows};
