// SPDX-License-Identifier: Apache-2.0

//! Scalar function registrations, split by domain.

mod array_elem;
mod array_fn;
mod datetime;
mod doc;
mod id_fn;
mod json_legacy;
mod math;
mod misc;
mod pg_fts;
mod pg_json;
mod sequence_fn;
mod spatial;
mod string;
mod vector;

use crate::functions::registry::FunctionMeta;

pub(super) fn scalar_functions() -> Vec<FunctionMeta> {
    let mut fns = Vec::new();
    fns.extend(vector::vector_functions());
    fns.extend(spatial::spatial_functions());
    fns.extend(datetime::datetime_functions());
    fns.extend(doc::doc_functions());
    fns.extend(string::string_functions());
    fns.extend(math::math_functions());
    fns.extend(pg_json::pg_json_functions());
    fns.extend(json_legacy::json_legacy_functions());
    fns.extend(pg_fts::pg_fts_functions());
    fns.extend(array_fn::array_fn_functions());
    fns.extend(array_elem::array_elem_functions());
    fns.extend(id_fn::id_fn_functions());
    fns.extend(sequence_fn::sequence_fn_functions());
    fns.extend(misc::misc_functions());
    fns
}
