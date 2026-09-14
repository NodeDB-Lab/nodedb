// SPDX-License-Identifier: BUSL-1.1

//! Pgwire-layer shared helpers.
//!
//! Split into focused submodules so each concern lives in one place; this
//! module is `pub mod` + `pub use` only.

pub mod error_map;
pub mod field;
pub mod parse;
pub mod privilege;

pub use error_map::{
    error_to_sqlstate, notice_warning, numeric_code_to_sqlstate, response_status_to_sqlstate,
    sqlstate_error,
};
pub use field::{
    bool_field, bytea_field, float4_array_field, float4_field, float8_array_field, float8_field,
    int2_field, int4_field, int8_field, json_field, jsonb_field, text_field, timestamp_field,
    timestamptz_field, type_name_to_pgwire, varchar_field,
};
pub use parse::parse_role;
pub use privilege::{
    require_cluster_admin, require_database_owner, require_database_owner_or_higher,
    require_superuser, require_tenant_admin,
};
