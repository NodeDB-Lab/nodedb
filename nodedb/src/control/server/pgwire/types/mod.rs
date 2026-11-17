// SPDX-License-Identifier: BUSL-1.1

//! Pgwire-layer shared helpers.
//!
//! Split into focused submodules so each concern lives in one place; this
//! module is `pub mod` + `pub use` only.

pub mod error_map;
pub mod field;
pub mod numeric_sqlstate;
pub mod parse;
pub mod privilege;
pub mod status_sqlstate;
pub mod wire_type;

pub use error_map::{
    dml_fold_error_to_pg, error_to_pg, error_to_pg_in_context, error_to_sqlstate,
    shape_error_to_pg, sqlstate_error,
};
pub use field::{text_field, type_name_to_pgwire};
pub use parse::parse_role;
pub use privilege::{
    require_cluster_admin, require_database_owner, require_database_owner_or_higher,
    require_superuser, require_tenant_admin,
};
pub use status_sqlstate::{notice_warning, response_status_to_sqlstate};
