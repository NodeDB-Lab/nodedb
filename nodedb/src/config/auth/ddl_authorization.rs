// SPDX-License-Identifier: BUSL-1.1

//! `DdlAuthorizationMode`: the node setting that chooses how schema DDL is
//! authorized.

use serde::{Deserialize, Serialize};

/// Policy for schema-defining DDL statements, set as `ddl_authorization`
/// under `[auth]`.
///
/// Every node of a cluster must carry the same value. The value is read once
/// at startup and a change needs a restart. The default is `open`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DdlAuthorizationMode {
    /// No check. Every authenticated user may run covered DDL.
    #[default]
    Open,
    /// Covered DDL needs the `TenantAdmin` role or superuser.
    TenantAdmin,
    /// Creating an object needs the `Create` permission. Changing or
    /// dropping an existing object needs the `Alter` or `Drop` permission on
    /// it. Tenant-wide statements need the `Admin` permission.
    Permission,
    /// Changing or dropping an existing object needs its owner, `TenantAdmin`
    /// or superuser. Creating a new object is not checked. Tenant-wide
    /// statements need `TenantAdmin` or superuser.
    Owner,
    /// Creating an object needs `Create`. An existing object needs its owner,
    /// `TenantAdmin` or superuser. Tenant-wide statements need `TenantAdmin`
    /// or superuser.
    Mix,
}
