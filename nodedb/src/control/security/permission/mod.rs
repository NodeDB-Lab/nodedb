//! Collection-level permission grants and ownership tracking.
//!
//! Grants: `GRANT READ ON collection TO user/role`
//! Ownership: every collection has an owner with implicit full permissions.
//!
//! Layout:
//! - [`types`] — `Grant`, `OwnerRecord`, target/permission helpers
//!   (`collection_target`, `function_target`, `parse_permission`,
//!   `format_permission`).
//! - [`store`] — `PermissionStore` struct + boot replay
//!   (`load_from`) + legacy `grant` / `revoke` / `grants_on` /
//!   `grants_for` CRUD.
//! - [`check`] — `check`, `check_function`, `is_owner` evaluators.
//! - [`owner`] — `set_owner` / `remove_owner` / `get_owner` /
//!   `list_owners` ownership CRUD.
//! - [`replication`] — applier-side helpers used by
//!   `CatalogEntry::{PutPermission, DeletePermission, PutOwner,
//!   DeleteOwner}`: `prepare_permission`,
//!   `install_replicated_permission`, `install_replicated_revoke`,
//!   `install_replicated_owner`, `install_replicated_remove_owner`,
//!   `permission_exists`.

pub mod check;
pub mod owner;
pub mod replication;
pub mod store;
pub mod types;

pub use replication::prepare_owner;
pub use store::PermissionStore;
pub use types::{
    Grant, OwnerRecord, collection_target, format_permission, function_target, owner_key,
    parse_permission,
};
