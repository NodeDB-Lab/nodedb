//! `GRANT` / `REVOKE` DDL handlers.
//!
//! Three sub-handlers, all driven through the metadata raft path:
//!
//! - `GRANT/REVOKE ROLE x TO/FROM user` — see [`role`]. Reuses the
//!   existing `CatalogEntry::PutUser` variant via
//!   `CredentialStore::prepare_user_with_roles` so a role-membership
//!   change ships the full updated `StoredUser` record. No new
//!   variant is needed.
//! - `GRANT/REVOKE <perm> ON <collection|FUNCTION name>
//!   TO/FROM <grantee>` — see [`permission`]. Proposes
//!   `CatalogEntry::{PutPermission, DeletePermission}` so every
//!   follower's `PermissionStore` and `OWNERS` redb stay in sync.
//! - The top-level [`dispatch`] router only decides which sub-handler
//!   to call based on whether the second token is `ROLE`.

pub mod dispatch;
pub mod permission;
pub mod role;

pub use dispatch::{handle_grant, handle_revoke};
