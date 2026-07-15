// SPDX-License-Identifier: BUSL-1.1

//! Authenticated identity, role, permission, and plan-to-permission mapping.
//!
//! Module layout:
//! - `database_set` — `DatabaseSet` (which databases an identity may access).
//! - `authenticated` — `AuthenticatedIdentity` + `AuthMethod` (who is bound to
//!   the session and how they proved it).
//! - `role` — built-in and custom role enum + `Display`/`FromStr`.
//! - `permission` — `Permission`, `PermissionTarget`, and the role → permission
//!   implicit-grant map.
//! - `plan_permission` — `required_permission(plan)` mapping every
//!   `PhysicalPlan` variant to the `Permission` needed to execute it.
//!
//! Every file in this module that pattern-matches on `PermissionTarget`,
//! `Role`, or `PhysicalPlan` must `#![deny(clippy::wildcard_enum_match_arm)]`
//! so that adding a new variant is a compile error at every call site rather
//! than silently falling through.

pub mod authenticated;
pub mod database_set;
pub mod external_authority;
pub mod permission;
pub mod plan_permission;
pub mod role;

pub use authenticated::{AuthMethod, AuthenticatedIdentity};
pub use database_set::DatabaseSet;
pub use external_authority::roles_from_external_claims;
pub use permission::{Permission, PermissionTarget, role_grants_permission};
pub use plan_permission::required_permission;
pub use role::Role;
