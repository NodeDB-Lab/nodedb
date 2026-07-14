// SPDX-License-Identifier: BUSL-1.1

//! pgwire connection factory: SCRAM-SHA-256 / Argon2 authentication and
//! session bootstrapping.
//!
//! **Auth scope**: pgwire authenticates exclusively via SCRAM-SHA-256 over
//! the Postgres wire protocol. OIDC bearer tokens are NOT accepted here —
//! the Postgres wire protocol has no clean way to carry a bearer without
//! a non-standard extension or a sidecar proxy. OIDC bearer logins live on
//! the native and HTTP entry points (see `control/security/oidc/`); do not
//! add a JWT branch to this factory.

mod auth;
mod provider;
mod server;
mod startup;

pub use auth::NodeDbAuthSource;
pub use server::NodeDbPgHandlerFactory;
