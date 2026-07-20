// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE` / `ALTER` / `DROP USER` DDL handlers.

mod alter;
mod create;
mod drop;
mod iso8601;
mod reassign_owned;

pub use alter::alter_user;
pub use create::create_user;
pub(super) use drop::drop_tenant_admin;
pub use drop::drop_user;
