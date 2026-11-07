// SPDX-License-Identifier: BUSL-1.1

pub mod allocate;
pub mod persist;
pub mod registry;

pub use allocate::allocate_database_id;
pub use persist::DatabaseHwmPersist;
pub use registry::{DatabaseAllocError, DatabaseRegistry, USER_DB_START, database_id_space};
