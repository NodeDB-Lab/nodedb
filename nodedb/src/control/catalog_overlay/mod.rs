// SPDX-License-Identifier: BUSL-1.1

//! Uncommitted-DDL overlay over the committed system catalog.
//!
//! DDL inside an explicit transaction is buffered per connection and lands in
//! the catalog only at COMMIT. Without an overlay every later statement in
//! that same transaction resolves names against the committed catalog and
//! misses its own `CREATE`. Each module here replays one connection's
//! buffered entries over a committed catalog read for one catalog kind, so a
//! transaction sees its own DDL in statement order while every other session
//! still sees only committed state. [`core`] holds the shared replay engine;
//! each other module supplies its kind's `targets` / `step` closures.
//!
//! Sequences use a different mechanism entirely — see
//! `control::sequence::ddl_overlay` — because `NEXTVAL` mutates shared
//! runtime state a rolled-back transaction must never let another connection
//! observe. Array DDL is not buffered at all: `CREATE`/`ALTER`/`DROP ARRAY`
//! apply and persist synchronously in the write funnel
//! (`array_catalog::ddl::apply_authorized_ddl`), regardless of transaction
//! state, so there is no uncommitted state for an overlay to replay.

mod collection;
mod core;
mod function;
mod index_record;
mod materialized_view;
mod procedure;
mod trigger;
mod vector_index_params;

pub use self::collection::{resolve_collection, resolve_tenant_collections};
pub use self::function::resolve_function;
pub use self::index_record::{resolve_index_record, resolve_index_records};
pub use self::materialized_view::resolve_materialized_view;
pub use self::procedure::resolve_procedure;
pub use self::trigger::resolve_trigger;
pub use self::vector_index_params::resolve_vector_index_params;
