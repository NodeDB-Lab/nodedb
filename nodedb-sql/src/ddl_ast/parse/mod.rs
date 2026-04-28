mod alert;
mod backup;
mod change_stream;
mod cluster_admin;
mod collection;
mod dispatch;
mod helpers;
mod index;
mod maintenance;
mod materialized_view;
mod retention;
mod rls;
mod schedule;
mod sequence;
mod trigger;
mod user_auth;
pub mod vector_primary;

pub use dispatch::parse;
