// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL family handlers + router.
//!
//! Handlers here build [`DdlResult`] / [`DdlError`] directly, carrying no
//! pgwire types. [`try_dispatch`] recognizes the migrated families and routes
//! to them; every other statement returns `None` so the transitional pgwire
//! delegation in the parent [`super::dispatch`] handles it.

pub mod alert;
pub mod apikey;
pub mod auth_key;
mod auth_support;
pub mod auth_user;
pub mod blacklist;
pub mod bulk;
pub mod change_stream;
pub mod chunk_text;
pub mod cluster;
pub mod collection;
mod column_default;
pub mod conflict_policy;
pub mod constraint;
pub mod consumer_group;
pub mod continuous_agg;
pub mod convert;
pub mod crdt_ops;
pub mod custom_type;
pub mod database;
pub mod dsl;
pub mod emergency_ddl;
pub mod estimate_count;
pub mod explain_ddl;
pub mod explain_tiers;
pub mod field_def;
pub mod function;
pub mod grant;
pub mod graph_ops;
pub mod impersonation;
pub mod inspect;
pub mod inspect_audit;
pub mod kv_atomic;
pub mod kv_sorted_index;
pub mod last_value;
pub mod maintenance;
pub mod match_ops;
pub mod materialized_view;
pub mod metering_ddl;
pub mod observability;
pub mod oidc;
pub mod org_ddl;
pub mod period_lock;
pub mod permission_tree;
pub mod planning;
pub mod procedure;
pub mod query_functions;
pub mod quota_ddl;
pub mod rate_gate;
pub mod read_gate;
pub mod redaction;
pub mod refuse_gate;
pub mod replicate;
pub mod retention_policy;
pub mod rls;
pub mod role;
pub mod router;
pub mod schedule;
pub mod scope_ddl;
pub mod scope_query_ddl;
pub mod sequence;
pub mod service_account;
pub mod session_admin;
pub mod show_changes;
pub mod spatial;
pub mod stream_select;
pub mod synonym_group;
pub mod system_ddl;
pub mod tenant;
pub mod timeseries;
pub mod topic;
pub mod topic_subscribe;
pub mod transfer;
pub mod tree_ops;
pub mod trigger;
pub mod typeguard;
pub mod user;
pub mod vector_replicate;
pub mod version_history;
pub mod weighted_pick;

pub use self::router::try_dispatch;
