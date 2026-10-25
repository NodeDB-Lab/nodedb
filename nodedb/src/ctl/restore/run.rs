// SPDX-License-Identifier: BUSL-1.1

//! `nodedb restore`: offline point-in-time restore of one node, alone or as
//! its part of a cluster restore.

use std::fmt;

use super::archive::Archive;
use super::args::{RestoreArgs, RestoreScope, RestoreTarget};
use super::cluster::{ClusterReport, execute_cluster_plan, plan_cluster_restore};
use super::env::RestoreEnv;
use super::error::RestoreError;
use super::execute::execute_plan;
use super::life::choose_life;
use super::plan::plan_restore;
use super::report::RestoreReport;
use crate::ServerConfig;
use crate::control::cluster::tls::TLS_SUBDIR;

/// What a restore planned, and wrote unless it was a dry run.
pub enum Restored {
    Node(RestoreReport),
    Cluster(ClusterReport),
}

impl fmt::Display for Restored {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Node(report) => fmt::Display::fmt(report, f),
            Self::Cluster(report) => fmt::Display::fmt(report, f),
        }
    }
}

/// Plan a restore, and execute it unless `args.dry_run`.
pub async fn restore(args: &RestoreArgs) -> Result<Restored, RestoreError> {
    let env = RestoreEnv::from_config_file(&args.config, kept_entries(&args.scope)).await?;
    restore_in(&env, &args.scope, args.incarnation.as_deref(), args.dry_run).await
}

/// [`restore`] with the server config already read.
pub async fn restore_with_config(
    config: &ServerConfig,
    scope: &RestoreScope,
    incarnation: Option<&str>,
    dry_run: bool,
) -> Result<Restored, RestoreError> {
    let env = RestoreEnv::from_config(config, kept_entries(scope)).await?;
    restore_in(&env, scope, incarnation, dry_run).await
}

/// A cluster restore keeps the node's TLS directory: the data directory can
/// hold it and nothing else.
fn kept_entries(scope: &RestoreScope) -> &'static [&'static str] {
    match scope {
        RestoreScope::Node(_) => &[],
        RestoreScope::Cluster { .. } => &[TLS_SUBDIR],
    }
}

async fn restore_in(
    env: &RestoreEnv,
    scope: &RestoreScope,
    incarnation: Option<&str>,
    dry_run: bool,
) -> Result<Restored, RestoreError> {
    let life = choose_life(&env.snapshot_root, &env.key, env.node_id, incarnation).await?;
    let archive = Archive::list(&env.cold, env.node_id, &life.incarnation).await?;
    match scope {
        RestoreScope::Node(target) => restore_node(env, &life, &archive, target, dry_run).await,
        RestoreScope::Cluster { restore_point } => {
            let plan = plan_cluster_restore(env, &life, &archive, *restore_point).await?;
            let outcome = if dry_run {
                None
            } else {
                Some(execute_cluster_plan(env, &life, &archive, &plan).await?)
            };
            Ok(Restored::Cluster(ClusterReport {
                data_dir: env.data_dir.clone(),
                plan,
                outcome,
            }))
        }
    }
}

async fn restore_node(
    env: &RestoreEnv,
    life: &super::life::Life,
    archive: &Archive<'_>,
    target: &RestoreTarget,
    dry_run: bool,
) -> Result<Restored, RestoreError> {
    let plan = plan_restore(env, life, archive, target).await?;
    let outcome = if dry_run {
        None
    } else {
        Some(execute_plan(env, life, archive, &plan).await?)
    };
    Ok(Restored::Node(RestoreReport {
        data_dir: env.data_dir.clone(),
        plan,
        outcome,
    }))
}

/// Run [`restore`] and return the process exit code.
///
/// The restore runs on its own thread and runtime: the caller can already be
/// inside a runtime, where blocking on another is refused.
pub fn run(args: RestoreArgs) -> i32 {
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| RestoreError::Node(crate::Error::Io(e)))?;
        runtime.block_on(restore(&args))
    });
    match worker.join() {
        Ok(Ok(report)) => {
            print!("{report}");
            0
        }
        Ok(Err(e)) => {
            eprintln!("error: {e}");
            1
        }
        Err(_) => {
            eprintln!("error: the restore thread panicked");
            1
        }
    }
}
