// SPDX-License-Identifier: BUSL-1.1

//! Open a link to a source the `[mirror.sources]` config names.

use std::sync::Arc;

use nodedb_cluster::mirror::{CrossClusterLink, MirrorLinkConfig, derive_link_id};

use crate::control::mirror::source::listener::read_mirror_credentials;
use crate::control::state::SharedState;

/// This cluster's link to `source_database` on `source_cluster`. A source the
/// config does not name is refused: the config is what a link trusts.
pub fn open_source_link(
    state: &SharedState,
    source_cluster: &str,
    source_database: &str,
    mode: nodedb_types::MirrorMode,
) -> crate::Result<Arc<CrossClusterLink>> {
    let source = state
        .mirror
        .settings
        .get()
        .and_then(|settings| settings.sources.get(source_cluster))
        .ok_or_else(|| crate::Error::Config {
            detail: format!(
                "[mirror.sources] names no source '{source_cluster}'; add its seeds, \
                 server_name and tls"
            ),
        })?;
    let mirror_cluster_id =
        state
            .mirror
            .cluster_id
            .get()
            .copied()
            .ok_or_else(|| crate::Error::Internal {
                detail: "this node has not joined its cluster yet".to_string(),
            })?;
    let credentials = read_mirror_credentials(&source.tls)?;
    let link = CrossClusterLink::open(
        MirrorLinkConfig {
            source_cluster: source_cluster.to_string(),
            source_database: source_database.to_string(),
            mirror_cluster_id,
            link_id: derive_link_id(mirror_cluster_id, source_database),
            mode,
            seeds: source.seeds.clone(),
            server_name: source.server_name.clone(),
        },
        &credentials,
    )
    .map_err(|e| crate::Error::Config {
        detail: format!("[mirror.sources.{source_cluster}]: {e}"),
    })?;
    Ok(Arc::new(link))
}
