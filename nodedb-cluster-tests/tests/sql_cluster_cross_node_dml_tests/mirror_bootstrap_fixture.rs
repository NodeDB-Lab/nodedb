// SPDX-License-Identifier: BUSL-1.1

//! Two clusters joined the way an operator joins them: real `[mirror]` settings,
//! real PEM files, the real listener and the real applier supervisor. Nothing
//! here seeds a descriptor or a position. `MIRROR DATABASE` does that.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use nodedb::config::server::{
    MirrorClientSettings, MirrorSettings, MirrorSourceSettings, MirrorTlsPaths,
};
use nodedb::control::cluster::pem_io::{write_pem_cert, write_pem_private_key};
use nodedb::control::mirror::applier::spawn_mirror_supervisor;
use nodedb::control::mirror::source::cut_loop::spawn_mirror_source_loops;
use nodedb::control::mirror::source::listener::spawn_mirror_listener;
use nodedb_cluster::transport::config::SNI_HOSTNAME;
use nodedb_cluster::{TlsCredentials, generate_node_credentials, issue_leaf_for_sans};
use nodedb_types::{DatabaseId, MirrorStatus};
use tempfile::TempDir;

use crate::common::cluster_harness::{
    TestCluster, TestClusterNode, read_once_a_leader_exists, wait_for, wait_for_async,
};

pub(super) const SOURCE_NAME: &str = "source";
const CLIENT_NAME: &str = "dr";

/// The PEM files of one link.
pub(super) struct LinkPki {
    _dir: TempDir,
    server: MirrorTlsPaths,
    client: MirrorTlsPaths,
    client_spki: String,
}

fn write_identity(dir: &TempDir, name: &str, creds: &TlsCredentials) -> MirrorTlsPaths {
    let (cert, key, ca) = (
        format!("{name}.pem"),
        format!("{name}.key"),
        format!("{name}-ca.pem"),
    );
    write_pem_cert(dir.path(), &cert, creds.cert.as_ref()).expect("write the certificate");
    write_pem_private_key(dir.path(), &key, creds.key.secret_der()).expect("write the key");
    write_pem_cert(dir.path(), &ca, creds.ca_cert.as_ref()).expect("write the CA");
    MirrorTlsPaths {
        cert: dir.path().join(cert),
        key: dir.path().join(key),
        ca: dir.path().join(ca),
    }
}

pub(super) fn link_pki() -> LinkPki {
    let dir = TempDir::new().expect("tempdir");
    let (ca, server) = generate_node_credentials(SOURCE_NAME).expect("server credentials");
    let client = issue_leaf_for_sans(&ca, &[CLIENT_NAME]).expect("client credentials");
    LinkPki {
        server: write_identity(&dir, "server", &server),
        client: write_identity(&dir, "client", &client),
        client_spki: hex::encode(client.spki_pin),
        _dir: dir,
    }
}

/// Serve `databases` from every node of `source`, and run its source loops.
/// Answers each node's listener address.
pub(super) fn serve_source(
    source: &TestCluster,
    pki: &LinkPki,
    databases: &[&str],
) -> Vec<SocketAddr> {
    let settings = MirrorSettings {
        cluster_name: Some(SOURCE_NAME.to_string()),
        listen: Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))),
        tls: Some(pki.server.clone()),
        clients: BTreeMap::from([(
            CLIENT_NAME.to_string(),
            MirrorClientSettings {
                spki: pki.client_spki.clone(),
                databases: databases.iter().map(ToString::to_string).collect(),
            },
        )]),
        sources: BTreeMap::new(),
    };
    source
        .nodes
        .iter()
        .map(|node| {
            let bound = spawn_mirror_listener(&node.shared, &settings)
                .expect("the listener binds")
                .expect("the config names a listener");
            spawn_mirror_source_loops(&node.shared);
            bound
        })
        .collect()
}

/// The `[mirror]` section of the mirror cluster.
pub(super) fn mirror_settings(pki: &LinkPki, seeds: &[SocketAddr]) -> MirrorSettings {
    MirrorSettings {
        cluster_name: Some(CLIENT_NAME.to_string()),
        listen: None,
        tls: None,
        clients: BTreeMap::new(),
        sources: BTreeMap::from([(
            SOURCE_NAME.to_string(),
            MirrorSourceSettings {
                seeds: seeds.to_vec(),
                server_name: SNI_HOSTNAME.to_string(),
                tls: pki.client.clone(),
            },
        )]),
    }
}

/// What the harness's boot leaves out: the settings and the supervisor that
/// `spawn_background_loops` installs on a production node.
pub(super) fn boot_mirror_loops(mirror: &TestCluster, settings: &MirrorSettings) {
    for node in &mirror.nodes {
        node.shared
            .mirror
            .settings
            .set(settings.clone())
            .expect("the settings install once per boot");
        spawn_mirror_supervisor(&node.shared);
    }
}

/// The mirror's id on `node`, once its catalog holds `name`.
pub(super) fn database_on(node: &TestClusterNode, name: &str) -> Option<DatabaseId> {
    node.shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("catalog")
}

/// Wait until every mirror node shows `name` following, and answer its id.
pub(super) async fn await_following(mirror: &TestCluster, name: &str) -> DatabaseId {
    wait_for(
        &format!("every mirror node shows '{name}' following"),
        Duration::from_secs(180),
        Duration::from_millis(100),
        || {
            mirror.nodes.iter().all(|node| {
                database_on(node, name)
                    .and_then(|id| {
                        node.shared
                            .credentials
                            .catalog()
                            .get_database(id)
                            .expect("catalog")
                    })
                    .and_then(|descriptor| descriptor.mirror_origin)
                    .is_some_and(|origin| matches!(origin.status, MirrorStatus::Following))
            })
        },
    )
    .await;
    database_on(&mirror.nodes[0], name).expect("the mirror exists")
}

pub(super) async fn use_database(cluster: &TestCluster, database: &str) {
    for node in &cluster.nodes {
        node.exec(&format!("USE DATABASE {database}"))
            .await
            .unwrap_or_else(|e| panic!("USE DATABASE {database}: {e}"));
    }
}

/// Every node reads its own replica of the mirror.
pub(super) async fn read_mirror(mirror: &TestCluster, name: &str) {
    use_database(mirror, name).await;
    for node in &mirror.nodes {
        node.exec("SET default_read_consistency = 'eventual'")
            .await
            .expect("a mirror serves eventual reads");
    }
}

/// Each row of `messages`, its cells joined, sorted.
fn joined_rows(messages: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    let mut out: Vec<String> = messages
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("NULL"))
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect();
    out.sort();
    out
}

/// Every row `sql` returns through `node`, once a leader serves it.
pub(super) async fn rows(node: &TestClusterNode, sql: &str) -> Vec<String> {
    let messages = read_once_a_leader_exists(
        sql,
        Duration::from_secs(30),
        Duration::from_millis(100),
        || node.client.simple_query(sql),
    )
    .await;
    joined_rows(&messages)
}

/// Wait until every mirror node answers `sql` with `expected`.
pub(super) async fn await_mirror_reads(mirror: &TestCluster, sql: &str, expected: &[String]) {
    wait_for_async(
        &format!("every mirror node answers {sql} with {expected:?}"),
        Duration::from_secs(90),
        Duration::from_millis(200),
        || async move {
            for node in &mirror.nodes {
                let Ok(messages) = node.client.simple_query(sql).await else {
                    return false;
                };
                if joined_rows(&messages) != expected {
                    return false;
                }
            }
            true
        },
    )
    .await;
}
