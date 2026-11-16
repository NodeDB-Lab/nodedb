// SPDX-License-Identifier: BUSL-1.1

//! Restored vector rows as redo units.
//!
//! A backup's vector section holds each index's live nodes, and its history
//! section the start of each live node a cut can read below and every ended
//! node a cut still reads. Each row of an index re-issues as one unit: a
//! `vector_restore` sub-record carrying every node of the row with its span.
//! A multi-vector document's live nodes travel together. The resolve keeps
//! the nodes the cut rule chooses (`restore_key_plan`), and every replica
//! installs each at its span. A RESTORE never truncates the destination: a
//! row the backup does not hold keeps its nodes.
//!
//! A row the backup binds to a primary key re-issues under the destination's
//! surrogate of that key. One it does not bind keeps the backup's surrogate.
//!
//! A row of a vector-primary collection's primary index carries every
//! version of its sidecar in the same sub-record (`sidecars`), so one
//! install writes the row's nodes and its sidecar together.

use std::collections::{BTreeMap, HashMap, HashSet};

use nodedb_physical::physical_plan::RestoredVectorRow;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

use crate::Error;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::types::{SurrogateBindEntry, TenantId, TraceId, VectorIndexHistory};
use crate::wal::{
    RedoSubRecord, VectorRestoreIndex, VectorRestoreLive, VectorRestoreNode, VectorRestoreRedo,
    VectorSidecarVersion,
};

use super::super::target::{DatabaseTarget, RestoredName};
use super::super::vector_reissue::split_vector_coll_key;
use super::commit::commit_collection;
use super::sidecars::{CollectionSidecars, RestoredSidecars};
use super::units::{CollectionUnits, RowUnit};

/// Every node of one backed-up row, and every version of its sidecar.
#[derive(Debug, Default, PartialEq)]
struct RowNodes {
    /// The live vectors, in node order.
    live: Vec<Vec<f32>>,
    /// The start of the live nodes: the newest one a cut reads below, or
    /// `i64::MIN` when every cut reads them.
    live_from: i64,
    /// Every ended node, ascending by `sys_from`.
    ended: Vec<VectorRestoreNode>,
    /// Every sidecar version, ascending by `sys_from`. Empty for a row of
    /// an index that is no vector-primary collection's primary index.
    sidecar: Vec<VectorSidecarVersion>,
}

impl RowNodes {
    /// A row the backup holds no node of.
    fn empty() -> Self {
        Self {
            live_from: i64::MIN,
            ..Self::default()
        }
    }
}

/// One index's backed-up rows by surrogate, with its dimension.
struct IndexRows {
    rows: BTreeMap<Surrogate, RowNodes>,
    dim: usize,
}

/// Group an index's live nodes `vectors` and its `history` by row.
///
/// Every stored vector is bound, so a node with no surrogate is a corrupt
/// capture and fails the group.
fn index_rows(
    vectors: Vec<(u32, Vec<f32>, Option<Surrogate>)>,
    history: VectorIndexHistory,
) -> crate::Result<IndexRows> {
    let starts: HashMap<u32, i64> = history.live_starts.into_iter().collect();
    let mut rows: BTreeMap<Surrogate, RowNodes> = BTreeMap::new();
    let mut dim = 0usize;
    for (node_id, vector, surrogate) in vectors {
        let Some(surrogate) = surrogate.filter(|s| *s != Surrogate::ZERO) else {
            return Err(Error::Internal {
                detail: format!(
                    "restored vector node {node_id} carries no surrogate; every stored vector \
                     is bound"
                ),
            });
        };
        dim = vector.len();
        let row = rows.entry(surrogate).or_insert_with(RowNodes::empty);
        row.live_from = row
            .live_from
            .max(starts.get(&node_id).copied().unwrap_or(i64::MIN));
        row.live.push(vector);
    }
    for node in history.ended {
        if node.surrogate == Surrogate::ZERO {
            continue;
        }
        dim = node.vector.len();
        let row = rows.entry(node.surrogate).or_insert_with(RowNodes::empty);
        row.ended.push(VectorRestoreNode {
            vector: node.vector,
            sys_from: node.sys_from,
            sys_to: node.sys_to,
        });
    }
    for row in rows.values_mut() {
        row.ended.sort_by_key(|node| node.sys_from);
    }
    Ok(IndexRows { rows, dim })
}

/// Where a restored row installs.
struct RowTarget<'a> {
    name: &'a RestoredName,
    field_name: &'a str,
    dim: usize,
    multi: bool,
    vector_primary: bool,
    primary_index: Option<&'a VectorRestoreIndex>,
}

/// The unit one restored row re-issues as, under `surrogate` in the
/// destination. `pk` is the primary key the backup binds the row to.
fn row_unit(
    target: &RowTarget<'_>,
    surrogate: Surrogate,
    pk: Option<&[u8]>,
    nodes: RowNodes,
) -> crate::Result<RowUnit> {
    let stored = target.name.stored.as_str().to_string();
    let dim = u32::try_from(target.dim).map_err(|_| Error::Internal {
        detail: format!(
            "restore of vector index '{}': dimension {} does not fit a u32",
            target.name.bare, target.dim
        ),
    })?;
    let payload = VectorRestoreRedo {
        collection: stored.clone(),
        field_name: target.field_name.to_string(),
        surrogate: surrogate.as_u32(),
        dim,
        multi: target.multi,
        ended: nodes.ended,
        live: (!nodes.live.is_empty()).then_some(VectorRestoreLive {
            vectors: nodes.live,
            sys_from: nodes.live_from,
        }),
        // The resolve names it from the destination's state.
        end_live_at: None,
        vector_primary: target.vector_primary,
        sidecar: nodes.sidecar,
        primary_index: target.primary_index.cloned(),
        clone_copy_at: None,
    }
    .encode()?;
    Ok(RowUnit {
        ops: vec![RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload,
        }],
        identities: pk
            .map(|pk| CarriedIdentity {
                collection: target.name.bare.clone(),
                pk_bytes: pk.to_vec(),
                surrogate,
            })
            .into_iter()
            .collect(),
        changes: Vec::new(),
        rows: Vec::new(),
        kv_keys: Vec::new(),
        columnar_keys: Vec::new(),
        vector_rows: vec![RestoredVectorRow {
            collection: stored,
            surrogate: surrogate.as_u32(),
        }],
        timeseries: Vec::new(),
    })
}

/// The backed-up indexes of one database: their live nodes, history and
/// multi-vector documents, each keyed `"{db}:{tid}:{coll_key}"`, and the
/// sidecar rows of every vector-primary collection.
pub(in crate::control::backup::restore) struct BackedUpVectors {
    pub vectors: Vec<(String, Vec<u8>)>,
    pub history: Vec<(String, Vec<u8>)>,
    pub multi_documents: Vec<(String, Vec<Surrogate>)>,
    pub sidecars: RestoredSidecars,
}

/// One index's backed-up rows, with where they install.
struct IndexReissue<'a> {
    name: &'a RestoredName,
    field_name: &'a str,
    rows: BTreeMap<Surrogate, RowNodes>,
    dim: usize,
    /// The rows that are multi-vector documents.
    multi: HashSet<Surrogate>,
    vector_primary: bool,
    /// The primary index settings, when the index is a vector-primary
    /// collection's primary index.
    primary_index: Option<VectorRestoreIndex>,
}

/// The primary index of a vector-primary collection, as its catalog entry
/// declares it.
struct PrimaryIndex {
    field: String,
    dim: usize,
    settings: VectorRestoreIndex,
}

/// What the catalog says of one restored collection's vector rows.
struct CollectionVectors {
    vector_primary: bool,
    /// The primary index, for a vector-primary collection.
    primary_index: Option<PrimaryIndex>,
}

fn collection_vectors(
    state: &SharedState,
    target: DatabaseTarget,
    tenant_id: u64,
    bare: &str,
) -> crate::Result<CollectionVectors> {
    let stored = state
        .credentials
        .catalog()
        .get_collection(target.dest, tenant_id, bare)?;
    let vector_primary = stored
        .as_ref()
        .is_some_and(|stored| matches!(stored.primary, nodedb_types::PrimaryEngine::Vector));
    let primary_index = stored
        .and_then(|stored| stored.vector_primary)
        .filter(|_| vector_primary)
        .map(|config| PrimaryIndex {
            field: config.vector_field,
            dim: config.dim as usize,
            settings: VectorRestoreIndex {
                quantization: config.quantization,
                storage_dtype: config.storage_dtype,
                payload_indexes: config.payload_indexes,
            },
        });
    Ok(CollectionVectors {
        vector_primary,
        primary_index,
    })
}

/// Add each sidecar row of `sidecars` to its row of `rows`. A row the
/// backup holds no node of installs its sidecar alone.
fn join_sidecars(rows: &mut BTreeMap<Surrogate, RowNodes>, sidecars: CollectionSidecars) {
    for (surrogate, versions) in sidecars {
        rows.entry(surrogate)
            .or_insert_with(RowNodes::empty)
            .sidecar = versions;
    }
}

/// Re-issue one index's rows as one Calvin transaction per batch. Returns
/// the number of vectors re-issued.
async fn reissue_index(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    binds: &[SurrogateBindEntry],
    index: IndexReissue<'_>,
) -> Result<usize, Error> {
    let IndexReissue {
        name,
        field_name,
        rows,
        dim,
        multi,
        vector_primary,
        primary_index,
    } = index;
    let pks: HashMap<u32, &[u8]> = binds
        .iter()
        .filter(|bind| bind.tenant_id == tenant_id && bind.collection == name.bare)
        .map(|bind| (bind.surrogate, bind.pk.as_slice()))
        .collect();
    let bound: Vec<&[u8]> = rows
        .keys()
        .filter_map(|surrogate| pks.get(&surrogate.as_u32()).copied())
        .collect();
    let assigned = if bound.is_empty() {
        Vec::new()
    } else {
        crate::control::server::surrogate_exchange::assign_surrogates_routed(
            state,
            name.key(target.dest),
            TenantId::new(tenant_id),
            &bound,
            TraceId::ZERO,
        )
        .await?
    };
    if assigned.len() != bound.len() {
        return Err(Error::Internal {
            detail: format!(
                "restore of vector index '{}' bound {} of {} rows",
                name.bare,
                assigned.len(),
                bound.len()
            ),
        });
    }
    let dest: HashMap<&[u8], Surrogate> = bound.into_iter().zip(assigned).collect();

    let mut reissued = 0usize;
    let mut units = Vec::with_capacity(rows.len());
    for (backed_up_surrogate, nodes) in rows {
        reissued += nodes.live.len();
        let pk = pks.get(&backed_up_surrogate.as_u32()).copied();
        let surrogate = pk
            .and_then(|pk| dest.get(pk).copied())
            .unwrap_or(backed_up_surrogate);
        let row_target = RowTarget {
            name,
            field_name,
            dim,
            multi: multi.contains(&backed_up_surrogate),
            vector_primary,
            primary_index: primary_index.as_ref(),
        };
        units.push(row_unit(&row_target, surrogate, pk, nodes)?);
    }
    let collection_units = CollectionUnits::rows(target.dest, name.bare.clone(), units);
    commit_collection(
        state,
        TenantId::new(tenant_id),
        target.mark(),
        collection_units,
    )
    .await?;
    Ok(reissued)
}

/// Decode and re-issue every restored vector row of one database, one
/// Calvin transaction per batch of rows. `binds` are the backup's surrogate
/// binds. Each vector-primary row's sidecar re-issues with its nodes, in the
/// unit of its collection's primary index. Returns the number of vectors
/// re-issued.
pub(in crate::control::backup::restore) async fn reissue_vector_rows(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    backed_up: BackedUpVectors,
    binds: &[SurrogateBindEntry],
) -> Result<usize, Error> {
    let mut sidecars = backed_up.sidecars;
    let mut history: HashMap<String, Vec<u8>> = backed_up.history.into_iter().collect();
    let mut reissued = 0usize;
    for (entry_key, bytes) in backed_up.vectors {
        let coll_key = target.scoped_rest(&entry_key, tenant_id)?;
        let (collection, field_name) = split_vector_coll_key(coll_key);
        let name = target.resolve(collection)?;
        let vectors: Vec<(u32, Vec<f32>, Option<Surrogate>)> = zerompk::from_msgpack(&bytes)
            .map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize vector snapshot for '{}': {e}",
                    name.bare
                ),
            })?;
        let index_history = match history.remove(&entry_key) {
            Some(bytes) => zerompk::from_msgpack(&bytes).map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!(
                    "restore reissue: deserialize vector history for '{}': {e}",
                    name.bare
                ),
            })?,
            None => VectorIndexHistory::default(),
        };
        let IndexRows { mut rows, dim } = index_rows(vectors, index_history)?;
        let catalog = collection_vectors(state, target, tenant_id, &name.bare)?;
        let primary = catalog
            .primary_index
            .filter(|primary| primary.field == field_name);
        if primary.is_some() {
            join_sidecars(&mut rows, sidecars.take(&name.bare));
        }
        if rows.is_empty() {
            continue;
        }
        // An index whose every row is a sidecar alone takes the dimension
        // the collection declares.
        let dim = match (&primary, dim) {
            (Some(primary), 0) => primary.dim,
            _ => dim,
        };
        let multi: HashSet<Surrogate> = backed_up
            .multi_documents
            .iter()
            .filter(|(members_key, _)| *members_key == entry_key)
            .flat_map(|(_, documents)| documents.iter().copied())
            .collect();
        reissued += reissue_index(
            state,
            tenant_id,
            target,
            binds,
            IndexReissue {
                name: &name,
                field_name,
                rows,
                dim,
                multi,
                vector_primary: catalog.vector_primary,
                primary_index: primary.map(|primary| primary.settings),
            },
        )
        .await?;
    }
    // A vector-primary collection the backup holds no index entry of
    // re-issues its sidecar rows under its primary index.
    for (name, collection_sidecars) in sidecars.into_remaining() {
        let catalog = collection_vectors(state, target, tenant_id, &name.bare)?;
        let Some(primary) = catalog.primary_index else {
            return Err(Error::Internal {
                detail: format!(
                    "restore: the backup holds sidecar rows of '{}', which the catalog does \
                     not declare vector-primary",
                    name.bare
                ),
            });
        };
        let mut rows = BTreeMap::new();
        join_sidecars(&mut rows, collection_sidecars);
        reissued += reissue_index(
            state,
            tenant_id,
            target,
            binds,
            IndexReissue {
                name: &name,
                field_name: &primary.field,
                rows,
                dim: primary.dim,
                multi: HashSet::new(),
                vector_primary: catalog.vector_primary,
                primary_index: Some(primary.settings),
            },
        )
        .await?;
    }
    Ok(reissued)
}

#[cfg(test)]
mod tests {
    use nodedb_vector::collection::EndedNodeExport;

    use super::*;

    /// Each row keeps its live vector with its start and every ended node
    /// in `sys_from` order. A row the backup holds deleted keeps its ended
    /// nodes.
    #[test]
    fn nodes_group_by_row_with_their_spans() {
        let s = Surrogate::new(3);
        let gone = Surrogate::new(4);
        let history = VectorIndexHistory {
            live_starts: vec![(0, 20)],
            ended: vec![
                EndedNodeExport {
                    vector: vec![1.0, 0.0],
                    surrogate: s,
                    sys_from: 10,
                    sys_to: 20,
                },
                EndedNodeExport {
                    vector: vec![0.5, 0.5],
                    surrogate: gone,
                    sys_from: 5,
                    sys_to: 8,
                },
            ],
        };
        let IndexRows { rows, dim } =
            index_rows(vec![(0, vec![0.0, 1.0], Some(s))], history).expect("group");
        assert_eq!(dim, 2);
        assert_eq!(rows[&s].live_from, 20);
        assert_eq!(rows[&s].ended.len(), 1);
        assert!(rows[&gone].live.is_empty());
        assert_eq!(rows[&gone].ended[0].sys_from, 5);
    }

    /// A node with no surrogate is a corrupt capture.
    #[test]
    fn an_unbound_node_fails_the_group() {
        assert!(index_rows(vec![(0, vec![1.0], None)], VectorIndexHistory::default()).is_err());
    }
}
