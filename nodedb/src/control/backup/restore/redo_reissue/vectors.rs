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

use std::collections::{BTreeMap, HashMap, HashSet};

use nodedb_physical::physical_plan::RestoredVectorRow;
use nodedb_types::Surrogate;
use nodedb_wal::record::RecordType;

use crate::Error;
use crate::control::state::SharedState;
use crate::control::surrogate::CarriedIdentity;
use crate::types::{SurrogateBindEntry, TenantId, TraceId, VectorIndexHistory};
use crate::wal::{RedoSubRecord, VectorRestoreLive, VectorRestoreNode, VectorRestoreRedo};

use super::super::target::{DatabaseTarget, RestoredName};
use super::super::vector_reissue::split_vector_coll_key;
use super::commit::commit_collection;
use super::units::{CollectionUnits, RowUnit};

/// Every node of one backed-up row.
#[derive(Debug, Default, PartialEq)]
struct RowNodes {
    /// The live vectors, in node order.
    live: Vec<Vec<f32>>,
    /// The start of the live nodes: the newest one a cut reads below, or
    /// `i64::MIN` when every cut reads them.
    live_from: i64,
    /// Every ended node, ascending by `sys_from`.
    ended: Vec<VectorRestoreNode>,
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
        let row = rows.entry(surrogate).or_insert_with(|| RowNodes {
            live_from: i64::MIN,
            ..RowNodes::default()
        });
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
        let row = rows.entry(node.surrogate).or_insert_with(|| RowNodes {
            live_from: i64::MIN,
            ..RowNodes::default()
        });
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
/// multi-vector documents, each keyed `"{db}:{tid}:{coll_key}"`.
pub(in crate::control::backup::restore) struct BackedUpVectors {
    pub vectors: Vec<(String, Vec<u8>)>,
    pub history: Vec<(String, Vec<u8>)>,
    pub multi_documents: Vec<(String, Vec<Surrogate>)>,
}

/// Decode and re-issue every restored vector row of one database, one
/// Calvin transaction per batch of rows. `binds` are the backup's surrogate
/// binds. Returns the number of vectors re-issued.
pub(in crate::control::backup::restore) async fn reissue_vector_rows(
    state: &SharedState,
    tenant_id: u64,
    target: DatabaseTarget,
    backed_up: BackedUpVectors,
    binds: &[SurrogateBindEntry],
) -> Result<usize, Error> {
    let tenant = TenantId::new(tenant_id);
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
        let IndexRows { rows, dim } = index_rows(vectors, index_history)?;
        if rows.is_empty() {
            continue;
        }
        let multi: HashSet<Surrogate> = backed_up
            .multi_documents
            .iter()
            .filter(|(members_key, _)| *members_key == entry_key)
            .flat_map(|(_, documents)| documents.iter().copied())
            .collect();

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
                tenant,
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
        let vector_primary = state
            .credentials
            .catalog()
            .get_collection(target.dest, tenant_id, &name.bare)?
            .is_some_and(|stored| matches!(stored.primary, nodedb_types::PrimaryEngine::Vector));

        let mut units = Vec::with_capacity(rows.len());
        for (backed_up_surrogate, nodes) in rows {
            reissued += nodes.live.len();
            let pk = pks.get(&backed_up_surrogate.as_u32()).copied();
            let surrogate = pk
                .and_then(|pk| dest.get(pk).copied())
                .unwrap_or(backed_up_surrogate);
            let row_target = RowTarget {
                name: &name,
                field_name,
                dim,
                multi: multi.contains(&backed_up_surrogate),
                vector_primary,
            };
            units.push(row_unit(&row_target, surrogate, pk, nodes)?);
        }
        let collection_units = CollectionUnits::rows(target.dest, name.bare.clone(), units);
        commit_collection(state, tenant, target.mark(), collection_units).await?;
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
