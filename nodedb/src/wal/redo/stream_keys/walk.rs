// SPDX-License-Identifier: BUSL-1.1

//! The walk that declares a stream marker's keys from its framed content.
//!
//! The proposer hands the walk each run of whole records it frames or
//! pulls: the header frames, then each resolved part. The walk reads:
//! - the header's materialized-sum sources and targets
//! - the collection of each read
//! - the sync stream and graph state of each op (see [`super::sub_record`])
//!
//! These are the records the Data Plane's validate walk keys. So the
//! declared keys cover every key that walk finds.

use std::collections::BTreeSet;

use nodedb_physical::physical_plan::RedoSumTargets;
use nodedb_types::calvin::VersionedReadEntry;

use crate::wal::RedoSubRecord;
use crate::wal::redo::frame::{RedoContentHeader, RedoFrameTag, RedoFrames, decode_frame};

use super::sub_record::{sub_record_provenance, sub_record_writes_graph};
use super::summary::{DeclaredStreamKeys, RedoStreamKeys, RedoSyncStream};

/// The keys walked so far.
#[derive(Debug, Clone, Default)]
pub struct StreamKeysWalk {
    reads: BTreeSet<String>,
    sum_collections: BTreeSet<String>,
    sync_streams: BTreeSet<RedoSyncStream>,
    graph: bool,
}

impl StreamKeysWalk {
    pub fn new() -> Self {
        Self::default()
    }

    /// The keys of `frames`, a whole framed content.
    pub fn of_frames(frames: &[u8]) -> crate::Result<RedoStreamKeys> {
        let mut walk = Self::new();
        walk.walk(frames)?;
        Ok(walk.keys())
    }

    /// Walk `frames`, a run of whole records. A record that does not decode
    /// is an error.
    pub fn walk(&mut self, frames: &[u8]) -> crate::Result<()> {
        for frame in RedoFrames::new(frames) {
            let (tag, body) = frame?;
            match tag {
                RedoFrameTag::Header => {
                    let header: RedoContentHeader = decode_frame(tag, body)?;
                    self.sum_targets(&header.sum_targets);
                }
                RedoFrameTag::Read => {
                    let read: VersionedReadEntry = decode_frame(tag, body)?;
                    self.reads.insert(read.collection);
                }
                RedoFrameTag::Op => {
                    let sub: RedoSubRecord = decode_frame(tag, body)?;
                    self.op(&sub);
                }
                // A row change, a row source, a message and an identity
                // name no state the install fence keys.
                RedoFrameTag::RowChange
                | RedoFrameTag::RowSource
                | RedoFrameTag::Publish
                | RedoFrameTag::Identity => {}
            }
        }
        Ok(())
    }

    /// Note every source and target collection of `targets`.
    fn sum_targets(&mut self, targets: &[RedoSumTargets]) {
        for targets in targets {
            self.sum_collections.insert(targets.collection.clone());
            self.sum_collections.extend(
                targets
                    .resolved
                    .iter()
                    .map(|target| target.target_collection.clone()),
            );
        }
    }

    /// Note the sync stream and the graph state `sub` writes.
    fn op(&mut self, sub: &RedoSubRecord) {
        self.sync_streams.extend(
            sub_record_provenance(sub)
                .as_ref()
                .and_then(RedoSyncStream::of),
        );
        // The graph decode runs only until an op has set the flag.
        if !self.graph && sub_record_writes_graph(sub) {
            self.graph = true;
        }
    }

    /// The keys walked so far.
    pub fn keys(&self) -> RedoStreamKeys {
        RedoStreamKeys::Declared(DeclaredStreamKeys {
            reads: self.reads.iter().cloned().collect(),
            sum_collections: self.sum_collections.iter().cloned().collect(),
            sync_streams: self.sync_streams.iter().copied().collect(),
            graph: self.graph,
        })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::ResolvedSumTarget;
    use nodedb_types::Surrogate;
    use nodedb_types::calvin::{EngineTag, ReadKeyIdent};
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::record::RecordType;

    use super::*;
    use crate::wal::redo::frame::push_frame;

    fn header(sum_targets: Vec<RedoSumTargets>) -> RedoContentHeader {
        RedoContentHeader {
            version: 1,
            calvin_stamp: None,
            cross_shard_applied: None,
            user_roles: Vec::new(),
            sum_targets,
            pins: Default::default(),
        }
    }

    fn read(collection: &str) -> VersionedReadEntry {
        VersionedReadEntry {
            engine: EngineTag::Document,
            collection: collection.into(),
            key: ReadKeyIdent::Predicate,
            home_vshard: None,
        }
    }

    fn sync_frame(producer_id: u64) -> RedoSubRecord {
        let provenance = SyncProvenance {
            producer_id,
            epoch: 1,
            stream_id: 42,
            seq: 1,
        };
        RedoSubRecord {
            record_type: RecordType::VectorPut as u32,
            payload: crate::control::server::wal_dispatch::encode_vector_put_payload(
                "v",
                &[1.0, 2.0],
                2,
                "",
                Surrogate::new(9),
                Some(&provenance),
            )
            .expect("encode vector put"),
        }
    }

    fn label_set() -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::GraphNodeLabelSet as u32,
            payload: crate::wal::NodeLabelRedo::live("n1", &["Person".to_owned()])
                .encode()
                .expect("encode label delta"),
        }
    }

    /// A content split into runs at every record walks to the keys of the
    /// whole content at once.
    #[test]
    fn a_split_walk_names_the_keys_of_the_whole_content() {
        let targets = vec![RedoSumTargets {
            collection: "entries".into(),
            resolved: vec![ResolvedSumTarget::new("totals", "k", Surrogate::new(5))],
        }];
        let mut runs: Vec<Vec<u8>> = Vec::new();
        let mut run = Vec::new();
        push_frame(&mut run, RedoFrameTag::Header, &header(targets)).expect("header");
        runs.push(std::mem::take(&mut run));
        for collection in ["ledger", "", "ledger"] {
            push_frame(&mut run, RedoFrameTag::Read, &read(collection)).expect("read");
        }
        runs.push(std::mem::take(&mut run));
        for sub in [sync_frame(7), sync_frame(0), label_set(), sync_frame(7)] {
            push_frame(&mut run, RedoFrameTag::Op, &sub).expect("op");
            runs.push(std::mem::take(&mut run));
        }
        let mut walk = StreamKeysWalk::new();
        for run in &runs {
            walk.walk(run).expect("walk");
        }
        let expected = RedoStreamKeys::Declared(DeclaredStreamKeys {
            reads: vec!["".into(), "ledger".into()],
            sum_collections: vec!["entries".into(), "totals".into()],
            sync_streams: vec![RedoSyncStream {
                producer_id: 7,
                stream_id: 42,
            }],
            graph: true,
        });
        assert_eq!(walk.keys(), expected);
        assert_eq!(
            StreamKeysWalk::of_frames(&runs.concat()).expect("walk"),
            expected
        );
    }

    #[test]
    fn a_record_that_does_not_decode_is_refused() {
        let mut frames = Vec::new();
        push_frame(&mut frames, RedoFrameTag::Read, &7u32).expect("frame");
        assert!(StreamKeysWalk::of_frames(&frames).is_err());
    }
}
