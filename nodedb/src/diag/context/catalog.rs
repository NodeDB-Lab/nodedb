// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for catalog and metadata-applier capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A durable host-side effect failed while applying a committed metadata
/// entry, so the Raft applier stopped without advancing its watermark.
pub(in crate::diag) struct MetadataApplyWedged<'a> {
    pub raft_index: u64,
    pub last_applied_watermark: u64,
    pub entry_kind: &'a str,
    pub error_class: &'a str,
    /// The applier judged this failure deterministic in the entry and the
    /// local state, so re-delivery cannot clear it and the node withdrew from
    /// readiness. `false` means halt-and-retry is still expected to heal.
    pub permanent: bool,
}

impl DomainContext for MetadataApplyWedged<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.metadata_apply_wedged"
    }

    fn grouping_key(&self) -> String {
        // Entry variant + error class name the bug; raft index/watermark are
        // the occurrence and must collapse to one group.
        format!("entry={};cause={}", self.entry_kind, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "raft_index": self.raft_index,
            "last_applied_watermark": self.last_applied_watermark,
            "entry_kind": self.entry_kind,
            "error_class": self.error_class,
            "permanent": self.permanent,
            "why_fatal": "the apply loop never advances the watermark past an entry it \
                          could not durably apply; a deterministic failure re-fails on \
                          every re-delivery, so this node's Raft applier is wedged and \
                          callers only see an unrelated-looking lease timeout, never this. \
                          When 'permanent' is true the node has withdrawn from readiness \
                          instead of pretending a retry will heal it",
            "operator_action": "when 'permanent' is false, look for a clearing condition \
                                 (a full disk, redb contention, a subsystem handle not \
                                 installed yet) — the applier resumes on its own once the \
                                 same entry applies cleanly. When it is true, the entry \
                                 and the local state fully determine the failure: inspect \
                                 this node's catalog against the replicated log for the \
                                 named descriptor, since no retry will change the outcome",
        })
    }
}

/// A `catalog_entry::apply_to` call left redb with a parent-replicated
/// primary row missing its `StoredOwner` row (or vice versa), detected
/// right after the entry that created it was applied.
pub(in crate::diag) struct CatalogApplyOrphanRow<'a> {
    /// The `CatalogEntry` variant whose apply produced the orphan.
    pub entry_kind: &'a str,
    /// Object kind of the first orphan found (`collection`, `function`, ...).
    pub orphan_kind: &'a str,
    /// How many orphan rows this apply call left behind.
    pub orphan_count: usize,
}

impl DomainContext for CatalogApplyOrphanRow<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.catalog_apply_orphan_row"
    }

    fn grouping_key(&self) -> String {
        // Entry variant + orphan kind name the bug; orphan count is the
        // occurrence, collapsing a replayed batch to one report per cause.
        format!("entry={};orphan_kind={}", self.entry_kind, self.orphan_kind)
    }

    fn to_json(&self) -> Value {
        json!({
            "entry_kind": self.entry_kind,
            "orphan_kind": self.orphan_kind,
            "orphan_count": self.orphan_count,
            "why_fatal": "the OWNERS redb table is the persistent backing for the in-memory \
                          PermissionStore.owners map; a primary row written without its \
                          owner row (or the reverse) is invisible until the next restart, \
                          when PermissionStore::load_from rebuilds from redb and silently \
                          drops the object's ownership — degrading permission checks with \
                          no error anywhere",
            "operator_action": "the named CatalogEntry variant's apply/<type>.rs::put is \
                                 missing its owner::put_parent_owner call, or a \
                                 sibling delete path is missing the matching removal; fix the \
                                 applier so the primary and owner rows are written or deleted \
                                 in lockstep",
        })
    }
}

/// A collection purge found no catalog row to deactivate, even though its
/// caller had just read that row.
pub(in crate::diag) struct CollectionPurgeRowMissing<'a> {
    /// Database the purge looked the collection up under.
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection the purge targeted.
    pub name: &'a str,
}

impl DomainContext for CollectionPurgeRowMissing<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.collection_purge_row_missing"
    }

    fn grouping_key(&self) -> String {
        // Database names the bug; tenant and collection name are the
        // occurrence, keeping one report per root cause.
        format!("database={}", self.database_id)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.name,
            "why_fatal": "the inactive row this step writes is the restart-durable barrier \
                          that stops a same-name CREATE or UNDROP from crossing an \
                          incomplete storage reclaim. Writing nothing and reporting success \
                          lets the reclaim run against a row that is still active, so a \
                          re-CREATE registers over storage keys the old incarnation still \
                          owns and its rows resurrect",
            "operator_action": "compare the database_id the purge looked under with the one \
                                 on the stored collection row: a caller passing a fixed or \
                                 session-default database for a collection in another \
                                 namespace misses every row. If they match, another writer \
                                 removed the row concurrently and the purge raced a \
                                 same-name lifecycle operation",
        })
    }
}

/// Consumer-group offsets that a replicated topic or group deletion could not
/// clear on this node, so a recreated stream resumes from a stale cursor.
pub(in crate::diag) struct ConsumerGroupOffsetsRetained<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    /// Stream the group consumed.
    pub stream: &'a str,
    /// Group whose cursors survived.
    pub group: &'a str,
    /// Offset-store call that failed (`delete_group`).
    pub operation: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for ConsumerGroupOffsetsRetained<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.consumer_group_offsets_retained"
    }

    fn grouping_key(&self) -> String {
        // Operation + error class name the bug; the ids are the occurrence.
        format!("op={};cause={}", self.operation, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "stream": self.stream,
            "group": self.group,
            "operation": self.operation,
            "error_class": self.error_class,
            "why_fatal": "the offset store is node-local durable state that the catalog \
                          entry cannot carry. A cursor left behind on this node alone \
                          survives DROP, so recreating the stream or topic makes this \
                          node resume mid-history and silently skip events every other \
                          node delivers",
            "operator_action": "inspect the offset database on this node for the named \
                                 stream and group, and delete the group again once the \
                                 underlying I/O failure clears. A recreated stream must \
                                 not be consumed from this node until then",
        })
    }
}

/// A committed synonym group whose per-node FTS install failed, so this node
/// expands query terms differently from the catalog every node agreed on.
pub(in crate::diag) struct SynonymGroupNotApplied<'a> {
    /// Stage that failed (`put_serialize`, `put_dispatch`, `delete_dispatch`).
    pub stage: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Group the entry names.
    pub group: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for SynonymGroupNotApplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.synonym_group_not_applied"
    }

    fn grouping_key(&self) -> String {
        // Stage + error class name the bug; the group identity is the
        // occurrence, so one broken node files one report.
        format!("stage={};cause={}", self.stage, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "group": self.group,
            "error_class": self.error_class,
            "why_fatal": "the catalog row is already committed by consensus, so this node \
                          lists the group in SHOW SYNONYM GROUPS while its FTS backend \
                          never received it. A create that fails here answers a text \
                          query with fewer rows than every other node returns; a drop \
                          that fails here keeps expanding terms the statement removed. \
                          Both answer with no error",
            "operator_action": "re-run the CREATE or DROP SYNONYM GROUP statement once the \
                                 underlying dispatch failure clears. The FTS backend holds \
                                 its own durable copy and no boot seed rebuilds it from the \
                                 catalog, so a restart does not heal this node",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synonym_sample() -> SynonymGroupNotApplied<'static> {
        SynonymGroupNotApplied {
            stage: "put_dispatch",
            database_id: 1,
            tenant_id: 2,
            group: "db_terms",
            error_class: "dispatch",
        }
    }

    #[test]
    fn synonym_grouping_ignores_the_group_identity() {
        let first = synonym_sample();
        let second = SynonymGroupNotApplied {
            database_id: 90,
            tenant_id: 91,
            group: "colours",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(first.grouping_key(), "stage=put_dispatch;cause=dispatch");
    }

    #[test]
    fn synonym_grouping_separates_create_from_drop() {
        let create = synonym_sample();
        let drop = SynonymGroupNotApplied {
            stage: "delete_dispatch",
            ..create
        };
        assert_ne!(create.grouping_key(), drop.grouping_key());
    }
}
