// SPDX-License-Identifier: BUSL-1.1

//! Node-label staging and read-your-own-writes accessors for
//! [`GraphTxnOverlay`]. A label mutation reads the node's merged delta from
//! every tier, applies its labels, and stages the result as the node's
//! newest hot version.

use std::borrow::Cow;

use super::txn_overlay::GraphTxnOverlay;
use super::types::{GraphCollKey, NodeLabelDelta};
use crate::types::{DatabaseId, TenantId};

impl GraphTxnOverlay {
    /// Stage labels added to `node_id`. Each label leaves the removed set.
    pub fn stage_node_labels_set(
        &mut self,
        coll_key: GraphCollKey,
        node_id: &str,
        labels: &[String],
    ) -> crate::Result<()> {
        self.stage_labels(coll_key, node_id, |delta| {
            for label in labels {
                delta.removed.remove(label);
                delta.added.insert(label.clone());
            }
        })
    }

    /// Stage labels removed from `node_id`. Each label leaves the added set.
    pub fn stage_node_labels_remove(
        &mut self,
        coll_key: GraphCollKey,
        node_id: &str,
        labels: &[String],
    ) -> crate::Result<()> {
        self.stage_labels(coll_key, node_id, |delta| {
            for label in labels {
                delta.added.remove(label);
                delta.removed.insert(label.clone());
            }
        })
    }

    fn stage_labels(
        &mut self,
        coll_key: GraphCollKey,
        node_id: &str,
        apply: impl FnOnce(&mut NodeLabelDelta),
    ) -> crate::Result<()> {
        let mut delta = self.labels_delta(&coll_key, node_id)?.unwrap_or_default();
        apply(&mut delta);
        let seq = self.record_labels_undo(&coll_key, node_id)?;
        self.collections
            .entry(coll_key)
            .or_default()
            .node_labels
            .insert(node_id.to_string(), delta, seq);
        self.spill_past_threshold()
    }

    /// The staged label delta of `node_id` in `coll_key`.
    pub fn labels_delta(
        &self,
        coll_key: &GraphCollKey,
        node_id: &str,
    ) -> crate::Result<Option<NodeLabelDelta>> {
        self.guard()?;
        let Some(overlay) = self.collections.get(coll_key) else {
            return Ok(None);
        };
        Ok(overlay
            .node_labels
            .get(node_id, self.space.reader())?
            .map(Cow::into_owned))
    }

    /// The staged label delta of `node_id` in the first collection of the
    /// tenant that stages one.
    pub fn labels_delta_any_collection(
        &self,
        database_id: DatabaseId,
        tenant: TenantId,
        node_id: &str,
    ) -> crate::Result<Option<NodeLabelDelta>> {
        for coll_key in self
            .collections
            .keys()
            .filter(|(db, t, _)| *db == database_id && *t == tenant)
        {
            if let Some(delta) = self.labels_delta(coll_key, node_id)? {
                return Ok(Some(delta));
            }
        }
        Ok(None)
    }

    /// Every staged label delta of `coll_key`: `(node_id, delta)`, in key
    /// order, merged across the hot tier and the runs.
    pub fn staged_node_label_deltas_for_collection<'a>(
        &'a self,
        coll_key: &GraphCollKey,
    ) -> impl Iterator<Item = crate::Result<(String, NodeLabelDelta)>> + 'a {
        let poisoned = self.guard().err();
        let scan = match poisoned {
            Some(_) => None,
            None => self
                .collections
                .get(coll_key)
                .map(|overlay| overlay.node_labels.iter()),
        };
        poisoned.map(Err).into_iter().chain(
            scan.into_iter()
                .flatten()
                .map(|item| item.map(|(node_id, delta)| (node_id, delta.into_owned()))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> GraphCollKey {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn node_label_set_then_remove_resolves_last_writer() {
        let mut overlay = GraphTxnOverlay::new();
        overlay
            .stage_node_labels_set(key("g"), "n1", &["Person".to_string()])
            .unwrap();
        overlay
            .stage_node_labels_remove(key("g"), "n1", &["Person".to_string()])
            .unwrap();
        let delta = overlay.labels_delta(&key("g"), "n1").unwrap().unwrap();
        assert!(delta.added.is_empty());
        assert!(delta.removed.contains("Person"));
    }
}
