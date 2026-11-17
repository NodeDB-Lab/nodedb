// SPDX-License-Identifier: BUSL-1.1

//! The collections and shapes one sync session receives pushes for.
//!
//! A session starts with an empty set, and an empty set receives nothing.
//! A shape enters the set only after its subscription is authorized. A row
//! push reaches a session only for a collection one of its shapes names. The
//! push-time checks then judge the row against those shapes.
//!
//! A held collection is one the client already stores without a shape: named
//! in its handshake vector clock, or written by its own admitted delta, each
//! under a read grant. A held collection receives `CollectionPurged` notices
//! and no row pushes.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use nodedb_types::filter::MetadataFilter;

/// One authorized shape, in the form its row pushes are filtered by.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribedShape {
    collection: String,
    /// The encoded predicate the subscription carried. Empty matches every row.
    predicate_bytes: Vec<u8>,
    /// The predicate, decoded once at subscribe and shared by every drain.
    /// `None` when `predicate_bytes` is empty.
    predicate: Option<Arc<MetadataFilter>>,
}

impl SubscribedShape {
    /// A shape on `collection` that passes the rows `predicate_bytes` matches.
    ///
    /// Empty bytes match every row. Returns `None` when the bytes do not
    /// decode: a shape with an unreadable predicate is never held.
    pub fn new(collection: String, predicate_bytes: Vec<u8>) -> Option<Self> {
        let predicate = if predicate_bytes.is_empty() {
            None
        } else {
            Some(Arc::new(
                zerompk::from_msgpack::<MetadataFilter>(&predicate_bytes).ok()?,
            ))
        };
        Some(Self {
            collection,
            predicate_bytes,
            predicate,
        })
    }

    /// The bare collection name the shape reads.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The encoded predicate. Empty when the shape matches every row.
    pub fn predicate_bytes(&self) -> &[u8] {
        &self.predicate_bytes
    }

    /// The decoded predicate. `None` when the shape matches every row.
    pub fn predicate(&self) -> Option<&Arc<MetadataFilter>> {
        self.predicate.as_ref()
    }
}

/// How one shape change moved the collections a session subscribes to.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct CollectionChange {
    /// A collection the session subscribes to now and did not before.
    pub gained: Option<String>,
    /// A collection the session subscribed to before and does not now.
    pub lost: Option<String>,
}

/// The subscription state of one sync session.
#[derive(Debug, Clone, Default)]
pub struct SessionSubscriptions {
    /// shape_id → authorized shape.
    shapes: HashMap<String, SubscribedShape>,
    /// collection → IDs of the shapes on it. Kept in step with `shapes`, so a
    /// collection lookup is one hash probe.
    by_collection: HashMap<String, HashSet<String>>,
    /// Collections the client stores without a shape.
    held: HashSet<String>,
}

impl SessionSubscriptions {
    /// Whether a shape of this session names `collection`.
    pub fn subscribes(&self, collection: &str) -> bool {
        self.by_collection.contains_key(collection)
    }

    /// Whether the client stores `collection`, by a shape or as held.
    pub fn holds(&self, collection: &str) -> bool {
        self.held.contains(collection) || self.subscribes(collection)
    }

    /// The shapes of this session that name `collection`.
    pub fn shapes_on<'a>(
        &'a self,
        collection: &'a str,
    ) -> impl Iterator<Item = &'a SubscribedShape> + 'a {
        self.by_collection
            .get(collection)
            .into_iter()
            .flatten()
            .filter_map(|shape_id| self.shapes.get(shape_id))
    }

    /// Every collection a shape of this session names.
    pub fn subscribed_collections(&self) -> impl Iterator<Item = &str> + '_ {
        self.by_collection.keys().map(String::as_str)
    }

    /// Number of shapes held.
    pub fn shape_count(&self) -> usize {
        self.shapes.len()
    }

    /// Hold `shape` under `shape_id`. Returns the shape it replaced and how
    /// the subscribed collections moved.
    pub(super) fn insert_shape(
        &mut self,
        shape_id: String,
        shape: SubscribedShape,
    ) -> (Option<SubscribedShape>, CollectionChange) {
        let (replaced, mut change) = self.detach(&shape_id);
        let shape_ids = self
            .by_collection
            .entry(shape.collection.clone())
            .or_default();
        if shape_ids.is_empty() {
            if change.lost.as_deref() == Some(shape.collection.as_str()) {
                change.lost = None;
            } else {
                change.gained = Some(shape.collection.clone());
            }
        }
        shape_ids.insert(shape_id.clone());
        self.shapes.insert(shape_id, shape);
        (replaced, change)
    }

    /// Release the shape under `shape_id`. Returns the shape released and how
    /// the subscribed collections moved.
    pub(super) fn remove_shape(
        &mut self,
        shape_id: &str,
    ) -> (Option<SubscribedShape>, CollectionChange) {
        self.detach(shape_id)
    }

    fn detach(&mut self, shape_id: &str) -> (Option<SubscribedShape>, CollectionChange) {
        let mut change = CollectionChange::default();
        let Some(shape) = self.shapes.remove(shape_id) else {
            return (None, change);
        };
        if let Some(shape_ids) = self.by_collection.get_mut(&shape.collection) {
            shape_ids.remove(shape_id);
            if shape_ids.is_empty() {
                self.by_collection.remove(&shape.collection);
                change.lost = Some(shape.collection.clone());
            }
        }
        (Some(shape), change)
    }

    /// Record that the client stores `collection`. Returns false when it was
    /// already recorded.
    pub(super) fn hold(&mut self, collection: &str) -> bool {
        if self.held.contains(collection) {
            return false;
        }
        self.held.insert(collection.to_string())
    }
}

/// One session's subscription state behind its own lock.
///
/// A subscription change locks only its own session, never the registry of
/// every session, and a drain reads the shapes of one collection at a time.
#[derive(Debug, Default)]
pub struct SharedSubscriptions(RwLock<SessionSubscriptions>);

impl SharedSubscriptions {
    /// Read the state. A lock poisoned by a panicking writer still holds a
    /// complete state, because every change is applied in place in one call.
    pub fn read(&self) -> RwLockReadGuard<'_, SessionSubscriptions> {
        self.0.read().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn write(&self) -> RwLockWriteGuard<'_, SessionSubscriptions> {
        self.0.write().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eq_predicate(field: &str, value: &str) -> Vec<u8> {
        zerompk::to_msgpack_vec(&MetadataFilter::Eq {
            field: field.into(),
            value: nodedb_types::Value::String(value.into()),
        })
        .expect("encode predicate")
    }

    fn shape(collection: &str) -> SubscribedShape {
        SubscribedShape::new(collection.into(), Vec::new()).expect("empty predicate decodes")
    }

    #[test]
    fn empty_state_subscribes_and_holds_nothing() {
        let subs = SessionSubscriptions::default();
        assert!(!subs.subscribes("orders"));
        assert!(!subs.holds("orders"));
        assert_eq!(subs.subscribed_collections().count(), 0);
        assert_eq!(subs.shapes_on("orders").count(), 0);
    }

    #[test]
    fn shape_subscribes_its_collection_only() {
        let mut subs = SessionSubscriptions::default();
        let (replaced, change) = subs.insert_shape("sh1".into(), shape("orders"));
        assert!(replaced.is_none());
        assert_eq!(change.gained.as_deref(), Some("orders"));
        assert!(change.lost.is_none());

        assert!(subs.subscribes("orders"));
        assert!(subs.holds("orders"));
        assert!(!subs.subscribes("users"));
        assert_eq!(subs.shapes_on("orders").count(), 1);
        assert_eq!(subs.shapes_on("users").count(), 0);
    }

    #[test]
    fn replacing_a_shape_moves_its_collection() {
        let mut subs = SessionSubscriptions::default();
        subs.insert_shape("sh1".into(), shape("orders"));
        let (replaced, change) = subs.insert_shape("sh1".into(), shape("users"));

        assert_eq!(replaced.map(|s| s.collection), Some("orders".to_string()));
        assert_eq!(change.gained.as_deref(), Some("users"));
        assert_eq!(change.lost.as_deref(), Some("orders"));
        assert!(!subs.subscribes("orders"));
        assert!(subs.subscribes("users"));
        assert_eq!(subs.shape_count(), 1);
    }

    #[test]
    fn replacing_on_the_same_collection_moves_nothing() {
        let mut subs = SessionSubscriptions::default();
        subs.insert_shape("sh1".into(), shape("orders"));
        let (replaced, change) = subs.insert_shape("sh1".into(), shape("orders"));

        assert!(replaced.is_some());
        assert_eq!(change, CollectionChange::default());
        assert!(subs.subscribes("orders"));
    }

    #[test]
    fn removing_the_last_shape_releases_the_collection() {
        let mut subs = SessionSubscriptions::default();
        subs.insert_shape("sh1".into(), shape("orders"));
        subs.insert_shape("sh2".into(), shape("orders"));

        let (removed, change) = subs.remove_shape("sh1");
        assert!(removed.is_some());
        assert_eq!(change, CollectionChange::default());
        assert!(subs.subscribes("orders"));

        let (removed, change) = subs.remove_shape("sh2");
        assert!(removed.is_some());
        assert_eq!(change.lost.as_deref(), Some("orders"));
        assert!(!subs.subscribes("orders"));
        assert!(subs.remove_shape("sh2").0.is_none());
    }

    #[test]
    fn held_collection_is_held_but_not_subscribed() {
        let mut subs = SessionSubscriptions::default();
        assert!(subs.hold("orders"));
        assert!(!subs.hold("orders"));

        assert!(subs.holds("orders"));
        assert!(!subs.subscribes("orders"));
        assert_eq!(subs.subscribed_collections().count(), 0);
    }

    #[test]
    fn predicate_decodes_once_and_is_shared() {
        let bytes = eq_predicate("status", "open");
        let shape =
            SubscribedShape::new("orders".into(), bytes.clone()).expect("predicate decodes");
        let copy = shape.clone();

        assert_eq!(shape.predicate_bytes(), bytes.as_slice());
        let (Some(a), Some(b)) = (shape.predicate(), copy.predicate()) else {
            panic!("predicate decoded");
        };
        assert!(Arc::ptr_eq(a, b));
    }

    #[test]
    fn undecodable_predicate_is_never_held() {
        assert!(SubscribedShape::new("orders".into(), vec![0xc1, 0xff]).is_none());
    }
}
