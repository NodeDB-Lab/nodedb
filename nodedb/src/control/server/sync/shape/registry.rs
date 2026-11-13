// SPDX-License-Identifier: BUSL-1.1

//! Per-client shape registry: tracks which shapes each connected client
//! is subscribed to and evaluates mutations against active shapes.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

use tracing::{debug, info, warn};

use nodedb_query::metadata_filter::matches_metadata_filter;
use nodedb_types::filter::MetadataFilter;

use super::definition::{ShapeDefinition, ShapeId, ShapeScope, ShapeType};

/// Per-session shape subscription state.
struct ClientShapes {
    /// Active shape subscriptions: shape_id → definition.
    shapes: HashMap<ShapeId, ShapeDefinition>,
    /// Authenticated tenant and database binding for this session.
    scope: ShapeScope,
    /// When shapes were last modified.
    last_modified: Instant,
}

/// Registry of all active shape subscriptions across all sync sessions.
///
/// Thread-safe (RwLock): mutations are evaluated against shapes from
/// the WAL tail loop (single writer), while subscribe/unsubscribe
/// comes from sync sessions (multiple writers).
pub struct ShapeRegistry {
    /// Per-session shapes: session_id → ClientShapes.
    sessions: RwLock<HashMap<String, ClientShapes>>,
}

impl Default for ShapeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ShapeRegistry {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a shape subscription for a session in its authenticated scope.
    pub fn subscribe(&self, session_id: &str, scope: ShapeScope, shape: ShapeDefinition) {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        let client = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| ClientShapes {
                shapes: HashMap::new(),
                scope,
                last_modified: Instant::now(),
            });
        if client.scope != scope {
            warn!(
                session = session_id,
                "refusing shape subscription with a different tenant or database binding"
            );
            return;
        }
        info!(
            session = session_id,
            shape_id = %shape.shape_id,
            "shape subscribed"
        );
        client
            .shapes
            .insert(ShapeId::from_validated(shape.shape_id.clone()), shape);
        client.last_modified = Instant::now();
    }

    /// Unsubscribe a shape for a session.
    pub fn unsubscribe(&self, session_id: &str, shape_id: &str) -> bool {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        if let Some(client) = sessions.get_mut(session_id) {
            let removed = client.shapes.remove(shape_id).is_some();
            if removed {
                debug!(session = session_id, shape_id, "shape unsubscribed");
                client.last_modified = Instant::now();
            }
            removed
        } else {
            false
        }
    }

    /// Remove all shapes for a session (disconnect cleanup).
    pub fn remove_session(&self, session_id: &str) {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        if let Some(client) = sessions.remove(session_id) {
            info!(
                session = session_id,
                shapes = client.shapes.len(),
                "session shapes removed"
            );
        }
    }

    /// Whether the session can hold `shape_id` without exceeding `limit`
    /// shapes: it holds the ID already, or it holds fewer than `limit`.
    pub fn can_hold(&self, session_id: &str, shape_id: &str, limit: usize) -> bool {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions.get(session_id).is_none_or(|client| {
            client.shapes.contains_key(shape_id) || client.shapes.len() < limit
        })
    }

    /// Get all shape IDs for a session.
    pub fn shapes_for_session(&self, session_id: &str) -> Vec<ShapeId> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions
            .get(session_id)
            .map(|c| c.shapes.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Evaluate a mutation against all active shapes.
    ///
    /// Returns a list of `(session_id, shape_id)` pairs for shapes that
    /// match the mutation. The caller then pushes ShapeDelta messages
    /// to the matching sessions.
    ///
    /// For `ShapeType::Document` shapes with a non-empty predicate, the
    /// predicate bytes (MessagePack-encoded `MetadataFilter`) are decoded
    /// and evaluated against `doc_json`. An empty predicate matches all
    /// documents in the collection. A predicate that cannot be decoded is
    /// logged as a warning and treated as non-matching (fail-closed).
    pub fn evaluate_mutation(
        &self,
        scope: ShapeScope,
        collection: &str,
        doc_id: &str,
        doc_json: &serde_json::Value,
    ) -> Vec<(String, ShapeId)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        let mut matches = Vec::new();

        for (session_id, client) in sessions.iter() {
            if client.scope != scope {
                continue;
            }
            for (shape_id, shape) in &client.shapes {
                if !shape.could_match(collection, doc_id) {
                    continue;
                }
                if let ShapeType::Document { predicate, .. } = &shape.shape_type
                    && !predicate.is_empty()
                {
                    match zerompk::from_msgpack::<MetadataFilter>(predicate) {
                        Ok(filter) => {
                            if !matches_metadata_filter(doc_json, &filter) {
                                continue;
                            }
                        }
                        Err(err) => {
                            warn!(
                                shape_id = %shape_id,
                                error = %err,
                                "failed to decode shape predicate; treating shape as non-matching"
                            );
                            continue;
                        }
                    }
                }
                matches.push((session_id.clone(), shape_id.clone()));
            }
        }

        matches
    }

    /// Evaluate an array operation against shapes in its tenant and database.
    pub fn evaluate_array_mutation(
        &self,
        scope: ShapeScope,
        array_name: &str,
        coord: &[u64],
    ) -> Vec<(String, ShapeId)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        let mut matches = Vec::new();
        for (session_id, client) in sessions.iter() {
            if client.scope != scope {
                continue;
            }
            for (shape_id, shape) in &client.shapes {
                if shape.matches_array_op(array_name, coord) {
                    matches.push((session_id.clone(), shape_id.clone()));
                }
            }
        }
        matches
    }

    /// Get the session ID for a session's shape state.
    pub fn session_info(&self, session_id: &str) -> Option<(u64, usize)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions
            .get(session_id)
            .map(|c| (c.scope.tenant_id, c.shapes.len()))
    }

    /// Total active shape subscriptions across all sessions.
    pub fn total_shapes(&self) -> usize {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions.values().map(|c| c.shapes.len()).sum()
    }

    /// Number of sessions with active shapes.
    pub fn active_sessions(&self) -> usize {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions.len()
    }

    /// Get a shape definition only when the session remains bound to `scope`.
    ///
    /// A session ID can be reused after reconnecting, so callers must provide
    /// the authenticated tenant and database scope rather than relying on the
    /// session ID alone.
    pub fn get_shape(
        &self,
        session_id: &str,
        scope: ShapeScope,
        shape_id: &str,
    ) -> Option<ShapeDefinition> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions
            .get(session_id)
            .filter(|client| client.scope == scope)
            .and_then(|client| client.shapes.get(shape_id).cloned())
    }

    /// Export all shapes for persistence (serializable snapshot).
    ///
    /// Each entry carries its session's tenant and database scope, so an
    /// import restores every shape into the database it was subscribed in.
    pub fn export_all(&self) -> Vec<(String, ShapeScope, ShapeDefinition)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        let mut result = Vec::new();
        for (session_id, client) in sessions.iter() {
            for shape in client.shapes.values() {
                result.push((session_id.clone(), client.scope, shape.clone()));
            }
        }
        result
    }

    /// Import shapes from a persisted snapshot (called on startup).
    pub fn import(&self, shapes: Vec<(String, ShapeScope, ShapeDefinition)>) {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        for (session_id, scope, shape) in shapes {
            let client = sessions.entry(session_id).or_insert_with(|| ClientShapes {
                shapes: HashMap::new(),
                scope,
                last_modified: Instant::now(),
            });
            client
                .shapes
                .insert(ShapeId::from_validated(shape.shape_id.clone()), shape);
        }
    }

    /// Compact stale sessions: remove sessions with no shapes or sessions
    /// that haven't been modified within the given duration.
    pub fn compact(&self, max_idle: std::time::Duration) -> usize {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        let before = sessions.len();
        sessions.retain(|_, client| {
            !client.shapes.is_empty() && client.last_modified.elapsed() < max_idle
        });
        before - sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::DatabaseId;

    use super::super::definition::ShapeType;
    use super::*;

    fn scope(tenant_id: u64) -> ShapeScope {
        ShapeScope {
            tenant_id,
            database_id: DatabaseId::DEFAULT,
        }
    }

    fn make_doc_shape(id: &str, collection: &str) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: id.into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate: Vec::new(),
            },
            description: format!("all {collection}"),
            field_filter: vec![],
        }
    }

    /// A session at its shape limit can replace a held shape but not add one.
    #[test]
    fn can_hold_bounds_new_shapes_only() {
        let reg = ShapeRegistry::new();
        assert!(
            reg.can_hold("s1", "sh1", 2),
            "a new session holds nothing yet"
        );
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", scope(1), make_doc_shape("sh2", "users"));

        assert!(!reg.can_hold("s1", "sh3", 2));
        assert!(reg.can_hold("s1", "sh1", 2));
        assert!(reg.can_hold("s1", "sh3", 3));
    }

    #[test]
    fn subscribe_and_query() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", scope(1), make_doc_shape("sh2", "users"));

        assert_eq!(reg.total_shapes(), 2);
        assert_eq!(reg.active_sessions(), 1);
        assert_eq!(reg.shapes_for_session("s1").len(), 2);
    }

    #[test]
    fn evaluate_mutation_matches() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "orders"));
        reg.subscribe("s2", scope(1), make_doc_shape("sh2", "orders"));
        reg.subscribe("s3", scope(2), make_doc_shape("sh3", "orders")); // Different tenant.

        let doc = serde_json::json!({"status": "open"});
        let matches = reg.evaluate_mutation(scope(1), "orders", "o1", &doc);
        assert_eq!(matches.len(), 2); // s1 and s2, not s3 (wrong tenant).
    }

    #[test]
    fn get_shape_requires_exact_session_scope() {
        let reg = ShapeRegistry::new();
        let subscribed_scope = ShapeScope {
            tenant_id: 1,
            database_id: DatabaseId::new(7),
        };
        reg.subscribe("s1", subscribed_scope, make_doc_shape("sh1", "orders"));

        assert!(reg.get_shape("s1", subscribed_scope, "sh1").is_some());
        assert!(
            reg.get_shape(
                "s1",
                ShapeScope {
                    tenant_id: 1,
                    database_id: DatabaseId::new(8),
                },
                "sh1",
            )
            .is_none()
        );
        assert!(
            reg.get_shape(
                "s1",
                ShapeScope {
                    tenant_id: 2,
                    database_id: DatabaseId::new(7),
                },
                "sh1",
            )
            .is_none()
        );
    }

    #[test]
    fn unsubscribe() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "orders"));
        assert_eq!(reg.total_shapes(), 1);

        assert!(reg.unsubscribe("s1", "sh1"));
        assert_eq!(reg.total_shapes(), 0);

        assert!(!reg.unsubscribe("s1", "sh1")); // Already removed.
    }

    #[test]
    fn remove_session() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", scope(1), make_doc_shape("sh2", "users"));

        reg.remove_session("s1");
        assert_eq!(reg.total_shapes(), 0);
        assert_eq!(reg.active_sessions(), 0);
    }

    #[test]
    fn no_match_wrong_collection() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "orders"));

        let doc = serde_json::json!({});
        let matches = reg.evaluate_mutation(scope(1), "users", "u1", &doc);
        assert!(matches.is_empty());
    }

    fn make_doc_shape_with_predicate(
        id: &str,
        collection: &str,
        predicate: Vec<u8>,
    ) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: id.into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate,
            },
            description: format!("filtered {collection}"),
            field_filter: vec![],
        }
    }

    #[test]
    fn predicate_eq_routes_matching_doc() {
        use nodedb_types::filter::MetadataFilter;
        let reg = ShapeRegistry::new();
        let filter = MetadataFilter::eq("kind", "Rule");
        let predicate = zerompk::to_msgpack_vec(&filter).expect("encode");
        reg.subscribe(
            "s1",
            scope(1),
            make_doc_shape_with_predicate("sh1", "entries", predicate),
        );

        let matching = serde_json::json!({"kind": "Rule", "name": "test"});
        let non_matching = serde_json::json!({"kind": "Note", "name": "test"});

        let m1 = reg.evaluate_mutation(scope(1), "entries", "doc1", &matching);
        assert_eq!(m1.len(), 1, "matching doc must route to shape");

        let m2 = reg.evaluate_mutation(scope(1), "entries", "doc2", &non_matching);
        assert!(m2.is_empty(), "non-matching doc must not route to shape");
    }

    #[test]
    fn predicate_and_routes_correctly() {
        use nodedb_types::filter::MetadataFilter;
        let reg = ShapeRegistry::new();
        let filter = MetadataFilter::and(vec![
            MetadataFilter::eq("kind", "Rule"),
            MetadataFilter::eq("share", "team"),
        ]);
        let predicate = zerompk::to_msgpack_vec(&filter).expect("encode");
        reg.subscribe(
            "s1",
            scope(1),
            make_doc_shape_with_predicate("sh1", "entries", predicate),
        );

        let both_match = serde_json::json!({"kind": "Rule", "share": "team"});
        let partial = serde_json::json!({"kind": "Rule", "share": "personal"});

        assert_eq!(
            reg.evaluate_mutation(scope(1), "entries", "d1", &both_match)
                .len(),
            1
        );
        assert!(
            reg.evaluate_mutation(scope(1), "entries", "d2", &partial)
                .is_empty()
        );
    }

    #[test]
    fn empty_predicate_matches_all() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_doc_shape("sh1", "entries"));

        let any_doc = serde_json::json!({"anything": "goes"});
        let matches = reg.evaluate_mutation(scope(1), "entries", "d1", &any_doc);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn invalid_predicate_bytes_excluded() {
        let reg = ShapeRegistry::new();
        // Garbage bytes — cannot decode to MetadataFilter.
        let bad_predicate = vec![0xFF, 0xFE, 0x01, 0x02];
        reg.subscribe(
            "s1",
            scope(1),
            make_doc_shape_with_predicate("sh1", "entries", bad_predicate),
        );

        let doc = serde_json::json!({"kind": "Rule"});
        let matches = reg.evaluate_mutation(scope(1), "entries", "d1", &doc);
        assert!(
            matches.is_empty(),
            "bad predicate must be non-matching (fail-closed)"
        );
    }

    fn make_array_shape(id: &str, array: &str) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: id.into(),
            tenant_id: 1,
            shape_type: ShapeType::Array {
                array_name: array.into(),
                coord_range: None,
            },
            description: format!("all {array}"),
            field_filter: vec![],
        }
    }

    #[test]
    fn evaluate_array_mutation_matches() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", scope(1), make_array_shape("ah1", "prices"));
        reg.subscribe("s2", scope(1), make_array_shape("ah2", "prices"));
        reg.subscribe("s3", scope(2), make_array_shape("ah3", "prices")); // Different tenant.
        reg.subscribe("s4", scope(1), make_array_shape("ah4", "other")); // Different array.

        let matches = reg.evaluate_array_mutation(scope(1), "prices", &[5, 5]);
        assert_eq!(matches.len(), 2);
        let session_ids: Vec<&str> = matches.iter().map(|(s, _)| s.as_str()).collect();
        assert!(session_ids.contains(&"s1"));
        assert!(session_ids.contains(&"s2"));
    }

    #[test]
    fn database_scoped_shapes_do_not_cross_deliver() {
        let reg = ShapeRegistry::new();
        reg.subscribe(
            "db-one",
            ShapeScope {
                tenant_id: 1,
                database_id: DatabaseId::new(1),
            },
            make_doc_shape("one", "orders"),
        );
        reg.subscribe(
            "db-two",
            ShapeScope {
                tenant_id: 1,
                database_id: DatabaseId::new(2),
            },
            make_doc_shape("two", "orders"),
        );
        let doc = serde_json::json!({});
        let one = reg.evaluate_mutation(
            ShapeScope {
                tenant_id: 1,
                database_id: DatabaseId::new(1),
            },
            "orders",
            "o1",
            &doc,
        );
        let two = reg.evaluate_mutation(
            ShapeScope {
                tenant_id: 1,
                database_id: DatabaseId::new(2),
            },
            "orders",
            "o1",
            &doc,
        );
        assert_eq!(one[0].0, "db-one");
        assert_eq!(two[0].0, "db-two");
    }

    #[test]
    fn evaluate_array_mutation_coord_range_filter() {
        use nodedb_types::sync::shape::ArrayCoordRange;
        let reg = ShapeRegistry::new();
        let in_range = ShapeDefinition {
            shape_id: "ar1".into(),
            tenant_id: 1,
            shape_type: ShapeType::Array {
                array_name: "mat".into(),
                coord_range: Some(ArrayCoordRange {
                    start: vec![0],
                    end: Some(vec![10]),
                }),
            },
            description: "narrow".into(),
            field_filter: vec![],
        };
        let all = make_array_shape("ar2", "mat");
        reg.subscribe("s1", scope(1), in_range);
        reg.subscribe("s2", scope(1), all);

        // coord = [5] — in range for both.
        let matches = reg.evaluate_array_mutation(scope(1), "mat", &[5]);
        assert_eq!(matches.len(), 2);

        // coord = [50] — only the unbounded shape matches.
        let matches = reg.evaluate_array_mutation(scope(1), "mat", &[50]);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "s2");
    }

    /// An export and import keep each session's database, so a shape
    /// subscribed outside the default database is restored into it.
    #[test]
    fn export_and_import_keep_the_session_database() {
        let reg = ShapeRegistry::new();
        let other = ShapeScope {
            tenant_id: 1,
            database_id: DatabaseId::new(7),
        };
        reg.subscribe("s1", other, make_doc_shape("d1", "orders"));

        let restored = ShapeRegistry::new();
        restored.import(reg.export_all());

        assert!(restored.get_shape("s1", other, "d1").is_some());
        assert!(restored.get_shape("s1", scope(1), "d1").is_none());
    }
}
