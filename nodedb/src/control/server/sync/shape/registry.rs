//! Per-client shape registry: tracks which shapes each connected client
//! is subscribed to and evaluates mutations against active shapes.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

use tracing::{debug, info};

use super::definition::{ShapeDefinition, ShapeId};

/// Per-session shape subscription state.
struct ClientShapes {
    /// Active shape subscriptions: shape_id → definition.
    shapes: HashMap<ShapeId, ShapeDefinition>,
    /// Tenant ID.
    tenant_id: u32,
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

    /// Register a shape subscription for a session.
    pub fn subscribe(&self, session_id: &str, tenant_id: u32, shape: ShapeDefinition) {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        let client = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| ClientShapes {
                shapes: HashMap::new(),
                tenant_id,
                last_modified: Instant::now(),
            });
        info!(
            session = session_id,
            shape_id = %shape.shape_id,
            "shape subscribed"
        );
        client.shapes.insert(shape.shape_id.clone(), shape);
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
    pub fn evaluate_mutation(
        &self,
        tenant_id: u32,
        collection: &str,
        doc_id: &str,
    ) -> Vec<(String, ShapeId)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        let mut matches = Vec::new();

        for (session_id, client) in sessions.iter() {
            if client.tenant_id != tenant_id {
                continue;
            }
            for (shape_id, shape) in &client.shapes {
                if shape.could_match(collection, doc_id) {
                    matches.push((session_id.clone(), shape_id.clone()));
                }
            }
        }

        matches
    }

    /// Get the session ID for a session's shape state.
    pub fn session_info(&self, session_id: &str) -> Option<(u32, usize)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions
            .get(session_id)
            .map(|c| (c.tenant_id, c.shapes.len()))
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

    /// Evaluate an array op against all active shapes.
    ///
    /// Returns a list of `(session_id, shape_id)` pairs for `ShapeType::Array`
    /// shapes that match the mutation. The caller then pushes `ArrayDeltaMsg`
    /// frames to the matching sessions.
    ///
    /// This is a sibling of [`evaluate_mutation`] rather than a refactor so
    /// that the existing document/vector matching path is not disturbed.
    pub fn evaluate_array_mutation(
        &self,
        tenant_id: u32,
        array_name: &str,
        coord: &[u64],
    ) -> Vec<(String, ShapeId)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        let mut matches = Vec::new();

        for (session_id, client) in sessions.iter() {
            if client.tenant_id != tenant_id {
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

    /// Get a shape definition by session + shape_id.
    pub fn get_shape(&self, session_id: &str, shape_id: &str) -> Option<ShapeDefinition> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        sessions
            .get(session_id)
            .and_then(|c| c.shapes.get(shape_id).cloned())
    }

    /// Export all shapes for persistence (serializable snapshot).
    pub fn export_all(&self) -> Vec<(String, u32, ShapeDefinition)> {
        let sessions =
            crate::control::lock_utils::read_or_recover(self.sessions.read(), "shape_sessions");
        let mut result = Vec::new();
        for (session_id, client) in sessions.iter() {
            for shape in client.shapes.values() {
                result.push((session_id.clone(), client.tenant_id, shape.clone()));
            }
        }
        result
    }

    /// Import shapes from a persisted snapshot (called on startup).
    pub fn import(&self, shapes: Vec<(String, u32, ShapeDefinition)>) {
        let mut sessions =
            crate::control::lock_utils::write_or_recover(self.sessions.write(), "shape_sessions");
        for (session_id, tenant_id, shape) in shapes {
            let client = sessions.entry(session_id).or_insert_with(|| ClientShapes {
                shapes: HashMap::new(),
                tenant_id,
                last_modified: Instant::now(),
            });
            client.shapes.insert(shape.shape_id.clone(), shape);
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
    use super::super::definition::ShapeType;
    use super::*;

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

    #[test]
    fn subscribe_and_query() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", 1, make_doc_shape("sh2", "users"));

        assert_eq!(reg.total_shapes(), 2);
        assert_eq!(reg.active_sessions(), 1);
        assert_eq!(reg.shapes_for_session("s1").len(), 2);
    }

    #[test]
    fn evaluate_mutation_matches() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        reg.subscribe("s2", 1, make_doc_shape("sh2", "orders"));
        reg.subscribe("s3", 2, make_doc_shape("sh3", "orders")); // Different tenant.

        let matches = reg.evaluate_mutation(1, "orders", "o1");
        assert_eq!(matches.len(), 2); // s1 and s2, not s3 (wrong tenant).
    }

    #[test]
    fn unsubscribe() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        assert_eq!(reg.total_shapes(), 1);

        assert!(reg.unsubscribe("s1", "sh1"));
        assert_eq!(reg.total_shapes(), 0);

        assert!(!reg.unsubscribe("s1", "sh1")); // Already removed.
    }

    #[test]
    fn remove_session() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));
        reg.subscribe("s1", 1, make_doc_shape("sh2", "users"));

        reg.remove_session("s1");
        assert_eq!(reg.total_shapes(), 0);
        assert_eq!(reg.active_sessions(), 0);
    }

    #[test]
    fn no_match_wrong_collection() {
        let reg = ShapeRegistry::new();
        reg.subscribe("s1", 1, make_doc_shape("sh1", "orders"));

        let matches = reg.evaluate_mutation(1, "users", "u1");
        assert!(matches.is_empty());
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
        reg.subscribe("s1", 1, make_array_shape("ah1", "prices"));
        reg.subscribe("s2", 1, make_array_shape("ah2", "prices"));
        reg.subscribe("s3", 2, make_array_shape("ah3", "prices")); // Different tenant.
        reg.subscribe("s4", 1, make_array_shape("ah4", "other")); // Different array.

        let matches = reg.evaluate_array_mutation(1, "prices", &[5, 5]);
        assert_eq!(matches.len(), 2);
        let session_ids: Vec<&str> = matches.iter().map(|(s, _)| s.as_str()).collect();
        assert!(session_ids.contains(&"s1"));
        assert!(session_ids.contains(&"s2"));
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
        reg.subscribe("s1", 1, in_range);
        reg.subscribe("s2", 1, all);

        // coord = [5] — in range for both.
        let matches = reg.evaluate_array_mutation(1, "mat", &[5]);
        assert_eq!(matches.len(), 2);

        // coord = [50] — only the unbounded shape matches.
        let matches = reg.evaluate_array_mutation(1, "mat", &[50]);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].0, "s2");
    }
}
