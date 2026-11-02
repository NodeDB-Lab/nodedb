// SPDX-License-Identifier: BUSL-1.1

//! Durable topic definitions for the system catalog. Messages live in
//! `topic_messages`.

use std::collections::HashMap;

use redb::{ReadableDatabase, ReadableTable};

use super::consumer_groups::decode_consumer_group;
use super::topic_messages::scoped_message_keys;
use super::types::{CONSUMER_GROUPS, SystemCatalog, TOPIC_MESSAGES, TOPICS_EP, catalog_err};
use crate::event::topic::{TopicDef, validate_topic_name};
use crate::types::DatabaseId;

impl SystemCatalog {
    /// Store a topic under an unambiguous database-scoped v2 key. Replacing a
    /// definition cannot move either durable high-water mark backwards.
    pub fn put_ep_topic(&self, def: &TopicDef) -> crate::Result<()> {
        validate_topic_name(&def.name).map_err(|error| catalog_err("put topic", error))?;
        let key = topic_key(def.database_id, def.tenant_id, &def.name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            let existing =
                find_topic_definition(&table, def.database_id, def.tenant_id, &def.name)?;
            let mut stored = def.clone();
            if let Some(existing) = existing {
                stored.last_sequence = stored.last_sequence.max(existing.last_sequence);
                if (existing.last_epoch, existing.last_lsn) > (stored.last_epoch, stored.last_lsn) {
                    stored.last_epoch = existing.last_epoch;
                    stored.last_lsn = existing.last_lsn;
                }
            }
            let bytes =
                zerompk::to_msgpack_vec(&stored).map_err(|e| catalog_err("serialize topic", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert topic", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Insert a topic definition only if the durable identity is absent.
    ///
    /// The check and insert share a write transaction, so concurrent creators
    /// cannot both observe success.
    pub fn create_ep_topic(&self, def: &TopicDef) -> crate::Result<bool> {
        validate_topic_name(&def.name).map_err(|error| catalog_err("create topic", error))?;
        self.create_ep_topic_unchecked(def)
    }

    /// Insert a topic definition without re-checking its name.
    ///
    /// Replicated apply uses this: the leader validated before proposing, so a
    /// rejection here diverges this node from the accepted entry.
    pub fn create_ep_topic_unchecked(&self, def: &TopicDef) -> crate::Result<bool> {
        let key = topic_key(def.database_id, def.tenant_id, &def.name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("create topic txn", e))?;
        {
            let mut table = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            if find_topic_definition(&table, def.database_id, def.tenant_id, &def.name)?.is_some() {
                return Ok(false);
            }
            let bytes =
                zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize topic", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert topic", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit create topic", e))?;
        Ok(true)
    }

    /// Delete a topic and every one of its durable messages atomically.
    pub fn delete_ep_topic(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        validate_topic_name(name).map_err(|error| catalog_err("delete topic", error))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let mut existed;
        {
            let mut definitions = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            existed = definitions
                .remove(topic_key(database_id, tenant_id, name).as_str())
                .map_err(|e| catalog_err("delete topic", e))?
                .is_some();
            let mut messages = write_txn
                .open_table(TOPIC_MESSAGES)
                .map_err(|e| catalog_err("open topic_messages", e))?;
            let keys = scoped_message_keys(&messages, database_id, tenant_id, name)?;
            existed |= !keys.is_empty();
            for key in keys {
                messages
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("delete topic message", e))?;
            }
            super::topic_publish_marks::forget_topic_marks(
                &write_txn,
                database_id,
                tenant_id,
                name,
            )?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Return every consumer-group name attached to a topic's `topic:<name>`
    /// stream.
    ///
    /// Callers use this before the cross-database offset cleanup so the offset
    /// store can be durably cleared before the catalog transaction commits.
    pub fn topic_consumer_group_names(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Vec<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read topic groups txn", e))?;
        let table = read_txn
            .open_table(CONSUMER_GROUPS)
            .map_err(|e| catalog_err("open consumer_groups", e))?;
        let canonical = format!("topic:{name}");
        let mut names = std::collections::BTreeSet::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range consumer_groups", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read consumer_group", e))?;
            let group = decode_consumer_group(value.value())?;
            if group.database_id == database_id
                && group.tenant_id == tenant_id
                && group.stream_name == canonical
            {
                names.insert(group.name);
            }
        }
        Ok(names.into_iter().collect())
    }

    /// Delete a topic, its messages, and every consumer-group definition on
    /// its `topic:<name>` stream in one redb transaction. Offset deletion is
    /// intentionally coordinated by the caller before this transaction: the
    /// offset store is a separate database and this method must never expose a
    /// successful DROP with stale cursors left behind.
    pub fn delete_ep_topic_with_consumer_groups(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        validate_topic_name(name).map_err(|error| catalog_err("delete topic", error))?;
        self.delete_ep_topic_with_consumer_groups_unchecked(database_id, tenant_id, name)
    }

    /// Delete a topic and its groups without re-checking the topic name.
    ///
    /// Replicated apply uses this: the name was validated before the entry was
    /// proposed, and a rejection here leaves the row on this node alone.
    pub fn delete_ep_topic_with_consumer_groups_unchecked(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("delete topic txn", e))?;
        let mut existed = false;
        {
            let mut definitions = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            existed |= definitions
                .remove(topic_key(database_id, tenant_id, name).as_str())
                .map_err(|e| catalog_err("delete topic", e))?
                .is_some();
            let mut messages = write_txn
                .open_table(TOPIC_MESSAGES)
                .map_err(|e| catalog_err("open topic_messages", e))?;
            let keys = scoped_message_keys(&messages, database_id, tenant_id, name)?;
            existed |= !keys.is_empty();
            for key in keys {
                messages
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("delete topic message", e))?;
            }
            super::topic_publish_marks::forget_topic_marks(
                &write_txn,
                database_id,
                tenant_id,
                name,
            )?;
            let mut groups = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            let canonical = format!("topic:{name}");
            let mut keys = Vec::new();
            for entry in groups
                .range(..)
                .map_err(|e| catalog_err("range consumer_groups", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("read consumer_group", e))?;
                let group = decode_consumer_group(value.value())?;
                if group.database_id == database_id
                    && group.tenant_id == tenant_id
                    && group.stream_name == canonical
                {
                    keys.push(key.value().to_owned());
                }
            }
            for key in keys {
                groups
                    .remove(key.as_str())
                    .map_err(|e| catalog_err("delete topic consumer_group", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit topic deletion", e))?;
        Ok(existed)
    }

    /// Load every durable topic, sorted by database, tenant, and name.
    pub fn load_all_ep_topics(&self) -> crate::Result<Vec<TopicDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TOPICS_EP)
            .map_err(|e| catalog_err("open topics_ep", e))?;
        let mut topics = HashMap::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range topics_ep", e))?
        {
            let (_, value) = entry.map_err(|e| catalog_err("read topic", e))?;
            let def = decode_topic(value.value())?;
            let identity = (def.database_id, def.tenant_id, def.name.clone());
            topics.insert(identity, def);
        }
        let mut topics: Vec<_> = topics.into_values().collect();
        topics.sort_by(|left, right| {
            (left.database_id.as_u64(), left.tenant_id, &left.name).cmp(&(
                right.database_id.as_u64(),
                right.tenant_id,
                &right.name,
            ))
        });
        Ok(topics)
    }
}

pub(super) fn find_topic_definition(
    table: &redb::Table<&str, &[u8]>,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> crate::Result<Option<TopicDef>> {
    let key = topic_key(database_id, tenant_id, name);
    let Some(value) = table
        .get(key.as_str())
        .map_err(|e| catalog_err("get topic", e))?
    else {
        return Ok(None);
    };
    let def = decode_topic(value.value())?;
    validate_topic_identity(&def, database_id, tenant_id, name)?;
    Ok(Some(def))
}

pub(super) fn validate_topic_identity(
    def: &TopicDef,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> crate::Result<()> {
    if (def.database_id, def.tenant_id, def.name.as_str()) != (database_id, tenant_id, name) {
        return Err(catalog_err(
            "decode topic",
            "definition identity does not match key",
        ));
    }
    Ok(())
}

pub(super) fn topic_key(database_id: DatabaseId, tenant_id: u64, name: &str) -> String {
    format!(
        "{:016x}/{:016x}/{:08x}/{}",
        database_id.as_u64(),
        tenant_id,
        name.len(),
        hex::encode(name)
    )
}

pub(super) fn decode_topic(bytes: &[u8]) -> crate::Result<TopicDef> {
    zerompk::from_msgpack(bytes).map_err(|e| catalog_err("decode topic", e))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::topic_messages::current_time_ms;
    use super::*;
    use crate::event::cdc::consumer_group::ConsumerGroupDef;
    use crate::event::cdc::stream_def::RetentionConfig;

    fn catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        (dir, catalog)
    }

    fn topic(database_id: DatabaseId, tenant_id: u64, name: &str, max_events: u64) -> TopicDef {
        TopicDef {
            database_id,
            tenant_id,
            name: name.into(),
            retention: RetentionConfig {
                max_events,
                max_age_secs: 3_600,
            },
            owner: "admin".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
            last_epoch: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    /// Append one message at the Raft entry `(0, index)`.
    fn append_at(
        catalog: &SystemCatalog,
        scope: (DatabaseId, u64, &str),
        payload: &str,
        event_time: u64,
        index: u64,
    ) -> crate::event::topic::TopicMessage {
        catalog
            .append_replicated_topic_message(scope, payload, event_time, (0, index), None)
            .expect("append")
            .expect("the entry appends")
    }

    /// The apply appends a group's entries in log order: their messages take
    /// contiguous sequences, and survive a reopen.
    #[test]
    fn entry_appends_are_contiguous_and_survive_reopen() {
        let (dir, catalog) = catalog();
        catalog
            .put_ep_topic(&topic(DatabaseId::new(7), 1, "events", 100))
            .expect("topic");
        for index in 1..=16 {
            append_at(
                &catalog,
                (DatabaseId::new(7), 1, "events"),
                &index.to_string(),
                current_time_ms(),
                index,
            );
        }
        let messages = catalog
            .load_ep_topic_messages(DatabaseId::new(7), 1, "events")
            .expect("load");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            (1..=16).collect::<Vec<_>>()
        );
        drop(catalog);
        let reopened = SystemCatalog::open(&dir.path().join("system.redb")).expect("reopen");
        assert_eq!(
            reopened
                .load_ep_topic_messages(DatabaseId::new(7), 1, "events")
                .expect("reload")
                .len(),
            16
        );
    }

    #[test]
    fn concurrent_creates_have_one_durable_winner_per_scope() {
        let (_dir, catalog) = catalog();
        let catalog = Arc::new(catalog);
        let mut workers = Vec::new();
        for _ in 0..16 {
            let catalog = Arc::clone(&catalog);
            workers.push(std::thread::spawn(move || {
                catalog.create_ep_topic(&topic(DatabaseId::new(7), 1, "events", 100))
            }));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker").expect("create"))
            .filter(|created| *created)
            .count();
        assert_eq!(successes, 1);
        assert_eq!(catalog.load_all_ep_topics().expect("topics").len(), 1);
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::new(8), 1, "events", 100))
                .expect("other database")
        );
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::new(7), 2, "events", 100))
                .expect("other tenant")
        );
    }

    #[test]
    fn catalog_rejects_invalid_or_oversized_topic_names() {
        let (_dir, catalog) = catalog();
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::DEFAULT, 1, "1invalid", 1))
                .is_err()
        );
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::DEFAULT, 1, &"a".repeat(257), 1))
                .is_err()
        );
    }

    #[test]
    fn retention_prunes_messages_without_moving_high_water_marks_backwards() {
        let (_dir, catalog) = catalog();
        catalog
            .put_ep_topic(&topic(DatabaseId::DEFAULT, 1, "events", 2))
            .expect("topic");
        let now = current_time_ms();
        for index in 1..=3 {
            append_at(
                &catalog,
                (DatabaseId::DEFAULT, 1, "events"),
                "{}",
                now,
                index,
            );
        }
        let messages = catalog
            .load_ep_topic_messages(DatabaseId::DEFAULT, 1, "events")
            .expect("load");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        let definition = catalog
            .load_all_ep_topics()
            .expect("definitions")
            .pop()
            .expect("definition");
        assert_eq!((definition.last_sequence, definition.last_lsn), (3, 3));
        let replacement = topic(DatabaseId::DEFAULT, 1, "events", 2);
        catalog.put_ep_topic(&replacement).expect("replace");
        let definition = catalog
            .load_all_ep_topics()
            .expect("definitions")
            .pop()
            .expect("definition");
        assert_eq!((definition.last_sequence, definition.last_lsn), (3, 3));
    }

    #[test]
    fn max_age_prunes_expired_messages() {
        let (_dir, catalog) = catalog();
        let mut definition = topic(DatabaseId::DEFAULT, 1, "events", 10);
        definition.retention.max_age_secs = 1;
        catalog.put_ep_topic(&definition).expect("topic");
        append_at(&catalog, (DatabaseId::DEFAULT, 1, "events"), "old", 0, 1);
        assert!(
            catalog
                .load_ep_topic_messages(DatabaseId::DEFAULT, 1, "events")
                .expect("load")
                .is_empty()
        );
        let definition = catalog
            .load_all_ep_topics()
            .expect("definitions")
            .pop()
            .expect("definition");
        assert_eq!((definition.last_sequence, definition.last_lsn), (1, 1));
    }

    #[test]
    fn topic_drop_transaction_removes_topic_groups_and_keeps_stream_groups() {
        let (_dir, catalog) = catalog();
        let database_id = DatabaseId::new(7);
        catalog
            .create_ep_topic(&topic(database_id, 1, "events", 10))
            .expect("create");
        for stream_name in ["topic:events", "events"] {
            catalog
                .put_consumer_group(&ConsumerGroupDef {
                    database_id,
                    tenant_id: 1,
                    name: format!("group_{}", stream_name.replace(':', "_")),
                    stream_name: stream_name.into(),
                    owner: "admin".into(),
                    created_at: 0,
                    modification_hlc: nodedb_types::Hlc::ZERO,
                })
                .expect("group");
        }
        append_at(
            &catalog,
            (database_id, 1, "events"),
            "before",
            current_time_ms(),
            1,
        );
        assert_eq!(
            catalog
                .topic_consumer_group_names(database_id, 1, "events")
                .expect("names"),
            vec!["group_topic_events".to_string()],
            "a group on the change stream of the same name is not a topic group"
        );
        assert!(
            catalog
                .delete_ep_topic_with_consumer_groups(database_id, 1, "events")
                .expect("drop")
        );
        assert!(
            catalog
                .load_ep_topic_messages(database_id, 1, "events")
                .expect("messages")
                .is_empty()
        );
        assert!(
            catalog
                .topic_consumer_group_names(database_id, 1, "events")
                .expect("names")
                .is_empty()
        );
        let remaining: Vec<String> = catalog
            .load_all_consumer_groups()
            .expect("groups")
            .into_iter()
            .map(|group| group.stream_name)
            .collect();
        assert_eq!(
            remaining,
            vec!["events".to_string()],
            "DROP TOPIC must keep the change stream's group"
        );
    }

    #[test]
    fn drop_then_recreate_has_a_fresh_durable_lifecycle() {
        let (_dir, catalog) = catalog();
        let database_id = DatabaseId::new(7);
        catalog
            .create_ep_topic(&topic(database_id, 1, "events", 10))
            .expect("create");
        append_at(
            &catalog,
            (database_id, 1, "events"),
            "before",
            current_time_ms(),
            1,
        );
        assert!(
            catalog
                .delete_ep_topic(database_id, 1, "events")
                .expect("drop")
        );
        assert!(
            catalog
                .create_ep_topic(&topic(database_id, 1, "events", 10))
                .expect("recreate")
        );
        let message = append_at(
            &catalog,
            (database_id, 1, "events"),
            "after",
            current_time_ms(),
            1,
        );
        assert_eq!(message.sequence, 1);
        assert_eq!(
            catalog
                .load_ep_topic_messages(database_id, 1, "events")
                .expect("messages")
                .len(),
            1
        );
    }

    #[test]
    fn message_scopes_are_isolated_and_delete_is_atomic_for_one_scope() {
        let (_dir, catalog) = catalog();
        let first = (DatabaseId::new(1), 1, "events");
        let second = (DatabaseId::new(2), 1, "events");
        catalog
            .put_ep_topic(&topic(first.0, first.1, first.2, 10))
            .expect("first topic");
        catalog
            .put_ep_topic(&topic(second.0, second.1, second.2, 10))
            .expect("second topic");
        let now = current_time_ms();
        append_at(&catalog, first, "one", now, 1);
        append_at(&catalog, second, "two", now, 1);
        assert!(
            catalog
                .delete_ep_topic(first.0, first.1, first.2)
                .expect("delete")
        );
        assert!(
            catalog
                .load_ep_topic_messages(first.0, first.1, first.2)
                .expect("first load")
                .is_empty()
        );
        assert_eq!(
            catalog
                .load_ep_topic_messages(second.0, second.1, second.2)
                .expect("second load")
                .len(),
            1
        );
    }
}
