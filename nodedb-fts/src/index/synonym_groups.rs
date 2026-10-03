// SPDX-License-Identifier: Apache-2.0

//! Tenant-scoped synonym group persistence in the FTS backend.
//!
//! Synonym groups are stored in backend meta using a sentinel collection
//! `"_synonym_groups"` with:
//! - subkey `"_index"` → JSON array of all group names for the tenant (the index)
//! - subkey `<group_name>` → JSON-serialized `SynonymGroupRecord`
//!
//! At query time, all groups for the tenant are loaded, merged into a
//! `SynonymMap`, and applied to the query token stream.
//!
//! **OR-expansion semantics**: for a group `{a, b, c}`, querying any term
//! expands to all other terms. A query for `db` with group `{db, database,
//! datastore}` matches documents containing `database` or `datastore` as
//! well. This is the only sensible default for synonym search.

use crate::analyzer::synonym::SynonymMap;
use crate::backend::FtsBackend;
use crate::index::writer::FtsIndex;
use crate::scope::IndexScope;

/// Sentinel index whose metadata holds the synonym groups.
const SYNONYM_GROUPS_COLLECTION: IndexScope<'static> = IndexScope::document("_synonym_groups");

/// Special meta subkey that holds the JSON array of all group names.
const INDEX_SUBKEY: &str = "_index";

/// Serialized group record: name + terms list.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SynonymGroupRecord {
    pub name: String,
    pub terms: Vec<String>,
    pub created_at: u64,
}

impl<B: FtsBackend> FtsIndex<B> {
    /// Persist a synonym group. Overwrites any existing group with the same name.
    pub fn put_synonym_group(
        &self,
        database_id: u64,
        tid: u64,
        record: &SynonymGroupRecord,
    ) -> Result<(), B::Error> {
        // Write the record itself.
        let bytes = sonic_rs::to_vec(record).unwrap_or_default();
        self.backend.write_meta(
            database_id,
            tid,
            SYNONYM_GROUPS_COLLECTION,
            &record.name,
            &bytes,
        )?;

        // Update the name index.
        let mut names = self.read_name_index(database_id, tid)?;
        if !names.contains(&record.name) {
            names.push(record.name.clone());
            self.write_name_index(database_id, tid, &names)?;
        }
        Ok(())
    }

    /// Delete a synonym group. Returns `true` if it existed.
    pub fn delete_synonym_group(
        &self,
        database_id: u64,
        tid: u64,
        name: &str,
    ) -> Result<bool, B::Error> {
        // An empty byte slice is a tombstone written by a prior delete.
        // Only treat a non-empty (non-tombstoned) record as "existing".
        let existed = self
            .backend
            .read_meta(database_id, tid, SYNONYM_GROUPS_COLLECTION, name)?
            .is_some_and(|b| !b.is_empty());

        if existed {
            // Tombstone the record.
            self.backend
                .write_meta(database_id, tid, SYNONYM_GROUPS_COLLECTION, name, &[])?;
            // Remove from the name index.
            let mut names = self.read_name_index(database_id, tid)?;
            names.retain(|n| n != name);
            self.write_name_index(database_id, tid, &names)?;
        }
        Ok(existed)
    }

    /// Read a single synonym group by name. Returns `None` if not found or tombstoned.
    pub fn get_synonym_group(
        &self,
        database_id: u64,
        tid: u64,
        name: &str,
    ) -> Result<Option<SynonymGroupRecord>, B::Error> {
        match self
            .backend
            .read_meta(database_id, tid, SYNONYM_GROUPS_COLLECTION, name)?
        {
            None => Ok(None),
            Some(bytes) if bytes.is_empty() => Ok(None),
            Some(bytes) => Ok(sonic_rs::from_slice::<SynonymGroupRecord>(&bytes).ok()),
        }
    }

    /// List all synonym group records for a tenant.
    pub fn list_synonym_groups(
        &self,
        database_id: u64,
        tid: u64,
    ) -> Result<Vec<SynonymGroupRecord>, B::Error> {
        let names = self.read_name_index(database_id, tid)?;
        let mut groups = Vec::with_capacity(names.len());
        for name in &names {
            if let Some(rec) = self.get_synonym_group(database_id, tid, name)? {
                groups.push(rec);
            }
        }
        Ok(groups)
    }

    /// Build an in-memory `SynonymMap` from a slice of synonym group records.
    ///
    /// Each term in every group maps to all other terms in that group
    /// (bidirectional OR-expansion). Terms are analyzed with the default
    /// analyzer so synonym keys match the stemmed tokens produced at query
    /// time by `search_with_params`.
    pub fn build_synonym_map_for_tenant(
        &self,
        _database_id: u64,
        _tid: u64,
        all_groups: &[SynonymGroupRecord],
    ) -> SynonymMap {
        let mut map = SynonymMap::new();
        for group in all_groups {
            if group.terms.len() < 2 {
                continue;
            }
            // Analyze each term through the same pipeline used at query time
            // so that synonym keys align with stemmed query tokens.
            let analyzed: Vec<Vec<String>> = group
                .terms
                .iter()
                .map(|t| crate::analyzer::pipeline::analyze(t))
                .collect();

            for (i, my_tokens) in analyzed.iter().enumerate() {
                let other_tokens: Vec<&str> = analyzed
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .flat_map(|(_, ts)| ts.iter().map(|s| s.as_str()))
                    .collect();
                for my_token in my_tokens {
                    map.add(my_token, &other_tokens);
                }
            }
        }
        map
    }

    /// Load all synonym groups for a tenant and build the expansion map.
    ///
    /// Called at FTS query time inside `search_with_params` to expand query
    /// tokens before BM25 scoring.
    pub fn expand_query_with_synonyms(
        &self,
        database_id: u64,
        tid: u64,
        tokens: Vec<String>,
    ) -> Result<Vec<String>, B::Error> {
        let groups = self.list_synonym_groups(database_id, tid)?;
        if groups.is_empty() {
            return Ok(tokens);
        }
        let map = self.build_synonym_map_for_tenant(database_id, tid, &groups);
        let expanded = map.expand(&tokens);
        Ok(expanded)
    }

    /// Expand each analyzed query token into its word group: the token
    /// first, then its distinct synonyms. One group per token, in token
    /// order. A document matches a word when it holds any term of the group.
    pub fn expand_query_groups(
        &self,
        database_id: u64,
        tid: u64,
        tokens: Vec<String>,
    ) -> Result<Vec<Vec<String>>, B::Error> {
        let groups = self.list_synonym_groups(database_id, tid)?;
        if groups.is_empty() {
            return Ok(tokens.into_iter().map(|token| vec![token]).collect());
        }
        let map = self.build_synonym_map_for_tenant(database_id, tid, &groups);
        Ok(tokens
            .into_iter()
            .map(|token| {
                let mut group = map.expand(std::slice::from_ref(&token));
                let mut seen = std::collections::HashSet::new();
                group.retain(|term| seen.insert(term.clone()));
                group
            })
            .collect())
    }

    // ── internal helpers ──────────────────────────────────────────────────────

    fn read_name_index(&self, database_id: u64, tid: u64) -> Result<Vec<String>, B::Error> {
        match self
            .backend
            .read_meta(database_id, tid, SYNONYM_GROUPS_COLLECTION, INDEX_SUBKEY)?
        {
            None => Ok(Vec::new()),
            Some(bytes) if bytes.is_empty() => Ok(Vec::new()),
            Some(bytes) => Ok(sonic_rs::from_slice::<Vec<String>>(&bytes).unwrap_or_default()),
        }
    }

    fn write_name_index(
        &self,
        database_id: u64,
        tid: u64,
        names: &[String],
    ) -> Result<(), B::Error> {
        let bytes = sonic_rs::to_vec(names).unwrap_or_default();
        self.backend.write_meta(
            database_id,
            tid,
            SYNONYM_GROUPS_COLLECTION,
            INDEX_SUBKEY,
            &bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::memory::MemoryBackend;
    use crate::index::writer::FtsIndex;
    use crate::test_support::test_governor;

    use super::SynonymGroupRecord;

    const DB: u64 = 0;
    const T: u64 = 1;

    fn idx() -> FtsIndex<MemoryBackend> {
        FtsIndex::new(MemoryBackend::new(), test_governor())
    }

    fn rec(name: &str, terms: &[&str]) -> SynonymGroupRecord {
        SynonymGroupRecord {
            name: name.to_string(),
            terms: terms.iter().map(|s| s.to_string()).collect(),
            created_at: 0,
        }
    }

    #[test]
    fn put_and_get() {
        let i = idx();
        i.put_synonym_group(DB, T, &rec("db_terms", &["database", "db", "datastore"]))
            .unwrap();
        let got = i.get_synonym_group(DB, T, "db_terms").unwrap().unwrap();
        assert_eq!(got.name, "db_terms");
        assert_eq!(got.terms.len(), 3);
    }

    #[test]
    fn delete_removes() {
        let i = idx();
        i.put_synonym_group(DB, T, &rec("g1", &["a", "b"])).unwrap();
        assert!(i.delete_synonym_group(DB, T, "g1").unwrap());
        assert!(!i.delete_synonym_group(DB, T, "g1").unwrap());
        assert!(i.get_synonym_group(DB, T, "g1").unwrap().is_none());
    }

    #[test]
    fn list_reflects_puts_and_deletes() {
        let i = idx();
        i.put_synonym_group(DB, T, &rec("g1", &["a", "b"])).unwrap();
        i.put_synonym_group(DB, T, &rec("g2", &["x", "y"])).unwrap();
        let names: Vec<String> = i
            .list_synonym_groups(DB, T)
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(names.len(), 2);

        i.delete_synonym_group(DB, T, "g1").unwrap();
        let names2: Vec<String> = i
            .list_synonym_groups(DB, T)
            .unwrap()
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(names2, vec!["g2"]);
    }

    #[test]
    fn synonym_map_bidirectional() {
        let i = idx();
        let recs = vec![rec("db_terms", &["db", "database", "datastore"])];
        let map = i.build_synonym_map_for_tenant(DB, T, &recs);

        // Terms are analyzed/stemmed before building the map:
        // "database" → "databas", "datastore" → "datastor", "db" → "db"
        let expanded = map.expand(&["db".to_string()]);
        assert!(expanded.contains(&"databas".to_string()));
        assert!(expanded.contains(&"datastor".to_string()));

        let expanded2 = map.expand(&["databas".to_string()]);
        assert!(expanded2.contains(&"db".to_string()));
        assert!(expanded2.contains(&"datastor".to_string()));
    }

    #[test]
    fn expand_query_with_synonyms_no_groups() {
        let i = idx();
        let tokens = vec!["hello".to_string(), "world".to_string()];
        let expanded = i.expand_query_with_synonyms(DB, T, tokens.clone()).unwrap();
        assert_eq!(expanded, tokens);
    }

    #[test]
    fn expand_query_expands_matching_token() {
        let i = idx();
        i.put_synonym_group(DB, T, &rec("db_terms", &["db", "database", "datastore"]))
            .unwrap();
        // expand_query_with_synonyms receives already-analyzed tokens ("databas" not "database")
        // because search_with_params analyzes first, then expands.
        // The synonym map stores analyzed stems: "database" → "databas".
        let tokens = vec!["db".to_string(), "perform".to_string()];
        let expanded = i.expand_query_with_synonyms(DB, T, tokens).unwrap();
        assert!(expanded.contains(&"db".to_string()));
        assert!(expanded.contains(&"databas".to_string()));
        assert!(expanded.contains(&"datastor".to_string()));
        assert!(expanded.contains(&"perform".to_string()));
    }
}
