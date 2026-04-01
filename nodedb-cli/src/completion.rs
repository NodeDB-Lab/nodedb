//! Tab completion engine for SQL keywords, metacommands, and collection names.

use nodedb_client::NativeClient;

use crate::keywords::SQL_KEYWORDS;

/// Metacommands for completion.
const METACOMMANDS: &[&str] = &[
    "\\d",
    "\\df",
    "\\di",
    "\\dpr",
    "\\dtr",
    "\\du",
    "\\functions",
    "\\nodes",
    "\\node",
    "\\cluster",
    "\\raft",
    "\\migrations",
    "\\health",
    "\\rebalance",
    "\\s",
    "\\status",
    "\\connections",
    "\\format",
    "\\timing",
    "\\conninfo",
    "\\e",
    "\\i",
    "\\g",
    "\\watch",
    "\\x",
    "\\procedures",
    "\\triggers",
    "\\?",
    "\\q",
    "\\quit",
];

/// Completion engine state.
pub struct Completor {
    collections: Vec<String>,
    /// Cached column names per collection: { "users" => ["id", "name", "age"] }
    columns: std::collections::HashMap<String, Vec<String>>,
    /// Cached function names for CALL/DROP/ALTER completion.
    functions: Vec<String>,
    /// Cached trigger names.
    triggers: Vec<String>,
    /// Cached procedure names for CALL completion.
    procedures: Vec<String>,
}

/// Active completion popup state.
pub struct CompletionState {
    pub candidates: Vec<String>,
    pub selected: usize,
    pub prefix_start: usize,
    pub prefix_len: usize,
    pub active: bool,
}

impl CompletionState {
    pub fn empty() -> Self {
        Self {
            candidates: Vec::new(),
            selected: 0,
            prefix_start: 0,
            prefix_len: 0,
            active: false,
        }
    }

    /// Get the currently selected candidate, if any.
    pub fn selected_candidate(&self) -> Option<&str> {
        if self.candidates.is_empty() {
            return None;
        }
        self.candidates.get(self.selected).map(|s| s.as_str())
    }

    /// Cycle to next candidate.
    pub fn next(&mut self) {
        if !self.candidates.is_empty() {
            self.selected = (self.selected + 1) % self.candidates.len();
        }
    }

    /// Cycle to previous candidate.
    pub fn prev(&mut self) {
        if !self.candidates.is_empty() {
            self.selected = if self.selected == 0 {
                self.candidates.len() - 1
            } else {
                self.selected - 1
            };
        }
    }
}

impl Completor {
    pub fn new() -> Self {
        Self {
            collections: Vec::new(),
            columns: std::collections::HashMap::new(),
            functions: Vec::new(),
            triggers: Vec::new(),
            procedures: Vec::new(),
        }
    }

    /// Refresh cached collection names and their columns from the server.
    pub async fn refresh_collections(&mut self, client: &NativeClient) {
        if let Ok(result) = client.query("SHOW COLLECTIONS").await {
            self.collections = result
                .rows
                .iter()
                .filter_map(|row| row.first()?.as_str().map(String::from))
                .collect();
        }

        // Fetch columns for each collection via DESCRIBE.
        self.columns.clear();
        for coll in &self.collections.clone() {
            if let Ok(result) = client.query(&format!("DESCRIBE {coll}")).await {
                let cols: Vec<String> = result
                    .rows
                    .iter()
                    .filter_map(|row| row.first()?.as_str().map(String::from))
                    .collect();
                if !cols.is_empty() {
                    self.columns.insert(coll.clone(), cols);
                }
            }
        }

        // Fetch function, trigger, procedure names for completion.
        self.functions = fetch_names(client, "SHOW FUNCTIONS").await;
        self.triggers = fetch_names(client, "SHOW TRIGGERS").await;
        self.procedures = fetch_names(client, "SHOW PROCEDURES").await;
    }

    /// Generate completions for the word at the cursor position.
    pub fn complete(&self, buffer: &str, cursor: usize) -> CompletionState {
        let (prefix_start, prefix) = extract_word_before_cursor(buffer, cursor);
        if prefix.is_empty() {
            return CompletionState::empty();
        }

        let mut candidates = Vec::new();

        if prefix.starts_with('\\') {
            // Complete metacommands.
            for mc in METACOMMANDS {
                if mc.starts_with(prefix) {
                    candidates.push(mc.to_string());
                }
            }
        } else {
            let upper = prefix.to_uppercase();

            // Context-aware: if previous word is FROM/INTO/DESCRIBE/COLLECTION,
            // prioritize collection names.
            let context = context_before(buffer, prefix_start);

            // Column names: if we're after a collection context (e.g., WHERE, SELECT ... FROM coll WHERE col)
            if let Some(ref coll) = find_collection_context(buffer, &self.collections)
                && let Some(cols) = self.columns.get(coll)
            {
                for col in cols {
                    if col.to_uppercase().starts_with(&upper) && !candidates.contains(col) {
                        candidates.push(col.clone());
                    }
                }
            }

            // SQL keywords.
            for kw in SQL_KEYWORDS {
                if kw.starts_with(&upper) {
                    candidates.push(kw.to_string());
                }
            }

            // Collection names (prioritize after FROM/INTO/DESCRIBE).
            let is_collection_context = matches!(
                context.as_deref(),
                Some("FROM")
                    | Some("INTO")
                    | Some("DESCRIBE")
                    | Some("COLLECTION")
                    | Some("JOIN")
                    | Some("TABLE")
                    | Some("ON")
            );
            for c in &self.collections {
                if c.to_uppercase().starts_with(&upper) && !candidates.contains(c) {
                    if is_collection_context {
                        candidates.insert(0, c.clone());
                    } else {
                        candidates.push(c.clone());
                    }
                }
            }

            // Function/procedure names (prioritize after CALL, FUNCTION, PROCEDURE, TRIGGER).
            let is_callable_context = matches!(
                context.as_deref(),
                Some("CALL") | Some("FUNCTION") | Some("PROCEDURE") | Some("TRIGGER")
            );
            if is_callable_context {
                for name in self
                    .functions
                    .iter()
                    .chain(self.procedures.iter())
                    .chain(self.triggers.iter())
                {
                    if name.to_uppercase().starts_with(&upper) && !candidates.contains(name) {
                        candidates.insert(0, name.clone());
                    }
                }
            }
        }

        if candidates.is_empty() {
            return CompletionState::empty();
        }

        // Sort: exact prefix matches first, then alphabetical.
        candidates.sort();

        CompletionState {
            candidates,
            selected: 0,
            prefix_start,
            prefix_len: prefix.len(),
            active: true,
        }
    }
}

/// Fetch the first column of each row from a SHOW query as a list of names.
async fn fetch_names(client: &NativeClient, query: &str) -> Vec<String> {
    client
        .query(query)
        .await
        .ok()
        .into_iter()
        .flat_map(|r| r.rows)
        .filter_map(|row| row.first()?.as_str().map(String::from))
        .collect()
}

/// Get the word immediately before the current prefix (for context).
/// e.g., in "SELECT * FROM us|", context = "FROM".
fn context_before(buffer: &str, prefix_start: usize) -> Option<String> {
    let before = buffer[..prefix_start].trim_end();
    before
        .split_whitespace()
        .next_back()
        .map(|w| w.to_uppercase())
}

/// Find which collection is referenced in the query (for column completion).
/// Scans for FROM/INTO <collection> patterns.
fn find_collection_context(buffer: &str, collections: &[String]) -> Option<String> {
    let upper = buffer.to_uppercase();
    let words: Vec<&str> = upper.split_whitespace().collect();

    for (i, word) in words.iter().enumerate() {
        if (*word == "FROM" || *word == "INTO" || *word == "JOIN") && i + 1 < words.len() {
            let coll_name = words[i + 1].to_lowercase();
            if collections.iter().any(|c| c == &coll_name) {
                return Some(coll_name);
            }
        }
    }
    None
}

/// Extract the word being typed at the cursor position.
/// Returns (start_byte_offset, word_slice).
fn extract_word_before_cursor(buffer: &str, cursor: usize) -> (usize, &str) {
    let before = &buffer[..cursor];
    // Walk backwards to find word start.
    let start = before
        .rfind(|c: char| c.is_whitespace() || c == '(' || c == ')' || c == ',' || c == ';')
        .map(|i| i + 1)
        .unwrap_or(0);
    (start, &before[start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_keyword() {
        let c = Completor::new();
        let state = c.complete("SEL", 3);
        assert!(state.active);
        assert!(state.candidates.contains(&"SELECT".to_string()));
    }

    #[test]
    fn complete_metacommand() {
        let c = Completor::new();
        let state = c.complete("\\no", 3);
        assert!(state.active);
        assert!(state.candidates.contains(&"\\nodes".to_string()));
    }

    #[test]
    fn complete_mid_query() {
        let c = Completor::new();
        let state = c.complete("SELECT * FR", 11);
        assert!(state.active);
        assert!(state.candidates.contains(&"FROM".to_string()));
    }

    #[test]
    fn no_match() {
        let c = Completor::new();
        let state = c.complete("xyzzy", 5);
        assert!(!state.active);
    }

    #[test]
    fn empty_prefix() {
        let c = Completor::new();
        let state = c.complete("SELECT ", 7);
        assert!(!state.active);
    }

    #[test]
    fn extract_word() {
        let (start, word) = extract_word_before_cursor("SELECT * FROM us", 16);
        assert_eq!(word, "us");
        assert_eq!(start, 14);
    }

    #[test]
    fn cycle_candidates() {
        let mut state = CompletionState {
            candidates: vec!["A".into(), "B".into(), "C".into()],
            selected: 0,
            prefix_start: 0,
            prefix_len: 0,
            active: true,
        };
        assert_eq!(state.selected_candidate(), Some("A"));
        state.next();
        assert_eq!(state.selected_candidate(), Some("B"));
        state.next();
        state.next();
        assert_eq!(state.selected_candidate(), Some("A")); // wraps
        state.prev();
        assert_eq!(state.selected_candidate(), Some("C"));
    }
}
