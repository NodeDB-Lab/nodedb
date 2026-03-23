//! Persistent command history.

use std::fs;
use std::path::PathBuf;

/// Command history stored in `~/.nodedb/history`.
pub struct History {
    entries: Vec<String>,
    path: PathBuf,
    max_entries: usize,
}

impl History {
    /// Load history from disk (or create empty).
    pub fn load() -> Self {
        let path = history_path();
        let entries = if path.exists() {
            fs::read_to_string(&path)
                .unwrap_or_default()
                .lines()
                .map(String::from)
                .collect()
        } else {
            Vec::new()
        };
        Self {
            entries,
            path,
            max_entries: 1000,
        }
    }

    /// Add an entry to history.
    pub fn add(&mut self, entry: &str) {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            return;
        }
        // Don't duplicate the last entry.
        if self.entries.last().is_some_and(|last| last == trimmed) {
            return;
        }
        self.entries.push(trimmed.to_string());
        if self.entries.len() > self.max_entries {
            self.entries.remove(0);
        }
    }

    /// Save history to disk.
    pub fn save(&self) {
        if let Some(parent) = self.path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(&self.path, self.entries.join("\n"));
    }

    /// Get entry at index from the end (0 = most recent).
    pub fn get_from_end(&self, offset: usize) -> Option<&str> {
        if offset >= self.entries.len() {
            return None;
        }
        self.entries
            .get(self.entries.len() - 1 - offset)
            .map(|s| s.as_str())
    }

    /// Number of entries.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

fn history_path() -> PathBuf {
    dirs::data_local_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("nodedb")
        .join("history")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get() {
        let mut h = History {
            entries: Vec::new(),
            path: PathBuf::from("/tmp/test_history"),
            max_entries: 10,
        };
        h.add("SELECT 1");
        h.add("SELECT 2");
        assert_eq!(h.get_from_end(0), Some("SELECT 2"));
        assert_eq!(h.get_from_end(1), Some("SELECT 1"));
        assert_eq!(h.get_from_end(2), None);
    }

    #[test]
    fn no_consecutive_duplicates() {
        let mut h = History {
            entries: Vec::new(),
            path: PathBuf::from("/tmp/test"),
            max_entries: 10,
        };
        h.add("SELECT 1");
        h.add("SELECT 1");
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn skip_empty() {
        let mut h = History {
            entries: Vec::new(),
            path: PathBuf::from("/tmp/test"),
            max_entries: 10,
        };
        h.add("");
        h.add("  ");
        assert_eq!(h.len(), 0);
    }
}
