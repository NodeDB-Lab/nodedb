//! Multi-line input buffer with cursor and history navigation.

/// Input state for the SQL editor area.
pub struct InputState {
    /// Current input buffer (may span multiple lines).
    buffer: String,
    /// Cursor position within the buffer (byte offset).
    cursor: usize,
    /// History navigation index (None = not browsing history).
    history_index: Option<usize>,
    /// Saved buffer when browsing history.
    saved_buffer: Option<String>,
}

impl InputState {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            cursor: 0,
            history_index: None,
            saved_buffer: None,
        }
    }

    pub fn buffer(&self) -> &str {
        &self.buffer
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.trim().is_empty()
    }

    /// Insert a character at the cursor.
    pub fn insert(&mut self, ch: char) {
        self.buffer.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
        self.history_index = None;
    }

    /// Insert a newline.
    pub fn newline(&mut self) {
        self.insert('\n');
    }

    /// Delete the character before the cursor (backspace).
    pub fn backspace(&mut self) {
        if self.cursor > 0 {
            let prev = self.buffer[..self.cursor]
                .char_indices()
                .next_back()
                .map(|(i, _)| i)
                .unwrap_or(0);
            self.buffer.drain(prev..self.cursor);
            self.cursor = prev;
        }
    }

    /// Delete the character at the cursor (delete key).
    pub fn delete(&mut self) {
        if self.cursor < self.buffer.len() {
            let next = self.cursor
                + self.buffer[self.cursor..]
                    .chars()
                    .next()
                    .map(|c| c.len_utf8())
                    .unwrap_or(0);
            self.buffer.drain(self.cursor..next);
        }
    }

    /// Move cursor left one character.
    pub fn move_left(&mut self) {
        if self.cursor > 0 {
            self.cursor = self.buffer[..self.cursor]
                .char_indices()
                .next_back()
                .map(|(i, _)| i)
                .unwrap_or(0);
        }
    }

    /// Move cursor right one character.
    pub fn move_right(&mut self) {
        if self.cursor < self.buffer.len() {
            self.cursor += self.buffer[self.cursor..]
                .chars()
                .next()
                .map(|c| c.len_utf8())
                .unwrap_or(0);
        }
    }

    /// Move cursor to start of line.
    pub fn home(&mut self) {
        self.cursor = 0;
    }

    /// Move cursor to end of line.
    pub fn end(&mut self) {
        self.cursor = self.buffer.len();
    }

    /// Delete from cursor to end of line (Ctrl+K).
    pub fn kill_to_end(&mut self) {
        self.buffer.truncate(self.cursor);
    }

    /// Delete the word before cursor (Ctrl+W).
    pub fn delete_word(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let before = &self.buffer[..self.cursor];
        let trimmed = before.trim_end();
        let word_start = trimmed
            .rfind(|c: char| c.is_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        self.buffer.drain(word_start..self.cursor);
        self.cursor = word_start;
    }

    /// Take the buffer contents and reset.
    pub fn take(&mut self) -> String {
        let s = std::mem::take(&mut self.buffer);
        self.cursor = 0;
        self.history_index = None;
        self.saved_buffer = None;
        s
    }

    /// Set the buffer contents (for history navigation).
    pub fn set(&mut self, s: &str) {
        self.buffer = s.to_string();
        self.cursor = self.buffer.len();
    }

    /// Navigate history up.
    pub fn history_up(&mut self, history: &crate::history::History) {
        if self.history_index.is_none() {
            self.saved_buffer = Some(self.buffer.clone());
            self.history_index = Some(0);
        } else {
            self.history_index = self.history_index.map(|i| i + 1);
        }

        if let Some(idx) = self.history_index {
            if let Some(entry) = history.get_from_end(idx) {
                self.set(entry);
            } else {
                // At the top — don't go further.
                self.history_index = self.history_index.map(|i| i.saturating_sub(1));
            }
        }
    }

    /// Navigate history down.
    pub fn history_down(&mut self, _history: &crate::history::History) {
        match self.history_index {
            Some(0) => {
                // Back to the saved buffer.
                self.history_index = None;
                if let Some(saved) = self.saved_buffer.take() {
                    self.set(&saved);
                }
            }
            Some(idx) => {
                self.history_index = Some(idx - 1);
                if let Some(idx) = self.history_index
                    && let Some(entry) = _history.get_from_end(idx)
                {
                    self.set(entry);
                }
            }
            None => {}
        }
    }

    /// Whether the input ends with a semicolon (ready to execute).
    pub fn ends_with_semicolon(&self) -> bool {
        self.buffer.trim().ends_with(';')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_editing() {
        let mut input = InputState::new();
        input.insert('a');
        input.insert('b');
        input.insert('c');
        assert_eq!(input.buffer(), "abc");
        assert_eq!(input.cursor(), 3);

        input.backspace();
        assert_eq!(input.buffer(), "ab");

        input.move_left();
        input.insert('x');
        assert_eq!(input.buffer(), "axb");
    }

    #[test]
    fn take_resets() {
        let mut input = InputState::new();
        input.insert('x');
        let taken = input.take();
        assert_eq!(taken, "x");
        assert!(input.is_empty());
    }

    #[test]
    fn semicolon_detection() {
        let mut input = InputState::new();
        input.set("SELECT 1;");
        assert!(input.ends_with_semicolon());

        input.set("SELECT 1");
        assert!(!input.ends_with_semicolon());
    }
}
