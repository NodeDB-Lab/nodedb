pub mod classify;
pub mod collector;
pub mod when_filter;

/// Configuration for trigger batch processing.
pub struct BatchConfig {
    /// Maximum rows per trigger batch (default 1024).
    pub batch_size: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self { batch_size: 1024 }
    }
}
