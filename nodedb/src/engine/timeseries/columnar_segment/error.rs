#[derive(Debug)]
pub enum SegmentError {
    Io(String),
    Corrupt(String),
}

impl std::fmt::Display for SegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "segment I/O error: {msg}"),
            Self::Corrupt(msg) => write!(f, "segment corrupt: {msg}"),
        }
    }
}

impl std::error::Error for SegmentError {}
