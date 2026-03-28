//! RESP command parsing: extract command name + args from RESP arrays.

use super::codec::RespValue;

/// A parsed RESP command with its arguments.
#[derive(Debug)]
pub struct RespCommand {
    /// Uppercase command name (e.g., "GET", "SET").
    pub name: String,
    /// Raw argument bytes (one per argument after the command name).
    pub args: Vec<Vec<u8>>,
}

impl RespCommand {
    /// Parse a RESP value (expected to be an array of bulk strings) into a command.
    ///
    /// Returns `None` if the value is not a valid command (empty array, non-array, etc.).
    pub fn parse(value: &RespValue) -> Option<Self> {
        let items = match value {
            RespValue::Array(Some(items)) if !items.is_empty() => items,
            _ => return None,
        };

        let name = match &items[0] {
            RespValue::BulkString(Some(data)) => String::from_utf8_lossy(data).to_uppercase(),
            _ => return None,
        };

        let args: Vec<Vec<u8>> = items[1..]
            .iter()
            .filter_map(|item| match item {
                RespValue::BulkString(Some(data)) => Some(data.clone()),
                _ => None,
            })
            .collect();

        Some(Self { name, args })
    }

    /// Get argument at index as bytes. Returns None if out of bounds.
    pub fn arg(&self, index: usize) -> Option<&[u8]> {
        self.args.get(index).map(|v| v.as_slice())
    }

    /// Get argument at index as UTF-8 string. Returns None if out of bounds or invalid UTF-8.
    pub fn arg_str(&self, index: usize) -> Option<&str> {
        self.args
            .get(index)
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Get argument at index parsed as i64.
    pub fn arg_i64(&self, index: usize) -> Option<i64> {
        self.arg_str(index).and_then(|s| s.parse().ok())
    }

    /// Number of arguments (excluding the command name).
    pub fn argc(&self) -> usize {
        self.args.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_get_command() {
        let value = RespValue::array(vec![
            RespValue::bulk_str("get"),
            RespValue::bulk_str("mykey"),
        ]);
        let cmd = RespCommand::parse(&value).unwrap();
        assert_eq!(cmd.name, "GET");
        assert_eq!(cmd.arg(0), Some(b"mykey".as_slice()));
        assert_eq!(cmd.argc(), 1);
    }

    #[test]
    fn parse_set_with_ex() {
        let value = RespValue::array(vec![
            RespValue::bulk_str("SET"),
            RespValue::bulk_str("key"),
            RespValue::bulk_str("value"),
            RespValue::bulk_str("EX"),
            RespValue::bulk_str("60"),
        ]);
        let cmd = RespCommand::parse(&value).unwrap();
        assert_eq!(cmd.name, "SET");
        assert_eq!(cmd.argc(), 4);
        assert_eq!(cmd.arg_str(2), Some("EX"));
        assert_eq!(cmd.arg_i64(3), Some(60));
    }

    #[test]
    fn parse_empty_array_returns_none() {
        let value = RespValue::array(vec![]);
        assert!(RespCommand::parse(&value).is_none());
    }

    #[test]
    fn parse_non_array_returns_none() {
        let value = RespValue::ok();
        assert!(RespCommand::parse(&value).is_none());
    }
}
