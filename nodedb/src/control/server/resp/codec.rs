//! RESP2 protocol codec: frame parser and response serializer.
//!
//! Implements the Redis Serialization Protocol (RESP2) as specified at
//! <https://redis.io/docs/reference/protocol-spec/>.
//!
//! Supports:
//! - Inline commands (plain text, newline-terminated)
//! - Bulk string arrays (the standard client command format)
//! - All five RESP2 response types

use std::io;

/// A parsed RESP value (command or nested element).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespValue {
    /// `+OK\r\n`
    SimpleString(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n` or `$-1\r\n` (nil)
    BulkString(Option<Vec<u8>>),
    /// `*N\r\n...` (array of values)
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Create a simple string response.
    pub fn ok() -> Self {
        Self::SimpleString("OK".into())
    }

    /// Create a nil bulk string.
    pub fn nil() -> Self {
        Self::BulkString(None)
    }

    /// Create a nil array.
    pub fn nil_array() -> Self {
        Self::Array(None)
    }

    /// Create an error response.
    pub fn err(msg: impl Into<String>) -> Self {
        Self::Error(msg.into())
    }

    /// Create an integer response.
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Create a bulk string from bytes.
    pub fn bulk(data: impl Into<Vec<u8>>) -> Self {
        Self::BulkString(Some(data.into()))
    }

    /// Create a bulk string from a str.
    pub fn bulk_str(s: &str) -> Self {
        Self::BulkString(Some(s.as_bytes().to_vec()))
    }

    /// Create an array of values.
    pub fn array(items: Vec<RespValue>) -> Self {
        Self::Array(Some(items))
    }

    /// Serialize this value to RESP2 wire format.
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Self::SimpleString(s) => {
                buf.push(b'+');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Error(s) => {
                buf.push(b'-');
                buf.extend_from_slice(s.as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => {
                buf.push(b':');
                buf.extend_from_slice(n.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
            Self::BulkString(None) => {
                buf.extend_from_slice(b"$-1\r\n");
            }
            Self::BulkString(Some(data)) => {
                buf.push(b'$');
                buf.extend_from_slice(data.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
            }
            Self::Array(None) => {
                buf.extend_from_slice(b"*-1\r\n");
            }
            Self::Array(Some(items)) => {
                buf.push(b'*');
                buf.extend_from_slice(items.len().to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
                for item in items {
                    item.serialize(buf);
                }
            }
        }
    }

    /// Serialize to a new Vec.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        self.serialize(&mut buf);
        buf
    }
}

/// RESP2 frame parser.
///
/// Incrementally parses RESP frames from a byte buffer. Returns `Ok(Some(value))`
/// when a complete frame is available, `Ok(None)` when more data is needed.
pub struct RespParser {
    buf: Vec<u8>,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
        }
    }

    /// Feed bytes from the network into the parser buffer.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Try to parse a complete RESP frame from the buffer.
    ///
    /// Returns `Ok(Some(value))` if a complete frame was parsed (consumed from buffer).
    /// Returns `Ok(None)` if more data is needed.
    /// Returns `Err` on protocol errors.
    pub fn try_parse(&mut self) -> io::Result<Option<RespValue>> {
        if self.buf.is_empty() {
            return Ok(None);
        }

        match self.buf[0] {
            b'+' | b'-' | b':' | b'$' | b'*' => {
                let (value, consumed) = match parse_value(&self.buf) {
                    Ok(Some((v, n))) => (v, n),
                    Ok(None) => return Ok(None),
                    Err(e) => return Err(e),
                };
                self.buf.drain(..consumed);
                Ok(Some(value))
            }
            // Inline command: plain text terminated by \r\n or \n.
            _ => {
                if let Some(pos) = find_line_end(&self.buf) {
                    let line = &self.buf[..pos];
                    let parts = parse_inline_command(line);
                    let consumed = if self.buf.get(pos + 1) == Some(&b'\n') {
                        pos + 2
                    } else {
                        pos + 1
                    };
                    self.buf.drain(..consumed);
                    Ok(Some(parts))
                } else {
                    Ok(None) // Need more data.
                }
            }
        }
    }

    /// Number of bytes in the parser buffer (for backpressure).
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Internal parsing
// ---------------------------------------------------------------------------

/// Find the position of `\r\n` or `\n` in the buffer.
fn find_line_end(buf: &[u8]) -> Option<usize> {
    let i = buf.iter().position(|&b| b == b'\n')?;
    Some(if i > 0 && buf[i - 1] == b'\r' {
        i - 1
    } else {
        i
    })
}

/// Find `\r\n` and return the position of `\r`. Returns None if not found.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Parse a RESP value starting at `buf[0]`. Returns `(value, bytes_consumed)`.
fn parse_value(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[0] {
        b'+' => parse_simple_string(&buf[1..])
            .map(|opt| opt.map(|(s, n)| (RespValue::SimpleString(s), n + 1))),
        b'-' => {
            parse_simple_string(&buf[1..]).map(|opt| opt.map(|(s, n)| (RespValue::Error(s), n + 1)))
        }
        b':' => {
            parse_integer(&buf[1..]).map(|opt| opt.map(|(i, n)| (RespValue::Integer(i), n + 1)))
        }
        b'$' => parse_bulk_string(&buf[1..]).map(|opt| opt.map(|(v, n)| (v, n + 1))),
        b'*' => parse_array(&buf[1..]).map(|opt| opt.map(|(v, n)| (v, n + 1))),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected RESP type byte: 0x{other:02x}"),
        )),
    }
}

fn parse_simple_string(buf: &[u8]) -> io::Result<Option<(String, usize)>> {
    match find_crlf(buf) {
        Some(pos) => {
            let s = String::from_utf8_lossy(&buf[..pos]).into_owned();
            Ok(Some((s, pos + 2)))
        }
        None => Ok(None),
    }
}

fn parse_integer(buf: &[u8]) -> io::Result<Option<(i64, usize)>> {
    match find_crlf(buf) {
        Some(pos) => {
            let s = std::str::from_utf8(&buf[..pos])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let n: i64 = s
                .parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(Some((n, pos + 2)))
        }
        None => Ok(None),
    }
}

fn parse_bulk_string(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    let crlf_pos = match find_crlf(buf) {
        Some(p) => p,
        None => return Ok(None),
    };

    let len_str = std::str::from_utf8(&buf[..crlf_pos])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len: i64 = len_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if len < 0 {
        return Ok(Some((RespValue::nil(), crlf_pos + 2)));
    }

    let len = len as usize;
    let data_start = crlf_pos + 2;
    let data_end = data_start + len;
    let frame_end = data_end + 2; // trailing \r\n

    if buf.len() < frame_end {
        return Ok(None); // Need more data.
    }

    let data = buf[data_start..data_end].to_vec();
    Ok(Some((RespValue::BulkString(Some(data)), frame_end)))
}

fn parse_array(buf: &[u8]) -> io::Result<Option<(RespValue, usize)>> {
    let crlf_pos = match find_crlf(buf) {
        Some(p) => p,
        None => return Ok(None),
    };

    let count_str = std::str::from_utf8(&buf[..crlf_pos])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let count: i64 = count_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    if count < 0 {
        return Ok(Some((RespValue::nil_array(), crlf_pos + 2)));
    }

    let count = count as usize;
    let mut offset = crlf_pos + 2;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        match parse_value(&buf[offset..])? {
            Some((value, consumed)) => {
                items.push(value);
                offset += consumed;
            }
            None => return Ok(None), // Need more data.
        }
    }

    Ok(Some((RespValue::Array(Some(items)), offset)))
}

/// Parse an inline command (plain text) into a RESP array of bulk strings.
fn parse_inline_command(line: &[u8]) -> RespValue {
    let text = String::from_utf8_lossy(line);
    let parts: Vec<RespValue> = text
        .split_whitespace()
        .map(|s| RespValue::bulk(s.as_bytes().to_vec()))
        .collect();
    RespValue::Array(Some(parts))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_simple_string() {
        assert_eq!(RespValue::ok().to_bytes(), b"+OK\r\n");
    }

    #[test]
    fn serialize_error() {
        assert_eq!(
            RespValue::err("ERR unknown command").to_bytes(),
            b"-ERR unknown command\r\n"
        );
    }

    #[test]
    fn serialize_integer() {
        assert_eq!(RespValue::integer(42).to_bytes(), b":42\r\n");
        assert_eq!(RespValue::integer(-1).to_bytes(), b":-1\r\n");
    }

    #[test]
    fn serialize_bulk_string() {
        assert_eq!(RespValue::bulk_str("hello").to_bytes(), b"$5\r\nhello\r\n");
        assert_eq!(RespValue::nil().to_bytes(), b"$-1\r\n");
    }

    #[test]
    fn serialize_array() {
        let arr = RespValue::array(vec![
            RespValue::bulk_str("SET"),
            RespValue::bulk_str("key"),
            RespValue::bulk_str("value"),
        ]);
        assert_eq!(
            arr.to_bytes(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn parse_bulk_string_array() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");

        let result = parser.try_parse().unwrap().unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], RespValue::bulk(b"GET".to_vec()));
                assert_eq!(items[1], RespValue::bulk(b"mykey".to_vec()));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn parse_inline_command() {
        let mut parser = RespParser::new();
        parser.feed(b"PING\r\n");

        let result = parser.try_parse().unwrap().unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], RespValue::bulk(b"PING".to_vec()));
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn parse_set_command() {
        let mut parser = RespParser::new();
        parser.feed(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");

        let result = parser.try_parse().unwrap().unwrap();
        match result {
            RespValue::Array(Some(items)) => {
                assert_eq!(items.len(), 3);
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn parse_incomplete_returns_none() {
        let mut parser = RespParser::new();
        parser.feed(b"*2\r\n$3\r\nGET\r\n$5\r\nmy"); // Incomplete.

        assert!(parser.try_parse().unwrap().is_none());

        // Feed the rest.
        parser.feed(b"key\r\n");
        assert!(parser.try_parse().unwrap().is_some());
    }

    #[test]
    fn parse_nil_bulk_string() {
        let mut parser = RespParser::new();
        parser.feed(b"$-1\r\n");

        let result = parser.try_parse().unwrap().unwrap();
        assert_eq!(result, RespValue::nil());
    }

    #[test]
    fn parse_multiple_commands() {
        let mut parser = RespParser::new();
        parser.feed(b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n");

        let first = parser.try_parse().unwrap().unwrap();
        assert!(matches!(first, RespValue::Array(Some(ref v)) if v.len() == 1));

        let second = parser.try_parse().unwrap().unwrap();
        assert!(matches!(second, RespValue::Array(Some(ref v)) if v.len() == 2));
    }

    #[test]
    fn roundtrip() {
        let values = vec![
            RespValue::ok(),
            RespValue::err("ERR test"),
            RespValue::integer(99),
            RespValue::bulk_str("hello"),
            RespValue::nil(),
            RespValue::array(vec![RespValue::bulk_str("a"), RespValue::integer(1)]),
        ];

        for original in &values {
            let bytes = original.to_bytes();
            let mut parser = RespParser::new();
            parser.feed(&bytes);
            let parsed = parser.try_parse().unwrap().unwrap();
            assert_eq!(&parsed, original, "roundtrip failed for {original:?}");
        }
    }
}
