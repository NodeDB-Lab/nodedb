//! Webhook delivery configuration types.

/// Webhook delivery configuration for a change stream.
///
/// When set on a stream, the Event Plane spawns a background task that
/// POSTs each event to the configured URL with retry and DLQ.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct WebhookConfig {
    /// Target URL to POST events to.
    pub url: String,
    /// Maximum retry attempts before DLQ. Default: 3.
    pub max_retries: u32,
    /// Per-request timeout in seconds. Default: 5.
    pub timeout_secs: u64,
    /// Optional custom headers (key → value).
    #[msgpack(default)]
    pub headers: Vec<(String, String)>,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_retries: 3,
            timeout_secs: 5,
            headers: Vec::new(),
        }
    }
}

impl WebhookConfig {
    pub fn is_configured(&self) -> bool {
        !self.url.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_not_configured() {
        let cfg = WebhookConfig::default();
        assert!(!cfg.is_configured());
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.timeout_secs, 5);
        assert!(cfg.headers.is_empty());
    }

    #[test]
    fn with_url_is_configured() {
        let cfg = WebhookConfig {
            url: "https://example.com/hook".into(),
            ..Default::default()
        };
        assert!(cfg.is_configured());
    }

    #[test]
    fn msgpack_roundtrip() {
        let cfg = WebhookConfig {
            url: "https://example.com".into(),
            max_retries: 5,
            timeout_secs: 10,
            headers: vec![("Authorization".into(), "Bearer tok".into())],
        };
        let bytes = zerompk::to_msgpack_vec(&cfg).unwrap();
        let decoded: WebhookConfig = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.url, cfg.url);
        assert_eq!(decoded.max_retries, 5);
        assert_eq!(decoded.headers.len(), 1);
    }

    #[test]
    fn deserialize_missing_headers_defaults_to_empty() {
        #[derive(zerompk::ToMessagePack)]
        #[msgpack(map)]
        struct PartialConfig {
            url: String,
            max_retries: u32,
            timeout_secs: u64,
        }
        let partial = PartialConfig {
            url: "http://x".into(),
            max_retries: 1,
            timeout_secs: 2,
        };
        let bytes = zerompk::to_msgpack_vec(&partial).unwrap();
        let cfg: WebhookConfig = zerompk::from_msgpack(&bytes).unwrap();
        assert!(cfg.headers.is_empty());
    }
}
