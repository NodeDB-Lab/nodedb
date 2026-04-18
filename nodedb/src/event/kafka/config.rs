//! Kafka delivery configuration parsed from `CREATE CHANGE STREAM ... WITH (DELIVERY = 'kafka', ...)`.

/// Configuration for Kafka bridge delivery on a change stream.
#[derive(Debug, Clone, Default, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct KafkaDeliveryConfig {
    /// Whether Kafka delivery is enabled for this stream.
    pub enabled: bool,
    /// Kafka broker addresses (comma-separated). e.g., "kafka1:9092,kafka2:9092".
    pub brokers: String,
    /// Target Kafka topic name.
    pub topic: String,
    /// Serialization format for events published to Kafka.
    pub format: KafkaFormat,
    /// Maximum pending Kafka publishes before backpressure.
    pub max_pending: usize,
    /// Enable Kafka transactional producer (exactly-once semantics).
    /// Sets `enable.idempotence = true` and assigns a `transactional.id`.
    pub transactional: bool,
}

/// Serialization format for Kafka-published events.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum KafkaFormat {
    #[default]
    Json = 0,
    Avro = 1,
}

impl KafkaFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Avro => "avro",
        }
    }

    pub fn from_str_opt(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "json" => Some(Self::Json),
            "avro" => Some(Self::Avro),
            _ => None,
        }
    }
}

/// Default max pending Kafka publishes per stream.
pub const DEFAULT_KAFKA_MAX_PENDING: usize = 50_000;

impl KafkaDeliveryConfig {
    /// Parse Kafka config from WITH clause key-value pairs.
    ///
    /// Expected keys: `DELIVERY='kafka'`, `BROKERS='...'`, `TOPIC='...'`,
    /// `FORMAT='json'|'avro'`, `TRANSACTIONAL='true'|'false'`.
    pub fn from_with_params(params: &[(String, String)]) -> Option<Self> {
        let delivery = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("DELIVERY"))?;
        if !delivery.1.eq_ignore_ascii_case("kafka") {
            return None;
        }

        let brokers = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("BROKERS"))
            .map(|(_, v)| v.clone())
            .unwrap_or_default();

        let topic = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("TOPIC"))
            .map(|(_, v)| v.clone())
            .unwrap_or_default();

        let format = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("FORMAT"))
            .and_then(|(_, v)| KafkaFormat::from_str_opt(v))
            .unwrap_or_default();

        let transactional = params
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("TRANSACTIONAL"))
            .is_some_and(|(_, v)| v.eq_ignore_ascii_case("true"));

        if brokers.is_empty() || topic.is_empty() {
            return None;
        }

        Some(Self {
            enabled: true,
            brokers,
            topic,
            format,
            max_pending: DEFAULT_KAFKA_MAX_PENDING,
            transactional,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kafka_config() {
        let params = vec![
            ("DELIVERY".into(), "kafka".into()),
            ("BROKERS".into(), "kafka1:9092,kafka2:9092".into()),
            ("TOPIC".into(), "orders_cdc".into()),
            ("FORMAT".into(), "avro".into()),
            ("TRANSACTIONAL".into(), "true".into()),
        ];
        let config = KafkaDeliveryConfig::from_with_params(&params).unwrap();
        assert!(config.enabled);
        assert_eq!(config.brokers, "kafka1:9092,kafka2:9092");
        assert_eq!(config.topic, "orders_cdc");
        assert_eq!(config.format, KafkaFormat::Avro);
        assert!(config.transactional);
    }

    #[test]
    fn returns_none_for_non_kafka_delivery() {
        let params = vec![
            ("DELIVERY".into(), "webhook".into()),
            ("URL".into(), "https://example.com".into()),
        ];
        assert!(KafkaDeliveryConfig::from_with_params(&params).is_none());
    }

    #[test]
    fn returns_none_for_missing_brokers() {
        let params = vec![
            ("DELIVERY".into(), "kafka".into()),
            ("TOPIC".into(), "t".into()),
        ];
        assert!(KafkaDeliveryConfig::from_with_params(&params).is_none());
    }

    #[test]
    fn default_format_is_json() {
        let params = vec![
            ("DELIVERY".into(), "kafka".into()),
            ("BROKERS".into(), "localhost:9092".into()),
            ("TOPIC".into(), "t".into()),
        ];
        let config = KafkaDeliveryConfig::from_with_params(&params).unwrap();
        assert_eq!(config.format, KafkaFormat::Json);
        assert!(!config.transactional);
    }
}
