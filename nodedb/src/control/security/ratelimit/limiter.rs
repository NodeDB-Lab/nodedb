//! Hierarchical rate limiter: per-user → per-org → per-tenant.
//!
//! Each identity gets a token bucket. Requests consume tokens based on
//! endpoint cost multipliers. When empty, requests are rejected with 429.
//!
//! Hierarchy: per-key → per-user → per-org → per-tenant.
//! A request is allowed only if ALL applicable buckets have tokens.

use std::collections::HashMap;
use std::sync::RwLock;

use tracing::debug;

use super::bucket::TokenBucket;
use super::config::RateLimitConfig;

/// Rate limit check result.
pub struct RateLimitResult {
    /// Whether the request is allowed.
    pub allowed: bool,
    /// Remaining tokens in the most constrained bucket.
    pub remaining: u64,
    /// Total limit of the most constrained bucket.
    pub limit: u64,
    /// Seconds until reset (0 if allowed).
    pub retry_after_secs: u64,
}

/// Hierarchical rate limiter.
pub struct RateLimiter {
    config: RateLimitConfig,
    /// Per-identity buckets. Key = identity key (user_id, api_key_id, org_id).
    buckets: RwLock<HashMap<String, TokenBucket>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: RwLock::new(HashMap::new()),
        }
    }

    /// Check rate limit for a request.
    ///
    /// `user_id` = authenticated user.
    /// `org_ids` = user's org memberships (for org-level rate limiting).
    /// `plan_tier` = tier name from `$auth.metadata.plan` (e.g., "free", "pro").
    /// `operation` = endpoint name for cost multiplier lookup.
    pub fn check(
        &self,
        user_id: &str,
        org_ids: &[String],
        plan_tier: Option<&str>,
        operation: &str,
    ) -> RateLimitResult {
        if !self.config.enabled {
            return RateLimitResult {
                allowed: true,
                remaining: u64::MAX,
                limit: u64::MAX,
                retry_after_secs: 0,
            };
        }

        let cost = self.config.operation_cost(operation);

        // Resolve the tier (from JWT plan claim or default).
        let (qps, burst) = self.resolve_tier(plan_tier);

        // Check user-level bucket.
        let user_key = format!("user:{user_id}");
        let user_result = self.check_bucket(&user_key, qps, burst, cost);

        if !user_result.allowed {
            debug!(
                user_id = %user_id,
                operation = %operation,
                cost,
                "rate limited (user bucket)"
            );
            return user_result;
        }

        // Check org-level bucket (shared across members).
        for org_id in org_ids {
            let org_key = format!("org:{org_id}");
            // Org gets 10x the user rate (shared budget).
            let org_result = self.check_bucket(&org_key, qps * 10, burst * 10, cost);
            if !org_result.allowed {
                debug!(
                    user_id = %user_id,
                    org_id = %org_id,
                    operation = %operation,
                    "rate limited (org bucket)"
                );
                return org_result;
            }
        }

        user_result
    }

    /// Check a single bucket, creating it if it doesn't exist.
    fn check_bucket(&self, key: &str, qps: u64, burst: u64, cost: u64) -> RateLimitResult {
        // Fast path: read-only check.
        {
            let buckets = self.buckets.read().unwrap_or_else(|p| p.into_inner());
            if let Some(bucket) = buckets.get(key) {
                let allowed = bucket.try_acquire(cost);
                return RateLimitResult {
                    allowed,
                    remaining: bucket.available(),
                    limit: bucket.capacity(),
                    retry_after_secs: if allowed {
                        0
                    } else {
                        (bucket.retry_after_ms() / 1000).max(1)
                    },
                };
            }
        }

        // Slow path: create bucket.
        let mut buckets = self.buckets.write().unwrap_or_else(|p| p.into_inner());
        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket::new(burst, qps as f64));

        let allowed = bucket.try_acquire(cost);
        RateLimitResult {
            allowed,
            remaining: bucket.available(),
            limit: bucket.capacity(),
            retry_after_secs: if allowed {
                0
            } else {
                (bucket.retry_after_ms() / 1000).max(1)
            },
        }
    }

    /// Resolve rate limit tier from plan name.
    fn resolve_tier(&self, plan_tier: Option<&str>) -> (u64, u64) {
        if let Some(tier_name) = plan_tier
            && let Some(tier) = self.config.tier(tier_name)
        {
            return (tier.qps, tier.burst);
        }
        (self.config.default_qps, self.config.default_burst)
    }

    /// Build HTTP response headers for rate limit info.
    pub fn response_headers(result: &RateLimitResult) -> Vec<(String, String)> {
        vec![
            ("X-RateLimit-Limit".into(), result.limit.to_string()),
            ("X-RateLimit-Remaining".into(), result.remaining.to_string()),
            (
                "X-RateLimit-Reset".into(),
                result.retry_after_secs.to_string(),
            ),
        ]
    }

    /// Build Retry-After header value (seconds).
    pub fn retry_after_header(result: &RateLimitResult) -> Option<(String, String)> {
        if result.allowed {
            None
        } else {
            Some(("Retry-After".into(), result.retry_after_secs.to_string()))
        }
    }

    /// Whether rate limiting is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Number of active buckets (for metrics).
    pub fn active_buckets(&self) -> usize {
        self.buckets.read().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Get the config for inspection.
    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> RateLimitConfig {
        use crate::control::security::ratelimit::config::RateLimitTier;
        let mut config = RateLimitConfig {
            enabled: true,
            default_qps: 10,
            default_burst: 20,
            ..Default::default()
        };
        config.tiers.insert(
            "pro".into(),
            RateLimitTier {
                qps: 5000,
                burst: 10000,
            },
        );
        config
    }

    #[test]
    fn disabled_allows_all() {
        let limiter = RateLimiter::new(RateLimitConfig::default());
        let result = limiter.check("u1", &[], None, "point_get");
        assert!(result.allowed);
    }

    #[test]
    fn basic_rate_limiting() {
        let limiter = RateLimiter::new(enabled_config());

        // Burst of 20, cost 1 each.
        for _ in 0..20 {
            let r = limiter.check("u1", &[], None, "point_get");
            assert!(r.allowed);
        }
        // 21st request should be rejected.
        let r = limiter.check("u1", &[], None, "point_get");
        assert!(!r.allowed);
        assert!(r.retry_after_secs > 0);
    }

    #[test]
    fn cost_multiplier_drains_faster() {
        let limiter = RateLimiter::new(enabled_config());

        // vector_search costs 20 tokens. Burst is 20. First request OK.
        let r = limiter.check("u1", &[], None, "vector_search");
        assert!(r.allowed);
        // Second should fail (20 tokens consumed, 0 remaining).
        let r = limiter.check("u1", &[], None, "vector_search");
        assert!(!r.allowed);
    }

    #[test]
    fn tier_resolution() {
        let limiter = RateLimiter::new(enabled_config());

        // Pro tier: 5000 QPS, 10000 burst.
        for _ in 0..100 {
            let r = limiter.check("u1", &[], Some("pro"), "point_get");
            assert!(r.allowed);
        }
    }

    #[test]
    fn per_user_isolation() {
        let limiter = RateLimiter::new(enabled_config());

        // Exhaust u1's bucket.
        for _ in 0..20 {
            limiter.check("u1", &[], None, "point_get");
        }
        let r = limiter.check("u1", &[], None, "point_get");
        assert!(!r.allowed);

        // u2 should still have tokens.
        let r = limiter.check("u2", &[], None, "point_get");
        assert!(r.allowed);
    }

    #[test]
    fn response_headers() {
        let result = RateLimitResult {
            allowed: true,
            remaining: 50,
            limit: 100,
            retry_after_secs: 0,
        };
        let headers = RateLimiter::response_headers(&result);
        assert_eq!(headers.len(), 3);
        assert_eq!(headers[0].0, "X-RateLimit-Limit");
        assert_eq!(headers[0].1, "100");
    }
}
