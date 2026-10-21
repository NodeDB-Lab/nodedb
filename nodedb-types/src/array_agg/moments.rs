// SPDX-License-Identifier: Apache-2.0

//! Running count, mean and sum of squared deviations (Welford), merged by
//! Chan's parallel formula. Variance and standard deviation finish from it.

/// The moments of a set of values.
#[derive(
    Debug, Clone, Copy, PartialEq, Default, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct Moments {
    pub count: u64,
    pub mean: f64,
    /// Sum of squared deviations from the mean.
    pub m2: f64,
}

impl Moments {
    /// The moments of one value.
    pub fn one(v: f64) -> Self {
        Self {
            count: 1,
            mean: v,
            m2: 0.0,
        }
    }

    /// Add one value (Welford's update).
    pub fn push(&mut self, v: f64) {
        self.count += 1;
        let delta = v - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (v - self.mean);
    }

    /// The moments of both sets.
    pub fn merge(self, other: Moments) -> Moments {
        if other.count == 0 {
            return self;
        }
        if self.count == 0 {
            return other;
        }
        let count = self.count + other.count;
        let (na, nb, n) = (self.count as f64, other.count as f64, count as f64);
        let delta = other.mean - self.mean;
        Moments {
            count,
            mean: self.mean + delta * nb / n,
            m2: self.m2 + other.m2 + delta * delta * na * nb / n,
        }
    }

    /// The population variance `m2 / n`, or `None` with no value.
    pub fn var_pop(&self) -> Option<f64> {
        (self.count > 0).then(|| self.m2 / self.count as f64)
    }

    /// The sample variance `m2 / (n - 1)`, or `None` with fewer than two
    /// values, as PostgreSQL's `var_samp`.
    pub fn var_samp(&self) -> Option<f64> {
        (self.count > 1).then(|| self.m2 / (self.count - 1) as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(values: &[f64]) -> Moments {
        values
            .iter()
            .fold(Moments::default(), |m, v| m.merge(Moments::one(*v)))
    }

    #[test]
    fn variance_matches_the_definition() {
        let m = of(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert!((m.var_pop().unwrap() - 4.0).abs() < 1e-12);
        assert!((m.var_samp().unwrap() - 32.0 / 7.0).abs() < 1e-12);
    }

    #[test]
    fn split_merge_equals_one_pass() {
        let all = [1.5, -3.0, 8.25, 0.0, 2.0];
        let (a, b) = all.split_at(2);
        let merged = of(a).merge(of(b));
        let one = of(&all);
        assert_eq!(merged.count, one.count);
        assert!((merged.mean - one.mean).abs() < 1e-12);
        assert!((merged.m2 - one.m2).abs() < 1e-9);
    }

    #[test]
    fn push_equals_merge_of_ones() {
        let all = [1.5, -3.0, 8.25, 0.0, 2.0];
        let mut pushed = Moments::default();
        for v in all {
            pushed.push(v);
        }
        let merged = of(&all);
        assert_eq!(pushed.count, merged.count);
        assert!((pushed.mean - merged.mean).abs() < 1e-12);
        assert!((pushed.m2 - merged.m2).abs() < 1e-9);
    }

    #[test]
    fn too_few_values_have_no_variance() {
        assert_eq!(Moments::default().var_pop(), None);
        assert_eq!(Moments::one(3.0).var_samp(), None);
        assert_eq!(Moments::one(3.0).var_pop(), Some(0.0));
    }
}
