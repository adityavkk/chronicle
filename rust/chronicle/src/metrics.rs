//! Fixed, label-free latency histograms. Counters are diagnostic, never read authority.
use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

const BOUNDS_US: [u64; 9] = [
    1_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    500_000,
    2_000_000,
    u64::MAX,
];

pub struct Histogram {
    buckets: [AtomicU64; 9],
    micros: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

impl Histogram {
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; 9],
            micros: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, duration: Duration) {
        let micros = duration.as_micros().min(u64::MAX as u128) as u64;
        self.micros.fetch_add(micros, Ordering::Relaxed);
        for (bound, bucket) in BOUNDS_US.iter().zip(&self.buckets) {
            if micros <= *bound {
                bucket.fetch_add(1, Ordering::Relaxed);
                break;
            }
        }
    }

    pub fn count(&self) -> u64 {
        self.buckets.iter().map(|b| b.load(Ordering::Relaxed)).sum()
    }

    pub fn render(&self, name: &str, text: &mut String) {
        // Formatting into a String is infallible. Names are fixed by our callers.
        let _ = writeln!(text, "# TYPE {name} histogram");
        let _ = writeln!(
            text,
            "{name}_sum {}",
            self.micros.load(Ordering::Relaxed) as f64 / 1e6
        );
        // Load each non-cumulative bin once; concurrent observations cannot make
        // the emitted buckets decrease or make +Inf disagree with count.
        let mut cumulative = 0;
        for (bound, bucket) in BOUNDS_US.iter().zip(&self.buckets) {
            cumulative += bucket.load(Ordering::Relaxed);
            let bound = if *bound == u64::MAX {
                "+Inf".into()
            } else {
                (*bound as f64 / 1e6).to_string()
            };
            let _ = writeln!(text, "{name}_bucket{{le=\"{bound}\"}} {}", cumulative);
        }
        let _ = writeln!(text, "{name}_count {cumulative}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inclusive_buckets_and_sum_have_independent_expectations() {
        let histogram = Histogram::new();
        for us in [999, 1000, 1001, 3_000_000] {
            histogram.observe(Duration::from_micros(us));
        }
        let mut text = String::new();
        histogram.render("test_seconds", &mut text);
        assert!(text.contains("test_seconds_bucket{le=\"0.001\"} 2\n"));
        assert!(text.contains("test_seconds_bucket{le=\"0.005\"} 3\n"));
        assert!(text.contains("test_seconds_bucket{le=\"+Inf\"} 4\n"));
        assert!(text.contains("test_seconds_sum 3.003\n"));
        assert_eq!(histogram.count(), 4);
    }

    #[test]
    fn concurrent_scrapes_keep_cumulative_buckets_consistent() {
        let histogram = Histogram::new();
        std::thread::scope(|scope| {
            for us in [999, 1001, 3_000_000] {
                let h = &histogram;
                scope.spawn(move || {
                    for _ in 0..100_000 {
                        h.observe(Duration::from_micros(us));
                    }
                });
            }
            for _ in 0..1000 {
                let mut text = String::new();
                histogram.render("test", &mut text);
                let counts: Vec<u64> = text
                    .lines()
                    .filter(|l| l.starts_with("test_bucket") || l.starts_with("test_count"))
                    .map(|l| l.rsplit_once(' ').unwrap().1.parse().unwrap())
                    .collect();
                assert!(counts.windows(2).all(|pair| pair[0] <= pair[1]));
                assert_eq!(counts[8], counts[9]);
            }
        });
        assert_eq!(histogram.count(), 300_000);
    }
}
