//! A partition's committed logical wall clock. MAX denotes native local mode.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Clock(AtomicU64);

impl Default for Clock {
    fn default() -> Self {
        Self(AtomicU64::new(u64::MAX))
    }
}

impl Clock {
    pub fn replicated(&self) -> bool {
        self.0.load(Ordering::Relaxed) != u64::MAX
    }

    /// Called under the owning machine's apply lock, or before serving.
    pub fn advance(&self, sample: u64) {
        let previous = self.0.load(Ordering::Relaxed);
        self.0.store(
            if previous == u64::MAX {
                sample
            } else {
                previous.max(sample)
            },
            Ordering::Relaxed,
        );
    }

    pub fn now(&self) -> SystemTime {
        match self.0.load(Ordering::Relaxed) {
            u64::MAX => SystemTime::now(),
            ms => UNIX_EPOCH + Duration::from_millis(ms),
        }
    }
}

pub fn millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn decreasing_clock_samples_cannot_reverse_expiry(samples in prop::collection::vec(0u64..100000, 1..100)) {
            let clock = Clock::default();
            clock.advance(0);
            for (i, time) in samples.iter().enumerate() {
                clock.advance(*time);
                prop_assert_eq!(millis(clock.now()), *samples[..=i].iter().max().unwrap());
            }
        }
    }
}
