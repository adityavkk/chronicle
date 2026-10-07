//! Optional cumulative wall-time probes, enabled by the existing stats_secs.
//! These nested/overlapping phases are NOT additive CPU or fsync syscall time.
//! A phase counts completed attempts, including errors. Buckets have inclusive
//! upper bounds 1,2,...,524288 microseconds; the last bucket is overflow.
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Default, serde::Serialize)]
struct Stats {
    count: u64,
    total_ns: u64,
    max_ns: u64,
    bytes: u64,
    buckets: [u64; 21],
}

impl Stats {
    fn record(&mut self, ns: u64, bytes: u64) {
        self.count += 1;
        self.total_ns += ns;
        self.max_ns = self.max_ns.max(ns);
        self.bytes += bytes;
        let bucket = (ns.saturating_sub(1) / 1000)
            .checked_ilog2()
            .map_or(0, |p| p + 1);
        self.buckets[(bucket as usize).min(20)] += 1;
    }
}

pub(super) struct Timer(Mutex<Stats>);
impl Timer {
    const fn new() -> Self {
        Self(Mutex::new(Stats {
            count: 0,
            total_ns: 0,
            max_ns: 0,
            bytes: 0,
            buckets: [0; 21],
        }))
    }
    pub(super) fn start(&'static self) -> Option<Probe> {
        crate::srvstats::enabled().then(|| Probe {
            timer: self,
            start: Instant::now(),
            bytes: 0,
        })
    }
}
pub(super) struct Probe {
    timer: &'static Timer,
    start: Instant,
    pub bytes: u64,
}
impl Drop for Probe {
    fn drop(&mut self) {
        let ns = self.start.elapsed().as_nanos() as u64;
        self.timer.0.lock().unwrap().record(ns, self.bytes);
    }
}

pub(super) static ENTRY_STAGE: Timer = Timer::new();
pub(super) static MARKER_STAGE: Timer = Timer::new();
pub(super) static OTHER_STAGE: Timer = Timer::new();
pub(super) static ENTRY_WAIT: Timer = Timer::new();
pub(super) static MARKER_WAIT: Timer = Timer::new();
pub(super) static OTHER_WAIT: Timer = Timer::new();
pub(super) static READ: Timer = Timer::new();
pub(super) static APPLY_LOCK: Timer = Timer::new();
pub(super) static APPLY: Timer = Timer::new();
pub(super) static SNAPSHOT: Timer = Timer::new();
pub(super) static RESOLVE: Timer = Timer::new();

pub(super) fn spawn(secs: u64) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tick.tick().await;
            for (phase, timer) in [
                ("entry_stage", &ENTRY_STAGE),
                ("marker_stage", &MARKER_STAGE),
                ("other_stage", &OTHER_STAGE),
                ("entry_wait", &ENTRY_WAIT),
                ("marker_wait", &MARKER_WAIT),
                ("other_wait", &OTHER_WAIT),
                ("read", &READ),
                ("apply_lock", &APPLY_LOCK),
                ("apply", &APPLY),
                ("snapshot", &SNAPSHOT),
                ("resolve", &RESOLVE),
            ] {
                let stats = timer.0.lock().unwrap().clone();
                eprintln!(
                    "RAFT_TIMING {}",
                    serde_json::json!({
                        "unix_ms": super::clock::millis(std::time::SystemTime::now()),
                        "phase": phase, "cumulative": stats,
                    })
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inclusive_bucket_boundaries_and_overflow_keep_counts() {
        let mut stats = Stats::default();
        for ns in [0, 1, 1000, 1001, 2000, 2001, 524_288_000, 524_288_001] {
            stats.record(ns, 7);
        }
        assert_eq!(&stats.buckets[..3], &[3, 2, 1]);
        assert_eq!(&stats.buckets[19..], &[1, 1]);
        assert_eq!(stats.buckets.iter().sum::<u64>(), 8);
        assert_eq!(stats.count, 8);
        assert_eq!(stats.bytes, 56);
        assert_eq!(stats.max_ns, 524_288_001);
        assert_eq!(stats.total_ns, 1_048_582_004);
    }
}
