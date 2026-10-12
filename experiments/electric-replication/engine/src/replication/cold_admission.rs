//! Node-local pressure policy, not replicated state or a physical reservation.
//! ColdWork.tla models the sample/credit handoff. See CONTRACT.md for limits.
use super::*;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct Policy {
    pub max_backlog_bytes: u64,
    pub min_free_bytes: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self { max_backlog_bytes: 512 * 1024 * 1024, min_free_bytes: 2 * 1024 * 1024 * 1024 }
    }
}
impl Policy {
    pub fn validate(&self, partitions: usize, segment: u64) {
        assert!(self.max_backlog_bytes >= segment.saturating_mul(2), "cold backlog must fit two payload segments");
        // Operational headroom for compaction/snapshot copies and WAL roll.
        // Pinned generations, other processes and provider storage are extra.
        assert!(self.min_free_bytes >= self.max_backlog_bytes.saturating_mul(2)
            .saturating_add((partitions as u64 + 2).saturating_mul(8 * 1024 * 1024)),
            "cold free-space floor must cover two backlog copies and WAL rolls");
    }
}

#[derive(Clone, Copy, Default, Serialize)]
struct Charge { backlog: u64, disk: u64 }
impl Charge {
    fn bytes(bytes: u64) -> Self {
        Self { backlog: bytes, disk: bytes.saturating_mul(4).saturating_add(8192) }
    }
    fn add(self, other: Self) -> Self {
        Self { backlog: self.backlog.saturating_add(other.backlog), disk: self.disk.saturating_add(other.disk) }
    }
    fn subtract(&mut self, other: Self) {
        self.backlog -= other.backlog;
        self.disk -= other.disk;
    }
}

#[derive(Default)]
struct State {
    sampled: Option<Instant>,
    local: u64,
    unapplied: u64,
    free: u64,
    active: Charge,
    spent: Charge,
    required_headroom: u64,
    counters: BTreeMap<&'static str, u64>,
}
pub(super) struct Budget { policy: Policy, state: Mutex<State> }
pub(super) struct Ticket { budget: Arc<Budget>, charge: Charge }
impl Drop for Ticket {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock().unwrap();
        state.active.subtract(self.charge);
        // A reply/drop is not an observation. Never free this credit early.
        state.spent = state.spent.add(self.charge);
    }
}

impl Budget {
    pub fn new(policy: Policy) -> Arc<Self> {
        Arc::new(Self { policy, state: Mutex::default() })
    }

    pub fn reserve(self: &Arc<Self>, bytes: u64, originator: bool) -> Result<Ticket, u16> {
        let charge = Charge::bytes(bytes);
        let mut state = self.state.lock().unwrap();
        let outstanding = state.active.add(state.spent).add(charge);
        let rejection = if originator && bytes > self.policy.max_backlog_bytes {
            Some(("request_exceeds_backlog_bound", 413))
        } else if state.sampled.is_none_or(|at| at.elapsed() > Duration::from_secs(1)) {
            Some(("stale_observation", 429))
        } else if originator && state.local.saturating_add(state.unapplied)
            .saturating_add(outstanding.backlog) > self.policy.max_backlog_bytes {
            // A large retry can need short seals below the high-water mark.
            // Remember only the largest demand, not an unbounded request queue.
            state.required_headroom = state.required_headroom.max(bytes);
            Some(("backlog_bound", 429))
        } else if self.policy.min_free_bytes.saturating_add(state.unapplied.saturating_mul(4))
            .saturating_add(outstanding.disk) > state.free {
            Some(("disk_headroom", 507))
        } else { None };
        if let Some((reason, status)) = rejection {
            *state.counters.entry(reason).or_default() += 1;
            timing::write_outcome(reason, 1);
            return Err(status);
        }
        state.active = state.active.add(charge);
        *state.counters.entry("admitted").or_default() += 1;
        Ok(Ticket { budget: self.clone(), charge })
    }

    pub fn pressure(&self) -> bool {
        let state = self.state.lock().unwrap();
        let used = state.local.saturating_add(state.unapplied).saturating_add(state.active.backlog)
            .saturating_add(state.spent.backlog);
        used >= self.policy.max_backlog_bytes / 4 * 3
            || used.saturating_add(state.required_headroom) > self.policy.max_backlog_bytes
    }

    pub fn command(self: &Arc<Self>, command: &Command) -> Result<Option<Ticket>, u16> {
        grows(command).then(|| self.reserve(command.encoded_len() as u64, true)).transpose()
    }

    /// Caller holds every group's view read lock, in group order. The mutex
    /// excludes ticket release during observation; in-flight journal writes
    /// may finish, but their active credits cannot disappear across this cut.
    fn sample(&self, observe: impl FnOnce() -> io::Result<(u64, u64, u64)>) {
        let mut state = self.state.lock().unwrap();
        match observe() {
            Ok((local, unapplied, free)) => {
                state.local = local;
                state.unapplied = unapplied;
                state.free = free;
                state.spent = Charge::default();
                if local.saturating_add(unapplied).saturating_add(state.active.backlog)
                    .saturating_add(state.required_headroom) <= self.policy.max_backlog_bytes {
                    state.required_headroom = 0;
                }
                state.sampled = Some(Instant::now());
                *state.counters.entry("samples").or_default() += 1;
            }
            Err(_) => {
                state.sampled = None;
                *state.counters.entry("sample_errors").or_default() += 1;
            }
        }
    }

    pub fn status(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap();
        serde_json::json!({"version":1,"scope":"node","policy":self.policy,
            "sample_age_ms":state.sampled.map(|at|at.elapsed().as_millis()),
            "local_backlog_bytes":state.local,"unapplied_bytes":state.unapplied,"filesystem_free_bytes":state.free,
            "active_credits":state.active,"spent_credits":state.spent,"required_headroom":state.required_headroom,
            "counters":state.counters})
    }
}

pub(super) fn grows(command: &Command) -> bool {
    // Existing committed maintenance must be able to release pressure, on
    // leaders AND followers. Fork references don't copy a parent's bytes.
    match command.method.as_str() {
        "PUT" | "POST" => true,
        "FORK" => matches!(bincode::deserialize::<forks::Action>(&command.body),
            Ok(forks::Action::Reserve { .. } | forks::Action::Chunk { .. })),
        _ => false,
    }
}

/// Includes sealed bytes not yet physically compacted and soft-deleted parents.
/// Fork reservation wire moves into a stream at Publish; don't lose it between.
pub(super) fn local_backlog(view: &machine::View) -> u64 {
    view.store.streams.iter().map(|s| {
        let shared = s.shared.read().unwrap();
        shared.tail.saturating_sub(shared.file_base)
    }).chain(view.forks.destinations.values().map(|d| d.wire.len() as u64))
        .fold(0u64, u64::saturating_add)
}

fn free_bytes(path: &std::path::Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs initializes the structure on success; f_bavail excludes reserved blocks.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 { return Err(io::Error::last_os_error()); }
    let stat = unsafe { stat.assume_init() };
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

impl Cluster {
    pub(super) async fn cold_pressure_sample(&self) {
        let Some(budget) = &self.cold_admission else { return; };
        let mut views = Vec::with_capacity(self.groups.len());
        for group in &self.groups { views.push(group.machine.view.read().await); }
        budget.sample(|| {
            let local = views.iter().map(|v| local_backlog(v)).fold(0u64, u64::saturating_add);
            let unapplied = self.groups.iter().zip(&views).map(|(g,v)|
                g.machine.journal.unapplied_bytes(v.applied)).fold(0u64, u64::saturating_add);
            Ok((local, unapplied, free_bytes(&self.config.dir)?))
        });
    }

    pub(super) async fn cold_pressure_worker(&self) {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            self.cold_pressure_sample().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn admission_never_reuses_unobserved_growth(
            operations in prop::collection::vec((0u8..5,1u64..32), 1..200),
        ) {
            let budget = Budget::new(Policy { max_backlog_bytes:64, min_free_bytes:0 });
            assert!(matches!(budget.reserve(1,true),Err(429)),"boot must observe recovery before admission");
            let mut retained = Vec::new();
            let mut used = 0u64;
            budget.sample(|| Ok((0,0,1_000_000)));
            for (op,size) in operations {
                match op {
                    0|1 => if let Ok(ticket) = budget.reserve(size,true) {
                        // Independently track bytes promised, including not yet applied.
                        prop_assert!(used+retained.iter().map(|(_,n)|n).sum::<u64>()+size <= 64);
                        retained.push((ticket,size));
                    },
                    2 => if let Some((ticket,n))=retained.pop() { used+=n; drop(ticket); },
                    3 => budget.sample(|| Ok((used,0,1_000_000))),
                    _ => { used=used.saturating_sub(size); },
                }
            }
            drop(retained);
            budget.sample(|| Err(io::Error::other("observation unavailable")));
            prop_assert!(matches!(budget.reserve(1,true),Err(429)));
        }
    }
}
