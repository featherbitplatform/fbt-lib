//! The session's disk pool: every storage job of every torrent runs on a
//! blocking thread behind one of [`Settings::disk_threads`] permits, and at
//! most [`Settings::max_active_checks`] piece checks run at once.
//!
//! [`Settings::disk_threads`]: crate::Settings::disk_threads
//! [`Settings::max_active_checks`]: crate::Settings::max_active_checks

use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Bounds the session's concurrent storage jobs and piece checks.
#[derive(Debug)]
pub(crate) struct DiskPool {
    threads: Arc<Semaphore>,
    checks: Arc<Semaphore>,
}

impl DiskPool {
    /// A pool running at most `disk_threads` jobs and `max_active_checks`
    /// checks at once (each at least 1).
    pub fn new(disk_threads: usize, max_active_checks: usize) -> DiskPool {
        DiskPool {
            threads: Arc::new(Semaphore::new(disk_threads.max(1))),
            checks: Arc::new(Semaphore::new(max_active_checks.max(1))),
        }
    }

    /// Runs `f` on a blocking thread once a disk permit is free; its result
    /// comes back through `jobs`. The permit is held until `f` returns,
    /// even if `jobs` is dropped or the job aborted while `f` runs, so the
    /// bound holds for every thread actually doing I/O.
    pub fn spawn<T: Send + 'static>(
        &self,
        jobs: &mut JoinSet<T>,
        f: impl FnOnce() -> T + Send + 'static,
    ) {
        let threads = Arc::clone(&self.threads);
        jobs.spawn(async move {
            // The semaphore is never closed; were it, the job still runs.
            let permit = threads.acquire_owned().await.ok();
            let job = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                f()
            });
            match job.await {
                Ok(v) => v,
                // A panic in `f` surfaces from `jobs` as it would from
                // `spawn_blocking`; cancellation only happens at runtime
                // shutdown, when nobody reads `jobs` any more.
                Err(e) => match e.try_into_panic() {
                    Ok(payload) => std::panic::resume_unwind(payload),
                    Err(e) => std::panic::resume_unwind(Box::new(e.to_string())),
                },
            }
        });
    }

    /// The `max_active_checks` semaphore: a check holds one of its permits
    /// while it runs.
    pub fn check_permit(&self) -> Arc<Semaphore> {
        Arc::clone(&self.checks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pool_never_exceeds_disk_threads() {
        let pool = DiskPool::new(3, 1);
        let now = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let mut jobs = JoinSet::new();
        for i in 0..20usize {
            let (now, peak) = (Arc::clone(&now), Arc::clone(&peak));
            pool.spawn(&mut jobs, move || {
                let n = now.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
                now.fetch_sub(1, Ordering::SeqCst);
                i
            });
        }
        let mut done = Vec::new();
        while let Some(r) = jobs.join_next().await {
            done.push(r.expect("job"));
        }
        done.sort_unstable();
        assert_eq!(done, (0..20).collect::<Vec<_>>());
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak <= 3, "{peak} jobs at once");
        assert!(peak >= 2, "jobs never overlapped");
        assert_eq!(pool.check_permit().available_permits(), 1);
    }
}
