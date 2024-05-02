use std::cmp::{max, min};
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::TryFutureExt;
use tokio::spawn;
use tokio::time::{interval_at, sleep, timeout, Interval};

use crate::api::{Builder, ConnectionState, ManageConnection, PooledConnection, RunError, State};
use crate::internals::{Approval, ApprovalIter, Conn, SharedPool};

pub(crate) struct PoolInner<M>
where
    M: ManageConnection + Send,
{
    inner: Arc<SharedPool<M>>,
    pool_inner_stats: Arc<SharedPoolInnerStatistics>,
}

impl<M> PoolInner<M>
where
    M: ManageConnection + Send,
{
    pub(crate) fn new(builder: Builder<M>, manager: M) -> Self {
        let inner = Arc::new(SharedPool::new(builder, manager));
        let pool_inner_stats = Arc::new(SharedPoolInnerStatistics::default());

        if inner.statics.max_lifetime.is_some() || inner.statics.idle_timeout.is_some() {
            let start = Instant::now() + inner.statics.reaper_rate;
            let interval = interval_at(start.into(), inner.statics.reaper_rate);
            tokio::spawn(
                Reaper {
                    interval,
                    pool: Arc::downgrade(&inner),
                    pool_inner_stats: Arc::downgrade(&pool_inner_stats),
                }
                .run(),
            );
        }

        Self {
            inner,
            pool_inner_stats,
        }
    }

    pub(crate) async fn start_connections(&self) -> Result<(), M::Error> {
        let wanted = self.inner.internals.lock().wanted(&self.inner.statics);
        let mut stream = self.replenish_idle_connections(wanted);
        while let Some(result) = stream.next().await {
            result?;
        }
        Ok(())
    }

    pub(crate) fn spawn_start_connections(&self) {
        let mut locked = self.inner.internals.lock();
        self.spawn_replenishing_approvals(locked.wanted(&self.inner.statics));
    }

    fn spawn_replenishing_approvals(&self, approvals: ApprovalIter) {
        if approvals.len() == 0 {
            return;
        }

        let this = self.clone();
        spawn(async move {
            let mut stream = this.replenish_idle_connections(approvals);
            while let Some(result) = stream.next().await {
                match result {
                    Ok(()) => {}
                    Err(e) => this.inner.forward_error(e),
                }
            }
        });
    }

    fn replenish_idle_connections(
        &self,
        approvals: ApprovalIter,
    ) -> FuturesUnordered<impl Future<Output = Result<(), M::Error>>> {
        let stream = FuturesUnordered::new();
        for approval in approvals {
            let this = self.clone();
            stream.push(async move { this.add_connection(approval).await });
        }
        stream
    }

    pub(crate) async fn get(&self) -> Result<PooledConnection<'_, M>, RunError<M::Error>> {
        let mut wait_time_start = None;

        let future = async {
            loop {
                let (conn, approvals) = self.inner.pop();
                self.spawn_replenishing_approvals(approvals);

                // Cancellation safety: make sure to wrap the connection in a `PooledConnection`
                // before allowing the code to hit an `await`, so we don't lose the connection.

                let mut conn = match conn {
                    Some(conn) => PooledConnection::new(self, conn),
                    None => {
                        wait_time_start = Some(Instant::now());
                        self.inner.notify.notified().await;
                        continue;
                    }
                };

                if !self.inner.statics.test_on_check_out {
                    return Ok(conn);
                }

                match self.inner.manager.is_valid(&mut conn).await {
                    Ok(()) => return Ok(conn),
                    Err(e) => {
                        self.pool_inner_stats
                            .invalid_closed_connections
                            .fetch_add(1, Ordering::SeqCst);
                        self.inner.forward_error(e);
                        conn.state = ConnectionState::Invalid;
                        continue;
                    }
                }
            }
        };

        let result = match timeout(self.inner.statics.connection_timeout, future).await {
            Ok(result) => result,
            _ => Err(RunError::TimedOut),
        };

        if let Some(wait_time_start) = wait_time_start {
            let wait_time = Instant::now() - wait_time_start;
            self.pool_inner_stats
                .gets_waited_wait_time_micro
                .fetch_add(wait_time.as_micros() as u64, Ordering::SeqCst);
            self.pool_inner_stats
                .gets_waited
                .fetch_add(1, Ordering::SeqCst);
        }
        self.pool_inner_stats.gets.fetch_add(1, Ordering::SeqCst);
        result
    }

    pub(crate) async fn connect(&self) -> Result<M::Connection, M::Error> {
        let mut conn = self.inner.manager.connect().await?;
        self.on_acquire_connection(&mut conn).await?;
        Ok(conn)
    }

    /// Return connection back in to the pool
    pub(crate) fn put_back(&self, mut conn: Conn<M::Connection>, state: ConnectionState) {
        debug_assert!(
            !matches!(state, ConnectionState::Extracted),
            "handled in caller"
        );

        let mut locked = self.inner.internals.lock();
        match (state, self.inner.manager.has_broken(&mut conn.conn)) {
            (ConnectionState::Present, false) => locked.put(conn, None, self.inner.clone()),
            (_, _) => {
                self.pool_inner_stats
                    .broken_closed_connections
                    .fetch_add(1, Ordering::SeqCst);
                let approvals = locked.dropped(1, &self.inner.statics);
                self.spawn_replenishing_approvals(approvals);
                self.inner.notify.notify_waiters();
            }
        }
    }

    /// Returns statistics about the historical usage of the pool.
    pub(crate) fn statistics(&self) -> Statistics {
        let gets = self.pool_inner_stats.gets.load(Ordering::SeqCst);
        let gets_waited = self.pool_inner_stats.gets_waited.load(Ordering::SeqCst);
        let openned_connections = self
            .pool_inner_stats
            .openned_connections
            .load(Ordering::SeqCst);
        let broken_closed_connections = self
            .pool_inner_stats
            .broken_closed_connections
            .load(Ordering::SeqCst);
        let invalid_closed_connections = self
            .pool_inner_stats
            .invalid_closed_connections
            .load(Ordering::SeqCst);
        let gets_waited_wait_time_micro = self
            .pool_inner_stats
            .gets_waited_wait_time_micro
            .load(Ordering::SeqCst);

        let locked = self.inner.internals.lock();
        let max_idle_time_closed_connections = locked.max_idle_time_closed_connections;
        let max_life_time_closed_connections = locked.max_life_time_closed_connections;

        Statistics {
            gets,
            gets_waited,
            gets_waited_wait_time_micro,
            max_idle_time_closed_connections,
            max_life_time_closed_connections,
            invalid_closed_connections,
            broken_closed_connections,
            openned_connections,
        }
    }

    /// Returns information about the current state of the pool.
    pub(crate) fn state(&self) -> State {
        self.inner.internals.lock().state()
    }

    // Outside of Pool to avoid borrow splitting issues on self
    async fn add_connection(&self, approval: Approval) -> Result<(), M::Error>
    where
        M: ManageConnection,
    {
        let new_shared = Arc::downgrade(&self.inner);
        let shared = match new_shared.upgrade() {
            None => return Ok(()),
            Some(shared) => shared,
        };

        let start = Instant::now();
        let mut delay = Duration::from_secs(0);
        loop {
            let conn = shared
                .manager
                .connect()
                .and_then(|mut c| async { self.on_acquire_connection(&mut c).await.map(|_| c) })
                .await;

            match conn {
                Ok(conn) => {
                    let conn = Conn::new(conn);
                    shared
                        .internals
                        .lock()
                        .put(conn, Some(approval), self.inner.clone());
                    self.pool_inner_stats
                        .openned_connections
                        .fetch_add(1, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    if !self.inner.statics.retry_connection
                        || Instant::now() - start > self.inner.statics.connection_timeout
                    {
                        let mut locked = shared.internals.lock();
                        locked.connect_failed(approval);
                        return Err(e);
                    } else {
                        delay = max(Duration::from_millis(200), delay);
                        delay = min(self.inner.statics.connection_timeout / 2, delay * 2);
                        sleep(delay).await;
                    }
                }
            }
        }
    }

    async fn on_acquire_connection(&self, conn: &mut M::Connection) -> Result<(), M::Error> {
        match self.inner.statics.connection_customizer.as_ref() {
            Some(customizer) => customizer.on_acquire(conn).await,
            None => Ok(()),
        }
    }
}

impl<M> Clone for PoolInner<M>
where
    M: ManageConnection,
{
    fn clone(&self) -> Self {
        PoolInner {
            inner: self.inner.clone(),
            pool_inner_stats: self.pool_inner_stats.clone(),
        }
    }
}

impl<M> fmt::Debug for PoolInner<M>
where
    M: ManageConnection,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_fmt(format_args!("PoolInner({:p})", self.inner))
    }
}

struct Reaper<M: ManageConnection> {
    interval: Interval,
    pool: Weak<SharedPool<M>>,
    pool_inner_stats: Weak<SharedPoolInnerStatistics>,
}

impl<M: ManageConnection> Reaper<M> {
    async fn run(mut self) {
        loop {
            let _ = self.interval.tick().await;
            let pool = match self.pool.upgrade() {
                Some(inner) => PoolInner {
                    inner,
                    pool_inner_stats: self.pool_inner_stats.upgrade().unwrap(),
                },
                None => break,
            };

            let approvals = pool.inner.reap();
            pool.spawn_replenishing_approvals(approvals);
        }
    }
}

#[derive(Default)]
struct SharedPoolInnerStatistics {
    gets: AtomicU64,
    gets_waited: AtomicU64,
    gets_waited_wait_time_micro: AtomicU64,
    invalid_closed_connections: AtomicU64,
    broken_closed_connections: AtomicU64,
    openned_connections: AtomicU64,
}

/// Statistics about the historical usage of the `Pool`.
#[derive(Debug)]
#[non_exhaustive]
pub struct Statistics {
    /// Information about gets
    /// Total gets performed, you should consider that the
    /// value can overflow and start from 0 eventually.
    pub gets: u64,
    /// Total gets performed that had to wait for having a
    /// connection available. The value can overflow and
    /// start from 0 eventually.
    pub gets_waited: u64,
    /// Total time waited by gets that suffered from contention
    /// in microseconds. The value can overflow and start
    /// from 0 eventually.
    pub gets_waited_wait_time_micro: u64,
    /// Total connections closed because they reached the
    /// max idle time configured. The value can
    /// overflow and start from 0 eventually.
    pub max_idle_time_closed_connections: u64,
    /// Total connections closed because they reached the
    /// max life time configured. The value can
    /// overflow and start from 0 eventually.
    pub max_life_time_closed_connections: u64,
    /// Total connections not used from the pool because they
    /// were considered invalid by the manager. The value can
    /// overflow and start from 0 eventually.
    pub invalid_closed_connections: u64,
    /// Total connections not returned to the pool because they
    /// were considered broken by the manager. The value can
    /// overflow and start from 0 eventually.
    pub broken_closed_connections: u64,
    /// Total connections openned. The value can overflow and
    /// start from 0 eventually.
    pub openned_connections: u64,
}
