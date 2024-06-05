use std::cmp::min;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Notify;

use crate::api::{Builder, ManageConnection, QueueStrategy, State, Statistics};
use crate::lock::Mutex;

/// The guts of a `Pool`.
#[allow(missing_debug_implementations)]
pub(crate) struct SharedPool<M>
where
    M: ManageConnection + Send,
{
    pub(crate) statics: Builder<M>,
    pub(crate) manager: M,
    pub(crate) internals: Mutex<PoolInternals<M>>,
    pub(crate) notify: Arc<Notify>,
    pub(crate) statistics: AtomicStatistics,
}

impl<M> SharedPool<M>
where
    M: ManageConnection + Send,
{
    pub(crate) fn new(statics: Builder<M>, manager: M) -> Self {
        Self {
            statics,
            manager,
            internals: Mutex::new(PoolInternals::default()),
            notify: Arc::new(Notify::new()),
            statistics: AtomicStatistics::default(),
        }
    }

    pub(crate) fn pop(&self) -> (Option<Conn<M::Connection>>, ApprovalIter) {
        let mut locked = self.internals.lock();
        let conn = locked.conns.pop_front().map(|idle| idle.conn);
        let approvals = match &conn {
            Some(_) => locked.wanted(&self.statics),
            None => locked.approvals(&self.statics, 1),
        };

        (conn, approvals)
    }

    pub(crate) fn reap(&self) -> ApprovalIter {
        let mut locked = self.internals.lock();
        let (iter, max_idle_timeout_closed, max_lifetime_closed) = locked.reap(&self.statics);
        drop(locked);
        self.statistics
            .connections_max_idle_timeout_closed
            .fetch_add(max_idle_timeout_closed, Ordering::SeqCst);
        self.statistics
            .connections_max_lifetime_closed
            .fetch_add(max_lifetime_closed, Ordering::SeqCst);
        iter
    }

    pub(crate) fn forward_error(&self, err: M::Error) {
        self.statics.error_sink.sink(err);
    }
}

/// The pool data that must be protected by a lock.
#[allow(missing_debug_implementations)]
pub(crate) struct PoolInternals<M>
where
    M: ManageConnection,
{
    conns: VecDeque<IdleConn<M::Connection>>,
    num_conns: u32,
    pending_conns: u32,
}

impl<M> PoolInternals<M>
where
    M: ManageConnection,
{
    pub(crate) fn put(
        &mut self,
        conn: Conn<M::Connection>,
        approval: Option<Approval>,
        pool: Arc<SharedPool<M>>,
    ) {
        if approval.is_some() {
            #[cfg(debug_assertions)]
            {
                self.pending_conns -= 1;
                self.num_conns += 1;
            }
            #[cfg(not(debug_assertions))]
            {
                self.pending_conns = self.pending_conns.saturating_sub(1);
                self.num_conns = self.num_conns.saturating_add(1);
            }
        }

        // Queue it in the idle queue
        let conn = IdleConn::from(conn);
        match pool.statics.queue_strategy {
            QueueStrategy::Fifo => self.conns.push_back(conn),
            QueueStrategy::Lifo => self.conns.push_front(conn),
        }

        pool.notify.notify_one();
    }

    pub(crate) fn connect_failed(&mut self, _: Approval) {
        #[cfg(debug_assertions)]
        {
            self.pending_conns -= 1;
        }
        #[cfg(not(debug_assertions))]
        {
            self.pending_conns = self.pending_conns.saturating_sub(1);
        }
    }

    pub(crate) fn dropped(&mut self, num: u32, config: &Builder<M>) -> ApprovalIter {
        #[cfg(debug_assertions)]
        {
            self.num_conns -= num;
        }
        #[cfg(not(debug_assertions))]
        {
            self.num_conns = self.num_conns.saturating_sub(num);
        }

        self.wanted(config)
    }

    pub(crate) fn wanted(&mut self, config: &Builder<M>) -> ApprovalIter {
        let available = self.conns.len() as u32 + self.pending_conns;
        let min_idle = config.min_idle.unwrap_or(0);
        let wanted = min_idle.saturating_sub(available);
        self.approvals(config, wanted)
    }

    fn approvals(&mut self, config: &Builder<M>, num: u32) -> ApprovalIter {
        let current = self.num_conns + self.pending_conns;
        let num = min(num, config.max_size.saturating_sub(current));
        self.pending_conns += num;
        ApprovalIter { num: num as usize }
    }

    pub(crate) fn reap(&mut self, config: &Builder<M>) -> (ApprovalIter, u64, u64) {
        let mut max_lifetime_closed: u64 = 0;
        let mut max_idle_timeout_closed: u64 = 0;
        let now = Instant::now();
        let before = self.conns.len();

        self.conns.retain(|conn| {
            let mut keep = true;
            if let Some(timeout) = config.idle_timeout {
                if now - conn.idle_start >= timeout {
                    max_idle_timeout_closed += 1;
                    keep &= false;
                }
            }
            if let Some(lifetime) = config.max_lifetime {
                if now - conn.conn.birth >= lifetime {
                    max_lifetime_closed += 1;
                    keep &= false;
                }
            }
            keep
        });

        (
            self.dropped((before - self.conns.len()) as u32, config),
            max_idle_timeout_closed,
            max_lifetime_closed,
        )
    }

    pub(crate) fn state(&self, statistics: Statistics) -> State {
        State {
            connections: self.num_conns,
            idle_connections: self.conns.len() as u32,
            statistics,
        }
    }
}

impl<M> Default for PoolInternals<M>
where
    M: ManageConnection,
{
    fn default() -> Self {
        Self {
            conns: VecDeque::new(),
            num_conns: 0,
            pending_conns: 0,
        }
    }
}

#[must_use]
pub(crate) struct ApprovalIter {
    num: usize,
}

impl Iterator for ApprovalIter {
    type Item = Approval;

    fn next(&mut self) -> Option<Self::Item> {
        match self.num {
            0 => None,
            _ => {
                self.num -= 1;
                Some(Approval { _priv: () })
            }
        }
    }
}

impl ExactSizeIterator for ApprovalIter {
    fn len(&self) -> usize {
        self.num
    }
}

#[must_use]
pub(crate) struct Approval {
    _priv: (),
}

#[derive(Debug)]
pub(crate) struct Conn<C>
where
    C: Send,
{
    pub(crate) conn: C,
    birth: Instant,
}

impl<C: Send> Conn<C> {
    pub(crate) fn new(conn: C) -> Self {
        Self {
            conn,
            birth: Instant::now(),
        }
    }
}

impl<C: Send> From<IdleConn<C>> for Conn<C> {
    fn from(conn: IdleConn<C>) -> Self {
        conn.conn
    }
}

struct IdleConn<C>
where
    C: Send,
{
    conn: Conn<C>,
    idle_start: Instant,
}

impl<C: Send> From<Conn<C>> for IdleConn<C> {
    fn from(conn: Conn<C>) -> Self {
        IdleConn {
            conn,
            idle_start: Instant::now(),
        }
    }
}

#[derive(Default)]
pub(crate) struct AtomicStatistics {
    pub(crate) get_direct: AtomicU64,
    pub(crate) get_waited: AtomicU64,
    pub(crate) get_timed_out: AtomicU64,
    pub(crate) get_waited_time_micro: AtomicU64,
    pub(crate) connections_created: AtomicU64,
    pub(crate) connections_broken_closed: AtomicU64,
    pub(crate) connections_invalid_closed: AtomicU64,
    pub(crate) connections_max_lifetime_closed: AtomicU64,
    pub(crate) connections_max_idle_timeout_closed: AtomicU64,
}

impl AtomicStatistics {
    pub(crate) fn record_get(&self, kind: StatsKind) {
        match kind {
            StatsKind::Direct => self.get_direct.fetch_add(1, Ordering::SeqCst),
            StatsKind::Waited => self.get_waited.fetch_add(1, Ordering::SeqCst),
            StatsKind::TimedOut => self.get_timed_out.fetch_add(1, Ordering::SeqCst),
        };
    }
}

impl From<&AtomicStatistics> for Statistics {
    fn from(item: &AtomicStatistics) -> Self {
        Self {
            get_direct: item.get_direct.load(Ordering::SeqCst),
            get_waited: item.get_waited.load(Ordering::SeqCst),
            get_timed_out: item.get_timed_out.load(Ordering::SeqCst),
            get_waited_time_micro: item.get_waited_time_micro.load(Ordering::SeqCst),
            connections_created: item.connections_created.load(Ordering::SeqCst),
            connections_broken_closed: item.connections_broken_closed.load(Ordering::SeqCst),
            connections_invalid_closed: item.connections_invalid_closed.load(Ordering::SeqCst),
            connections_max_lifetime_closed: item
                .connections_max_lifetime_closed
                .load(Ordering::SeqCst),
            connections_max_idle_timeout_closed: item
                .connections_max_idle_timeout_closed
                .load(Ordering::SeqCst),
        }
    }
}

pub(crate) enum StatsKind {
    Direct,
    Waited,
    TimedOut,
}
