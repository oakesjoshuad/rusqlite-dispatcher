//! Worker pool management for concurrent SQLite access.
//!
//! This module provides query worker pool abstraction.
//! QueryWorkerPool encapsulates all query worker lifecycle
//! and routing concerns.

use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, atomic::AtomicUsize};

use arc_swap::ArcSwap;

use super::metadata::WorkerId;
use super::query_worker::SupervisedQueryWorker;
use crate::DomainHandler;
use crate::Request;
use crate::channel::QuerySender;
use crate::{Error, Result};

/// Cloneable handle for query dispatch with lock-free hot path.
///
/// Contains only the data needed for query execution, enabling cheap clones
/// for sharing across threads without additional Arc wrapper.
///
/// # Lock-Free Design
///
/// - `senders`: ArcSwap for lock-free reads on hot path
/// - `next_worker`: Atomic counter for round-robin selection
///
/// Both operations avoid locks entirely during query execution
#[derive(Clone, Debug)]
pub(crate) struct QueryDispatchHandle<H: DomainHandler> {
    /// Fast lookup for dispatch (lock-free via ArcSwap)
    senders: QueryPoolSenders<H>,

    /// Selection state (round-robin counter)
    next_worker: Arc<AtomicUsize>,
}

type QueryPoolSenders<H> = Arc<ArcSwap<Vec<(WorkerId, QuerySender<H>)>>>;

/// Query worker pool managing N readers with round-robin selection
///
/// Owns worker lifecycle (join handles and metadata) while providing cloneable
/// dispatch handle for lock-free query execution
///
/// # Ownership Model
///
/// - **Pool**: Owns workers (non-Clone) for metrics aggregation and shutdown
/// - **DispatchHandle**: Cloneable reference for query execution (lock-free)
///
/// This separation enables `Dispatcher` to be `Clone` without requiring
/// `Arc<Dispatcher>` wrapper while maintaining lock-free hot path
#[derive(Debug)]
pub(crate) struct QueryWorkerPool<H: DomainHandler> {
    workers: HashMap<WorkerId, SupervisedQueryWorker>,
    dispatch: QueryDispatchHandle<H>,
}

/// Aggregated metrics from query pool
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueryPoolMetrics {
    pub total_queries: u64,
    pub avg_duration_micros: u64,
    pub worker_count: u8,
}

impl<H: DomainHandler> QueryDispatchHandle<H> {
    /// Create new dispatch handle
    fn new() -> Self {
        Self {
            senders: Arc::new(ArcSwap::from_pointee(Vec::new())),
            next_worker: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Get next query worker index using round-robin selection (hot path)
    ///
    /// Returns error if pool is empty. Uses atomic fetch-add with modulo
    /// for lock-free round-robin selection
    #[inline]
    fn get_next_worker_index(&self) -> Result<usize> {
        let senders = self.senders.load();

        match senders.is_empty() {
            true => Err(Error::NoQueryWorkers),
            false => {
                let index = self.next_worker.fetch_add(1, Relaxed) % senders.len();
                Ok(index)
            }
        }
    }

    /// Execute query with fail-fast backpressure (hot path - lock-free)
    ///
    /// Selects next worker via round-robin, checks channel capacity before
    /// creating request/response pair, and awaits result
    ///
    /// # Errors
    ///
    /// - [`Error::NoQueryWorkers`] - Pool is empty
    /// - [`Error::QueryWorkersBusy`] - All workers at capacity
    /// - [`Error::Database`] - Handler returned database error
    /// - [`Error::Internal`] - Channel receive failed
    pub(crate) async fn execute_query(&self, query: H::Query) -> Result<H::QueryResponse> {
        let index = self.get_next_worker_index()?;
        let senders = self.senders.load();
        let (_worker_id, sender) = &senders[index];

        // Fail-fast: check capacity before creating request
        match sender.try_reserve() {
            Ok(permit) => {
                let (request, response) = Request::new(query);
                permit.send(request);

                match response.await {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(worker_err)) => Err(worker_err.into()),
                    Err(recv_err) => Err(Error::Internal(recv_err.to_string())),
                }
            }
            Err(_) => Err(Error::QueryWorkersBusy),
        }
    }
}

impl<H: DomainHandler> QueryWorkerPool<H> {
    /// Create new empty pool
    pub(crate) fn new() -> Self {
        Self {
            workers: HashMap::new(),
            dispatch: QueryDispatchHandle::new(),
        }
    }

    /// Add a worker to the pool
    ///
    /// Updates both the worker HashMap (for metrics/lifecycle) and the
    /// senders vector (for hot-path dispatch via ArcSwap)
    pub(crate) fn add_worker(&mut self, worker: SupervisedQueryWorker, sender: QuerySender<H>) {
        let worker_id = worker.worker_id;
        self.workers.insert(worker_id, worker);

        // Update senders vector (ArcSwap for lock-free reads)
        let mut current = self.dispatch.senders.load().as_ref().clone();
        current.push((worker_id, sender));
        self.dispatch.senders.store(Arc::new(current));
    }

    /// Get cloneable dispatch handle for query execution
    ///
    /// Returns a cheap-to-clone handle that can execute queries with lock-free
    /// hot path. This enables `Dispatcher` to be `Clone` without `Arc` wrapper
    pub(crate) fn dispatch_handle(&self) -> QueryDispatchHandle<H> {
        self.dispatch.clone()
    }

    /// Aggregate metrics from all query workers
    ///
    /// Collects total queries and computes average duration across workers
    /// Used by supervisor for metrics reporting and scaling recommendations
    pub(crate) fn aggregate_metrics(&self) -> QueryPoolMetrics {
        let mut total_queries = 0;
        let mut avg_duration_micros = 0;

        for worker in self.workers.values() {
            total_queries += worker.metrics.fetch_queries_processed();
            avg_duration_micros += worker.metrics.fetch_avg_query_duration_micros();
        }

        // Average across workers
        let count = self.workers.len() as u64;
        match count {
            0 => QueryPoolMetrics {
                total_queries: 0,
                avg_duration_micros: 0,
                worker_count: 0,
            },
            _ => QueryPoolMetrics {
                total_queries,
                avg_duration_micros: avg_duration_micros / count,
                worker_count: count as u8,
            },
        }
    }

    /// Shutdown all workers gracefully
    ///
    /// Drops senders to signal workers, then awaits all join handles.
    /// Reports errors but continues shutdown for remaining workers.
    pub(crate) async fn shutdown(self) {
        // Drop senders to signal workers by replacing with empty Vec
        // This causes the old Arc (containing all senders) to be dropped
        self.dispatch.senders.store(Arc::new(Vec::new()));

        for (worker_id, worker) in self.workers {
            match worker.join_handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    eprintln!("Query worker {worker_id} error during shutdown: {err:?}");
                }
                Err(err) => {
                    eprintln!("Query worker {worker_id} panicked during shutdown: {err:?}");
                }
            }
        }
    }
}

// --- TESTS ---

#[cfg(test)]
mod tests {
    use super::*;

    // Mock handler for testing
    #[derive(Clone)]
    struct MockHandler;

    impl DomainHandler for MockHandler {
        type Command = ();
        type Query = ();
        type QueryResponse = u64;

        fn handle_command(
            &mut self,
            _tx: &rusqlite::Transaction,
            _cmd: Self::Command,
        ) -> std::result::Result<(), rusqlite::Error> {
            Ok(())
        }

        fn handle_query(
            &self,
            _conn: &rusqlite::Connection,
            _query: Self::Query,
        ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
            Ok(42)
        }
    }

    #[test]
    fn pool_starts_empty() {
        let pool: QueryWorkerPool<MockHandler> = QueryWorkerPool::new();
        let metrics = pool.aggregate_metrics();
        assert_eq!(metrics.worker_count, 0);
        assert_eq!(metrics.total_queries, 0);
    }

    #[test]
    fn pool_get_next_index_fails_when_empty() {
        let pool: QueryWorkerPool<MockHandler> = QueryWorkerPool::new();
        let handle = pool.dispatch_handle();
        let result = handle.get_next_worker_index();
        assert!(matches!(result, Err(Error::NoQueryWorkers)));
    }

    #[tokio::test]
    async fn pool_execute_query_fails_when_empty() {
        let pool: QueryWorkerPool<MockHandler> = QueryWorkerPool::new();
        let handle = pool.dispatch_handle();
        let result = handle.execute_query(()).await;
        assert!(matches!(result, Err(Error::NoQueryWorkers)));
    }

    #[test]
    fn pool_aggregate_metrics_empty_pool() {
        let pool: QueryWorkerPool<MockHandler> = QueryWorkerPool::new();
        let metrics = pool.aggregate_metrics();
        assert_eq!(metrics.total_queries, 0);
        assert_eq!(metrics.avg_duration_micros, 0);
        assert_eq!(metrics.worker_count, 0);
    }

    #[test]
    fn dispatch_handle_is_cloneable() {
        let pool: QueryWorkerPool<MockHandler> = QueryWorkerPool::new();
        let handle1 = pool.dispatch_handle();
        let handle2 = handle1.clone();
        // Verify both handles fail when empty (share same state)
        assert!(matches!(
            handle1.get_next_worker_index(),
            Err(Error::NoQueryWorkers)
        ));
        assert!(matches!(
            handle2.get_next_worker_index(),
            Err(Error::NoQueryWorkers)
        ));
    }
}
