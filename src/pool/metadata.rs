//! Worker identification and type classification.
//!
//! This module provides the minimal types needed to classify and identify
//! workers in the dispatcher pool. Each worker receives a unique ULID
//! (time-ordered ID) and is classified as either Command (single R/W) or
//! Query (one of N read-only workers).

/// Unique worker identifier using ULID.
///
/// ULIDs are time-ordered unique identifiers that maintain sortability
/// by creation time. This makes logs and debugging easier since workers
/// appear in creation order.
///
/// Generated fresh with `ulid::Ulid::new()`.
pub(crate) type WorkerId = ulid::Ulid;

/// Classification of worker by access pattern and concurrency.
///
/// Determines both which handler method is called (handle_command vs
/// handle_query) and concurrency model:
/// - Command: Single writer with transaction support
/// - Query: Multiple concurrent readers, no transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerType {
    /// Single command worker with read-write access.
    ///
    /// Processes commands serially with automatic transaction management.
    /// Only one command worker exists in the dispatcher.
    Command,
    /// Query worker with read-only access (one of N).
    ///
    /// Processes queries in parallel with other query workers.
    /// The dispatcher maintains a pool of N query workers.
    Query,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_type_command_creation() {
        let cmd_type = WorkerType::Command;
        assert_eq!(cmd_type, WorkerType::Command);
    }

    #[test]
    fn worker_type_query_creation() {
        let query_type = WorkerType::Query;
        assert_eq!(query_type, WorkerType::Query);
    }

    #[test]
    fn worker_types_are_distinguishable() {
        let cmd = WorkerType::Command;
        let query = WorkerType::Query;
        assert_ne!(cmd, query);
    }

    #[test]
    fn worker_id_is_unique() {
        let id1 = ulid::Ulid::new();
        let id2 = ulid::Ulid::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn worker_type_is_copyable() {
        let original = WorkerType::Command;
        let copied = original;
        // If this compiles and runs, Copy is working
        assert_eq!(original, copied);
    }
}
