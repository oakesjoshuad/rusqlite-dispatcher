//! Error types for rusqlite-dispatcher.
//!
//! This module provides a unified error type for all dispatcher operations,
//! consolidating database errors with infrastructure failures introduced by
//! async task management. The design separates user-facing errors (database
//! constraints, validation failures) from operator-facing errors (backpressure,
//! capacity), enabling clients to implement appropriate recovery strategies.

use thiserror::Error;

/// Unified error type for all dispatcher operations.
///
/// This error type consolidates all failure modes across the dispatcher,
/// worker pool, and database operations. The [`Error::source`] implementation
/// preserves the full error chain for debugging and observability.
///
/// # Error Categories
///
/// - **Database**: SQL errors from the handler or schema violations
/// - **CommandWorkerBusy**: Command queue at capacity (backpressure signal)
/// - **QueryWorkersBusy**: All query workers saturated (backpressure signal)
/// - **NoQueryWorkers**: Configuration error or shutdown state
/// - **Internal**: Async runtime or channel communication failures
#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Command worker busy - backpressure applied")]
    CommandWorkerBusy,

    #[error("Query workers busy - backpressure applied")]
    QueryWorkersBusy,

    #[error("No query workers available")]
    NoQueryWorkers,

    #[error("Internal infrastructure error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    // Test 1: Display Formatting
    // Validates that error messages are user-friendly and informative

    #[test]
    fn database_error_display() {
        let rusqlite_err = rusqlite::Error::InvalidQuery;
        let err = Error::Database(rusqlite_err);
        let display = err.to_string();
        assert!(display.contains("Database error"));
        assert!(display.contains("Query is not read-only"));
    }

    #[test]
    fn command_worker_busy_display() {
        let err = Error::CommandWorkerBusy;
        assert_eq!(
            err.to_string(),
            "Command worker busy - backpressure applied"
        );
    }

    #[test]
    fn query_workers_busy_display() {
        let err = Error::QueryWorkersBusy;
        assert_eq!(
            err.to_string(),
            "Query workers busy - backpressure applied"
        );
    }

    #[test]
    fn no_query_workers_display() {
        let err = Error::NoQueryWorkers;
        assert_eq!(err.to_string(), "No query workers available");
    }

    #[test]
    fn internal_error_display() {
        let err = Error::Internal("test failure".to_string());
        assert_eq!(
            err.to_string(),
            "Internal infrastructure error: test failure"
        );
    }

    // Test 2: From Conversions
    // Validates automatic error conversion using the ? operator

    #[test]
    fn from_rusqlite_error() {
        let rusqlite_err = rusqlite::Error::InvalidQuery;
        let err: Error = rusqlite_err.into();

        match err {
            Error::Database(_) => {
                // Correct variant
            }
            _ => panic!("Expected Database variant"),
        }
    }

    #[test]
    fn from_rusqlite_via_question_mark() {
        fn returns_error() -> Result<()> {
            Err(rusqlite::Error::InvalidQuery)?
        }

        let result = returns_error();
        assert!(result.is_err());
        match result {
            Err(Error::Database(_)) => {
                // Correct - rusqlite error wrapped in Database variant
            }
            _ => panic!("Expected Database variant"),
        }
    }

    // Test 3: source() Preservation
    // Validates error chain preservation for observability and debugging

    #[test]
    fn database_error_preserves_source() {
        let rusqlite_err = rusqlite::Error::InvalidQuery;
        let err = Error::Database(rusqlite_err);

        // source() should return Some with the original rusqlite error
        let source = err.source();
        assert!(source.is_some());

        let source_err = source.unwrap();
        assert_eq!(source_err.to_string(), "Query is not read-only");
    }

    #[test]
    fn backpressure_errors_have_no_source() {
        assert!(Error::CommandWorkerBusy.source().is_none());
        assert!(Error::QueryWorkersBusy.source().is_none());
        assert!(Error::NoQueryWorkers.source().is_none());
    }

    #[test]
    fn internal_error_source_is_none() {
        let err = Error::Internal("infrastructure failure".to_string());
        assert!(err.source().is_none());
    }

    // Test 4: Pattern Matching
    // Validates that users can discriminate between error types

    #[test]
    fn pattern_match_backpressure_errors() {
        let errors = vec![
            Error::CommandWorkerBusy,
            Error::QueryWorkersBusy,
            Error::NoQueryWorkers,
        ];

        for err in errors {
            match err {
                Error::CommandWorkerBusy | Error::QueryWorkersBusy => {
                    // Should implement retry logic
                }
                Error::NoQueryWorkers => {
                    // Configuration error
                }
                _ => panic!("Unexpected error variant"),
            }
        }
    }

    #[test]
    fn pattern_match_database_error() {
        let err: Result<()> = Err(Error::Database(rusqlite::Error::InvalidQuery));

        match err {
            Err(Error::Database(db_err)) => {
                // User can access underlying rusqlite error
                assert_eq!(db_err.to_string(), "Query is not read-only");
            }
            _ => panic!("Expected Database error"),
        }
    }

    #[test]
    fn pattern_match_internal_error() {
        let err: Result<()> = Err(Error::Internal("channel closed".to_string()));

        match err {
            Err(Error::Internal(msg)) => {
                assert_eq!(msg, "channel closed");
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[test]
    fn result_type_alias_works() {
        fn returns_result() -> Result<u32> {
            Ok(42)
        }

        let result = returns_result();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
