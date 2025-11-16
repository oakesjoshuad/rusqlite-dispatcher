//! Domain handler trait defining the boundary between dispatcher and user code.
//!
//! This trait allows users to integrate their domain logic with the dispatcher
//! infrastructure. Handlers receive direct access to rusqlite's Transaction
//! and Connection types for maximum flexibility and performance.

/// Domain handler trait for command and query execution.
///
/// Implementors define how commands modify state and how queries read state.
/// The trait provides direct access to rusqlite types, enabling users to
/// leverage the full SQLite API without abstraction overhead.
///
/// # Design Philosophy
///
/// This trait intentionally exposes rusqlite types (Transaction, Connection)
/// rather than wrapping them in abstractions. This provides:
///
/// - **Full API access**: Use `prepare_cached()`, named parameters, blobs, etc.
/// - **Zero-cost abstraction**: No wrapper overhead
/// - **Transaction safety**: Worker manages commit/rollback automatically
/// - **Type safety**: CQRS separation at type level
///
/// # Examples
///
/// ```
/// use rusqlite_dispatcher::DomainHandler;
///
/// #[derive(Clone)]
/// struct MyHandler;
///
/// impl DomainHandler for MyHandler {
///     type Command = MyCommand;
///     type Query = MyQuery;
///     type QueryResponse = MyResponse;
///
///     fn handle_command(
///         &mut self,
///         tx: &rusqlite::Transaction,
///         command: Self::Command,
///     ) -> Result<(), rusqlite::Error> {
///         // Direct rusqlite API access
///         match command {
///             MyCommand::CreateUser { email } => {
///                 tx.execute(
///                     "INSERT INTO users (email) VALUES (?1)",
///                     [email],
///                 )?;
///             }
///         }
///         Ok(())
///     }
///
///     fn handle_query(
///         &self,
///         conn: &rusqlite::Connection,
///         query: Self::Query,
///     ) -> Result<Self::QueryResponse, rusqlite::Error> {
///         // Full rusqlite API available
///         match query {
///             MyQuery::GetUser { user_id } => {
///                 let mut stmt = conn.prepare_cached(
///                     "SELECT * FROM users WHERE id = :id"
///                 )?;
///
///                 let user = stmt.query_row(
///                     &[(":id", &user_id)],
///                     |row| {
///                         Ok(MyResponse {
///                             id: row.get(0)?,
///                             email: row.get(1)?,
///                         })
///                     }
///                 )?;
///
///                 Ok(user)
///             }
///         }
///     }
/// }
/// # #[derive(Clone)]
/// # enum MyCommand {
/// #     CreateUser { email: String },
/// # }
/// # #[derive(Clone)]
/// # enum MyQuery {
/// #     GetUser { user_id: i64 },
/// # }
/// # #[derive(Clone)]
/// # struct MyResponse {
/// #     id: i64,
/// #     email: String,
/// # }
/// ```
pub trait DomainHandler: Send + Clone + 'static {
    /// Command type for write operations.
    ///
    /// Commands modify state and are processed serially by a single worker.
    type Command: Send + Clone + 'static;

    /// Query type for read operations.
    ///
    /// Queries read state and can be processed concurrently by N workers.
    type Query: Send + Clone + 'static;

    /// Response type for query results.
    type QueryResponse: Send + Clone + 'static;

    /// Handle command with automatic transaction management.
    ///
    /// The worker automatically commits on `Ok(())` or rolls back on `Err`.
    /// Handlers focus on business logic without managing transaction lifecycle.
    ///
    /// # Arguments
    ///
    /// * `tx` - Active database transaction (automatically managed)
    /// * `command` - Command to execute
    ///
    /// # Returns
    ///
    /// - `Ok(())` - Command succeeded, transaction will commit
    /// - `Err(rusqlite::Error)` - Command failed, transaction will rollback
    fn handle_command(
        &mut self,
        tx: &rusqlite::Transaction,
        command: Self::Command,
    ) -> std::result::Result<(), rusqlite::Error>;

    /// Handle query without transaction overhead.
    ///
    /// Queries have read-only access and do not need transaction management.
    /// Multiple queries can execute concurrently via the query worker pool.
    ///
    /// # Arguments
    ///
    /// * `conn` - Database connection (read-only)
    /// * `query` - Query to execute
    ///
    /// # Returns
    ///
    /// - `Ok(response)` - Query succeeded with result
    /// - `Err(rusqlite::Error)` - Query failed
    fn handle_query(
        &self,
        conn: &rusqlite::Connection,
        query: Self::Query,
    ) -> std::result::Result<Self::QueryResponse, rusqlite::Error>;
}
