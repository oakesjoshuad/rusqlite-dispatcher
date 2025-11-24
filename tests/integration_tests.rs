//! Integration tests for dispatcher lifecycle and handle pattern
//!
//! These tests demonstrate real-world usage patterns based on the sentinel
//! domain repository implementation.
//!
//! Tests are feature-gated to support both sync (default) and async (tokio) APIs.

use rusqlite_dispatcher::{Dispatcher, DispatcherConfig, DomainHandler, Result, SupervisorHandle};
use std::sync::Arc;

// ============================================================================
// TEST DOMAIN: Simple notes application
// ============================================================================

/// Domain commands for notes operations
#[derive(Debug, Clone)]
pub enum NoteCommand {
    CreateTable,
    Create {
        id: u64,
        title: String,
        content: String,
    },
    Update {
        id: u64,
        title: String,
        content: String,
    },
    Delete {
        id: u64,
    },
}

/// Domain queries for notes operations
#[derive(Debug, Clone)]
pub enum NoteQuery {
    FindById { id: u64 },
    ListAll,
    Count,
}

/// Query response containing a single note
#[derive(Debug, Clone, PartialEq)]
pub struct Note {
    pub id: u64,
    pub title: String,
    pub content: String,
}

/// Query response type
#[derive(Debug, Clone, PartialEq)]
pub enum NoteQueryResponse {
    Note(Option<Note>),
    Notes(Vec<Note>),
    Count(u64),
}

/// Handler implementing the domain operations
#[derive(Debug, Clone)]
pub struct NoteHandler;

impl DomainHandler for NoteHandler {
    type Command = NoteCommand;
    type Query = NoteQuery;
    type QueryResponse = NoteQueryResponse;

    fn handle_command(
        &mut self,
        tx: &rusqlite::Transaction,
        command: Self::Command,
    ) -> std::result::Result<(), rusqlite::Error> {
        match command {
            NoteCommand::CreateTable => {
                tx.execute(
                    "CREATE TABLE IF NOT EXISTS notes (
                        id INTEGER PRIMARY KEY,
                        title TEXT NOT NULL,
                        content TEXT NOT NULL
                    )",
                    [],
                )?;
            }
            NoteCommand::Create { id, title, content } => {
                tx.execute(
                    "INSERT INTO notes (id, title, content) VALUES (?, ?, ?)",
                    rusqlite::params![id, title, content],
                )?;
            }
            NoteCommand::Update { id, title, content } => {
                tx.execute(
                    "UPDATE notes SET title = ?, content = ? WHERE id = ?",
                    rusqlite::params![title, content, id],
                )?;
            }
            NoteCommand::Delete { id } => {
                tx.execute("DELETE FROM notes WHERE id = ?", rusqlite::params![id])?;
            }
        }
        Ok(())
    }

    fn handle_query(
        &self,
        conn: &rusqlite::Connection,
        query: Self::Query,
    ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
        match query {
            NoteQuery::FindById { id } => {
                let result = conn.query_row(
                    "SELECT id, title, content FROM notes WHERE id = ? LIMIT 1",
                    rusqlite::params![id],
                    |row| {
                        Ok(Note {
                            id: row.get(0)?,
                            title: row.get(1)?,
                            content: row.get(2)?,
                        })
                    },
                );
                match result {
                    Ok(note) => Ok(NoteQueryResponse::Note(Some(note))),
                    Err(rusqlite::Error::QueryReturnedNoRows) => Ok(NoteQueryResponse::Note(None)),
                    Err(e) => Err(e),
                }
            }
            NoteQuery::ListAll => {
                let mut stmt = conn.prepare("SELECT id, title, content FROM notes")?;
                let notes = stmt.query_map([], |row| {
                    Ok(Note {
                        id: row.get(0)?,
                        title: row.get(1)?,
                        content: row.get(2)?,
                    })
                })?;

                let mut result = Vec::new();
                for note in notes {
                    result.push(note?);
                }
                Ok(NoteQueryResponse::Notes(result))
            }
            NoteQuery::Count => {
                let count: u64 =
                    conn.query_row("SELECT COUNT(*) FROM notes", [], |row| row.get(0))?;
                Ok(NoteQueryResponse::Count(count))
            }
        }
    }
}

// ============================================================================
// REPOSITORY: Mirrors sentinel Repository pattern
// ============================================================================

/// Repository using SupervisorHandle (NOT Dispatcher)
///
/// This demonstrates the correct separation: Repository only needs
/// the handle for operations, not the dispatcher for lifecycle.
#[allow(dead_code)]
pub struct NoteRepository {
    handle: SupervisorHandle<NoteHandler>,
}

impl NoteRepository {
    /// Create repository from handle
    pub fn new(handle: SupervisorHandle<NoteHandler>) -> Self {
        Self { handle }
    }
}

// ============================================================================
// SYNC TESTS (default feature)
// ============================================================================

#[cfg(not(feature = "tokio"))]
mod sync_tests {
    use super::*;

    #[test]
    fn test_dispatcher_creation_and_handle() -> Result<()> {
        let db_path = "/tmp/test_dispatcher_simple.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        // Get handle from dispatcher
        let handle = dispatcher.handle();

        // Verify handle is cloneable
        let handle_clone = handle.clone();
        let handle_clone2 = handle.clone();

        drop(handle_clone);
        drop(handle_clone2);

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_repository_with_handle() -> Result<()> {
        let db_path = "/tmp/test_repo_simple.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;
        let handle = dispatcher.handle();

        // Create repository with handle
        let repo = Arc::new(NoteRepository::new(handle));

        // Clone repo (like services do)
        let repo_clone = repo.clone();
        drop(repo_clone);

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_dispatcher_lifecycle() -> Result<()> {
        let db_path = "/tmp/test_lifecycle_simple.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        let handle = dispatcher.handle();
        drop(handle);

        dispatcher.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_handle_is_cloneable() -> Result<()> {
        let db_path = "/tmp/test_clone_simple.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;
        let handle = dispatcher.handle();

        let handle1 = handle.clone();
        let handle2 = handle.clone();
        let handle3 = handle.clone();

        drop(handle1);
        drop(handle2);
        drop(handle3);

        let _still_works = handle.clone();

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_multiple_services_share_handle() -> Result<()> {
        let db_path = "/tmp/test_services.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;
        let handle = dispatcher.handle();

        let repo1 = Arc::new(NoteRepository::new(handle.clone()));
        let repo2 = Arc::new(NoteRepository::new(handle.clone()));
        let repo3 = Arc::new(NoteRepository::new(handle.clone()));

        let repo1_clone = repo1.clone();
        let repo2_clone = repo2.clone();
        let repo3_clone = repo3.clone();

        drop(repo1_clone);
        drop(repo2_clone);
        drop(repo3_clone);

        drop(handle);
        drop(repo1);
        drop(repo2);
        drop(repo3);

        dispatcher.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_command_execution() -> Result<()> {
        let db_path = "/tmp/test_cmd_exec.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        // Create table
        dispatcher.execute_command(NoteCommand::CreateTable)?;

        // Create notes
        dispatcher.execute_command(NoteCommand::Create {
            id: 1,
            title: "First".into(),
            content: "Content 1".into(),
        })?;
        dispatcher.execute_command(NoteCommand::Create {
            id: 2,
            title: "Second".into(),
            content: "Content 2".into(),
        })?;

        // Verify count
        let response = dispatcher.execute_query(NoteQuery::Count)?;
        assert_eq!(response, NoteQueryResponse::Count(2));

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_query_execution() -> Result<()> {
        let db_path = "/tmp/test_qry_exec.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        dispatcher.execute_command(NoteCommand::CreateTable)?;
        dispatcher.execute_command(NoteCommand::Create {
            id: 1,
            title: "Test".into(),
            content: "Test Content".into(),
        })?;

        // Query by ID
        let response = dispatcher.execute_query(NoteQuery::FindById { id: 1 })?;
        match response {
            NoteQueryResponse::Note(Some(note)) => {
                assert_eq!(note.id, 1);
                assert_eq!(note.title, "Test");
                assert_eq!(note.content, "Test Content");
            }
            _ => panic!("Expected Note response"),
        }

        // Query non-existent
        let response = dispatcher.execute_query(NoteQuery::FindById { id: 999 })?;
        assert_eq!(response, NoteQueryResponse::Note(None));

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[test]
    fn test_update_and_delete() -> Result<()> {
        let db_path = "/tmp/test_update_delete.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        dispatcher.execute_command(NoteCommand::CreateTable)?;
        dispatcher.execute_command(NoteCommand::Create {
            id: 1,
            title: "Original".into(),
            content: "Original Content".into(),
        })?;

        // Update
        dispatcher.execute_command(NoteCommand::Update {
            id: 1,
            title: "Updated".into(),
            content: "Updated Content".into(),
        })?;

        let response = dispatcher.execute_query(NoteQuery::FindById { id: 1 })?;
        match response {
            NoteQueryResponse::Note(Some(note)) => {
                assert_eq!(note.title, "Updated");
                assert_eq!(note.content, "Updated Content");
            }
            _ => panic!("Expected Note response"),
        }

        // Delete
        dispatcher.execute_command(NoteCommand::Delete { id: 1 })?;
        let response = dispatcher.execute_query(NoteQuery::Count)?;
        assert_eq!(response, NoteQueryResponse::Count(0));

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }
}

// ============================================================================
// ASYNC TESTS (tokio feature)
// ============================================================================

#[cfg(feature = "tokio")]
mod async_tests {
    use super::*;

    #[tokio::test]
    async fn test_dispatcher_creation_and_handle() -> Result<()> {
        let db_path = "/tmp/test_dispatcher_simple_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        let handle = dispatcher.handle();
        let handle_clone = handle.clone();
        let handle_clone2 = handle.clone();

        drop(handle_clone);
        drop(handle_clone2);

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_repository_with_handle() -> Result<()> {
        let db_path = "/tmp/test_repo_simple_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;
        let handle = dispatcher.handle();

        let repo = Arc::new(NoteRepository::new(handle));
        let repo_clone = repo.clone();
        drop(repo_clone);

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_dispatcher_lifecycle() -> Result<()> {
        let db_path = "/tmp/test_lifecycle_simple_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        let handle = dispatcher.handle();
        drop(handle);

        dispatcher.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_handle_is_cloneable() -> Result<()> {
        let db_path = "/tmp/test_clone_simple_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;
        let handle = dispatcher.handle();

        let handle1 = handle.clone();
        let handle2 = handle.clone();
        let handle3 = handle.clone();

        drop(handle1);
        drop(handle2);
        drop(handle3);

        let _still_works = handle.clone();

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_services_share_handle() -> Result<()> {
        let db_path = "/tmp/test_services_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;
        let handle = dispatcher.handle();

        let repo1 = Arc::new(NoteRepository::new(handle.clone()));
        let repo2 = Arc::new(NoteRepository::new(handle.clone()));
        let repo3 = Arc::new(NoteRepository::new(handle.clone()));

        let repo1_clone = repo1.clone();
        let repo2_clone = repo2.clone();
        let repo3_clone = repo3.clone();

        drop(repo1_clone);
        drop(repo2_clone);
        drop(repo3_clone);

        drop(handle);
        drop(repo1);
        drop(repo2);
        drop(repo3);

        dispatcher.shutdown()?;

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_command_execution() -> Result<()> {
        let db_path = "/tmp/test_cmd_exec_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        // Create table
        dispatcher.execute_command(NoteCommand::CreateTable).await?;

        // Create notes
        dispatcher
            .execute_command(NoteCommand::Create {
                id: 1,
                title: "First".into(),
                content: "Content 1".into(),
            })
            .await?;
        dispatcher
            .execute_command(NoteCommand::Create {
                id: 2,
                title: "Second".into(),
                content: "Content 2".into(),
            })
            .await?;

        // Verify count
        let response = dispatcher.execute_query(NoteQuery::Count).await?;
        assert_eq!(response, NoteQueryResponse::Count(2));

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_query_execution() -> Result<()> {
        let db_path = "/tmp/test_qry_exec_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        dispatcher.execute_command(NoteCommand::CreateTable).await?;
        dispatcher
            .execute_command(NoteCommand::Create {
                id: 1,
                title: "Test".into(),
                content: "Test Content".into(),
            })
            .await?;

        // Query by ID
        let response = dispatcher
            .execute_query(NoteQuery::FindById { id: 1 })
            .await?;
        match response {
            NoteQueryResponse::Note(Some(note)) => {
                assert_eq!(note.id, 1);
                assert_eq!(note.title, "Test");
                assert_eq!(note.content, "Test Content");
            }
            _ => panic!("Expected Note response"),
        }

        // Query non-existent
        let response = dispatcher
            .execute_query(NoteQuery::FindById { id: 999 })
            .await?;
        assert_eq!(response, NoteQueryResponse::Note(None));

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }

    #[tokio::test]
    async fn test_update_and_delete() -> Result<()> {
        let db_path = "/tmp/test_update_delete_async.db".to_string();
        let _ = std::fs::remove_file(&db_path);

        let config = DispatcherConfig::default();
        let dispatcher = Dispatcher::new(&db_path, NoteHandler, config)?;

        dispatcher.execute_command(NoteCommand::CreateTable).await?;
        dispatcher
            .execute_command(NoteCommand::Create {
                id: 1,
                title: "Original".into(),
                content: "Original Content".into(),
            })
            .await?;

        // Update
        dispatcher
            .execute_command(NoteCommand::Update {
                id: 1,
                title: "Updated".into(),
                content: "Updated Content".into(),
            })
            .await?;

        let response = dispatcher
            .execute_query(NoteQuery::FindById { id: 1 })
            .await?;
        match response {
            NoteQueryResponse::Note(Some(note)) => {
                assert_eq!(note.title, "Updated");
                assert_eq!(note.content, "Updated Content");
            }
            _ => panic!("Expected Note response"),
        }

        // Delete
        dispatcher
            .execute_command(NoteCommand::Delete { id: 1 })
            .await?;
        let response = dispatcher.execute_query(NoteQuery::Count).await?;
        assert_eq!(response, NoteQueryResponse::Count(0));

        dispatcher.shutdown()?;
        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }
}
