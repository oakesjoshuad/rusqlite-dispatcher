# Integration Guide: Using rusqlite-dispatcher with Repositories and Services

This guide shows how to integrate rusqlite-dispatcher with a domain-driven architecture like Sentinel.

## Quick Start

### 1. Define Domain Types

```rust
// commands.rs
#[derive(Debug, Clone)]
pub enum UserCommand {
    Create { id: u64, name: String, email: String },
    UpdateEmail { id: u64, email: String },
    Delete { id: u64 },
}

// queries.rs
#[derive(Debug, Clone)]
pub enum UserQuery {
    FindById { id: u64 },
    ListAll,
}

#[derive(Debug, Clone)]
pub struct User {
    pub id: u64,
    pub name: String,
    pub email: String,
}
```

### 2. Implement DomainHandler

```rust
#[derive(Debug, Clone)]
pub struct UserHandler;

impl DomainHandler for UserHandler {
    type Command = UserCommand;
    type Query = UserQuery;
    type QueryResponse = User;

    fn handle_command(
        &mut self,
        tx: &rusqlite::Transaction,
        command: Self::Command,
    ) -> std::result::Result<(), rusqlite::Error> {
        match command {
            UserCommand::Create { id, name, email } => {
                tx.execute(
                    "INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
                    rusqlite::params![id, name, email],
                )?;
            }
            // ... other commands
        }
        Ok(())
    }

    fn handle_query(
        &self,
        conn: &rusqlite::Connection,
        query: Self::Query,
    ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
        match query {
            UserQuery::FindById { id } => {
                conn.query_row(
                    "SELECT id, name, email FROM users WHERE id = ?",
                    rusqlite::params![id],
                    |row| {
                        Ok(User {
                            id: row.get(0)?,
                            name: row.get(1)?,
                            email: row.get(2)?,
                        })
                    },
                )
            }
            // ... other queries
        }
    }
}
```

### 3. Create Repository

**Key principle**: Repository owns SupervisorHandle, not Dispatcher.

```rust
use rusqlite_dispatcher::SupervisorHandle;

pub struct UserRepository {
    handle: SupervisorHandle<UserHandler>,
}

impl UserRepository {
    pub fn new(handle: SupervisorHandle<UserHandler>) -> Self {
        Self { handle }
    }

    pub fn create(&self, user: User) -> Result<()> {
        let (request, response) = Request::new(
            UserCommand::Create {
                id: user.id,
                name: user.name,
                email: user.email,
            }
        );

        self.handle
            .command_sender
            .try_send(request)
            .map_err(|_| Error::CommandWorkerBusy)?;

        response.recv()?
    }

    pub fn find_by_id(&self, id: u64) -> Result<Option<User>> {
        let (request, response) = Request::new(UserQuery::FindById { id });

        self.handle
            .query_sender
            .try_send(request)
            .map_err(|_| Error::QueryWorkersBusy)?;

        response.recv()?
    }
}
```

### 4. Initialize in Application

```rust
use rusqlite_dispatcher::{Dispatcher, DispatcherConfig};
use std::sync::Arc;

async fn initialize() -> Result<AppContext> {
    // Create dispatcher - application owns lifecycle
    let config = DispatcherConfig {
        supervisor: SupervisorConfig {
            query_workers: 4,
            channel_size: 1000,
        },
    };

    let dispatcher = Dispatcher::new(
        "app.db",
        UserHandler,
        config,
    )?;

    // Get handle for repositories
    let handle = dispatcher.handle();

    // Create repository
    let repository = Arc::new(UserRepository::new(handle));

    // Create services with repository
    let user_service = Arc::new(UserService::new(repository.clone()));
    let auth_service = Arc::new(AuthService::new(repository.clone()));

    // Store dispatcher for shutdown
    Ok(AppContext {
        dispatcher,
        services: Services {
            user: user_service,
            auth: auth_service,
        },
    })
}

pub struct AppContext {
    dispatcher: Dispatcher<UserHandler>,
    services: Services,
}

impl AppContext {
    pub async fn shutdown(self) -> Result<()> {
        // Graceful shutdown
        self.dispatcher.shutdown()
    }
}
```

### 5. Use in Application

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let app = initialize().await?;

    // Use services
    app.services.user.create_user(user).await?;
    let user = app.services.user.find_by_id(1).await?;

    // Explicit shutdown
    app.shutdown().await?;

    Ok(())
}
```

## Pattern: Service Architecture

```rust
pub struct UserService<R: UserRepository> {
    repository: Arc<R>,
}

impl<R: UserRepository> UserService<R> {
    pub fn new(repository: Arc<R>) -> Self {
        Self { repository }
    }

    pub async fn create_user(&self, user: CreateUserRequest) -> Result<User> {
        // Domain logic here
        let user = User::create(user.name, user.email)?;

        // Delegate to repository
        self.repository.create(user).await?;

        Ok(user)
    }

    pub async fn find_by_id(&self, id: u64) -> Result<Option<User>> {
        self.repository.find_by_id(id).await
    }
}
```

## Pattern: Multiple Handlers (Sentinel-like)

If you have multiple domains (users, permissions, roles, etc.):

```rust
// unified handler
#[derive(Debug, Clone)]
pub enum SentinelCommand {
    User(UserCommand),
    Permission(PermissionCommand),
    Role(RoleCommand),
}

#[derive(Debug, Clone)]
pub enum SentinelQuery {
    User(UserQuery),
    Permission(PermissionQuery),
    Role(RoleQuery),
}

pub struct SentinelHandler;

impl DomainHandler for SentinelHandler {
    type Command = SentinelCommand;
    type Query = SentinelQuery;
    type QueryResponse = SentinelResponse;

    fn handle_command(
        &mut self,
        tx: &rusqlite::Transaction,
        command: Self::Command,
    ) -> std::result::Result<(), rusqlite::Error> {
        match command {
            SentinelCommand::User(cmd) => handle_user_command(tx, cmd)?,
            SentinelCommand::Permission(cmd) => handle_permission_command(tx, cmd)?,
            SentinelCommand::Role(cmd) => handle_role_command(tx, cmd)?,
        }
        Ok(())
    }

    fn handle_query(
        &self,
        conn: &rusqlite::Connection,
        query: Self::Query,
    ) -> std::result::Result<Self::QueryResponse, rusqlite::Error> {
        match query {
            SentinelQuery::User(q) => Ok(SentinelResponse::User(handle_user_query(conn, q)?)),
            SentinelQuery::Permission(q) => Ok(SentinelResponse::Permission(handle_permission_query(conn, q)?)),
            SentinelQuery::Role(q) => Ok(SentinelResponse::Role(handle_role_query(conn, q)?)),
        }
    }
}

// Single dispatcher for all domains
let dispatcher = Dispatcher::new("db.db", SentinelHandler, config)?;

// Multiple repositories can use same handle
let user_repo = Arc::new(UserRepository::new(dispatcher.handle().clone()));
let perm_repo = Arc::new(PermissionRepository::new(dispatcher.handle().clone()));
let role_repo = Arc::new(RoleRepository::new(dispatcher.handle().clone()));
```

## Error Handling

### Command Execution Errors

```rust
pub fn create_user(&self, user: User) -> Result<()> {
    let (request, response) = Request::new(UserCommand::Create { ... });

    // Send error
    match self.handle.command_sender.try_send(request) {
        Ok(()) => {},
        Err(TrySendError::Full(_)) => return Err(Error::CommandWorkerBusy),
        Err(TrySendError::Disconnected(_)) => {
            return Err(Error::Internal("Workers disconnected"))
        }
    }

    // Response error
    match response.recv()? {
        Ok(()) => Ok(()),
        Err(db_err) => Err(Error::from(db_err)),
    }
}
```

### Backpressure Handling

When command queue is full:

```rust
// Option 1: Fail fast (recommended)
match sender.try_send(request) {
    Ok(()) => Ok(()),
    Err(TrySendError::Full(_)) => {
        Err(Error::Overloaded("Too many concurrent requests"))
    }
}

// Option 2: Retry with backoff
for attempt in 0..3 {
    match sender.try_send(request) {
        Ok(()) => break,
        Err(TrySendError::Full(_)) => {
            std::thread::sleep(Duration::from_millis(10 << attempt));
        }
        Err(TrySendError::Disconnected(_)) => {
            return Err(Error::Internal("Dispatcher died"));
        }
    }
}
```

## Testing

### Unit Tests

Test repositories and services independently:

```rust
#[tokio::test]
async fn test_create_user() -> Result<()> {
    let dispatcher = Dispatcher::new(":memory:", UserHandler, config)?;
    let repo = UserRepository::new(dispatcher.handle());

    repo.create(user).await?;
    let found = repo.find_by_id(user.id).await?;

    assert_eq!(found, Some(user));
    Ok(())
}
```

### Integration Tests

Test with real services:

```rust
#[tokio::test]
async fn test_user_service() -> Result<()> {
    let app = initialize().await?;

    let user = app.services.user.create_user(request).await?;
    assert!(user.id > 0);

    app.shutdown().await?;
    Ok(())
}
```

## Performance Tips

### Configure Channel Size

```rust
DispatcherConfig {
    supervisor: SupervisorConfig {
        query_workers: num_cpus::get(),  // Match CPU count
        channel_size: 10_000,             // Tune for load
    },
}
```

### Batch Operations

For bulk operations, send multiple commands:

```rust
pub fn create_many(&self, users: Vec<User>) -> Result<()> {
    for user in users {
        self.create(user)?;  // Sequential sends
    }
    Ok(())
}
```

### Use Query Workers

Ensure reads are distributed:

```rust
// Default: 1 query worker
// Recommended: 4-8 for typical loads

let config = DispatcherConfig {
    supervisor: SupervisorConfig {
        query_workers: 4,  // Increase for read-heavy workloads
        channel_size: 1000,
    },
};
```

## Migration from Arc<Dispatcher>

If you're currently using Arc<Dispatcher>:

### Before

```rust
let dispatcher = Arc::new(Dispatcher::new(path, handler, config)?);
let repo = Repository::new(dispatcher.clone());
let svc = Service::new(repo);
```

### After

```rust
let dispatcher = Dispatcher::new(path, handler, config)?;
let handle = dispatcher.handle();
let repo = Repository::new(handle.clone());
let svc = Service::new(Arc::new(repo));

// ... later ...
dispatcher.shutdown()?;
```

**Benefits**:
- Explicit lifecycle management
- Clearer ownership semantics
- No Arc overhead on dispatcher
- Guaranteed shutdown

## Checklist

- [ ] Define domain Command and Query types
- [ ] Implement DomainHandler
- [ ] Create Repository with SupervisorHandle
- [ ] Initialize Dispatcher in application
- [ ] Create services with Repository
- [ ] Call dispatcher.shutdown() on application exit
- [ ] Test with both sync and async (if using tokio feature)
- [ ] Configure channel size and worker count for your load
- [ ] Add error handling for backpressure
- [ ] Monitor tracing events for operations
