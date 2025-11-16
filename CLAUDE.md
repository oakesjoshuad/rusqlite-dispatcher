# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**rusqlite-dispatcher** - A lightweight rusqlite extension providing concurrent command/query execution through a worker pool architecture. Built for applications requiring SQLite persistence with CQRS semantics.

This is a Rust library crate designed to be thread-safe and support the Command Query Responsibility Segregation (CQRS) pattern with SQLite.

## Development Commands

### Build
```bash
cargo build          # Debug build
cargo build --release # Release build
```

### Testing
```bash
cargo test           # Run all tests
cargo test <name>    # Run specific test
cargo test -- --nocapture # Run tests with output
```

### Documentation
```bash
cargo doc --open     # Generate and open documentation
```

### Code Quality
```bash
cargo clippy         # Run linter
cargo fmt            # Format code
```

## Git Workflow

### Branch Naming Convention
Use type-prefixed branches for all new work:

**Format**: `<type>/<description>`

**Types**:
- `feat/` - New features or functionality
- `fix/` - Bug fixes
- `refactor/` - Code restructuring without changing behavior
- `docs/` - Documentation changes
- `test/` - Adding or modifying tests

**Examples**:
- `feat/dispatcher-core`
- `feat/worker-pool`
- `fix/connection-leak`
- `refactor/error-handling`

### Merge Strategy
Use **rebase and fast-forward** for clean linear history:

```bash
# On feature branch, rebase onto latest development
git checkout feat/my-feature
git rebase development

# Switch to development and fast-forward merge
git checkout development
git merge --ff-only feat/my-feature

# Delete merged branch
git branch -d feat/my-feature
```

This maintains a linear commit history since this is primarily solo work.

## Architecture Concepts

### CQRS Pattern
This library is designed to support Command Query Responsibility Segregation:
- **Commands**: Write operations that modify state (INSERT, UPDATE, DELETE)
- **Queries**: Read operations that don't modify state (SELECT)

Separating these allows different optimization strategies and concurrency models for reads vs writes.

### Worker Pool Architecture
The library uses a worker pool pattern to handle concurrent SQLite access:
- SQLite has limitations around concurrent writes
- Worker pool coordinates access to ensure thread safety
- Commands and queries can be dispatched to the pool for execution

### Thread Safety with SQLite
Key considerations when implementing:
- SQLite connections are not thread-safe by default
- Multiple readers can operate concurrently, but writes must be serialized
- The dispatcher pattern helps manage these constraints at the application level
