// Internal modules
mod channel;
mod pool;
mod request;
mod supervisor;

// Crate-internal re-exports
pub(crate) use request::Request;

// Public modules
pub mod error;
pub mod handler;

// Public API
pub mod dispatcher;

// Public re-exports
pub use dispatcher::{Dispatcher, DispatcherConfig, SupervisorHandle};
pub use error::{Error, Result};
pub use handler::DomainHandler;
