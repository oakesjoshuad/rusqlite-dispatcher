// Internal modules
mod channel;
mod clock;
mod pool;
mod request;

// Crate-internal re-exports
pub(crate) use clock::{Duration, Timestamp};
pub(crate) use request::Request;

// Public modules
pub mod error;
pub mod handler;

// Public re-exports
pub use error::{Error, Result};
pub use handler::DomainHandler;
