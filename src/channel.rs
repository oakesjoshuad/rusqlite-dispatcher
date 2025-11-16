//! Channel type aliases for worker communication.

use crate::handler::DomainHandler;
use crate::request::Request;

/// Command sender type (to single command worker).
pub(crate) type CommandSender<H> = tokio::sync::mpsc::Sender<CommandRequest<H>>;

/// Command receiver type (command worker receives).
pub(crate) type CommandReceiver<H> = tokio::sync::mpsc::Receiver<CommandRequest<H>>;

/// Command request type with response channel.
pub(crate) type CommandRequest<H> =
    Request<<H as DomainHandler>::Command, Result<(), rusqlite::Error>>;

/// Query sender type (to one of N query workers).
pub(crate) type QuerySender<H> = tokio::sync::mpsc::Sender<QueryRequest<H>>;

/// Query receiver type (query worker receives).
pub(crate) type QueryReceiver<H> = tokio::sync::mpsc::Receiver<QueryRequest<H>>;

/// Query request type with response channel.
pub(crate) type QueryRequest<H> = Request<
    <H as DomainHandler>::Query,
    Result<<H as DomainHandler>::QueryResponse, rusqlite::Error>,
>;

/// Control sender for supervisor shutdown signal.
pub(crate) type ControlSender = tokio::sync::mpsc::Sender<()>;

/// Control receiver for supervisor shutdown signal.
pub(crate) type ControlReceiver = tokio::sync::mpsc::Receiver<()>;
