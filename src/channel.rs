//! Channel type aliases for worker communication.
//!
//! Uses crossbeam-channel for high-performance, lock-free message passing
//! between dispatcher and worker threads.

use crossbeam_channel as channel;

use crate::error::Result;
use crate::handler::DomainHandler;
use crate::request::Request;

/// Command sender type (to single command worker).
pub(crate) type CommandSender<H> = channel::Sender<CommandRequest<H>>;

/// Command receiver type (command worker receives).
pub(crate) type CommandReceiver<H> = channel::Receiver<CommandRequest<H>>;

/// Command request type with response channel.
pub(crate) type CommandRequest<H> = Request<<H as DomainHandler>::Command, Result<()>>;

/// Query sender type (to one of N query workers).
pub(crate) type QuerySender<H> = channel::Sender<QueryRequest<H>>;

/// Query receiver type (query worker receives).
pub(crate) type QueryReceiver<H> = channel::Receiver<QueryRequest<H>>;

/// Query request type with response channel.
pub(crate) type QueryRequest<H> =
    Request<<H as DomainHandler>::Query, Result<<H as DomainHandler>::QueryResponse>>;
