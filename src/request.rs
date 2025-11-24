//! Request/response wrapper for command and query execution.
//!
//! Provides request-response pattern with optional async support:
//! - Without tokio feature: sync bounded(1) channel (blocking receive)
//! - With tokio feature: tokio::sync::oneshot (async receive)

#[cfg(not(feature = "tokio"))]
use crossbeam_channel::{bounded, Receiver, RecvError, Sender};

#[cfg(feature = "tokio")]
use tokio::sync::oneshot::{channel, Receiver, Sender, error::RecvError};

/// Request wrapper containing command/query and response channel.
///
/// Workers receive requests, process them, and send responses back through
/// the response channel.
#[derive(Debug)]
pub(crate) struct Request<T, R = ()> {
    /// The command or query to execute.
    pub(crate) payload: T,
    /// Sender for response.
    pub(crate) response_tx: Sender<R>,
}

/// Response receiver for waiting on worker result.
///
/// Provides both sync and async receive methods depending on feature flags.
#[derive(Debug)]
pub(crate) struct ResponseReceiver<R> {
    rx: Receiver<R>,
}

impl<T, R> Request<T, R> {
    /// Create a new request with response channel.
    ///
    /// Returns the request and a receiver for waiting on the response.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let (request, response_rx) = Request::new(my_command);
    /// worker_tx.send(request)?;
    /// let result = response_rx.recv()?;  // or .await with tokio feature
    /// ```
    #[cfg(not(feature = "tokio"))]
    pub(crate) fn new(payload: T) -> (Self, ResponseReceiver<R>) {
        let (tx, rx) = bounded(1); // Bounded(1) acts as oneshot
        let request = Request {
            payload,
            response_tx: tx,
        };
        let response = ResponseReceiver { rx };
        (request, response)
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn new(payload: T) -> (Self, ResponseReceiver<R>) {
        let (tx, rx) = channel(); // Tokio oneshot
        let request = Request {
            payload,
            response_tx: tx,
        };
        let response = ResponseReceiver { rx };
        (request, response)
    }

    /// Send response back to caller.
    ///
    /// Used by workers to send results back through the response channel.
    /// Note: Workers may destructure the request and use response_tx directly,
    /// but this method is useful in tests.
    #[cfg(not(feature = "tokio"))]
    #[allow(dead_code)]
    pub(crate) fn respond(self, response: R) -> Result<(), crossbeam_channel::SendError<R>> {
        self.response_tx.send(response)
    }

    #[cfg(feature = "tokio")]
    #[allow(dead_code)]
    pub(crate) fn respond(self, response: R) -> Result<(), R> {
        self.response_tx.send(response)
    }
}

impl<R> ResponseReceiver<R> {
    /// Receive response (async when tokio feature enabled, blocking otherwise).
    ///
    /// # Without tokio feature
    /// Blocks the calling thread until worker sends response or channel is closed.
    ///
    /// # With tokio feature
    /// Awaits response asynchronously - yields to runtime while waiting, does not block thread.
    ///
    /// # Errors
    ///
    /// Returns `RecvError` if the sender was dropped before sending (worker died).
    #[cfg(not(feature = "tokio"))]
    pub(crate) fn recv(self) -> Result<R, RecvError> {
        self.rx.recv()
    }

    #[cfg(feature = "tokio")]
    pub(crate) async fn recv(self) -> Result<R, RecvError> {
        self.rx.await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "tokio"))]
    #[test]
    fn request_response_roundtrip() {
        let (request, response_rx) = Request::new(42u64);

        // Simulate worker processing
        std::thread::spawn(move || {
            let result = request.payload * 2;
            request.respond(result).unwrap();
        });

        // Caller receives response (blocking)
        let result = response_rx.recv().unwrap();
        assert_eq!(result, 84);
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn request_response_roundtrip() {
        let (request, response_rx) = Request::new(42u64);

        // Simulate worker processing
        tokio::spawn(async move {
            let result = request.payload * 2;
            request.respond(result).unwrap();
        });

        // Caller receives response (async)
        let result = response_rx.recv().await.unwrap();
        assert_eq!(result, 84);
    }

    #[test]
    fn request_response_handles_closed_channel() {
        let (request, _response_rx) = Request::<u64, u64>::new(42u64);

        // Drop receiver (simulate caller disconnect)
        drop(_response_rx);

        // Worker tries to respond
        let result = request.respond(84);
        assert!(result.is_err());
    }
}
