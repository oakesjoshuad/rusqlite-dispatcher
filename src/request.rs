//! Request/response messaging for async command and query execution.

use tokio::sync::oneshot;

/// Request wrapper containing payload and response channel.
///
/// Enables async request/response pattern over channels. The sender creates
/// a request with the payload, dispatches it to a worker via channel, and
/// awaits the response via the oneshot receiver.
///
/// # Type Parameters
///
/// * `T` - Request payload type (Command or Query)
/// * `R` - Response type (Result from handler)
#[derive(Debug)]
pub(crate) struct Request<T, R> {
    pub(crate) payload: T,
    pub(crate) response: oneshot::Sender<R>,
}

impl<T, R> Request<T, R> {
    /// Create new request with payload and oneshot response channel.
    ///
    /// Returns the request (to send to worker) and receiver (to await response).
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let (request, response) = Request::new(command);
    /// sender.send(request).await?;
    /// let result = response.await?;
    /// ```
    pub(crate) fn new(payload: T) -> (Self, oneshot::Receiver<R>) {
        let (tx, rx) = oneshot::channel();
        (Request { payload, response: tx }, rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn request_response_roundtrip() {
        let (request, response) = Request::new(42u64);

        // Simulate worker sending response
        request.response.send(100u64).unwrap();

        // Dispatcher receives response
        let result = response.await.unwrap();
        assert_eq!(result, 100);
    }

    #[tokio::test]
    async fn request_response_handles_closed_channel() {
        let (request, response) = Request::<u64, u64>::new(42u64);

        // Drop request (worker disconnects)
        drop(request);

        // Dispatcher detects closed channel
        let result = response.await;
        assert!(result.is_err());
    }
}
