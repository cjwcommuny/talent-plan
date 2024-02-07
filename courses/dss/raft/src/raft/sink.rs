use futures::sink::Sink;
use futures::task::Poll;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug)]
pub enum SinkError {
    ChannelClosed,
    SendFailed,
}
/// Wraps an UnboundedSender in a Sink
pub struct UnboundedSenderSink<T> {
    sender: Option<UnboundedSender<T>>,
}

impl<T> UnboundedSenderSink<T> {
    fn new(sender: Option<UnboundedSender<T>>) -> Self {
        Self { sender }
    }

    fn sender_if_open(&mut self) -> Option<&UnboundedSender<T>> {
        match &self.sender {
            None => None,
            Some(sender) => {
                if sender.is_closed() {
                    // drop the actual sender, leaving an empty option
                    drop(self.sender.take());
                    None
                } else {
                    self.sender.as_ref()
                }
            }
        }
    }
    fn ok_unless_closed(&mut self) -> std::task::Poll<std::result::Result<(), SinkError>> {
        Poll::Ready(
            self.sender_if_open()
                .map(|_| ())
                .ok_or(SinkError::ChannelClosed),
        )
    }
}

impl<T> Unpin for UnboundedSenderSink<T> {}

impl<T> From<UnboundedSender<T>> for UnboundedSenderSink<T> {
    fn from(sender: UnboundedSender<T>) -> Self {
        UnboundedSenderSink {
            sender: Some(sender),
        }
    }
}

impl<T> Sink<T> for UnboundedSenderSink<T> {
    type Error = SinkError;
    fn poll_ready(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), SinkError>> {
        self.ok_unless_closed()
    }

    fn start_send(
        mut self: std::pin::Pin<&mut Self>,
        item: T,
    ) -> std::result::Result<(), SinkError> {
        self.sender_if_open()
            .map(|sender| sender.send(item).map_err(|_| SinkError::SendFailed))
            .unwrap_or_else(|| Err(SinkError::ChannelClosed))
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), SinkError>> {
        self.ok_unless_closed()
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), SinkError>> {
        //drop the sender
        self.sender.take();
        Poll::Ready(Ok(()))
    }
}

impl<T> Clone for UnboundedSenderSink<T>
where
    UnboundedSender<T>: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.sender.clone())
    }
}

#[cfg(test)]
mod test {
    use futures::sink::SinkExt;
    use futures::FutureExt;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn it_sends_to_the_sender() {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let mut sink = super::UnboundedSenderSink::from(sender);

        sink.send("hello").await.expect("Send failed");

        let result = receiver.recv().now_or_never();

        assert!(matches!(result, Some(Some("hello"))));
    }

    #[tokio::test]
    async fn it_sends_multiple_times() {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let mut sink = super::UnboundedSenderSink::from(sender);

        sink.send("hello").await.expect("Send failed");
        sink.send("bye").await.expect("Send failed");

        let result = receiver.recv().now_or_never();

        assert!(matches!(result, Some(Some("hello"))));

        let result = receiver.recv().now_or_never();

        assert!(matches!(result, Some(Some("bye"))));
    }

    #[tokio::test]
    async fn it_closes_the_sender() {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let mut sink = super::UnboundedSenderSink::from(sender);

        sink.send("hello").await.expect("Send failed");

        let result = receiver.recv().now_or_never();

        assert!(matches!(result, Some(Some("hello"))));

        sink.close().await.expect("Close failed");

        let result = receiver.recv().now_or_never();

        assert!(matches!(result, Some(None)));
    }

    #[tokio::test]
    async fn it_fails_if_receiver_closed() {
        let (sender, mut receiver) = mpsc::unbounded_channel();

        let mut sink = super::UnboundedSenderSink::from(sender);

        sink.send("hello").await.expect("Send failed");

        let result = receiver.recv().now_or_never();

        assert!(matches!(result, Some(Some("hello"))));

        receiver.close();

        assert!(sink.send("Fails").await.is_err());
    }
}
