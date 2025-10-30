use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::Stream; // trait for .next()

/// A generic broadcast-based stream abstraction supporting multiple subscribers.
pub struct BroadcastStreamSource<T: Clone + Send + 'static> {
    sender: broadcast::Sender<T>,
}

impl<T: Clone + Send + 'static> BroadcastStreamSource<T> {
    /// Create a new broadcast stream with a bounded buffer.
    pub fn new(buffer: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer);
        Self { sender }
    }

    /// Publish a value into the stream.
    pub fn publish(&self, value: T) {
        // If there are no subscribers, send() returns an Err, which we simply ignore.
        let _ = self.sender.send(value);
    }

    /// Subscribe to the stream and receive an **async** stream of values.
    pub fn subscribe_async(&self) -> impl Stream<Item = Result<T, tokio_stream::wrappers::errors::BroadcastStreamRecvError>> {
        BroadcastStream::new(self.sender.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn both_consumers_receive_all_messages() {
        let stream = BroadcastStreamSource::new(8);

        let mut s1 = stream.subscribe_async();
        let mut s2 = stream.subscribe_async();

        stream.publish("a");
        stream.publish("b");

        assert_eq!(s1.next().await.unwrap().unwrap(), "a");
        assert_eq!(s1.next().await.unwrap().unwrap(), "b");

        assert_eq!(s2.next().await.unwrap().unwrap(), "a");
        assert_eq!(s2.next().await.unwrap().unwrap(), "b");
    }
}
