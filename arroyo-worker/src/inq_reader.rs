use futures::{
    ready,
    stream::{FusedStream, FuturesUnordered, StreamFuture},
    Stream, StreamExt,
};
use std::fmt::Debug;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pub struct InQReader<St> {
    inner: FuturesUnordered<StreamFuture<St>>,
}

impl<St: Debug> Debug for InQReader<St> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InQReader {{ ... }}")
    }
}

impl<St: Stream + Unpin> InQReader<St> {
    /// Constructs a new, empty `SelectAll`
    ///
    /// The returned `SelectAll` does not contain any streams and, in this
    /// state, `SelectAll::poll` will return `Poll::Ready(None)`.
    pub fn new() -> Self {
        Self {
            inner: FuturesUnordered::new(),
        }
    }

    /// Push a stream into the set.
    ///
    /// This function submits the given stream to the set for managing. This
    /// function will not call `poll` on the submitted stream. The caller must
    /// ensure that `SelectAll::poll` is called in order to receive task
    /// notifications.
    pub fn push(&mut self, stream: St) {
        self.inner.push(stream.into_future());
    }
}

impl<St: Stream + Unpin> Default for InQReader<St> {
    fn default() -> Self {
        Self::new()
    }
}

impl<St: Stream + Unpin> Stream for InQReader<St> {
    type Item = (St::Item, St);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.inner.poll_next_unpin(cx)) {
                Some((Some(item), remaining)) => {
                    return Poll::Ready(Some((item, remaining)));
                }
                Some((None, _)) => {
                    // `FuturesUnordered` thinks it isn't terminated
                    // because it yielded a Some.
                    // We do not return, but poll `FuturesUnordered`
                    // in the next loop iteration.
                    tracing::info!("hit this case");
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

impl<St: Stream + Unpin> FusedStream for InQReader<St> {
    fn is_terminated(&self) -> bool {
        self.inner.is_terminated()
    }
}
