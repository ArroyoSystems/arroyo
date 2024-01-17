use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use arroyo_types::Key;
use futures::ready;
use futures::{Future, FutureExt, StreamExt};
use tokio_stream::Stream;

pub struct CloneableStreamFuture<St: Stream + Unpin> {
    stream: Arc<Mutex<Option<St>>>,
}

impl<St: Stream + Unpin> Clone for CloneableStreamFuture<St> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

impl<St: Stream + Unpin> Future for CloneableStreamFuture<St> {
    type Output = Option<(St::Item, Self)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self
            .stream
            .try_lock()
            .expect("mutex should stay in a single execution sequence");

        let stream_option = guard.as_mut();
        if let Some(stream) = stream_option {
            let item = ready!(stream.poll_next_unpin(cx));
            match item {
                Some(batch) => {
                    let next_future = self.clone();
                    Poll::Ready(Some((batch, next_future)))
                }
                None => {
                    *guard = None;
                    Poll::Ready(None)
                }
            }
        } else {
            // Stream is already finished
            Poll::Ready(None)
        }
    }
}

pub struct KeyedCloneableStreamFuture<K, St: Stream + Unpin> {
    key: K,
    // Wrap CloneableStreamFuture inside KeyedCloneableStreamFuture.
    future: CloneableStreamFuture<St>,
}

impl<K: Copy, St: Stream + Unpin> KeyedCloneableStreamFuture<K, St> {
    pub fn new(key: K, stream: St) -> Self {
        Self {
            key,
            future: CloneableStreamFuture {
                stream: Arc::new(Mutex::new(Some(stream))),
            },
        }
    }
}

impl<K: Copy, St: Stream + Unpin> Clone for KeyedCloneableStreamFuture<K, St> {
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            future: self.future.clone(),
        }
    }
}

impl<K: Copy, St: Stream + Unpin> Future for KeyedCloneableStreamFuture<K, St> {
    type Output = (K, Option<(St::Item, Self)>);

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let key = self.key;
        let future = unsafe { self.map_unchecked_mut(|s| &mut s.future) };

        // Now you can safely call poll on the pinned future.
        match ready!(future.poll(cx)) {
            Some((item, next_future)) => {
                let next_keyed_future = Self {
                    key,
                    future: next_future,
                };
                Poll::Ready((key, Some((item, next_keyed_future))))
            }
            None => Poll::Ready((key, None)),
        }
    }
}
