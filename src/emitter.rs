//! Event dispatching: callbacks and async streams.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::task::{Context, Poll};

use futures_util::Stream;
use tokio::sync::mpsc;

use crate::events::{Event, EventKind};

/// Identifies a registered listener, to remove it with `off`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenerId(u64);

impl ListenerId {
    #[cfg(feature = "legacy")]
    pub(crate) fn from_raw(id: u64) -> Self {
        ListenerId(id)
    }
}

type Handler = Arc<dyn Fn(&Event) + Send + Sync>;

struct Listener {
    id: ListenerId,
    kind: Option<EventKind>,
    once: bool,
    handler: Handler,
}

#[derive(Default)]
pub(crate) struct Emitter {
    listeners: RwLock<Vec<Listener>>,
    subscribers: Mutex<Vec<mpsc::UnboundedSender<Event>>>,
    next_id: AtomicU64,
}

impl Emitter {
    pub(crate) fn add(&self, kind: Option<EventKind>, once: bool, handler: Handler) -> ListenerId {
        let id = ListenerId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.listeners.write().unwrap_or_else(std::sync::PoisonError::into_inner).push(Listener { id, kind, once, handler });
        id
    }

    pub(crate) fn remove(&self, id: ListenerId) -> bool {
        let mut listeners = self.listeners.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = listeners.len();
        listeners.retain(|l| l.id != id);
        listeners.len() != before
    }

    pub(crate) fn remove_all(&self, kind: Option<EventKind>) {
        let mut listeners = self.listeners.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        match kind {
            Some(kind) => listeners.retain(|l| l.kind != Some(kind)),
            None => listeners.clear(),
        }
    }

    pub(crate) fn subscribe(&self) -> EventStream {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(tx);
        EventStream { rx }
    }

    /// Number of callbacks that would receive an event of `kind` (catch-all listeners included).
    pub(crate) fn listener_count(&self, kind: EventKind) -> usize {
        let listeners = self.listeners.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        listeners.iter().filter(|l| l.kind.is_none_or(|k| k == kind)).count()
    }

    /// Whether anybody (callback or stream) would observe an event of `kind`.
    pub(crate) fn is_observed(&self, kind: EventKind) -> bool {
        if self.listener_count(kind) > 0 {
            return true;
        }
        let mut subscribers = self.subscribers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        subscribers.retain(|tx| !tx.is_closed());
        !subscribers.is_empty()
    }

    pub(crate) fn emit(&self, event: Event) {
        let kind = event.kind();

        let handlers: Vec<Handler> = {
            let listeners = self.listeners.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            let matching = |l: &&Listener| l.kind.is_none_or(|k| k == kind);
            let handlers: Vec<Handler> = listeners.iter().filter(matching).map(|l| l.handler.clone()).collect();
            let has_once = listeners.iter().filter(matching).any(|l| l.once);
            drop(listeners);
            if has_once {
                self.listeners
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .retain(|l| !(l.once && l.kind.is_none_or(|k| k == kind)));
            }
            handlers
        };

        for handler in handlers {
            if catch_unwind(AssertUnwindSafe(|| handler(&event))).is_err() {
                tracing::error!(event = kind.name(), "an event listener panicked");
            }
        }

        let mut subscribers = self.subscribers.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !subscribers.is_empty() {
            subscribers.retain(|tx| tx.send(event.clone()).is_ok());
        }
    }
}

/// An unbounded stream of every event of a connection, created by
/// [`TikTokLiveConnection::subscribe`](crate::TikTokLiveConnection::subscribe).
///
/// No event is ever dropped: consume the stream promptly, or drop it to unsubscribe.
#[derive(Debug)]
pub struct EventStream {
    rx: mpsc::UnboundedReceiver<Event>,
}

impl EventStream {
    /// Waits for the next event. Returns `None` once the connection has been dropped.
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// Returns the next event if one is already queued.
    pub fn try_recv(&mut self) -> Option<Event> {
        self.rx.try_recv().ok()
    }
}

impl Stream for EventStream {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Event>> {
        self.rx.poll_recv(cx)
    }
}
