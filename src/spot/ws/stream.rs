use crate::spot::ws::message::Message;
use crate::spot::ws::topic::Topic;
use crate::spot::ws::MexcSpotWebsocketClient;
use futures::stream::BoxStream;
use futures::StreamExt;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebsocketDisconnectReason {
    ConnectionClosed,
    AlreadyClosed,
    ResetWithoutClosingHandshake,
    ConnectionReset,
}

#[derive(Debug, Clone)]
pub enum WebsocketStreamEvent {
    Message(Arc<Message>),
    Disconnected {
        websocket_id: Uuid,
        reason: WebsocketDisconnectReason,
    },
    Reconnecting {
        websocket_id: Uuid,
        attempt: u32,
    },
    ReconnectFailed {
        websocket_id: Uuid,
        attempt: u32,
        error: String,
    },
    Reconnected {
        websocket_id: Uuid,
    },
    Resubscribed {
        websocket_id: Uuid,
        topics: Vec<Topic>,
    },
}

pub trait Stream {
    fn stream<'a>(self: Arc<Self>) -> BoxStream<'a, Arc<Message>>;

    fn stream_with_events<'a>(self: Arc<Self>) -> BoxStream<'a, WebsocketStreamEvent>;
}

impl Stream for MexcSpotWebsocketClient {
    fn stream<'a>(self: Arc<Self>) -> BoxStream<'a, Arc<Message>> {
        let mut rx = self.broadcast_tx.subscribe();
        let stream = async_stream::stream! {
            while let Ok(message) = rx.recv().await {
                yield message;
            }
        };
        stream.boxed()
    }

    fn stream_with_events<'a>(self: Arc<Self>) -> BoxStream<'a, WebsocketStreamEvent> {
        let mut rx = self.lifecycle_broadcast_tx.subscribe();
        let stream = async_stream::stream! {
            while let Ok(message) = rx.recv().await {
                yield message;
            }
        };
        stream.boxed()
    }
}
