//! The Webcast WebSocket: handshake, heartbeat, room switching, acknowledgements and frame
//! decoding (`WebcastWebSocketClient` in the Node library).

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::stream::{SplitStream, StreamExt};
use prost::Message as _;
use rand::RngExt as _;
use tokio::net::TcpStream;
use tokio::sync::{Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use super::config::WsConfig;
use crate::error::{Error, Result};
use crate::proto::{
    DecodedFetchResult, DecodedPushFrame, DeserializeConfig, HeartBeatMessage, WebcastImEnterRoomMessage, WebcastPushFrame,
    deserialize_fetch_result, deserialize_message, encode_push_frame,
};
use crate::web::config::OrderedMap;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Close code used when the connection dropped without a close frame.
pub const ABNORMAL_CLOSURE: u16 = 1006;

/// Generates a random positive, non-zero `i64` (a Java `nextLong(1, Long.MAX_VALUE)`), used as
/// `enter_unique_id` to prevent cross-room message bleeding.
pub fn generate_uniq_id() -> i64 {
    rand::rng().random_range(1..=i64::MAX)
}

/// Opens a TCP tunnel to the host of `target` through an HTTP proxy (`CONNECT` method).
async fn connect_through_proxy(proxy: &url::Url, target: &tungstenite::http::Uri) -> Result<TcpStream, tungstenite::Error> {
    use base64::Engine as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let invalid = |message: String| tungstenite::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, message));
    if proxy.scheme() != "http" {
        return Err(invalid(format!("unsupported WebSocket proxy scheme `{}` (only http:// is supported)", proxy.scheme())));
    }
    let proxy_host = proxy.host_str().ok_or_else(|| invalid("proxy URL has no host".to_owned()))?;
    let proxy_port = proxy.port_or_known_default().unwrap_or(80);
    let target_host = target.host().ok_or_else(|| invalid("WebSocket URL has no host".to_owned()))?;
    let target_port = target.port_u16().unwrap_or(if target.scheme_str() == Some("ws") { 80 } else { 443 });
    let authority = format!("{target_host}:{target_port}");

    let mut stream = TcpStream::connect((proxy_host, proxy_port)).await?;
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if !proxy.username().is_empty() {
        let user = percent_encoding::percent_decode_str(proxy.username()).decode_utf8_lossy();
        let password = percent_encoding::percent_decode_str(proxy.password().unwrap_or_default()).decode_utf8_lossy();
        let credentials = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
        request.push_str(&format!("Proxy-Authorization: Basic {credentials}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    while !response.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).await? == 0 || response.len() > 16 * 1024 {
            return Err(invalid("the proxy closed the connection during the CONNECT handshake".to_owned()));
        }
        response.push(byte[0]);
    }
    let status_line = String::from_utf8_lossy(&response).lines().next().unwrap_or_default().to_owned();
    if status_line.split_whitespace().nth(1) != Some("200") {
        return Err(invalid(format!("proxy refused the tunnel: {status_line}")));
    }
    Ok(stream)
}

/// Handle to an open Webcast WebSocket. Cheap to clone.
#[derive(Clone)]
pub struct WebcastWebSocketClient {
    shared: Arc<Shared>,
}

struct Shared {
    url: String,
    outgoing: mpsc::UnboundedSender<Message>,
    open: AtomicBool,
    seq_id: AtomicI64,
    room_id: Mutex<String>,
    enter_unique_id: Mutex<Option<i64>>,
    ping_interval: Duration,
    heartbeat: Mutex<Option<JoinHandle<()>>>,
    terminate: Notify,
    closed: watch::Sender<Option<(u16, String)>>,
}

impl std::fmt::Debug for WebcastWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebcastWebSocketClient")
            .field("url", &self.shared.url)
            .field("open", &self.is_open())
            .field("room_id", &*lock(&self.shared.room_id))
            .finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Something that happened on the socket, produced by [`WebcastWebSocketClient::run`].
#[derive(Debug)]
pub(crate) enum WsEvent {
    /// Raw binary message.
    Data(Bytes),
    /// A decoded frame (its `fetch_result` has already been acknowledged when required).
    Frame(Arc<DecodedPushFrame>),
    /// A binary message failed to decode.
    DecodeFailed(Error),
    /// Transport error; the socket is closed right after.
    Error(Error),
}

/// Parameters of a connection attempt.
#[derive(Debug, Clone)]
pub struct WsConnectParams {
    /// Push server URL returned by the sign server.
    pub base_url: String,
    /// Dynamic query parameters (merged on top of the configured defaults).
    pub params: OrderedMap,
    /// Extra handshake headers (merged on top of the configured defaults).
    pub headers: OrderedMap,
    /// Room the socket is opened for.
    pub room_id: String,
    /// HTTP proxy (`http://[user:pass@]host:port`) to tunnel the connection through.
    pub proxy: Option<url::Url>,
}

impl WebcastWebSocketClient {
    /// Opens the socket. Returns the handle and the read half, to be driven by [`Self::run`].
    pub(crate) async fn connect(
        config: &WsConfig,
        params: WsConnectParams,
        timeout: Duration,
    ) -> Result<(Self, SplitStream<WsStream>)> {
        let url = config.build_url(&params.base_url, &params.params);
        let mut request = url.as_str().into_client_request()?;

        let mut headers = config.default_ws_client_headers.clone();
        headers.extend(params.headers);
        for (name, value) in &headers {
            if value.is_empty() {
                continue;
            }
            if let (Ok(name), Ok(value)) = (HeaderName::try_from(name.as_str()), HeaderValue::try_from(value.as_str())) {
                request.headers_mut().insert(name, value);
            } else {
                tracing::warn!(header = %name, "skipping invalid WebSocket header")
            }
        }

        let handshake = async {
            match &params.proxy {
                None => tokio_tungstenite::connect_async(request).await,
                Some(proxy) => {
                    let tunnel = connect_through_proxy(proxy, request.uri()).await?;
                    tokio_tungstenite::client_async_tls(request, tunnel).await
                }
            }
        };
        let (stream, _response) = match tokio::time::timeout(timeout, handshake).await {
            Err(_) => return Err(Error::ConnectTimeout),
            Ok(Err(tungstenite::Error::Http(response))) => {
                let handshake_message =
                    response.headers().get("handshake-msg").and_then(|v| v.to_str().ok()).unwrap_or("N/A").to_owned();
                return Err(Error::WebSocketHandshake { status: response.status().as_u16(), handshake_message });
            }
            Ok(result) => result?,
        };

        let (mut sink, reader) = stream.split();
        let (outgoing, mut queue) = mpsc::unbounded_channel::<Message>();

        tokio::spawn(async move {
            while let Some(message) = queue.recv().await {
                let is_close = matches!(message, Message::Close(_));
                if let Err(err) = sink.send(message).await {
                    tracing::debug!(%err, "WebSocket write failed");
                    break;
                }
                if is_close {
                    break;
                }
            }
        });

        let client = WebcastWebSocketClient {
            shared: Arc::new(Shared {
                url,
                outgoing,
                open: AtomicBool::new(true),
                seq_id: AtomicI64::new(1),
                room_id: Mutex::new(params.room_id),
                enter_unique_id: Mutex::new(None),
                ping_interval: config.ping_interval,
                heartbeat: Mutex::new(None),
                terminate: Notify::new(),
                closed: watch::channel(None).0,
            }),
        };
        Ok((client, reader))
    }

    /// The full WebSocket URL.
    pub fn url(&self) -> &str {
        &self.shared.url
    }

    /// Whether the socket is open.
    pub fn is_open(&self) -> bool {
        self.shared.open.load(Ordering::Acquire)
    }

    /// Room the socket is currently subscribed to.
    pub fn room_id(&self) -> String {
        lock(&self.shared.room_id).clone()
    }

    /// The `enter_unique_id` sent with the last room switch.
    pub fn enter_unique_id(&self) -> Option<i64> {
        *lock(&self.shared.enter_unique_id)
    }

    /// Sends a binary message. Returns `false` when the socket is not open.
    pub fn send_bytes(&self, data: impl Into<Bytes>) -> bool {
        self.is_open() && self.shared.outgoing.send(Message::Binary(data.into())).is_ok()
    }

    /// Enters another room on the same socket and (re)starts the heartbeat.
    pub fn switch_rooms(&self, room_id: &str) {
        let shared = &self.shared;
        shared.seq_id.store(1, Ordering::Release);
        *lock(&shared.room_id) = room_id.to_owned();

        let enter_unique_id = generate_uniq_id();
        *lock(&shared.enter_unique_id) = Some(enter_unique_id);

        let enter_room = WebcastImEnterRoomMessage {
            room_id: room_id.parse().unwrap_or_default(),
            room_tag: String::new(),
            live_region: String::new(),
            live_id: 12,
            identity: "audience".to_owned(),
            cursor: String::new(),
            account_type: 0,
            enter_unique_id,
            filter_welcome_msg: "0".to_owned(),
            is_anchor_continue_keep_msg: false,
        };
        self.send_bytes(encode_push_frame("im_enter_room", enter_room.encode_to_vec(), 0));

        // For mobile compatibility the heartbeat only starts once a room has been entered.
        let weak: Weak<Shared> = Arc::downgrade(&self.shared);
        let period = shared.ping_interval;
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            loop {
                interval.tick().await;
                match weak.upgrade() {
                    Some(shared) => WebcastWebSocketClient { shared }.send_heartbeat(),
                    None => break,
                }
            }
        });
        if let Some(previous) = lock(&shared.heartbeat).replace(heartbeat) {
            previous.abort();
        }
    }

    /// Sends a keep-alive heartbeat.
    pub fn send_heartbeat(&self) {
        let heartbeat = HeartBeatMessage {
            room_id: lock(&self.shared.room_id).parse().unwrap_or_default(),
            send_packet_seq_id: self.shared.seq_id.load(Ordering::Acquire),
        };
        if self.send_bytes(encode_push_frame("hb", heartbeat.encode_to_vec(), 0)) {
            self.shared.seq_id.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Acknowledges a received frame.
    pub fn send_ack(&self, log_id: i64, internal_ext: &str) {
        if log_id == 0 {
            return;
        }
        if internal_ext.is_empty() {
            if std::env::var_os("DISABLE_ACK_LOG_WARNING").is_none() {
                tracing::warn!("No internalExt found for message that needs ACK, skipping ACK");
            }
            return;
        }
        self.send_bytes(encode_push_frame("ack", internal_ext.as_bytes().to_vec(), log_id));
    }

    /// Starts the closing handshake (normal closure).
    pub fn close(&self) {
        self.close_with(None);
    }

    /// Starts the closing handshake with a specific close code.
    pub fn close_with_code(&self, code: u16, reason: &str) {
        self.close_with(Some(CloseFrame { code: CloseCode::from(code), reason: reason.to_owned().into() }));
    }

    fn close_with(&self, frame: Option<CloseFrame>) {
        let _ = self.shared.outgoing.send(Message::Close(frame));
    }

    /// Drops the connection immediately, without closing handshake.
    pub fn terminate(&self) {
        self.shared.terminate.notify_one();
    }

    /// Waits until the socket is closed. Returns the close code and reason.
    pub async fn closed(&self) -> (u16, String) {
        let mut rx = self.shared.closed.subscribe();
        loop {
            if let Some(closed) = rx.borrow_and_update().clone() {
                return closed;
            }
            if rx.changed().await.is_err() {
                return (ABNORMAL_CLOSURE, String::new());
            }
        }
    }

    fn on_disconnect(&self) {
        let shared = &self.shared;
        shared.open.store(false, Ordering::Release);
        if let Some(heartbeat) = lock(&shared.heartbeat).take() {
            heartbeat.abort();
        }
        shared.seq_id.store(1, Ordering::Release);
    }

    /// Wakes up [`Self::closed`] waiters. Called once the owner has finished its own cleanup.
    pub(crate) fn mark_closed(&self, code: u16, reason: String) {
        self.on_disconnect();
        let _ = self.shared.closed.send(Some((code, reason)));
    }

    /// Whether both handles point to the same socket.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// Reads the socket until it closes, feeding every event to `handle`. Returns the close code
    /// and reason.
    pub(crate) async fn run(
        &self,
        mut reader: SplitStream<WsStream>,
        config: &DeserializeConfig,
        mut handle: impl FnMut(WsEvent),
    ) -> (u16, String) {
        let (code, reason) = loop {
            let message = tokio::select! {
                message = reader.next() => message,
                () = self.shared.terminate.notified() => break (ABNORMAL_CLOSURE, String::new()),
            };
            match message {
                Some(Ok(Message::Binary(data))) => {
                    handle(WsEvent::Data(data.clone()));
                    match self.decode(data, config) {
                        Ok(frame) => handle(WsEvent::Frame(frame)),
                        Err(err) => handle(WsEvent::DecodeFailed(err)),
                    }
                }
                Some(Ok(Message::Close(frame))) => {
                    break frame.map_or((1005, String::new()), |f| (u16::from(f.code), f.reason.to_string()));
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => {
                    handle(WsEvent::Error(err.into()));
                    break (ABNORMAL_CLOSURE, String::new());
                }
                None => break (ABNORMAL_CLOSURE, String::new()),
            }
        };

        self.on_disconnect();
        (code, reason)
    }

    fn decode(&self, data: Bytes, config: &DeserializeConfig) -> Result<Arc<DecodedPushFrame>> {
        let mut frame: WebcastPushFrame = deserialize_message("WebcastPushFrame", data, config)?;
        let mut fetch_result: Option<DecodedFetchResult> = None;

        if frame.payload_encoding == "pb" && !frame.payload.is_empty() {
            if crate::proto::is_gzip(&frame.payload) {
                frame.payload = crate::proto::gunzip(&frame.payload)?;
            }
            match deserialize_fetch_result(frame.payload.clone(), config) {
                Ok(result) => fetch_result = Some(result),
                // Only `msg` frames are guaranteed to carry a message batch. Others (heartbeat echoes
                // `hb`, `im_enter_room_resp`, ...) may carry other payloads, which the Node
                // library's lenient decoder turns into an empty batch: ignore them the same way.
                Err(err) if frame.payload_type != "msg" => {
                    tracing::trace!(payload_type = %frame.payload_type, %err, "frame payload is not a message batch");
                }
                Err(err) => return Err(err.into()),
            }
        }

        if let Some(result) = &fetch_result
            && result.result.need_ack
        {
            self.send_ack(frame.log_id, &result.result.internal_ext);
        }

        Ok(Arc::new(DecodedPushFrame { frame, fetch_result }))
    }
}
