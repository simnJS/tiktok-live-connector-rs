//! [`TikTokLiveConnection`], the entry point of the library.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::oneshot;

use crate::emitter::{Emitter, EventStream, ListenerId};
use crate::error::{Error, Result};
use crate::euler::{EulerStreamApiClient, RoomChatResponse};
use crate::events::{
    ConnectionState, DecodedDataEvent, DisconnectedEvent, ErrorEvent, Event, EventKind, GiftEvent, RawDataEvent, StreamEndEvent,
    basic_event,
};
use crate::proto::{ControlAction, DecodedFetchResult, DeserializeConfig, EnvelopeBusinessType, WebcastMessage};
use crate::web::config::{OrderedMap, WebConfig};
use crate::web::cookie_jar::{CookieJar, CookieSession};
use crate::web::http_client::{WebcastHttpClient, default_http_client, http_client_builder};
use crate::web::presets::ClientPresets;
use crate::web::routes::{DefaultRoutes, Routes, SignedWebSocketParams, route_id};
use crate::ws::client::{WebcastWebSocketClient, WsConnectParams, WsEvent};
use crate::ws::config::WsConfig;

/// Options of a [`TikTokLiveConnection`] (`TikTokLiveConnectionOptions`).
///
/// ```
/// use tiktok_live_connector::ConnectionOptions;
///
/// let options = ConnectionOptions {
///     enable_extended_gift_info: true,
///     ..ConnectionOptions::default()
/// };
/// ```
#[derive(Clone)]
pub struct ConnectionOptions {
    /// Euler Stream API key. Ignored when `euler_client` is set. Defaults to `SIGN_API_KEY`.
    pub sign_api_key: Option<String>,
    /// Pre-built sign server client. Takes precedence over `sign_api_key`.
    pub euler_client: Option<EulerStreamApiClient>,
    /// TikTok session cookie (`sessionid` + `tt-target-idc`). Required by `authenticate_ws`.
    pub session: Option<CookieSession>,
    /// OAuth token forwarded to the sign server.
    pub oauth_token: Option<String>,
    /// Forward the session to the sign server so that the WebSocket is authenticated.
    pub authenticate_ws: bool,
    /// Use the mobile WebSocket flow. Requires `authenticate_ws` and `session`.
    pub use_mobile: bool,
    /// Emit the messages of the initial batch returned by the sign server. Default: `true`.
    pub process_initial_data: bool,
    /// Fetch the room info on connect and fail with [`Error::UserOffline`] when not live. Default: `true`.
    pub fetch_room_info_on_connect: bool,
    /// Fetch the room gift list on connect so that [`GiftEvent::extended_gift_info`] is filled. Default: `false`.
    pub enable_extended_gift_info: bool,
    /// Device, screen and location presets. Defaults to [`ClientPresets::random`].
    pub client_presets: Option<ClientPresets>,
    /// HTTP transport (proxy, timeouts, TLS). Defaults to [`default_http_client`].
    pub http_client: Option<reqwest::Client>,
    /// Proxy for the TikTok traffic (`http://[user:pass@]host:port`): the WebSocket is tunneled
    /// with `CONNECT`, and the default HTTP client uses it too (ignored when `http_client` is set).
    /// The sign server is always reached directly.
    pub proxy: Option<String>,
    /// Replaces the HTTP configuration generated from the presets.
    pub web_config: Option<WebConfig>,
    /// Extra headers for every HTTP request (merged on top of the configuration).
    pub web_headers: OrderedMap,
    /// Extra query parameters for every HTTP request (merged on top of the configuration).
    pub web_params: OrderedMap,
    /// Replaces the WebSocket configuration generated from the presets.
    pub ws_config: Option<WsConfig>,
    /// Extra headers of the WebSocket handshake.
    pub ws_headers: OrderedMap,
    /// Route implementations. Defaults to [`DefaultRoutes`].
    pub routes: Option<Arc<dyn Routes>>,
    /// Which messages get decoded.
    pub deserialize: DeserializeConfig,
    /// WebSocket connect timeout. Defaults to `WS_CONNECT_TIMEOUT_MS` or 20 s.
    pub ws_connect_timeout: Duration,
    /// Time given to the closing handshake before the socket is dropped. Defaults to
    /// `WS_DISCONNECT_TIMEOUT_MS` or 2 s.
    pub ws_disconnect_timeout: Duration,
}

fn env_duration_ms(name: &str, default: u64) -> Duration {
    Duration::from_millis(std::env::var(name).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(default))
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        ConnectionOptions {
            sign_api_key: None,
            euler_client: None,
            session: None,
            oauth_token: None,
            authenticate_ws: false,
            use_mobile: false,
            process_initial_data: true,
            fetch_room_info_on_connect: true,
            enable_extended_gift_info: false,
            client_presets: None,
            http_client: None,
            proxy: None,
            web_config: None,
            web_headers: OrderedMap::new(),
            web_params: OrderedMap::new(),
            ws_config: None,
            ws_headers: OrderedMap::new(),
            routes: None,
            deserialize: DeserializeConfig::default(),
            ws_connect_timeout: env_duration_ms("WS_CONNECT_TIMEOUT_MS", 20_000),
            ws_disconnect_timeout: env_duration_ms("WS_DISCONNECT_TIMEOUT_MS", 2_000),
        }
    }
}

impl fmt::Debug for ConnectionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionOptions")
            .field("sign_api_key", &self.sign_api_key.as_ref().map(|_| "<redacted>"))
            .field("session", &self.session.as_ref().map(|_| "<redacted>"))
            .field("oauth_token", &self.oauth_token.as_ref().map(|_| "<redacted>"))
            .field("authenticate_ws", &self.authenticate_ws)
            .field("use_mobile", &self.use_mobile)
            .field("process_initial_data", &self.process_initial_data)
            .field("fetch_room_info_on_connect", &self.fetch_room_info_on_connect)
            .field("enable_extended_gift_info", &self.enable_extended_gift_info)
            .field("client_presets", &self.client_presets)
            .field("deserialize", &self.deserialize)
            .field("ws_connect_timeout", &self.ws_connect_timeout)
            .field("ws_disconnect_timeout", &self.ws_disconnect_timeout)
            .finish_non_exhaustive()
    }
}

/// The options that can be changed after construction (`TikTokLiveConnectionMutableOptions`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutableOptions {
    /// See [`ConnectionOptions::process_initial_data`].
    pub process_initial_data: bool,
    /// See [`ConnectionOptions::fetch_room_info_on_connect`].
    pub fetch_room_info_on_connect: bool,
    /// See [`ConnectionOptions::enable_extended_gift_info`].
    pub enable_extended_gift_info: bool,
    /// See [`ConnectionOptions::authenticate_ws`].
    pub authenticate_ws: bool,
    /// See [`ConnectionOptions::use_mobile`].
    pub use_mobile: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectState {
    Disconnected,
    Connecting,
    Connected,
}

struct State {
    connect_state: ConnectState,
    room_info: Option<Arc<Value>>,
    available_gifts: Option<Arc<Value>>,
    ws: Option<WebcastWebSocketClient>,
}

struct Inner {
    unique_id: String,
    options: Mutex<MutableOptions>,
    presets: ClientPresets,
    web: WebcastHttpClient,
    ws_config: WsConfig,
    ws_headers: OrderedMap,
    ws_proxy: Option<url::Url>,
    ws_connect_timeout: Duration,
    ws_disconnect_timeout: Duration,
    state: Mutex<State>,
    emitter: Emitter,
}

/// A connection to a TikTok LIVE stream. Cheap to clone: clones share the same connection.
///
/// ```no_run
/// use tiktok_live_connector::TikTokLiveConnection;
///
/// # async fn run() -> tiktok_live_connector::Result<()> {
/// let connection = TikTokLiveConnection::new("officialgeilegisela")?;
///
/// connection.on_chat(|msg| {
///     let user = msg.user.as_ref().map(|u| u.display_id.as_str()).unwrap_or_default();
///     println!("{user}: {}", msg.content);
/// });
/// connection.on_gift(|gift| println!("gift {}", gift.gift_id));
///
/// let state = connection.connect().await?;
/// println!("Connected to room {}", state.room_id);
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct TikTokLiveConnection {
    inner: Arc<Inner>,
}

impl fmt::Debug for TikTokLiveConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TikTokLiveConnection")
            .field("unique_id", &self.inner.unique_id)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

fn proxied_http_client(proxy: &url::Url) -> Result<reqwest::Client> {
    let proxy = reqwest::Proxy::all(proxy.as_str())?;
    Ok(http_client_builder().proxy(proxy).build()?)
}

/// A weak reference to a [`TikTokLiveConnection`], for listeners that must not keep it alive.
#[derive(Clone)]
pub struct WeakTikTokLiveConnection {
    inner: std::sync::Weak<Inner>,
}

impl WeakTikTokLiveConnection {
    /// The connection, if it is still alive.
    pub fn upgrade(&self) -> Option<TikTokLiveConnection> {
        self.inner.upgrade().map(|inner| TikTokLiveConnection { inner })
    }
}

impl fmt::Debug for WeakTikTokLiveConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WeakTikTokLiveConnection")
    }
}

/// Normalizes a username: accepts `user`, `@user` or `https://www.tiktok.com/@user/live`.
pub fn validate_and_normalize_unique_id(unique_id: &str) -> Result<String> {
    let normalized =
        unique_id.replacen("https://www.tiktok.com/", "", 1).replacen("/live", "", 1).replacen('@', "", 1).trim().to_owned();
    if normalized.is_empty() { Err(Error::InvalidUniqueId) } else { Ok(normalized) }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Resets the state to disconnected if a `connect()` future is dropped before completing.
struct ConnectGuard<'a> {
    connection: &'a TikTokLiveConnection,
    armed: bool,
}

impl Drop for ConnectGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            if let Some(ws) = lock(&self.connection.inner.state).ws.take() {
                ws.close();
            }
            self.connection.set_disconnected();
        }
    }
}

impl TikTokLiveConnection {
    /// Creates a connection with default options.
    ///
    /// `unique_id` is the TikTok username, with or without leading `@`, or the full
    /// `https://www.tiktok.com/@user/live` URL.
    pub fn new(unique_id: &str) -> Result<Self> {
        Self::with_options(unique_id, ConnectionOptions::default())
    }

    /// Creates a connection.
    pub fn with_options(unique_id: &str, options: ConnectionOptions) -> Result<Self> {
        let unique_id = validate_and_normalize_unique_id(unique_id)?;

        if options.use_mobile && options.session.is_none() {
            return Err(Error::InvalidOptions("Mobile WebSocket requires a session cookie!".to_owned()));
        }

        let presets = options.client_presets.unwrap_or_else(ClientPresets::random);

        let mut web_config = options.web_config.unwrap_or_else(|| WebConfig::from_presets(&presets));
        web_config.default_http_client_headers.extend(options.web_headers);
        web_config.default_http_client_params.extend(options.web_params);
        if let Some(session) = &options.session {
            let cookie = CookieJar::serialize_session(&web_config, session);
            web_config.default_http_client_headers.insert("Cookie".to_owned(), cookie);
        }

        let euler = match (options.euler_client, options.sign_api_key) {
            (Some(client), _) => client,
            (None, Some(key)) => EulerStreamApiClient::shared().with_api_key(key),
            (None, None) => EulerStreamApiClient::shared(),
        };
        let routes = options.routes.unwrap_or_else(|| Arc::new(DefaultRoutes::default()));

        let proxy = match options.proxy.as_deref().filter(|p| !p.is_empty()) {
            Some(proxy) => Some(url::Url::parse(proxy).map_err(|e| Error::InvalidOptions(format!("invalid proxy URL: {e}")))?),
            None => None,
        };
        let http = match (options.http_client, &proxy) {
            (Some(client), _) => client,
            (None, Some(proxy)) => proxied_http_client(proxy)?,
            (None, None) => default_http_client(),
        };

        let web = WebcastHttpClient::new(web_config, http, euler, routes, options.deserialize);
        web.set_oauth_token(options.oauth_token.filter(|t| !t.is_empty()));

        let ws_config = options.ws_config.unwrap_or_else(|| WsConfig::from_presets(&presets));

        let connection = TikTokLiveConnection {
            inner: Arc::new(Inner {
                unique_id,
                options: Mutex::new(MutableOptions {
                    process_initial_data: options.process_initial_data,
                    fetch_room_info_on_connect: options.fetch_room_info_on_connect,
                    enable_extended_gift_info: options.enable_extended_gift_info,
                    authenticate_ws: options.authenticate_ws,
                    use_mobile: options.use_mobile,
                }),
                presets,
                web,
                ws_config,
                ws_headers: options.ws_headers,
                ws_proxy: proxy,
                ws_connect_timeout: options.ws_connect_timeout,
                ws_disconnect_timeout: options.ws_disconnect_timeout,
                state: Mutex::new(State {
                    connect_state: ConnectState::Disconnected,
                    room_info: None,
                    available_gifts: None,
                    ws: None,
                }),
                emitter: Emitter::default(),
            }),
        };
        connection.set_disconnected();
        Ok(connection)
    }

    /// A weak handle that does not keep the connection alive.
    pub fn downgrade(&self) -> WeakTikTokLiveConnection {
        WeakTikTokLiveConnection { inner: Arc::downgrade(&self.inner) }
    }

    // ----- Accessors -----

    /// The normalized username.
    pub fn unique_id(&self) -> &str {
        &self.inner.unique_id
    }

    /// The HTTP client (client parameters, headers, cookies, session).
    pub fn web_client(&self) -> &WebcastHttpClient {
        &self.inner.web
    }

    /// The sign server client.
    pub fn api_client(&self) -> &EulerStreamApiClient {
        self.inner.web.euler()
    }

    /// The presets the connection was built with.
    pub fn client_presets(&self) -> &ClientPresets {
        &self.inner.presets
    }

    /// The WebSocket, while connected.
    pub fn ws_client(&self) -> Option<WebcastWebSocketClient> {
        lock(&self.inner.state).ws.clone()
    }

    /// The mutable options.
    pub fn options(&self) -> MutableOptions {
        *lock(&self.inner.options)
    }

    /// Changes the mutable options; they apply from the next `connect()`.
    pub fn update_options(&self, update: impl FnOnce(&mut MutableOptions)) {
        update(&mut lock(&self.inner.options));
    }

    /// Room info cached by the last `connect()` / [`Self::fetch_room_info`].
    pub fn room_info(&self) -> Option<Arc<Value>> {
        lock(&self.inner.state).room_info.clone()
    }

    /// Gifts cached on connect when `enable_extended_gift_info` is set.
    pub fn available_gifts(&self) -> Option<Arc<Value>> {
        lock(&self.inner.state).available_gifts.clone()
    }

    /// Whether a connection attempt is in flight.
    pub fn is_connecting(&self) -> bool {
        lock(&self.inner.state).connect_state == ConnectState::Connecting
    }

    /// Whether the connection is established.
    pub fn is_connected(&self) -> bool {
        lock(&self.inner.state).connect_state == ConnectState::Connected
    }

    /// The query parameters of the HTTP client (including `room_id`, `cursor`, `internal_ext`).
    pub fn client_params(&self) -> OrderedMap {
        self.inner.web.client_params()
    }

    /// The current room id (empty when unknown).
    pub fn room_id(&self) -> String {
        self.inner.web.room_id()
    }

    /// A snapshot of the connection state.
    pub fn state(&self) -> ConnectionState {
        let state = lock(&self.inner.state);
        ConnectionState {
            is_connected: state.connect_state == ConnectState::Connected,
            is_connecting: state.connect_state == ConnectState::Connecting,
            room_id: self.inner.web.room_id(),
            room_info: state.room_info.clone(),
            available_gifts: state.available_gifts.clone(),
        }
    }

    // ----- Events -----

    /// Calls `handler` for every event of `kind`.
    pub fn on<F>(&self, kind: EventKind, handler: F) -> ListenerId
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        self.inner.emitter.add(Some(kind), false, Arc::new(handler))
    }

    /// Calls `handler` for the next event of `kind` only.
    pub fn once<F>(&self, kind: EventKind, handler: F) -> ListenerId
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        self.inner.emitter.add(Some(kind), true, Arc::new(handler))
    }

    /// Calls `handler` for every event.
    pub fn on_any<F>(&self, handler: F) -> ListenerId
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        self.inner.emitter.add(None, false, Arc::new(handler))
    }

    /// Removes a listener. Returns whether it was registered.
    pub fn off(&self, id: ListenerId) -> bool {
        self.inner.emitter.remove(id)
    }

    /// Removes every listener of `kind`, or every listener when `None`.
    pub fn remove_all_listeners(&self, kind: Option<EventKind>) {
        self.inner.emitter.remove_all(kind);
    }

    /// Number of callbacks listening to `kind` (catch-all listeners included).
    pub fn listener_count(&self, kind: EventKind) -> usize {
        self.inner.emitter.listener_count(kind)
    }

    /// Returns a stream of every event emitted from now on.
    pub fn subscribe(&self) -> EventStream {
        self.inner.emitter.subscribe()
    }

    fn emit(&self, event: Event) {
        self.inner.emitter.emit(event);
    }

    /// Reports an error through [`Event::Error`], when someone listens to it.
    fn handle_error(&self, error: &Arc<Error>, info: &str) {
        if self.inner.emitter.is_observed(EventKind::Error) {
            self.emit(Event::Error(ErrorEvent { info: info.to_owned(), exception: error.clone() }));
        }
    }

    /// Reports the error of `result` (the Node library's `@HandleError` decorator).
    fn report<T>(&self, result: Result<T>, info: &str) -> Result<T> {
        result.map_err(|err| {
            let err = Arc::new(err);
            self.handle_error(&err, info);
            Arc::try_unwrap(err).unwrap_or_else(|shared| (*shared).clone())
        })
    }

    // ----- Connection lifecycle -----

    fn set_disconnected(&self) {
        {
            let mut state = lock(&self.inner.state);
            state.connect_state = ConnectState::Disconnected;
            state.room_info = None;
            state.available_gifts = None;
        }
        let web = &self.inner.web;
        web.set_client_param("cursor", "");
        web.set_client_param("room_id", "");
        web.set_client_param("internal_ext", "");
    }

    /// Connects to the stream of [`Self::unique_id`], resolving its room id.
    pub async fn connect(&self) -> Result<ConnectionState> {
        self.connect_to_room(None).await
    }

    /// Connects to a stream. When `room_id` is `None`, the cached room id is used, or resolved from
    /// the username.
    pub async fn connect_to_room(&self, room_id: Option<&str>) -> Result<ConnectionState> {
        {
            let mut state = lock(&self.inner.state);
            match state.connect_state {
                ConnectState::Connected => return Err(Error::AlreadyConnected),
                ConnectState::Connecting => return Err(Error::AlreadyConnecting),
                ConnectState::Disconnected => state.connect_state = ConnectState::Connecting,
            }
        }

        let mut guard = ConnectGuard { connection: self, armed: true };
        match self.connect_inner(room_id).await {
            Ok(start_reading) => {
                guard.armed = false;
                lock(&self.inner.state).connect_state = ConnectState::Connected;
                let state = self.state();
                self.emit(Event::Connected(state.clone()));
                let _ = start_reading.send(());
                Ok(state)
            }
            Err(err) => {
                guard.armed = false;
                if let Some(ws) = lock(&self.inner.state).ws.take() {
                    ws.close();
                }
                self.set_disconnected();
                self.report(Err(err), "Error while connecting")
            }
        }
    }

    async fn connect_inner(&self, room_id: Option<&str>) -> Result<oneshot::Sender<()>> {
        let web = &self.inner.web;
        let options = self.options();

        let mut room_id = match room_id.filter(|id| !id.is_empty()) {
            Some(id) => id.to_owned(),
            None => match web.room_id() {
                id if !id.is_empty() => id,
                _ => self.fetch_room_id(None).await?,
            },
        };
        web.set_room_id(&room_id);

        if options.fetch_room_info_on_connect {
            let info = self.fetch_room_info(None).await?;
            if info.pointer("/data/status").and_then(Value::as_i64) == Some(4) {
                return Err(Error::UserOffline);
            }
        }

        if options.enable_extended_gift_info {
            let gifts = self.fetch_available_gifts().await?;
            lock(&self.inner.state).available_gifts = Some(Arc::new(gifts));
        }

        if options.use_mobile && !options.authenticate_ws {
            return Err(Error::InvalidOptions("Mobile WebSocket requires authenticateWs=true!".to_owned()));
        }
        if options.authenticate_ws && web.cookie_jar().session().is_none() && web.oauth_token().is_none() {
            return Err(Error::InvalidOptions(
                "A cookie or OAuth bundle is required to use the authenticated WebSocket!".to_owned(),
            ));
        }

        let signed = web
            .routes()
            .fetch_signed_websocket_from_provider(
                web,
                SignedWebSocketParams {
                    room_id: room_id.clone(),
                    country: None,
                    cursor: Some(web.client_param("cursor").unwrap_or_default()),
                    authenticate_ws: options.authenticate_ws,
                    use_mobile: options.authenticate_ws && options.use_mobile,
                },
            )
            .await?;

        web.cookie_jar().process_set_cookie_header(&signed.cookie_header);

        if let Some(resolved) = signed.room_id {
            web.set_room_id(&resolved);
            room_id = resolved;
        }

        let fetch = signed.fetch_result;
        if options.process_initial_data {
            self.process_fetch_result(&fetch);
        }

        let result = &fetch.result;
        if result.cursor.is_empty() {
            return Err(Error::invalid_response(
                route_id::EULER_FETCH_SIGNED_WEBSOCKET,
                "Missing cursor in initial fetch response.",
            ));
        }

        web.set_client_param("cursor", &result.cursor);
        web.set_client_param("internal_ext", &result.internal_ext);

        let mut params = OrderedMap::new();
        params.insert("compress".to_owned(), "gzip".to_owned());
        params.insert("room_id".to_owned(), web.room_id());
        params.insert("internal_ext".to_owned(), result.internal_ext.clone());
        params.insert("cursor".to_owned(), result.cursor.clone());
        for (key, value) in &result.route_params {
            if !value.is_empty() {
                params.insert(key.clone(), value.clone());
            }
        }

        self.setup_websocket(&result.push_server, params, &room_id).await
    }

    async fn setup_websocket(&self, push_server: &str, params: OrderedMap, room_id: &str) -> Result<oneshot::Sender<()>> {
        let inner = &self.inner;
        let jar = inner.web.cookie_jar();
        let cookie = if self.options().authenticate_ws { jar.cookie_string() } else { jar.anonymous_cookie_string() };

        let mut headers = inner.ws_headers.clone();
        headers.insert("Cookie".to_owned(), cookie);

        let connect_params = WsConnectParams {
            base_url: push_server.to_owned(),
            params,
            headers,
            room_id: room_id.to_owned(),
            proxy: inner.ws_proxy.clone(),
        };
        let (ws, reader) = WebcastWebSocketClient::connect(&inner.ws_config, connect_params, inner.ws_connect_timeout).await?;
        lock(&inner.state).ws = Some(ws.clone());

        // Frames are buffered until `connected` has been emitted, so that events are ordered.
        let (start_reading, started) = oneshot::channel::<()>();
        let connection = self.clone();
        let socket = ws.clone();
        tokio::spawn(async move {
            if started.await.is_err() {
                socket.close();
                socket.mark_closed(crate::ws::client::ABNORMAL_CLOSURE, String::new());
                return;
            }
            let config = connection.inner.web.deserialize_config().clone();
            let (code, reason) = socket.run(reader, &config, |event| connection.handle_ws_event(event)).await;
            connection.on_socket_closed(&socket, code, reason.clone());
            socket.mark_closed(code, reason);
        });

        ws.switch_rooms(room_id);
        self.emit(Event::WebsocketConnected(ws));
        Ok(start_reading)
    }

    fn on_socket_closed(&self, socket: &WebcastWebSocketClient, code: u16, reason: String) {
        {
            let mut state = lock(&self.inner.state);
            if state.ws.as_ref().is_some_and(|ws| ws.ptr_eq(socket)) {
                state.ws = None;
            }
        }
        self.set_disconnected();
        self.emit(Event::Disconnected(DisconnectedEvent { code, reason }));
    }

    fn handle_ws_event(&self, event: WsEvent) {
        match event {
            WsEvent::Data(data) => self.emit(Event::WebsocketData(data)),
            WsEvent::Frame(frame) => {
                if let Some(fetch) = &frame.fetch_result {
                    self.process_fetch_result(fetch);
                }
                if frame.frame.payload_type == "im_enter_room_resp" {
                    self.emit(Event::EnterRoom(frame));
                }
            }
            WsEvent::DecodeFailed(err) => self.handle_error(&Arc::new(err), "Websocket message decoding failed"),
            WsEvent::Error(err) => self.handle_error(&Arc::new(err), "WebSocket Error after connecting"),
        }
    }

    /// Closes the connection and waits until the socket is closed.
    pub async fn disconnect(&self) {
        let ws = {
            let state = lock(&self.inner.state);
            match (&state.ws, state.connect_state) {
                (Some(ws), ConnectState::Connected) => Some(ws.clone()),
                _ => None,
            }
        };
        let Some(ws) = ws else {
            self.set_disconnected();
            return;
        };

        ws.close();
        if tokio::time::timeout(self.inner.ws_disconnect_timeout, ws.closed()).await.is_err() {
            ws.terminate();
            ws.closed().await;
        }
    }

    // ----- Message processing -----

    fn process_fetch_result(&self, fetch: &DecodedFetchResult) {
        if self.inner.emitter.is_observed(EventKind::RawData) {
            for message in &fetch.messages {
                self.emit(Event::RawData(RawDataEvent {
                    method: message.raw.method.clone(),
                    payload: message.raw.payload.clone(),
                }));
            }
        }
        for message in &fetch.messages {
            let Some(decoded) = &message.decoded else { continue };
            self.emit(Event::DecodedData(DecodedDataEvent {
                method: message.raw.method.clone(),
                message: decoded.clone(),
                payload: message.raw.payload.clone(),
            }));
            self.process_decoded(decoded);
        }
    }

    fn process_decoded(&self, message: &WebcastMessage) {
        let display_key = |message: &WebcastMessage| {
            message.common().and_then(|c| c.display_text.as_ref()).map(|t| t.key.clone()).unwrap_or_default()
        };

        match message {
            WebcastMessage::WebcastSocialMessage(social) => {
                let key = display_key(message);
                let event = if key.contains("follow") {
                    Event::Follow(social.clone())
                } else if key.contains("share") {
                    Event::Share(social.clone())
                } else {
                    Event::Social(social.clone())
                };
                self.emit(event);
            }
            WebcastMessage::WebcastControlMessage(control) => {
                self.emit(Event::ControlMessage(control.clone()));
                let action = ControlAction::try_from(control.action).unwrap_or(ControlAction::FallbackUnknown);
                if matches!(action, ControlAction::StreamEnded | ControlAction::StreamSuspended) {
                    self.emit(Event::StreamEnd(StreamEndEvent { action }));
                    if self.is_connected() {
                        let connection = self.clone();
                        tokio::spawn(async move { connection.disconnect().await });
                    }
                }
            }
            WebcastMessage::WebcastGiftMessage(gift) => {
                let extended_gift_info = self.find_gift(gift.gift_id);
                self.emit(Event::Gift(GiftEvent { message: gift.clone(), extended_gift_info }));
            }
            WebcastMessage::WebcastBarrageMessage(barrage) => {
                let keys = [barrage.content.as_ref(), barrage.common_barrage_content.as_ref()]
                    .into_iter()
                    .flatten()
                    .map(|text| text.key.to_lowercase())
                    .filter(|key| !key.is_empty())
                    .collect::<Vec<_>>();
                if keys.iter().any(|k| k.contains("ttlive_superfan_commentnotif_superfanjoined")) {
                    self.emit(Event::SuperFanJoin(barrage.clone()));
                } else if keys.iter().any(|k| k.contains("ttlive_superfan")) {
                    self.emit(Event::SuperFan(barrage.clone()));
                }
                self.emit(Event::Barrage(barrage.clone()));
            }
            WebcastMessage::WebcastEnvelopeMessage(envelope) => {
                let is_super_fan_box = display_key(message).to_lowercase().contains("ttlive_superfanbox")
                    || envelope
                        .envelope_info
                        .as_ref()
                        .is_some_and(|i| i.business_type == EnvelopeBusinessType::SuperFanBox as i32);
                if is_super_fan_box {
                    self.emit(Event::SuperFanBox(envelope.clone()));
                }
                self.emit(Event::Envelope(envelope.clone()));
            }
            other => {
                if let Some(event) = basic_event(other) {
                    self.emit(event);
                }
            }
        }
    }

    fn find_gift(&self, gift_id: i64) -> Option<Arc<Value>> {
        if gift_id == 0 {
            return None;
        }
        let gifts = lock(&self.inner.state).available_gifts.clone()?;
        let matches = |id: &Value| id.as_i64() == Some(gift_id) || id.as_str().is_some_and(|s| s.parse() == Ok(gift_id));
        gifts.as_array()?.iter().find(|gift| gift.get("id").is_some_and(matches)).map(|gift| Arc::new(gift.clone()))
    }

    // ----- HTTP helpers -----

    /// Resolves the room id of `unique_id` (default: this connection's user) and caches it.
    pub async fn fetch_room_id(&self, unique_id: Option<&str>) -> Result<String> {
        let web = &self.inner.web;
        let unique_id = unique_id.unwrap_or(&self.inner.unique_id);
        let result = web.routes().fetch_room_id_composite(web, unique_id).await;
        let room_id = self.report(result, "Failed to retrieve Room ID from all sources.")?;
        web.set_room_id(&room_id);
        Ok(room_id)
    }

    /// Whether `unique_id` (default: this connection's user) is live.
    pub async fn fetch_is_live(&self, unique_id: Option<&str>) -> Result<bool> {
        let web = &self.inner.web;
        let unique_id = unique_id.unwrap_or(&self.inner.unique_id);
        let result = web.routes().fetch_is_live_composite(web, unique_id).await;
        self.report(result, "Failed to retrieve live status from all sources.")
    }

    /// Fetches (and caches) the room info: streamer, status, statistics.
    pub async fn fetch_room_info(&self, room_id: Option<&str>) -> Result<Arc<Value>> {
        let result = async {
            let room_id = match room_id.map(str::to_owned).filter(|id| !id.is_empty()) {
                Some(id) => id,
                None => match self.room_id() {
                    id if !id.is_empty() => id,
                    _ => self.fetch_room_id(None).await?,
                },
            };
            let web = &self.inner.web;
            let info = Arc::new(web.routes().fetch_room_info(web, Some(&room_id)).await?);
            lock(&self.inner.state).room_info = Some(info.clone());
            Ok(info)
        }
        .await;
        self.report(result, "Failed to fetch room info.")
    }

    /// Fetches the gifts available in the current room.
    pub async fn fetch_available_gifts(&self) -> Result<Value> {
        let web = &self.inner.web;
        let room_id = self.room_id();
        let result = web.routes().fetch_room_gifts(web, Some(&room_id)).await;
        self.report(result, "Failed to fetch room gifts.")
    }

    /// Sends a chat message to `room_id` (default: the current room). Requires a session and an
    /// Euler Stream plan that includes chat.
    pub async fn send_message(&self, content: &str, room_id: Option<&str>) -> Result<RoomChatResponse> {
        let web = &self.inner.web;
        let room_id = room_id.map_or_else(|| self.room_id(), str::to_owned);
        let result = web.routes().send_room_chat_from_provider(web, Some(&room_id), content).await;
        self.report(result, "Failed to send message.")
    }

    /// Resolves once the user is live, checking every `interval` (at least 30 s). Drop the future
    /// to stop waiting.
    pub async fn wait_until_live(&self, interval: Duration) -> Result<()> {
        let interval = interval.max(Duration::from_secs(30));
        loop {
            if self.fetch_is_live(None).await? {
                return Ok(());
            }
            tokio::time::sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_unique_id() {
        assert_eq!(validate_and_normalize_unique_id("user").unwrap(), "user");
        assert_eq!(validate_and_normalize_unique_id("@user").unwrap(), "user");
        assert_eq!(validate_and_normalize_unique_id(" https://www.tiktok.com/@user/live ").unwrap(), "user");
        assert!(matches!(validate_and_normalize_unique_id("  "), Err(Error::InvalidUniqueId)));
    }

    #[test]
    fn constructor_sets_initial_state() {
        let connection = TikTokLiveConnection::new("@someone").unwrap();
        assert_eq!(connection.unique_id(), "someone");
        assert!(!connection.is_connected());
        assert!(!connection.is_connecting());
        let params = connection.client_params();
        assert_eq!(params.get("cursor").map(String::as_str), Some(""));
        assert_eq!(params.get("room_id").map(String::as_str), Some(""));
        assert_eq!(params.get("internal_ext").map(String::as_str), Some(""));
        assert_eq!(params.get("aid").map(String::as_str), Some("1988"));
    }

    #[test]
    fn session_cookie_is_seeded() {
        let options = ConnectionOptions { session: Some(CookieSession::new("abc", "useast2a")), ..Default::default() };
        let connection = TikTokLiveConnection::with_options("someone", options).unwrap();
        assert_eq!(connection.web_client().cookie_jar().session(), Some(CookieSession::new("abc", "useast2a")));
    }
}
