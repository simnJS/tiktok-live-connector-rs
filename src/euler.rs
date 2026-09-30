//! Minimal client for the [Euler Stream](https://www.eulerstream.com) sign server API — the
//! subset of `tiktok-live-api-sdk` used by the connector.
//!
//! Like the Node SDK (configured with `validateStatus: () => true`), requests never fail because
//! of the HTTP status: every call returns an [`EulerResponse`] and the routes interpret it.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, USER_AGENT};
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};

use crate::error::Result;
use crate::web::cookie_jar::encode_uri_component;

/// Identifies the library to the sign server (`client` query parameter).
///
/// The Node library's identity is reused so the sign server applies the same behavior.
pub const LIBRARY_IDENTITY: &str = "ttlive-node";

/// Default sign server URL.
pub const DEFAULT_SIGN_API_URL: &str = "https://api.eulerstream.com";

/// Configuration of an [`EulerStreamApiClient`] (the Node library's `SignConfig`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignConfig {
    /// Base URL of the sign server. Defaults to `SIGN_API_URL` or [`DEFAULT_SIGN_API_URL`].
    pub base_path: String,
    /// API key. Defaults to `SIGN_API_KEY`.
    pub api_key: Option<String>,
    /// User-Agent sent to the sign server.
    pub user_agent: String,
    /// Value of the `client` query parameter. Defaults to [`LIBRARY_IDENTITY`].
    pub client_identity: String,
}

impl Default for SignConfig {
    fn default() -> Self {
        SignConfig {
            base_path: std::env::var("SIGN_API_URL")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| DEFAULT_SIGN_API_URL.to_owned()),
            api_key: std::env::var("SIGN_API_KEY").ok().filter(|v| !v.is_empty()),
            user_agent: format!("tiktok-live-connector/{} {}", crate::VERSION, std::env::consts::OS),
            client_identity: LIBRARY_IDENTITY.to_owned(),
        }
    }
}

/// Platform of the WebSocket to sign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WebcastFetchPlatform {
    /// Mobile app WebSocket (requires an authenticated session).
    Mobile,
    /// Web WebSocket.
    Web,
}

impl WebcastFetchPlatform {
    fn as_str(self) -> &'static str {
        match self {
            WebcastFetchPlatform::Mobile => "mobile",
            WebcastFetchPlatform::Web => "web",
        }
    }
}

/// Raw response of the sign server.
#[derive(Debug, Clone)]
pub struct EulerResponse {
    /// HTTP status.
    pub status: StatusCode,
    /// Response headers.
    pub headers: HeaderMap,
    /// Raw body.
    pub body: Bytes,
}

impl EulerResponse {
    /// Parses the body as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.body)?)
    }

    /// Parses the body as JSON, falling back to an empty object for non-JSON bodies.
    pub fn json_or_default(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|_| serde_json::Value::Object(Default::default()))
    }

    /// The body as (lossy) UTF-8.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// A header value as a string.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }
}

/// Response of `GET /webcast/anchors/{unique_id}/room_id`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RoomIdResponse {
    /// Status code (`200` on success).
    pub code: i64,
    /// Error message.
    pub message: Option<String>,
    /// Whether the lookup succeeded.
    pub ok: bool,
    /// Sources tried by the server.
    pub routes_attempted: Vec<String>,
    /// Whether the user is live.
    pub is_live: Option<bool>,
    /// The room id.
    #[serde(deserialize_with = "string_or_number")]
    pub room_id: Option<String>,
    /// Where the room id came from.
    pub source: Option<String>,
}

/// Response of `GET /webcast/anchors/{unique_id}/room_info`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RoomInfoResponse {
    /// Status code (`200` on success).
    pub code: i64,
    /// Error message.
    pub message: Option<String>,
    /// Whether the lookup succeeded.
    pub ok: bool,
    /// Sources tried by the server.
    pub routes_attempted: Vec<String>,
    /// The room info.
    pub data: Option<serde_json::Value>,
}

/// Response of `POST /webcast/rooms/{room_id}/chat`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct RoomChatResponse {
    /// Status code (`200` on success).
    pub code: i64,
    /// Error message.
    pub message: Option<String>,
    /// TikTok's response.
    pub data: Option<serde_json::Value>,
}

/// Body of `POST /tiktok/sign_url`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignTikTokUrlBody {
    /// URL to sign.
    pub url: String,
    /// User-Agent the signed request will use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    /// HTTP method (`GET`, `POST`, ...).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Session to bind the signature to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Data center of the session.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tt_target_idc: Option<String>,
}

/// Response of `POST /tiktok/sign_url`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SignTikTokUrlResponse {
    /// Status code (`200` on success).
    pub code: i64,
    /// Error message.
    pub message: Option<String>,
    /// The signature.
    pub response: Option<SignedUrl>,
}

/// The signed URL and the tokens used to sign it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SignedUrl {
    /// The URL with its signature parameters.
    pub signed_url: Option<String>,
    /// User-Agent the signature was computed for.
    pub user_agent: Option<String>,
    /// Browser name the signature was computed for.
    pub browser_name: Option<String>,
    /// Browser version the signature was computed for.
    pub browser_version: Option<String>,
    /// Signature tokens (`X-Bogus`, `msToken`, ...).
    pub tokens: HashMap<String, String>,
    /// Headers to send with the signed request.
    pub request_headers: HashMap<String, String>,
    /// Cookies to send with the signed request.
    pub cookies: HashMap<String, String>,
}

fn string_or_number<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Ok(match Option::<serde_json::Value>::deserialize(deserializer)? {
        Some(serde_json::Value::String(s)) => Some(s),
        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
        _ => None,
    })
}

/// Parameters of `GET /webcast/rooms/{room_id}/connect`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FetchWebcastUrlParams {
    /// Username of the streamer (alternative to the room id).
    pub unique_id: Option<String>,
    /// Cursor to resume from.
    pub cursor: Option<String>,
    /// User-Agent the WebSocket will be opened with.
    pub user_agent: Option<String>,
    /// Whether this is the first connection of the client.
    pub client_enter: Option<bool>,
    /// Pooled proxy region (e.g. `US`, `GB`, `DE`).
    pub country: Option<String>,
    /// Web or mobile WebSocket.
    pub platform: Option<WebcastFetchPlatform>,
    /// OAuth token of the session.
    pub x_oauth_token: Option<String>,
    /// Cookie header of the session (authenticated WebSocket).
    pub x_cookie_header: Option<String>,
}

/// Client for the Euler Stream API. Cheap to clone: clones share the connection pool.
#[derive(Debug, Clone)]
pub struct EulerStreamApiClient {
    inner: Arc<EulerInner>,
}

#[derive(Debug)]
struct EulerInner {
    http: reqwest::Client,
    config: SignConfig,
}

/// Which auth headers an endpoint accepts (mirrors the OpenAPI security schemes of the SDK).
#[derive(Clone, Copy)]
enum Auth {
    ApiKey,
    ApiKeyOrJwt,
}

/// A request to the sign server.
struct Call {
    method: Method,
    path: String,
    auth: Auth,
    accept: &'static str,
    query: Vec<(&'static str, String)>,
    headers: Vec<(&'static str, String)>,
    body: Option<Vec<u8>>,
}

impl Call {
    fn get(path: String, auth: Auth) -> Self {
        Call { method: Method::GET, path, auth, accept: "application/json", query: Vec::new(), headers: Vec::new(), body: None }
    }

    fn post(path: String, auth: Auth, body: Vec<u8>) -> Self {
        Call { method: Method::POST, body: Some(body), ..Call::get(path, auth) }
    }
}

impl EulerStreamApiClient {
    /// Creates a client with its own connection pool.
    pub fn new(config: SignConfig) -> Self {
        let http =
            reqwest::Client::builder().timeout(Duration::from_secs(30)).build().expect("failed to build the default HTTP client");
        Self::with_http_client(config, http)
    }

    /// Creates a client on top of an existing `reqwest` client (proxies, custom TLS, ...).
    pub fn with_http_client(config: SignConfig, http: reqwest::Client) -> Self {
        EulerStreamApiClient { inner: Arc::new(EulerInner { http, config }) }
    }

    /// The process-wide client built from [`SignConfig::default`] (the Node library's
    /// `createEulerClient()` singleton).
    pub fn shared() -> Self {
        static SHARED: OnceLock<EulerStreamApiClient> = OnceLock::new();
        SHARED.get_or_init(|| Self::new(SignConfig::default())).clone()
    }

    /// Returns a client using `api_key` that shares this client's connection pool.
    pub fn with_api_key(&self, api_key: impl Into<String>) -> Self {
        let config = SignConfig { api_key: Some(api_key.into()), ..self.inner.config.clone() };
        Self::with_http_client(config, self.inner.http.clone())
    }

    /// The configuration.
    pub fn config(&self) -> &SignConfig {
        &self.inner.config
    }

    /// Host (and port) of the configured base path.
    pub fn base_host(&self) -> Option<String> {
        let url = url::Url::parse(&self.inner.config.base_path).ok()?;
        let host = url.host_str()?;
        Some(match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_owned(),
        })
    }

    async fn send(&self, call: Call) -> Result<EulerResponse> {
        let config = &self.inner.config;
        let mut url = url::Url::parse(&format!("{}{}", config.base_path.trim_end_matches('/'), call.path))
            .map_err(|e| crate::Error::InvalidOptions(format!("invalid sign server URL: {e}")))?;

        {
            let mut pairs = url.query_pairs_mut();
            if let Some(key) = &config.api_key {
                pairs.append_pair("apiKey", key);
            }
            for (name, value) in &call.query {
                pairs.append_pair(name, value);
            }
        }
        if url.query() == Some("") {
            url.set_query(None);
        }

        let mut request =
            self.inner.http.request(call.method, url).header(ACCEPT, call.accept).header(USER_AGENT, &config.user_agent);
        if let Some(key) = &config.api_key {
            if matches!(call.auth, Auth::ApiKeyOrJwt) {
                request = request.header("x-jwt-key", key);
            }
            request = request.header("x-api-key", key);
        }
        for (name, value) in call.headers {
            request = request.header(name, value);
        }
        if let Some(body) = call.body {
            request = request.header(CONTENT_TYPE, "application/json").body(body);
        }

        let response = request.send().await?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        Ok(EulerResponse { status, headers, body })
    }

    fn session_headers(x_oauth_token: Option<&str>, x_cookie_header: Option<&str>) -> Vec<(&'static str, String)> {
        let mut headers = Vec::new();
        if let Some(token) = x_oauth_token {
            headers.push(("x-oauth-token", token.to_owned()));
        }
        if let Some(cookie) = x_cookie_header {
            headers.push(("x-cookie-header", cookie.to_owned()));
        }
        headers
    }

    /// `GET /webcast/anchors/{unique_id}/room_id`
    pub async fn retrieve_room_id(&self, unique_id: &str) -> Result<EulerResponse> {
        let path = format!("/webcast/anchors/{}/room_id", encode_uri_component(unique_id));
        self.send(Call::get(path, Auth::ApiKeyOrJwt)).await
    }

    /// `GET /webcast/anchors/{unique_id}/room_info`
    pub async fn retrieve_room_info(&self, unique_id: &str) -> Result<EulerResponse> {
        let path = format!("/webcast/anchors/{}/room_info", encode_uri_component(unique_id));
        self.send(Call::get(path, Auth::ApiKey)).await
    }

    /// `GET /webcast/anchors/{unique_id}/gift_gallery`
    pub async fn retrieve_webcast_gift_gallery(
        &self,
        unique_id: &str,
        x_oauth_token: Option<&str>,
        x_cookie_header: Option<&str>,
    ) -> Result<EulerResponse> {
        let path = format!("/webcast/anchors/{}/gift_gallery", encode_uri_component(unique_id));
        let headers = Self::session_headers(x_oauth_token, x_cookie_header);
        self.send(Call { headers, ..Call::get(path, Auth::ApiKeyOrJwt) }).await
    }

    /// `GET /webcast/rooms/{room_id}/gifts`
    pub async fn retrieve_room_gifts(&self, room_id: &str, webcast_language: Option<&str>) -> Result<EulerResponse> {
        let path = format!("/webcast/rooms/{}/gifts", encode_uri_component(room_id));
        let query = webcast_language.map(|l| vec![("webcast_language", l.to_owned())]).unwrap_or_default();
        self.send(Call { query, ..Call::get(path, Auth::ApiKey) }).await
    }

    /// `GET /webcast/rooms/{room_id}/connect`: signed WebSocket URL + initial message batch
    /// (protobuf body).
    pub async fn fetch_webcast_url(&self, room_id: &str, params: &FetchWebcastUrlParams) -> Result<EulerResponse> {
        let path = format!("/webcast/rooms/{}/connect", encode_uri_component(room_id));
        let mut query = vec![("client", self.inner.config.client_identity.clone())];
        let optional = [
            ("unique_id", params.unique_id.clone()),
            ("cursor", params.cursor.clone()),
            ("user_agent", params.user_agent.clone()),
            ("client_enter", params.client_enter.map(|b| b.to_string())),
            ("country", params.country.clone()),
            ("platform", params.platform.map(|p| p.as_str().to_owned())),
        ];
        query.extend(optional.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))));
        let headers = Self::session_headers(params.x_oauth_token.as_deref(), params.x_cookie_header.as_deref());
        let call = Call::get(path, Auth::ApiKeyOrJwt);
        self.send(Call { query, headers, accept: "application/protobuf,application/json", ..call }).await
    }

    /// `POST /webcast/rooms/{room_id}/chat`
    pub async fn send_room_chat(
        &self,
        room_id: &str,
        content: &str,
        x_oauth_token: Option<&str>,
        x_cookie_header: Option<&str>,
    ) -> Result<EulerResponse> {
        let path = format!("/webcast/rooms/{}/chat", encode_uri_component(room_id));
        let body = serde_json::to_vec(&serde_json::json!({ "content": content }))?;
        let headers = Self::session_headers(x_oauth_token, x_cookie_header);
        self.send(Call { headers, ..Call::post(path, Auth::ApiKey, body) }).await
    }

    /// `POST /tiktok/sign_url`
    pub async fn sign_tiktok_url(&self, body: &SignTikTokUrlBody) -> Result<EulerResponse> {
        let query = vec![("client", self.inner.config.client_identity.clone())];
        let body = serde_json::to_vec(body)?;
        self.send(Call { query, ..Call::post("/tiktok/sign_url".to_owned(), Auth::ApiKey, body) }).await
    }
}

impl Default for EulerStreamApiClient {
    fn default() -> Self {
        Self::shared()
    }
}
