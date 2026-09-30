//! HTTP routes: direct TikTok calls (base), Euler Stream calls (provider) and fallback chains
//! (composite).
//!
//! The Node library exposes a global, mutable `RouteConfig` registry so that any route can be
//! swapped. Here every route is a method of the [`Routes`] trait with a default implementation:
//! implement the trait, override the methods you want to change, and pass it to the connection
//! through `ConnectionOptions::routes`. Routes call each other through `self`, so an override is
//! honored everywhere (including by the composite routes).

use std::time::Duration;

use async_trait::async_trait;
use regex::Regex;
use reqwest::Method;
use serde_json::Value;

use super::config::OrderedMap;
use super::http_client::WebcastHttpClient;
use crate::error::{Error, ErrorReason, Result, SignApiError};
use crate::euler::{
    FetchWebcastUrlParams, RoomChatResponse, RoomIdResponse, RoomInfoResponse, SignTikTokUrlBody, SignTikTokUrlResponse,
    WebcastFetchPlatform,
};
use crate::proto::{DecodedFetchResult, deserialize_fetch_result};

/// Identifiers of the routes, used as prefix in error messages.
#[allow(missing_docs)]
pub mod route_id {
    pub const FETCH_ROOM_GIFTS: &str = "fetchRoomGiftsRoute";
    pub const FETCH_ROOM_INFO: &str = "fetchRoomInfoRoute";
    pub const FETCH_ROOM_INFO_API_LIVE: &str = "fetchRoomInfoApiLiveRoute";
    pub const FETCH_ROOM_INFO_HTML: &str = "fetchRoomInfoHtmlRoute";

    pub const FETCH_IS_LIVE: &str = "fetchIsLiveRoute";
    pub const FETCH_ROOM_ID: &str = "fetchRoomIdRoute";

    pub const EULER_FETCH_ROOM_GIFTS: &str = "fetchRoomGiftsFromEulerRoute";
    pub const EULER_FETCH_ROOM_GIFT_GALLERY: &str = "fetchRoomGiftGalleryFromEulerRoute";
    pub const EULER_FETCH_ROOM_ID: &str = "fetchRoomIdFromEulerRoute";
    pub const EULER_FETCH_ROOM_INFO: &str = "fetchRoomInfoFromEulerRoute";
    pub const EULER_FETCH_SIGNED_WEBSOCKET: &str = "fetchSignedWebSocketFromEulerRoute";
    pub const EULER_FETCH_WEBCAST_SIGNATURE: &str = "fetchWebcastSignatureFromEulerRoute";
    pub const EULER_SEND_ROOM_CHAT: &str = "sendRoomChatFromEulerRoute";
}

/// Parameters of [`Routes::fetch_webcast_signature_from_provider`].
#[derive(Debug, Clone)]
pub struct SignerParams {
    /// The URL to sign.
    pub url: String,
    /// HTTP method the signed URL will be used with.
    pub method: Method,
    /// User-Agent the signed request will use.
    pub user_agent: String,
}

/// Parameters of [`Routes::fetch_signed_websocket_from_provider`].
#[derive(Debug, Clone, Default)]
pub struct SignedWebSocketParams {
    /// Room to connect to.
    pub room_id: String,
    /// Pooled proxy region override (e.g. `US`).
    pub country: Option<String>,
    /// Cursor to resume from.
    pub cursor: Option<String>,
    /// Forward the session cookies to the sign server to authenticate the WebSocket.
    pub authenticate_ws: bool,
    /// Sign a mobile WebSocket instead of a web one (requires `authenticate_ws`).
    pub use_mobile: bool,
}

/// Result of [`Routes::fetch_signed_websocket_from_provider`].
#[derive(Debug, Clone)]
pub struct SignedWebSocketResponse {
    /// Initial message batch, including the push server URL, cursor and route parameters.
    pub fetch_result: DecodedFetchResult,
    /// Cookies set by the sign server (`x-set-tt-cookie`).
    pub cookie_header: String,
    /// Room id resolved by the sign server (`x-room-id`), if any.
    pub room_id: Option<String>,
}

/// Which sources a composite route may use. All are enabled by default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CompositeSources {
    /// Skip scraping the `SIGI_STATE` blob of the live page.
    pub skip_html: bool,
    /// Skip the `api-live/user/room/` endpoint.
    pub skip_api_live: bool,
    /// Skip the Euler Stream fallback.
    pub skip_euler: bool,
}

/// Every HTTP route used by the connector. All methods have default implementations.
#[async_trait]
pub trait Routes: Send + Sync + 'static {
    /// Sources used by [`Routes::fetch_room_id_composite`] (`RoomIdRouteConfig`).
    fn room_id_sources(&self) -> CompositeSources {
        CompositeSources::default()
    }

    /// Sources used by [`Routes::fetch_is_live_composite`] (`IsLiveRouteConfig`).
    fn is_live_sources(&self) -> CompositeSources {
        CompositeSources::default()
    }

    /// Regex extracting the `SIGI_STATE` JSON from the live page (`RoomInfoFromHtmlRouteConfig`).
    fn html_extraction_pattern(&self) -> &Regex {
        default_html_extraction_pattern()
    }

    // ----- Base routes (TikTok) -----

    /// Gifts available in a room (signed `webcast/gift/list/`). Returns `data.gifts`.
    async fn fetch_room_gifts(&self, web: &WebcastHttpClient, room_id: Option<&str>) -> Result<Value> {
        let route_id = route_id::FETCH_ROOM_GIFTS;
        let room_id = resolve_room_id(web, room_id).ok_or_else(|| Error::invalid_request(route_id, MISSING_ROOM_ID))?;
        let params = OrderedMap::from([("room_id".to_owned(), room_id)]);
        let mut response = web.get_json_object_from_webcast_api("gift/list/", params, true).await?;
        Ok(response.pointer_mut("/data/gifts").map_or(Value::Null, Value::take))
    }

    /// Room info (`webcast/room/info/`): streamer, status, statistics.
    async fn fetch_room_info(&self, web: &WebcastHttpClient, room_id: Option<&str>) -> Result<Value> {
        let route_id = route_id::FETCH_ROOM_INFO;
        let room_id = resolve_room_id(web, room_id).ok_or_else(|| Error::invalid_request(route_id, MISSING_ROOM_ID))?;
        let params = OrderedMap::from([("room_id".to_owned(), room_id)]);
        web.get_json_object_from_webcast_api("room/info/", params, false).await
    }

    /// User and live room info from `api-live/user/room/`.
    async fn fetch_room_info_from_api_live(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<Value> {
        let route_id = route_id::FETCH_ROOM_INFO_API_LIVE;
        let params =
            OrderedMap::from([("uniqueId".to_owned(), unique_id.to_owned()), ("sourceType".to_owned(), "54".to_owned())]);
        let room_data = web.get_json_object_from_tiktok_api("api-live/user/room/", params, false).await?;

        let status_code = room_data.get("statusCode").and_then(Value::as_i64).unwrap_or(0);
        if status_code != 0 {
            let message = room_data.get("message").and_then(Value::as_str).filter(|m| !m.is_empty()).unwrap_or("Unknown Error");
            return Err(Error::invalid_response(route_id, format!("API Error {status_code} ({message})")));
        }
        if non_empty_id(room_data.pointer("/data/user/roomId")).is_none() {
            return Err(Error::invalid_response(route_id, format!("Invalid response from API: {room_data}")));
        }
        Ok(room_data)
    }

    /// Scrapes the `SIGI_STATE` blob of `@{unique_id}/live` and returns `LiveRoom.liveRoomUserInfo`
    /// (`{ user, liveRoom, ... }`).
    async fn fetch_room_info_from_html(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<Value> {
        let route_id = route_id::FETCH_ROOM_INFO_HTML;
        let html = web.get_html_from_tiktok_website(&format!("@{unique_id}/live")).await?;

        let json = self.html_extraction_pattern().captures(&html).and_then(|c| c.get(1)).ok_or_else(|| {
            Error::invalid_response(route_id, "Failed to extract the SIGI_STATE HTML tag, you might be blocked by TikTok.")
        })?;

        let mut sigi_state: Value = serde_json::from_str(json.as_str()).map_err(|_| {
            Error::invalid_response(route_id, "Failed to parse SIGI_STATE into JSON. Are you captcha-blocked by TikTok?")
        })?;

        match sigi_state.pointer_mut("/LiveRoom/liveRoomUserInfo") {
            Some(info) if truthy(info) => Ok(info.take()),
            _ => Err(Error::invalid_response(route_id, "Failed to extract the LiveRoom object from SIGI_STATE.")),
        }
    }

    // ----- Provider routes (Euler Stream) -----

    /// Gifts available in a room, from Euler Stream.
    async fn fetch_room_gifts_from_provider(
        &self,
        web: &WebcastHttpClient,
        room_id: Option<&str>,
        webcast_language: Option<&str>,
    ) -> Result<Value> {
        let route_id = route_id::EULER_FETCH_ROOM_GIFTS;
        let room_id = resolve_room_id(web, room_id).ok_or_else(|| Error::invalid_request(route_id, MISSING_ROOM_ID))?;
        let response = web.euler().retrieve_room_gifts(&room_id, webcast_language).await?;
        Ok(response.json_or_default())
    }

    /// A creator's gift gallery, from Euler Stream. Authenticated with the session cookies when
    /// available, otherwise with the OAuth token.
    async fn fetch_room_gift_gallery_from_provider(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<Value> {
        let (oauth, cookie) = euler_session(web);
        let response = web.euler().retrieve_webcast_gift_gallery(unique_id, oauth.as_deref(), cookie.as_deref()).await?;
        Ok(response.json_or_default())
    }

    /// Room id (and live status) of a user, from Euler Stream.
    async fn fetch_room_id_from_provider(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<RoomIdResponse> {
        web.euler().retrieve_room_id(unique_id).await?.json()
    }

    /// Full room info of a user, from Euler Stream.
    async fn fetch_room_info_from_provider(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<RoomInfoResponse> {
        web.euler().retrieve_room_info(unique_id).await?.json()
    }

    /// Signed WebSocket URL plus the initial message batch, from Euler Stream.
    async fn fetch_signed_websocket_from_provider(
        &self,
        web: &WebcastHttpClient,
        params: SignedWebSocketParams,
    ) -> Result<SignedWebSocketResponse> {
        fetch_signed_websocket_from_euler(web, params).await
    }

    /// Signs an arbitrary TikTok URL through Euler Stream.
    async fn fetch_webcast_signature_from_provider(
        &self,
        web: &WebcastHttpClient,
        params: SignerParams,
    ) -> Result<SignTikTokUrlResponse> {
        fetch_webcast_signature_from_euler(web, params).await
    }

    /// Sends a chat message to a room through Euler Stream (requires a session and a paid plan).
    async fn send_room_chat_from_provider(
        &self,
        web: &WebcastHttpClient,
        room_id: Option<&str>,
        content: &str,
    ) -> Result<RoomChatResponse> {
        let route_id = route_id::EULER_SEND_ROOM_CHAT;
        let room_id = resolve_room_id(web, room_id)
            .ok_or_else(|| Error::invalid_request(route_id, "Room ID must be specified in all cases to send a chat to a room"))?;
        let (oauth, cookie) = euler_session(web);
        let response = web.euler().send_room_chat(&room_id, content, oauth.as_deref(), cookie.as_deref()).await?;
        let data = response.json_or_default();
        let message = data.get("message").and_then(Value::as_str).filter(|m| !m.is_empty());

        match response.status.as_u16() {
            200 => Ok(serde_json::from_value(data)?),
            401 | 403 => Err(SignApiError::premium_feature(
                Some("Sending chats requires an API key & a paid plan, as it uses cloud managed services."),
                [message.unwrap_or("Unauthorized").to_owned(), data.to_string()],
            )
            .into()),
            _ => Err(Error::invalid_response(route_id, format!("Failed to send chat: {}", message.unwrap_or("Unknown error")))),
        }
    }

    // ----- Composite routes -----

    /// Resolves a room id, trying the HTML page, the API and finally Euler Stream.
    async fn fetch_room_id_composite(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<String> {
        let route_id = route_id::FETCH_ROOM_ID;
        let sources = self.room_id_sources();
        let mut errors = Vec::new();

        if !sources.skip_html {
            match self.fetch_room_info_from_html(web, unique_id).await {
                Ok(info) => match non_empty_id(info.pointer("/user/roomId")) {
                    Some(room_id) => return Ok(room_id),
                    None => errors.push(Error::invalid_response(route_id, "Failed to extract Room ID from HTML.")),
                },
                Err(err) => errors.push(err),
            }
        }

        if !sources.skip_api_live {
            match self.fetch_room_info_from_api_live(web, unique_id).await {
                Ok(data) => match non_empty_id(data.pointer("/data/user/roomId")) {
                    Some(room_id) => return Ok(room_id),
                    None => errors.push(Error::invalid_response(route_id, "Failed to extract Room ID from API.")),
                },
                Err(err) => errors.push(err),
            }
        }

        if !sources.skip_euler {
            match self.fetch_room_id_from_provider(web, unique_id).await {
                Ok(response) if matches!(response.code, 401..=403) => errors.push(Error::invalid_response(
                    route_id,
                    "Failed to retrieve Room ID from Euler Stream, which was made as a last resort due to the previous methods failing. \
                     This happened due to a >>lack of permission<< for you to use Euler Stream's (https://www.eulerstream.com) fallback method. \
                     If you do not want to use Euler Stream, disable this fallback method with `CompositeSources::skip_euler`.",
                )),
                Ok(response) if !response.ok => errors.push(Error::invalid_response(
                    route_id,
                    format!("Failed to retrieve Room ID from Euler due to an error: {}", response.message.unwrap_or_default()),
                )),
                Ok(response) => match response.room_id.filter(|id| !id.is_empty()) {
                    Some(room_id) => return Ok(room_id),
                    None => errors.push(Error::invalid_response(route_id, "Failed to extract Room ID from Euler.")),
                },
                Err(err) => errors.push(err),
            }
        }

        Err(Error::InvalidResponseComposite {
            route_id: route_id.to_owned(),
            message: "Failed to retrieve Room ID from all sources.".to_owned(),
            errors,
        })
    }

    /// Whether a user is live, trying the HTML page, the API and finally Euler Stream.
    async fn fetch_is_live_composite(&self, web: &WebcastHttpClient, unique_id: &str) -> Result<bool> {
        let route_id = route_id::FETCH_IS_LIVE;
        let sources = self.is_live_sources();
        let is_online = |status: i64| status != 4;
        let mut errors = Vec::new();

        if !sources.skip_html {
            match self.fetch_room_info_from_html(web, unique_id).await {
                Ok(info) => match info.pointer("/liveRoom/status").and_then(Value::as_i64) {
                    Some(status) => return Ok(is_online(status)),
                    None => errors.push(Error::invalid_response(route_id, "Failed to extract status from HTML.")),
                },
                Err(err) => errors.push(err),
            }
        }

        if !sources.skip_api_live {
            match self.fetch_room_info_from_api_live(web, unique_id).await {
                Ok(data) => match data.pointer("/data/liveRoom/status").and_then(Value::as_i64) {
                    Some(status) => return Ok(is_online(status)),
                    None => errors.push(Error::invalid_response(route_id, "Failed to extract status from API.")),
                },
                Err(err) => errors.push(err),
            }
        }

        if !sources.skip_euler {
            match self.fetch_room_id_from_provider(web, unique_id).await {
                Ok(response) if response.code != 200 => {
                    errors.push(Error::invalid_response(route_id, "Failed to extract status from Euler."));
                }
                Ok(response) => match response.is_live {
                    Some(is_live) => return Ok(is_live),
                    None => errors.push(Error::invalid_response(route_id, "Failed to extract is_live from Euler.")),
                },
                Err(err) => errors.push(err),
            }
        }

        Err(Error::InvalidResponseComposite {
            route_id: route_id.to_owned(),
            message: "Failed to retrieve live status from all sources.".to_owned(),
            errors,
        })
    }
}

/// The stock route implementations, with configurable composite sources and HTML pattern.
#[derive(Debug, Clone, Default)]
pub struct DefaultRoutes {
    /// Sources of [`Routes::fetch_room_id_composite`].
    pub room_id_sources: CompositeSources,
    /// Sources of [`Routes::fetch_is_live_composite`].
    pub is_live_sources: CompositeSources,
    /// Overrides the `SIGI_STATE` extraction regex (capture group 1 must be the JSON).
    pub html_extraction_pattern: Option<Regex>,
}

impl Routes for DefaultRoutes {
    fn room_id_sources(&self) -> CompositeSources {
        self.room_id_sources
    }

    fn is_live_sources(&self) -> CompositeSources {
        self.is_live_sources
    }

    fn html_extraction_pattern(&self) -> &Regex {
        self.html_extraction_pattern.as_ref().unwrap_or_else(|| default_html_extraction_pattern())
    }
}

const MISSING_ROOM_ID: &str = "Missing roomId. Please provide a roomId to the HTTP client.";

/// `<script id="SIGI_STATE" type="application/json">(.*?)</script>`
pub fn default_html_extraction_pattern() -> &'static Regex {
    static PATTERN: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r#"<script id="SIGI_STATE" type="application/json">(.*?)</script>"#).expect("valid regex"))
}

fn resolve_room_id(web: &WebcastHttpClient, room_id: Option<&str>) -> Option<String> {
    room_id.map(str::to_owned).filter(|id| !id.is_empty()).or_else(|| Some(web.room_id()).filter(|id| !id.is_empty()))
}

/// JavaScript truthiness of a JSON value.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// A room id that may be serialized as string or number.
fn non_empty_id(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(s) if !s.is_empty() && s != "0" => Some(s.clone()),
        Value::Number(n) if n.as_u64().is_some_and(|n| n != 0) => Some(n.to_string()),
        _ => None,
    }
}

/// Session credentials forwarded to Euler: the cookie header when available, else the OAuth token.
fn euler_session(web: &WebcastHttpClient) -> (Option<String>, Option<String>) {
    let cookie = Some(web.cookie_jar().cookie_string()).filter(|c| !c.is_empty());
    let oauth = if cookie.is_some() { None } else { web.oauth_token().filter(|t| !t.is_empty()) };
    (oauth, cookie)
}

async fn fetch_signed_websocket_from_euler(
    web: &WebcastHttpClient,
    params: SignedWebSocketParams,
) -> Result<SignedWebSocketResponse> {
    let route_id = route_id::EULER_FETCH_SIGNED_WEBSOCKET;
    let euler = web.euler();

    if params.authenticate_ws {
        let expected_host = euler.base_host();
        match std::env::var("WHITELIST_AUTHENTICATED_SESSION_ID_HOST").ok().filter(|h| !h.is_empty()) {
            None => {
                return Err(SignApiError::authenticated_ws(
                    "authenticate_websocket is true, but no whitelist host defined. Set the env var WHITELIST_AUTHENTICATED_SESSION_ID_HOST to proceed.",
                )
                .into());
            }
            Some(host) if Some(&host) != expected_host.as_ref() => {
                return Err(SignApiError::authenticated_ws(format!(
                    "The env var WHITELIST_AUTHENTICATED_SESSION_ID_HOST \"{host}\" does not match sign server host \"{}\".",
                    expected_host.unwrap_or_default()
                ))
                .into());
            }
            Some(_) => {}
        }
    }

    let cookie_header = Some(web.cookie_jar().cookie_string()).filter(|c| !c.is_empty());
    if params.authenticate_ws && cookie_header.is_none() {
        return Err(SignApiError::authenticated_ws("authenticate_websocket is true, but no session cookies found.").into());
    }

    let request = FetchWebcastUrlParams {
        unique_id: None,
        cursor: params.cursor,
        user_agent: web.client_header("User-Agent"),
        client_enter: Some(true),
        country: params.country,
        platform: Some(if params.use_mobile { WebcastFetchPlatform::Mobile } else { WebcastFetchPlatform::Web }),
        x_oauth_token: None,
        x_cookie_header: if params.authenticate_ws { cookie_header } else { None },
    };

    let response = euler.fetch_webcast_url(&params.room_id, &request).await.map_err(|err| {
        SignApiError::new(
            ErrorReason::ConnectError,
            None,
            None,
            ["Failed to connect to sign server.".to_owned(), err.to_string()],
        )
    })?;

    let server_message = |data: &Value| -> Option<String> {
        if std::env::var_os("SIGN_SERVER_MESSAGE_DISABLED").is_some() {
            return None;
        }
        data.get("message").and_then(Value::as_str).map(str::to_owned)
    };

    match response.status.as_u16() {
        429 => {
            let data = response.json_or_default();
            let label = data.get("limit_label").and_then(Value::as_str).filter(|l| !l.is_empty());
            let mut args = vec![format!(
                "{}Too many connections started, try again later.",
                label.map(|l| format!("({l}) ")).unwrap_or_default()
            )];
            if let Some(message) = server_message(&data) {
                args.push(SignApiError::format_sign_server_message(&message));
            }
            let header = |name: &str| response.header(name).map(str::to_owned);
            let mut error = SignApiError::new(ErrorReason::RateLimit, header("x-log-id"), header("x-agent-id"), args);
            error.retry_after = Some(Duration::from_secs(header("retry-after").and_then(|v| v.trim().parse().ok()).unwrap_or(0)));
            error.reset_time = header("x-ratelimit-reset").and_then(|v| v.trim().parse::<u64>().ok()).map(|s| s * 1000);
            return Err(error.into());
        }
        402 => {
            let data = response.json_or_default();
            return Err(SignApiError::premium_feature(
                server_message(&data).as_deref(),
                ["Error fetching the signed TikTok WebSocket"],
            )
            .into());
        }
        _ => {}
    }

    let log_id = response.header("x-request-id").map(str::to_owned);
    let agent_id = response.header("x-agent-id").map(str::to_owned);

    if response.status.as_u16() != 200 {
        return Err(SignApiError::new(
            ErrorReason::SignNot200,
            log_id,
            agent_id,
            [format!("[{route_id}] Unexpected sign server status {}. Payload:\n{}", response.status.as_u16(), response.text())],
        )
        .into());
    }

    let Some(cookie_header) = response.header("x-set-tt-cookie").filter(|c| !c.is_empty()).map(str::to_owned) else {
        let extra = response.header("handshake-msg").map(|m| format!("Handshake Message: {m}")).unwrap_or_default();
        return Err(SignApiError::new(
            ErrorReason::EmptyCookies,
            log_id,
            agent_id,
            [format!("[{route_id}] No cookies received from sign server."), extra],
        )
        .into());
    };

    Ok(SignedWebSocketResponse {
        fetch_result: deserialize_fetch_result(response.body.clone(), web.deserialize_config())?,
        cookie_header,
        room_id: response.header("x-room-id").filter(|id| !id.is_empty()).map(str::to_owned),
    })
}

async fn fetch_webcast_signature_from_euler(web: &WebcastHttpClient, params: SignerParams) -> Result<SignTikTokUrlResponse> {
    let route_id = route_id::EULER_FETCH_WEBCAST_SIGNATURE;
    let session = web.cookie_jar().session();

    let url = strip_signature_params(&params.url);

    let response = web
        .euler()
        .sign_tiktok_url(&SignTikTokUrlBody {
            url,
            user_agent: Some(params.user_agent),
            method: Some(params.method.as_str().to_ascii_uppercase()),
            session_id: session.as_ref().map(|s| s.session_id.clone()),
            tt_target_idc: session.map(|s| s.tt_target_idc),
        })
        .await?;

    let data = response.json_or_default();
    let message = data.get("message").and_then(Value::as_str).filter(|m| !m.is_empty());

    match response.status.as_u16() {
        200 => {}
        403 => {
            return Err(SignApiError::premium_feature(
                Some("You do not have permission from the signature provider to sign this URL."),
                [message.unwrap_or("Forbidden").to_owned(), data.to_string()],
            )
            .into());
        }
        _ => {
            return Err(SignApiError::missing_tokens(format!(
                "[{route_id}] Failed to sign a request: {}",
                message.unwrap_or("Unknown error")
            ))
            .into());
        }
    }

    let signed: SignTikTokUrlResponse = serde_json::from_value(data)?;
    if signed.response.as_ref().is_none_or(|r| r.tokens.is_empty()) {
        return Err(SignApiError::missing_tokens(format!(
            "[{route_id}] Failed to sign a request due to missing tokens in response!"
        ))
        .into());
    }
    Ok(signed)
}

/// Removes previous signature parameters (`X-Bogus`, `X-Gnarly`, `msToken`) from a URL.
pub fn strip_signature_params(url: &str) -> String {
    static PATTERNS: std::sync::OnceLock<Vec<Regex>> = std::sync::OnceLock::new();
    let patterns = PATTERNS.get_or_init(|| {
        ["X-Bogus", "X-Gnarly", "msToken"].iter().map(|p| Regex::new(&format!("([&?]){p}=[^&]*")).expect("valid regex")).collect()
    });

    let mut clean = url.to_owned();
    for pattern in patterns {
        clean = pattern.replace_all(&clean, "$1").into_owned();
        if clean.ends_with(['&', '?']) {
            clean.pop();
        }
    }
    clean
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_signature_params() {
        assert_eq!(strip_signature_params("https://a.b/c?x=1&X-Bogus=abc&msToken=zz"), "https://a.b/c?x=1&");
        assert_eq!(strip_signature_params("https://a.b/c?X-Gnarly=1"), "https://a.b/c");
        assert_eq!(strip_signature_params("https://a.b/c?x=1"), "https://a.b/c?x=1");
    }

    #[test]
    fn html_pattern_extracts_sigi_state() {
        let html = r#"<html><script id="SIGI_STATE" type="application/json">{"LiveRoom":{"liveRoomUserInfo":{"user":{"roomId":"42"}}}}</script></html>"#;
        let captured = default_html_extraction_pattern().captures(html).unwrap().get(1).unwrap().as_str();
        let value: Value = serde_json::from_str(captured).unwrap();
        assert_eq!(non_empty_id(value.pointer("/LiveRoom/liveRoomUserInfo/user/roomId")), Some("42".to_owned()));
    }
}

#[cfg(test)]
mod composite_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::euler::EulerStreamApiClient;
    use crate::proto::DeserializeConfig;
    use crate::web::config::WebConfig;
    use crate::web::http_client::default_http_client;

    /// HTML and API sources fail; the provider answers. Counts the calls to each source.
    #[derive(Default)]
    struct FallbackRoutes {
        sources: CompositeSources,
        calls: [AtomicUsize; 3],
    }

    #[async_trait]
    impl Routes for FallbackRoutes {
        fn room_id_sources(&self) -> CompositeSources {
            self.sources
        }

        async fn fetch_room_info_from_html(&self, _: &WebcastHttpClient, _: &str) -> Result<Value> {
            self.calls[0].fetch_add(1, Ordering::SeqCst);
            Err(Error::invalid_response(route_id::FETCH_ROOM_INFO_HTML, "blocked"))
        }

        async fn fetch_room_info_from_api_live(&self, _: &WebcastHttpClient, _: &str) -> Result<Value> {
            self.calls[1].fetch_add(1, Ordering::SeqCst);
            Ok(serde_json::json!({ "data": { "user": {} } }))
        }

        async fn fetch_room_id_from_provider(&self, _: &WebcastHttpClient, _: &str) -> Result<RoomIdResponse> {
            self.calls[2].fetch_add(1, Ordering::SeqCst);
            Ok(RoomIdResponse { code: 200, ok: true, room_id: Some("99".to_owned()), ..Default::default() })
        }
    }

    fn client(routes: Arc<FallbackRoutes>) -> WebcastHttpClient {
        let euler = EulerStreamApiClient::new(Default::default());
        WebcastHttpClient::new(WebConfig::default(), default_http_client(), euler, routes, DeserializeConfig::default())
    }

    #[tokio::test]
    async fn room_id_falls_back_through_every_source() {
        let routes = Arc::new(FallbackRoutes::default());
        let web = client(routes.clone());
        assert_eq!(routes.fetch_room_id_composite(&web, "user").await.unwrap(), "99");
        assert_eq!(routes.calls.each_ref().map(|c| c.load(Ordering::SeqCst)), [1, 1, 1]);
    }

    #[tokio::test]
    async fn skipped_sources_are_reported() {
        let routes = Arc::new(FallbackRoutes {
            sources: CompositeSources { skip_euler: true, ..Default::default() },
            ..Default::default()
        });
        let web = client(routes.clone());
        let Err(Error::InvalidResponseComposite { errors, .. }) = routes.fetch_room_id_composite(&web, "user").await else {
            panic!("expected a composite error");
        };
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[1].to_string(), "[fetchRoomIdRoute] Failed to extract Room ID from API.");
        assert_eq!(routes.calls[2].load(Ordering::SeqCst), 0);
    }
}
