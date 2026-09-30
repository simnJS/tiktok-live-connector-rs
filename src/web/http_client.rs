//! HTTP client for TikTok's web and webcast APIs (`WebcastHttpClient` in the Node library).

use std::net::IpAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{COOKIE, HeaderMap, HeaderName, HeaderValue, LOCATION, SET_COOKIE};
use reqwest::{Method, StatusCode};
use url::Url;

use super::config::{OrderedMap, WebConfig};
use super::cookie_jar::CookieJar;
use super::routes::{Routes, SignerParams};
use crate::error::{Error, Result};
use crate::euler::EulerStreamApiClient;
use crate::proto::{DeserializeConfig, deserialize_message};

const MAX_REDIRECTS: usize = 10;

/// A single request issued through [`WebcastHttpClient::request`].
#[derive(Debug, Clone)]
pub struct WebcastRequest {
    /// Host of the request URL (e.g. `webcast.tiktok.com`).
    pub host: String,
    /// Path appended to the host, without leading slash.
    pub path: String,
    /// Per-call query parameters, merged on top of the client parameters (per-call values win).
    pub search_params: OrderedMap,
    /// Per-call headers, merged on top of the client headers (per-call values win).
    pub headers: OrderedMap,
    /// Route the URL through the sign server before sending it.
    pub sign_request: bool,
    /// HTTP method.
    pub method: Method,
    /// Request body.
    pub body: Option<Bytes>,
}

impl WebcastRequest {
    /// A `GET` request to `host/path`.
    pub fn get(host: impl Into<String>, path: impl Into<String>) -> Self {
        WebcastRequest {
            host: host.into(),
            path: path.into(),
            search_params: OrderedMap::new(),
            headers: OrderedMap::new(),
            sign_request: false,
            method: Method::GET,
            body: None,
        }
    }
}

/// HTTP client holding the live client state: query parameters, headers, cookies and session.
///
/// Every request is sent with the client parameters and headers, the cookie jar content, and
/// stores the cookies the server sets.
pub struct WebcastHttpClient {
    http: reqwest::Client,
    config: WebConfig,
    params: Mutex<OrderedMap>,
    headers: Mutex<OrderedMap>,
    cookie_jar: CookieJar,
    oauth_token: Mutex<Option<String>>,
    euler: EulerStreamApiClient,
    routes: Arc<dyn Routes>,
    deserialize_config: DeserializeConfig,
}

impl std::fmt::Debug for WebcastHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebcastHttpClient")
            .field("config", &self.config)
            .field("params", &*lock(&self.params))
            .field("cookie_jar", &self.cookie_jar)
            .finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Builds the default transport: gzip, no automatic redirects (they are followed manually so
/// that cookies set along the way are captured) and a timeout of `TIKTOK_CLIENT_TIMEOUT` ms
/// (10 s by default).
pub fn default_http_client() -> reqwest::Client {
    http_client_builder().build().expect("failed to build the default HTTP client")
}

/// The builder behind [`default_http_client`], to customize it (proxy, TLS, ...).
pub fn http_client_builder() -> reqwest::ClientBuilder {
    let timeout = std::env::var("TIKTOK_CLIENT_TIMEOUT").ok().and_then(|v| v.parse().ok()).unwrap_or(10_000);
    reqwest::Client::builder().timeout(Duration::from_millis(timeout)).redirect(reqwest::redirect::Policy::none())
}

impl WebcastHttpClient {
    /// Creates a client.
    ///
    /// * `config` — hosts, default headers and parameters.
    /// * `http` — transport; use [`default_http_client`] unless you need a proxy or custom TLS.
    /// * `euler` — sign server client.
    /// * `routes` — route implementations, used to sign requests.
    pub fn new(
        config: WebConfig,
        http: reqwest::Client,
        euler: EulerStreamApiClient,
        routes: Arc<dyn Routes>,
        deserialize_config: DeserializeConfig,
    ) -> Self {
        let initial_cookies = config.default_http_client_headers.get("Cookie").cloned().unwrap_or_default();
        WebcastHttpClient {
            http,
            params: Mutex::new(config.default_http_client_params.clone()),
            headers: Mutex::new(config.default_http_client_headers.clone()),
            cookie_jar: CookieJar::new(&config, &initial_cookies),
            oauth_token: Mutex::new(None),
            config,
            euler,
            routes,
            deserialize_config,
        }
    }

    /// Static configuration.
    pub fn config(&self) -> &WebConfig {
        &self.config
    }

    /// The sign server client.
    pub fn euler(&self) -> &EulerStreamApiClient {
        &self.euler
    }

    /// The route implementations.
    pub fn routes(&self) -> &Arc<dyn Routes> {
        &self.routes
    }

    /// The cookie jar.
    pub fn cookie_jar(&self) -> &CookieJar {
        &self.cookie_jar
    }

    /// Protobuf decoding options.
    pub fn deserialize_config(&self) -> &DeserializeConfig {
        &self.deserialize_config
    }

    /// The underlying `reqwest` client.
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// A snapshot of the query parameters sent with every request.
    pub fn client_params(&self) -> OrderedMap {
        lock(&self.params).clone()
    }

    /// Reads one client parameter.
    pub fn client_param(&self, name: &str) -> Option<String> {
        lock(&self.params).get(name).cloned()
    }

    /// Sets a query parameter sent with every request.
    pub fn set_client_param(&self, name: impl Into<String>, value: impl Into<String>) {
        lock(&self.params).insert(name.into(), value.into());
    }

    /// Removes a client parameter.
    pub fn remove_client_param(&self, name: &str) -> Option<String> {
        lock(&self.params).shift_remove(name)
    }

    /// A snapshot of the headers sent with every request.
    pub fn client_headers(&self) -> OrderedMap {
        lock(&self.headers).clone()
    }

    /// Reads one client header.
    pub fn client_header(&self, name: &str) -> Option<String> {
        lock(&self.headers).get(name).cloned()
    }

    /// Sets a header sent with every request.
    pub fn set_client_header(&self, name: impl Into<String>, value: impl Into<String>) {
        lock(&self.headers).insert(name.into(), value.into());
    }

    /// The room id of the client (`room_id` client parameter), empty when unset.
    pub fn room_id(&self) -> String {
        self.client_param("room_id").unwrap_or_default()
    }

    /// Sets the room id of the client.
    pub fn set_room_id(&self, room_id: impl Into<String>) {
        self.set_client_param("room_id", room_id);
    }

    /// The OAuth token forwarded to the sign server, if any.
    pub fn oauth_token(&self) -> Option<String> {
        lock(&self.oauth_token).clone()
    }

    /// Sets the OAuth token forwarded to the sign server.
    pub fn set_oauth_token(&self, token: Option<String>) {
        *lock(&self.oauth_token) = token;
    }

    /// `https://host`, or `http://host` for loopback hosts (local mock servers).
    pub fn base_url(host: &str) -> String {
        let hostname = host
            .rsplit_once(':')
            .filter(|(h, p)| !h.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
            .map_or(host, |(h, _)| h);
        let hostname = hostname.trim_start_matches('[').trim_end_matches(']');
        let loopback = hostname == "localhost" || hostname.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
        format!("{}://{host}", if loopback { "http" } else { "https" })
    }

    fn build_url(&self, host: &str, path: &str, search_params: &OrderedMap) -> String {
        let mut params = self.client_params();
        params.extend(search_params.iter().map(|(k, v)| (k.clone(), v.clone())));
        let query = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(&params).finish();
        format!("{}/{path}?{query}", Self::base_url(host))
    }

    /// Issues a request, merging the per-call options with the live client state and signing the
    /// URL first when `sign_request` is set. Non-2xx responses are returned as errors.
    pub async fn request(&self, request: WebcastRequest) -> Result<reqwest::Response> {
        let WebcastRequest { host, path, search_params, headers, sign_request, method, body } = request;

        let mut merged_headers = self.client_headers();
        merged_headers.extend(headers);
        let mut url = self.build_url(&host, &path, &search_params);

        if sign_request {
            let user_agent = merged_headers.get("User-Agent").cloned().unwrap_or_default();
            let signed = self
                .routes
                .fetch_webcast_signature_from_provider(
                    self,
                    SignerParams { url: url.clone(), method: method.clone(), user_agent },
                )
                .await?;
            if let Some(response) = signed.response
                && let Some(signed_url) = response.signed_url.filter(|u| !u.is_empty())
            {
                url = signed_url;
                if let Some(ua) = response.user_agent.filter(|u| !u.is_empty()) {
                    merged_headers.insert("User-Agent".to_owned(), ua);
                }
            }
        }

        let mut header_map = HeaderMap::with_capacity(merged_headers.len() + 1);
        for (name, value) in &merged_headers {
            let (Ok(name), Ok(value)) = (HeaderName::try_from(name.as_str()), HeaderValue::try_from(value.as_str())) else {
                tracing::warn!(header = %name, "skipping invalid header");
                continue;
            };
            header_map.insert(name, value);
        }

        let mut url = Url::parse(&url).map_err(|e| Error::InvalidOptions(format!("invalid request URL {url}: {e}")))?;
        let mut method = method;
        let mut body = body;

        for _ in 0..=MAX_REDIRECTS {
            let cookies = self.cookie_jar.cookie_string();
            if cookies.is_empty() {
                header_map.remove(COOKIE);
            } else if let Ok(value) = HeaderValue::try_from(cookies) {
                header_map.insert(COOKIE, value);
            }

            let mut builder = self.http.request(method.clone(), url.clone()).headers(header_map.clone());
            if let Some(body) = &body {
                builder = builder.body(body.clone());
            }
            let response = builder.send().await?;

            for value in response.headers().get_all(SET_COOKIE) {
                if let Ok(value) = value.to_str() {
                    self.cookie_jar.process_set_cookie_header(value);
                }
            }

            let status = response.status();
            let location = response.headers().get(LOCATION).and_then(|l| l.to_str().ok()).and_then(|l| url.join(l).ok());
            match location {
                Some(next) if status.is_redirection() => {
                    if status == StatusCode::SEE_OTHER
                        || (matches!(status, StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND) && method == Method::POST)
                    {
                        method = Method::GET;
                        body = None;
                    }
                    url = next;
                }
                _ => return Ok(response.error_for_status()?),
            }
        }

        Err(Error::invalid_response("request", format!("too many redirects while requesting {url}")))
    }

    /// Fetches an HTML page of the TikTok website.
    pub async fn get_html_from_tiktok_website(&self, path: &str) -> Result<String> {
        let request = WebcastRequest::get(&self.config.tiktok_host_web, path);
        Ok(self.request(request).await?.text().await?)
    }

    /// Fetches and decodes a protobuf object from the webcast API (`webcast/{path}`).
    pub async fn get_deserialized_object_from_webcast_api<M: prost::Message + Default>(
        &self,
        path: &str,
        params: OrderedMap,
        schema_name: &str,
        sign_request: bool,
    ) -> Result<M> {
        let mut request = WebcastRequest::get(&self.config.tiktok_host_webcast, format!("webcast/{path}"));
        request.search_params = params;
        request.sign_request = sign_request;
        let body = self.request(request).await?.bytes().await?;
        Ok(deserialize_message(schema_name, body, &self.deserialize_config)?)
    }

    /// Fetches a JSON object from the webcast API (`webcast/{path}`).
    pub async fn get_json_object_from_webcast_api(
        &self,
        path: &str,
        params: OrderedMap,
        sign_request: bool,
    ) -> Result<serde_json::Value> {
        let mut request = WebcastRequest::get(&self.config.tiktok_host_webcast, format!("webcast/{path}"));
        request.search_params = params;
        request.sign_request = sign_request;
        self.json(request).await
    }

    /// Posts a JSON object to the webcast API (`webcast/{path}`) and parses the JSON response.
    pub async fn post_json_object_to_webcast_api(
        &self,
        path: &str,
        params: OrderedMap,
        data: &serde_json::Value,
        sign_request: bool,
    ) -> Result<serde_json::Value> {
        let mut request = WebcastRequest::get(&self.config.tiktok_host_webcast, format!("webcast/{path}"));
        request.method = Method::POST;
        request.search_params = params;
        request.sign_request = sign_request;
        request.body = Some(serde_json::to_vec(data)?.into());
        request.headers.insert("Content-Type".to_owned(), "application/json; charset=UTF-8".to_owned());
        self.json(request).await
    }

    /// Fetches a JSON object from the TikTok web API (`www.tiktok.com/{path}`).
    pub async fn get_json_object_from_tiktok_api(
        &self,
        path: &str,
        params: OrderedMap,
        sign_request: bool,
    ) -> Result<serde_json::Value> {
        let mut request = WebcastRequest::get(&self.config.tiktok_host_web, path);
        request.search_params = params;
        request.sign_request = sign_request;
        self.json(request).await
    }

    async fn json(&self, request: WebcastRequest) -> Result<serde_json::Value> {
        let body = self.request(request).await?.bytes().await?;
        Ok(serde_json::from_slice(&body)?)
    }
}

#[cfg(test)]
mod tests {
    use super::WebcastHttpClient;

    #[test]
    fn base_url_scheme() {
        assert_eq!(WebcastHttpClient::base_url("www.tiktok.com"), "https://www.tiktok.com");
        assert_eq!(WebcastHttpClient::base_url("localhost:8080"), "http://localhost:8080");
        assert_eq!(WebcastHttpClient::base_url("127.0.0.1"), "http://127.0.0.1");
        assert_eq!(WebcastHttpClient::base_url("[::1]:3000"), "http://[::1]:3000");
    }
}
