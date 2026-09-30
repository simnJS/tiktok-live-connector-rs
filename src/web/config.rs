//! Static configuration of the HTTP client (`WebcastWebConfigDefaults` in the Node library).

use indexmap::IndexMap;

use super::presets::{ClientPresets, generate_device_id};

/// An ordered string map. Order matters: query strings and headers are emitted in insertion
/// order, exactly like the JavaScript objects they mirror.
pub type OrderedMap = IndexMap<String, String>;

/// Hosts, default headers, default query parameters and cookie names used for TikTok requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebConfig {
    /// `www.tiktok.com`
    pub tiktok_host_web: String,
    /// `webcast.tiktok.com`
    pub tiktok_host_webcast: String,
    /// `https://www.tiktok.com`
    pub tiktok_http_origin: String,
    /// Headers sent with every request.
    pub default_http_client_headers: OrderedMap,
    /// Query parameters sent with every request.
    pub default_http_client_params: OrderedMap,
    /// Cookies that carry the session id.
    pub session_cookie_names: Vec<String>,
    /// Cookie that carries the data center of the session.
    pub target_idc_cookie_name: String,
}

pub(crate) fn map<const N: usize>(entries: [(&str, &str); N]) -> OrderedMap {
    entries.into_iter().map(|(k, v)| (k.to_owned(), v.to_owned())).collect()
}

impl WebConfig {
    /// Builds the configuration for the given presets (the Node library's `getWebConfig`).
    pub fn from_presets(presets: &ClientPresets) -> Self {
        let ClientPresets { device, location, screen } = presets;
        let screen_height = screen.screen_height.to_string();
        let screen_width = screen.screen_width.to_string();
        let device_id = generate_device_id();

        WebConfig {
            tiktok_host_web: "www.tiktok.com".to_owned(),
            tiktok_host_webcast: "webcast.tiktok.com".to_owned(),
            tiktok_http_origin: "https://www.tiktok.com".to_owned(),
            default_http_client_headers: map([
                ("Cookie", "tt-target-idc=useast1a"),
                ("Connection", "keep-alive"),
                ("Cache-Control", "max-age=0"),
                ("User-Agent", &device.user_agent),
                ("Accept", "text/html,application/json,application/protobuf"),
                ("Referer", "https://www.tiktok.com/"),
                ("Origin", "https://www.tiktok.com"),
                ("Accept-Language", "en-US,en;q=0.9"),
                ("Accept-Encoding", "gzip, deflate"),
                ("Sec-Fetch-Site", "same-site"),
                ("Sec-Fetch-Mode", "cors"),
                ("Sec-Fetch-Dest", "empty"),
                ("Sec-Fetch-Ua-Mobile", "?0"),
            ]),
            default_http_client_params: map([
                ("aid", "1988"),
                ("app_language", &location.lang),
                ("app_name", "tiktok_web"),
                ("browser_language", &location.lang_country),
                ("browser_name", &device.browser_name),
                ("browser_online", "true"),
                ("browser_platform", &device.browser_platform),
                ("browser_version", &device.browser_version),
                ("cookie_enabled", "true"),
                ("device_platform", "web_pc"),
                ("focus_state", "true"),
                ("from_page", "user"),
                ("history_len", "10"),
                ("is_fullscreen", "false"),
                ("is_page_visible", "true"),
                ("screen_height", &screen_height),
                ("screen_width", &screen_width),
                ("tz_name", &location.tz_name),
                ("referer", "https://www.tiktok.com/"),
                ("root_referer", "https://www.tiktok.com/"),
                ("channel", "tiktok_web"),
                ("data_collection_enabled", "true"),
                ("os", &device.os),
                ("priority_region", &location.country),
                ("region", &location.country),
                ("user_is_login", "true"),
                ("webcast_language", &location.lang),
                ("device_id", &device_id),
            ]),
            session_cookie_names: ["sessionid", "sessionid_ss", "sid_tt", "sid_guard"].map(str::to_owned).to_vec(),
            target_idc_cookie_name: "tt-target-idc".to_owned(),
        }
    }
}

impl Default for WebConfig {
    fn default() -> Self {
        Self::from_presets(&ClientPresets::random())
    }
}
