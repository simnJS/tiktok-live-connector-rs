//! Static configuration of the WebSocket client (`WebcastWebSocketConfigDefaults`).

use std::time::Duration;

use rand::RngExt as _;

use crate::web::config::{OrderedMap, map};
use crate::web::presets::ClientPresets;

/// Default query parameters, headers and timings of the Webcast WebSocket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsConfig {
    /// Host of the TikTok website (`www.tiktok.com`).
    pub tiktok_host_ws: String,
    /// Query parameters of the WebSocket URL. The dynamic parameters returned by the sign server
    /// (`room_id`, `cursor`, `internal_ext`, route params) are merged on top.
    pub default_ws_client_params: OrderedMap,
    /// Raw suffix appended to the query string.
    pub default_ws_client_params_append_parameter: String,
    /// Headers of the WebSocket handshake.
    pub default_ws_client_headers: OrderedMap,
    /// Heartbeat interval.
    pub ping_interval: Duration,
}

impl WsConfig {
    /// Builds the configuration for the given presets (the Node library's `getWebSocketConfigDefaults`).
    pub fn from_presets(presets: &ClientPresets) -> Self {
        let ClientPresets { device, location, screen } = presets;
        let screen_height = screen.screen_height.to_string();
        let screen_width = screen.screen_width.to_string();
        let last_rtt = (rand::rng().random::<f64>() * 100.0 + 100.0).to_string();

        WsConfig {
            tiktok_host_ws: "www.tiktok.com".to_owned(),
            default_ws_client_params: map([
                ("version_code", "180800"),
                ("aid", "1988"),
                ("app_language", &location.lang),
                ("app_name", "tiktok_web"),
                ("browser_platform", &device.browser_platform),
                ("browser_language", &location.lang_country),
                ("browser_name", &device.browser_name),
                ("browser_version", &device.browser_version),
                ("browser_online", "true"),
                ("cookie_enabled", "true"),
                ("tz_name", &location.tz_name),
                ("device_platform", "web"),
                ("identity", "audience"),
                ("live_id", "12"),
                ("webcast_language", &location.lang),
                ("ws_direct", "0"),
                ("sup_ws_ds_opt", "1"),
                ("update_version_code", "2.0.0"),
                ("did_rule", "3"),
                ("screen_height", &screen_height),
                ("screen_width", &screen_width),
                ("heartbeat_duration", "0"),
                ("resp_content_type", "protobuf"),
                ("history_comment_count", "6"),
                ("client_enter", "1"),
                ("last_rtt", &last_rtt),
                ("User-Agent", &device.user_agent),
            ]),
            default_ws_client_params_append_parameter: "&version_code=270000".to_owned(),
            default_ws_client_headers: map([("User-Agent", &device.user_agent)]),
            ping_interval: Duration::from_secs(10),
        }
    }

    /// Builds the WebSocket URL: `base_url?{default params + params}{append parameter}`.
    pub fn build_url(&self, base_url: &str, params: &OrderedMap) -> String {
        let mut merged = self.default_ws_client_params.clone();
        merged.extend(params.iter().map(|(k, v)| (k.clone(), v.clone())));
        let query = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(&merged).finish();
        format!("{base_url}?{query}{}", self.default_ws_client_params_append_parameter)
    }
}

impl Default for WsConfig {
    fn default() -> Self {
        Self::from_presets(&ClientPresets::random())
    }
}
