//! HTTP layer: configuration, presets, cookies, the HTTP client and the routes.

pub mod config;
pub mod cookie_jar;
pub mod http_client;
pub mod presets;
pub mod routes;

pub use config::{OrderedMap, WebConfig};
pub use cookie_jar::{CookieJar, CookieSession};
pub use http_client::{WebcastHttpClient, WebcastRequest, default_http_client, http_client_builder};
pub use presets::{ClientPresets, DevicePreset, LocationPreset, ScreenPreset};
pub use routes::{CompositeSources, DefaultRoutes, Routes, SignedWebSocketParams, SignedWebSocketResponse, SignerParams};
