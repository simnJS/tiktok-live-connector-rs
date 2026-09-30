//! # tiktok-live-connector
//!
//! Receive real-time events (chat, gifts, likes, follows, viewers, battles, ...) from any TikTok
//! LIVE stream. A Rust port of the Node.js
//! [TikTok-Live-Connector](https://github.com/zerodytrash/TikTok-Live-Connector).
//!
//! ```no_run
//! use tiktok_live_connector::{Event, TikTokLiveConnection};
//!
//! #[tokio::main]
//! async fn main() -> tiktok_live_connector::Result<()> {
//!     let connection = TikTokLiveConnection::new("officialgeilegisela")?;
//!
//!     // Callback style, like the Node library.
//!     connection.on_chat(|msg| {
//!         let user = msg.user.as_ref().map(|u| u.display_id.as_str()).unwrap_or_default();
//!         println!("{user} writes: {}", msg.content);
//!     });
//!
//!     let state = connection.connect().await?;
//!     println!("Connected to room {}", state.room_id);
//!
//!     // Or stream style.
//!     let mut events = connection.subscribe();
//!     while let Some(event) = events.recv().await {
//!         match event {
//!             Event::Gift(gift) => println!("gift {} x{}", gift.gift_id, gift.repeat_count),
//!             Event::Like(like) => println!("+{} likes ({} total)", like.count, like.total),
//!             Event::Disconnected(_) => break,
//!             _ => {}
//!         }
//!     }
//!     Ok(())
//! }
//! ```
//!
//! ## Signing
//!
//! Connecting requires a signed WebSocket URL, provided by the
//! [Euler Stream](https://www.eulerstream.com) sign server. Set `SIGN_API_KEY` (or
//! [`ConnectionOptions::sign_api_key`]) to use your own key and quota.
//!
//! ## Environment variables
//!
//! | Variable | Effect |
//! |---|---|
//! | `SIGN_API_KEY` / `SIGN_API_URL` | Sign server credentials and URL. |
//! | `WHITELIST_AUTHENTICATED_SESSION_ID_HOST` | Required to forward a session to the sign server (`authenticate_ws`). |
//! | `SIGN_SERVER_MESSAGE_DISABLED` | Hide sign server messages in errors. |
//! | `TIKTOK_CLIENT_TIMEOUT` | HTTP timeout in ms (default 10000). |
//! | `WS_CONNECT_TIMEOUT_MS` / `WS_DISCONNECT_TIMEOUT_MS` | WebSocket timeouts (default 20000 / 2000). |
//! | `RANDOMIZE_TIKTOK_DEVICE` / `_LOCATION` / `_SCREEN` | Randomize the browser fingerprint presets. |
//! | `DISABLE_ACK_LOG_WARNING` | Silence the missing `internal_ext` warning. |
//!
//! ## Cargo features
//!
//! * `serde` — `Serialize`/`Deserialize` on every protobuf message (camelCase, like the Node
//!   library's JSON output).
//! * `legacy` — the deprecated `WebcastPushConnection` emitting flattened JSON events.

#![forbid(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms, unreachable_pub)]
#![cfg_attr(docsrs, feature(doc_cfg))]

mod client;
mod emitter;
pub mod error;
pub mod euler;
pub mod events;
#[cfg(feature = "legacy")]
#[cfg_attr(docsrs, doc(cfg(feature = "legacy")))]
pub mod legacy;
pub mod proto;
pub mod web;
pub mod ws;

pub use client::{
    ConnectionOptions, MutableOptions, TikTokLiveConnection, WeakTikTokLiveConnection, validate_and_normalize_unique_id,
};
pub use emitter::{EventStream, ListenerId};
pub use error::{Error, ErrorReason, Result, SchemaDecodeError, SignApiError};
pub use euler::{EulerStreamApiClient, SignConfig};
pub use events::{
    ConnectionState, DecodedDataEvent, DisconnectedEvent, ErrorEvent, Event, EventKind, GiftEvent, RawDataEvent, StreamEndEvent,
};
pub use proto::{DeserializeConfig, WebcastMessage};
pub use web::{ClientPresets, CookieSession, WebcastHttpClient};
pub use ws::WebcastWebSocketClient;

/// Version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
