//! Webcast WebSocket client.

pub mod client;
pub mod config;

pub use client::{WebcastWebSocketClient, WsConnectParams, generate_uniq_id};
pub use config::WsConfig;
