//! TikTok Webcast protobuf schema (v3) and the decoding pipeline for WebSocket frames.
//!
//! The raw bindings live under [`webcast`], one module per protobuf package. The top-level event
//! messages (everything carrying a `common` header, e.g. [`WebcastChatMessage`]) are re-exported at
//! the root of this module and gathered in the [`WebcastMessage`] enum.

use std::collections::HashSet;
use std::io::Read;
use std::sync::Arc;

use base64::Engine as _;
use bytes::Bytes;
use flate2::read::GzDecoder;
use prost::Message as _;

use crate::error::{Error, Result, SchemaDecodeError};

#[allow(missing_docs, clippy::all, clippy::pedantic, rustdoc::all, non_camel_case_types, unreachable_pub)]
#[path = "generated/mod.rs"]
mod generated;

pub use generated::webcast;

pub use webcast::im::{ControlAction, HeartBeatMessage, PushHeader, WebcastImEnterRoomMessage, WebcastPushFrame};
pub use webcast::model::base::user::User;
pub use webcast::model::data::EnvelopeBusinessType;
pub use webcast::model::message::common::Text;
pub use webcast::shared::message::{BaseProtoMessage, CommonMessageData, ProtoMessageFetchResult};

macro_rules! webcast_messages {
    ($($name:ident => $($segment:ident)::+,)*) => {
        $(pub use self::generated::$($segment)::+ as $name;)*

        /// A decoded top-level Webcast message. Each variant is named after its protobuf message
        /// (which is also the `method` TikTok sends on the wire).
        ///
        /// Payloads are reference counted so that events can be fanned out to many listeners
        /// without copying.
        #[derive(Debug, Clone, PartialEq)]
        #[cfg_attr(feature = "serde", derive(serde::Serialize))]
        #[non_exhaustive]
        #[allow(missing_docs)]
        pub enum WebcastMessage {
            $($name(Arc<$name>),)*
        }

        /// Names of every message type that [`WebcastMessage`] can decode.
        pub const WEBCAST_MESSAGE_TYPES: &[&str] = &[$(stringify!($name)),*];

        impl WebcastMessage {
            /// Decodes `payload` as the message named `method`. Returns `None` when the message type
            /// is not part of the schema.
            pub fn decode(method: &str, payload: Bytes) -> Option<Result<Self, prost::DecodeError>> {
                Some(match method {
                    $(stringify!($name) => $name::decode(payload).map(|m| Self::$name(Arc::new(m))),)*
                    _ => return None,
                })
            }

            /// The protobuf message name (e.g. `WebcastChatMessage`).
            pub fn method(&self) -> &'static str {
                match self {
                    $(Self::$name(_) => stringify!($name),)*
                }
            }

            /// The common header shared by every event message.
            pub fn common(&self) -> Option<&CommonMessageData> {
                match self {
                    $(Self::$name(m) => m.common.as_ref(),)*
                }
            }

            /// The message as JSON (camelCase field names, like the Node library).
            #[cfg(feature = "serde")]
            pub fn to_json(&self) -> serde_json::Value {
                let value = match self {
                    $(Self::$name(m) => serde_json::to_value(&**m),)*
                };
                value.unwrap_or_default()
            }

            /// Whether `method` names a message type that [`WebcastMessage`] can decode.
            pub fn is_known_method(method: &str) -> bool {
                matches!(method, $(stringify!($name))|*)
            }
        }
    };
}

include!("generated/registry.rs");

/// Controls which nested messages get decoded (the Node library's `WebcastDeserializeConfig`).
///
/// Skipping message types you do not need saves CPU and memory on busy streams.
#[derive(Debug, Clone)]
pub struct DeserializeConfig {
    /// Message types that are never decoded. Ignored when `include_message_types` is set.
    pub skip_message_types: HashSet<String>,
    /// When set, only these message types are decoded.
    pub include_message_types: Option<HashSet<String>>,
    /// Append the base64-encoded payload to decode errors (handy for bug reports).
    pub show_base64_on_decode_error: bool,
}

impl Default for DeserializeConfig {
    fn default() -> Self {
        DeserializeConfig { skip_message_types: HashSet::new(), include_message_types: None, show_base64_on_decode_error: true }
    }
}

impl DeserializeConfig {
    /// Whether messages named `method` should be decoded under this configuration.
    pub fn should_decode(&self, method: &str) -> bool {
        match &self.include_message_types {
            Some(include) => include.contains(method),
            None => !self.skip_message_types.contains(method),
        }
    }

    fn decode_error(&self, message_type: &str, source: prost::DecodeError, payload: &[u8]) -> SchemaDecodeError {
        SchemaDecodeError {
            message_type: message_type.to_owned(),
            source,
            base64: self.show_base64_on_decode_error.then(|| base64::engine::general_purpose::STANDARD.encode(payload)),
        }
    }
}

/// A message of a [`ProtoMessageFetchResult`] together with its decoded form.
#[derive(Debug, Clone)]
pub struct DecodedProtoMessage {
    /// The raw message as received (method name, payload, ids, ...).
    pub raw: BaseProtoMessage,
    /// The decoded payload. `None` when the message type is unknown, filtered out by the
    /// [`DeserializeConfig`], or failed to decode (see `decode_error`).
    pub decoded: Option<WebcastMessage>,
    /// Set when the payload failed to decode.
    pub decode_error: Option<SchemaDecodeError>,
}

/// A decoded [`ProtoMessageFetchResult`]. Its `messages` are moved into [`Self::messages`] with
/// their decoded payloads attached, so `result.messages` is always empty.
#[derive(Debug, Clone)]
pub struct DecodedFetchResult {
    /// Cursor, internal ext, route params, push server, ack flag, ...
    pub result: ProtoMessageFetchResult,
    /// The contained messages.
    pub messages: Vec<DecodedProtoMessage>,
}

/// A decoded WebSocket frame.
#[derive(Debug, Clone)]
pub struct DecodedPushFrame {
    /// The frame. When the payload was gzip-compressed, `frame.payload` holds the inflated bytes.
    pub frame: WebcastPushFrame,
    /// Present when the frame carried a protobuf-encoded [`ProtoMessageFetchResult`].
    pub fetch_result: Option<DecodedFetchResult>,
}

/// Decodes a single protobuf message, mapping failures to [`SchemaDecodeError`].
pub fn deserialize_message<M: prost::Message + Default>(
    message_type: &str,
    payload: Bytes,
    config: &DeserializeConfig,
) -> Result<M, SchemaDecodeError> {
    M::decode(payload.clone()).map_err(|err| config.decode_error(message_type, err, &payload))
}

/// Decodes a [`ProtoMessageFetchResult`] and every nested message allowed by `config`.
///
/// Unknown message types and nested decode failures do not fail the whole batch; they are
/// reported on the individual [`DecodedProtoMessage`].
pub fn deserialize_fetch_result(payload: Bytes, config: &DeserializeConfig) -> Result<DecodedFetchResult, SchemaDecodeError> {
    let mut result: ProtoMessageFetchResult = deserialize_message("ProtoMessageFetchResult", payload, config)?;

    let messages = std::mem::take(&mut result.messages)
        .into_iter()
        .map(|raw| {
            if !config.should_decode(&raw.method) {
                return DecodedProtoMessage { raw, decoded: None, decode_error: None };
            }
            match WebcastMessage::decode(&raw.method, raw.payload.clone()) {
                None => {
                    tracing::trace!(method = %raw.method, "skipping message type unknown to the schema");
                    DecodedProtoMessage { raw, decoded: None, decode_error: None }
                }
                Some(Ok(decoded)) => DecodedProtoMessage { raw, decoded: Some(decoded), decode_error: None },
                Some(Err(err)) => {
                    let error = config.decode_error(&raw.method, err, &raw.payload);
                    tracing::debug!(%error, "failed to decode nested message");
                    DecodedProtoMessage { raw, decoded: None, decode_error: Some(error) }
                }
            }
        })
        .collect();

    Ok(DecodedFetchResult { result, messages })
}

/// Decodes a binary WebSocket message: the [`WebcastPushFrame`] container, then (for `pb`
/// payloads, gunzipping when needed) the [`ProtoMessageFetchResult`] inside it.
pub fn deserialize_websocket_message(data: Bytes, config: &DeserializeConfig) -> Result<DecodedPushFrame> {
    let mut frame: WebcastPushFrame = deserialize_message("WebcastPushFrame", data, config)?;
    let mut fetch_result = None;

    if frame.payload_encoding == "pb" && !frame.payload.is_empty() {
        if is_gzip(&frame.payload) {
            frame.payload = gunzip(&frame.payload)?;
        }
        fetch_result = Some(deserialize_fetch_result(frame.payload.clone(), config)?);
    }

    Ok(DecodedPushFrame { frame, fetch_result })
}

/// Encodes a push frame. Fields left at their default value are not sent on the wire.
pub fn encode_push_frame(payload_type: &str, payload: impl Into<Bytes>, log_id: i64) -> Vec<u8> {
    WebcastPushFrame {
        log_id,
        payload_encoding: "pb".to_owned(),
        payload_type: payload_type.to_owned(),
        payload: payload.into(),
        ..Default::default()
    }
    .encode_to_vec()
}

pub(crate) fn is_gzip(data: &[u8]) -> bool {
    data.len() > 2 && data[0] == 0x1f && data[1] == 0x8b && data[2] == 0x08
}

pub(crate) fn gunzip(data: &[u8]) -> Result<Bytes> {
    let mut out = Vec::with_capacity(data.len() * 4);
    GzDecoder::new(data).read_to_end(&mut out)?;
    Ok(out.into())
}

impl From<prost::DecodeError> for Error {
    fn from(source: prost::DecodeError) -> Self {
        Error::SchemaDecode(SchemaDecodeError { message_type: String::new(), source, base64: None })
    }
}
