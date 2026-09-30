//! Error types. Every error class of the Node library maps to a variant of [`Error`]; the sign
//! server family (`SignAPIError`, `SignatureRateLimitError`, `PremiumFeatureError`, ...) is
//! represented by [`SignApiError`] discriminated by its [`ErrorReason`].

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Convenience alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Every error the library can produce.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The `uniqueId` passed to the connection was empty or malformed.
    #[error("Missing or invalid value for 'uniqueId'. Please provide the username from TikTok URL.")]
    InvalidUniqueId,

    /// A route received a response it could not make sense of.
    #[error("[{route_id}] {message}")]
    InvalidResponse {
        /// The failing route.
        route_id: String,
        /// What was wrong.
        message: String,
    },

    /// Every source of a composite route failed. The individual failures are kept in `errors`.
    #[error("{message}{}", format_nested(errors))]
    InvalidResponseComposite {
        /// The composite route.
        route_id: String,
        /// Summary.
        message: String,
        /// The error of each source, in the order they were tried.
        errors: Vec<Error>,
    },

    /// A route was called with missing or invalid arguments.
    #[error("[{route_id}] {message}")]
    InvalidRequest {
        /// The route.
        route_id: String,
        /// What is missing.
        message: String,
    },

    /// `connect()` was called while a connection attempt is already in flight.
    #[error("Already connecting!")]
    AlreadyConnecting,

    /// `connect()` was called while already connected.
    #[error("Already connected!")]
    AlreadyConnected,

    /// The streamer is not live (room status `4`).
    #[error("The requested user isn't online :(")]
    UserOffline,

    /// The WebSocket did not open within the configured timeout.
    #[error("Timed out whilst connecting to WebSocket")]
    ConnectTimeout,

    /// The WebSocket server rejected the upgrade request.
    #[error("Error connecting to WebSocket with message: {handshake_message}")]
    WebSocketHandshake {
        /// HTTP status of the rejected upgrade.
        status: u16,
        /// The `handshake-msg` header sent by TikTok (`N/A` when absent).
        handshake_message: String,
    },

    /// A protobuf message name that is not part of the schema.
    #[error("Invalid schema name: {0}")]
    InvalidSchemaName(String),

    /// A protobuf payload could not be decoded.
    #[error(transparent)]
    SchemaDecode(#[from] SchemaDecodeError),

    /// The sign server (Euler Stream) returned an error.
    #[error(transparent)]
    SignApi(#[from] SignApiError),

    /// The options passed to the connection are inconsistent (a `TypeError` in the Node library).
    #[error("{0}")]
    InvalidOptions(String),

    /// Transport-level HTTP failure (including non-2xx status codes from TikTok).
    #[error(transparent)]
    Http(Arc<reqwest::Error>),

    /// Transport-level WebSocket failure.
    #[error(transparent)]
    WebSocket(Arc<tokio_tungstenite::tungstenite::Error>),

    /// A JSON body could not be parsed.
    #[error(transparent)]
    Json(Arc<serde_json::Error>),

    /// I/O failure (e.g. while inflating a gzip payload).
    #[error(transparent)]
    Io(Arc<std::io::Error>),
}

macro_rules! from_shared {
    ($($variant:ident($ty:ty),)*) => {$(
        impl From<$ty> for Error {
            fn from(err: $ty) -> Self {
                Error::$variant(Arc::new(err))
            }
        }
    )*};
}

from_shared! {
    Http(reqwest::Error),
    WebSocket(tokio_tungstenite::tungstenite::Error),
    Json(serde_json::Error),
    Io(std::io::Error),
}

impl Error {
    pub(crate) fn invalid_response(route_id: &str, message: impl Into<String>) -> Self {
        Error::InvalidResponse { route_id: route_id.to_owned(), message: message.into() }
    }

    pub(crate) fn invalid_request(route_id: &str, message: impl Into<String>) -> Self {
        Error::InvalidRequest { route_id: route_id.to_owned(), message: message.into() }
    }

    /// For [`Error::SignApi`], the underlying sign server error.
    pub fn as_sign_api_error(&self) -> Option<&SignApiError> {
        match self {
            Error::SignApi(err) => Some(err),
            _ => None,
        }
    }
}

fn format_nested(errors: &[Error]) -> String {
    errors.iter().map(|e| format!("\n  - {e}")).collect()
}

/// A protobuf payload failed to decode.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Failed to decode message type: {message_type}: {source}{}", base64.as_ref().map(|b| format!(" Base64 - Use this to create an issue: {b}")).unwrap_or_default())]
pub struct SchemaDecodeError {
    /// Name of the protobuf message that was being decoded.
    pub message_type: String,
    /// The underlying decoder error.
    pub source: prost::DecodeError,
    /// The raw payload, base64-encoded, when `show_base64_on_decode_error` is enabled.
    pub base64: Option<String>,
}

/// Why the sign server call failed. Mirrors the Node library's `ErrorReason` enum; each value
/// corresponds to one of its `SignAPIError` subclasses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorReason {
    /// `SignatureRateLimitError`.
    RateLimit,
    /// The sign server could not be reached.
    ConnectError,
    /// `SignatureMissingTokensError`.
    EmptyPayload,
    /// The sign server answered with an unexpected status code.
    SignNot200,
    /// The sign server did not return any TikTok cookies.
    EmptyCookies,
    /// `PremiumFeatureError`.
    PremiumFeature,
    /// `AuthenticatedWebSocketConnectionError`.
    AuthenticatedWs,
}

impl ErrorReason {
    /// The human readable label used as message prefix (`Rate Limited`, `Sign Error`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorReason::RateLimit => "Rate Limited",
            ErrorReason::ConnectError => "Connect Error",
            ErrorReason::EmptyPayload => "Empty Payload",
            ErrorReason::SignNot200 => "Sign Error",
            ErrorReason::EmptyCookies => "Empty Cookies",
            ErrorReason::PremiumFeature => "Premium Feature",
            ErrorReason::AuthenticatedWs => "Authenticated WS",
        }
    }
}

impl fmt::Display for ErrorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An error returned by (or while talking to) the sign server.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct SignApiError {
    /// What went wrong.
    pub reason: ErrorReason,
    /// `x-request-id` / `x-log-id` of the failing request, when available.
    pub request_id: Option<String>,
    /// `x-agent-id` of the failing request, when available.
    pub agent_id: Option<String>,
    /// Rate limits only: how long to wait before retrying (from `retry-after`).
    pub retry_after: Option<Duration>,
    /// Rate limits only: the UNIX timestamp in milliseconds at which the limit resets.
    pub reset_time: Option<u64>,
    message: String,
}

impl SignApiError {
    pub(crate) fn new<I, S>(reason: ErrorReason, request_id: Option<String>, agent_id: Option<String>, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parts = vec![format!("[{reason}]")];
        parts.extend(args.into_iter().map(Into::into));
        SignApiError { reason, request_id, agent_id, retry_after: None, reset_time: None, message: parts.join(" ") }
    }

    /// `SignatureMissingTokensError`.
    pub(crate) fn missing_tokens(message: impl Into<String>) -> Self {
        Self::new(ErrorReason::EmptyPayload, None, None, [message.into()])
    }

    /// `PremiumFeatureError`: `args` followed by the boxed sign server message.
    pub(crate) fn premium_feature<I, S>(api_message: Option<&str>, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parts: Vec<String> = args.into_iter().map(Into::into).collect();
        if let Some(message) = api_message {
            parts.push(Self::format_sign_server_message(message));
        }
        Self::new(ErrorReason::PremiumFeature, None, None, parts)
    }

    /// `AuthenticatedWebSocketConnectionError`.
    pub(crate) fn authenticated_ws(message: impl Into<String>) -> Self {
        Self::new(ErrorReason::AuthenticatedWs, None, None, [message.into()])
    }

    /// The full error message, prefixed with the reason (e.g. `[Rate Limited] ...`).
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Draws the sign server's message inside an ASCII box, exactly like the Node library.
    pub fn format_sign_server_message(message: &str) -> String {
        let message = message.trim();
        let msg_len = message.chars().count();
        let header_text = "SIGN SERVER MESSAGE";
        let spare = msg_len.saturating_sub(header_text.len());
        let header_len = spare / 2;
        let padding_len = spare % 2;

        let footer = format!("+{}+", "-".repeat(msg_len + 2));
        let header = format!("+{} {header_text} {}+", "-".repeat(header_len), "-".repeat(header_len + padding_len));
        let prefix = format!("|{}|", " ".repeat(header.chars().count().saturating_sub(2)));
        let body = format!("| {message} |");

        format!("\n\t{prefix}\n\t{header}\n\t{body}\n\t{footer}\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_sign_server_message_like_node() {
        let boxed = SignApiError::format_sign_server_message("  This is a sign server message!!  ");
        let expected = [
            "",
            "\t|                                 |",
            "\t+------ SIGN SERVER MESSAGE ------+",
            "\t| This is a sign server message!! |",
            "\t+---------------------------------+",
            "",
        ]
        .join("\n");
        assert_eq!(boxed, expected);
        // Short messages do not panic (the Node version throws a RangeError).
        assert!(SignApiError::format_sign_server_message("hi").contains("| hi |"));
    }

    #[test]
    fn sign_api_error_messages() {
        let error =
            SignApiError::premium_feature(Some("Upgrade your plan to use this"), ["Error fetching the signed TikTok WebSocket"]);
        assert_eq!(error.reason, ErrorReason::PremiumFeature);
        assert!(error.message().starts_with("[Premium Feature] Error fetching the signed TikTok WebSocket \n\t|"));

        let composite = Error::InvalidResponseComposite {
            route_id: "fetchRoomIdRoute".to_owned(),
            message: "Failed to retrieve Room ID from all sources.".to_owned(),
            errors: vec![Error::invalid_response("a", "first"), Error::UserOffline],
        };
        assert_eq!(
            composite.to_string(),
            "Failed to retrieve Room ID from all sources.\n  - [a] first\n  - The requested user isn't online :("
        );
    }
}
