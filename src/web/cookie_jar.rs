//! A flat cookie store. TikTok cookies are not scoped by path or domain in practice, so every
//! cookie is sent with every request (same behavior as the Node library's `WebcastCookieJar`).

use std::sync::{Mutex, MutexGuard};

use indexmap::IndexMap;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};

use super::config::WebConfig;

/// Characters escaped by JavaScript's `encodeURIComponent`.
const URI_COMPONENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

pub(crate) fn encode_uri_component(value: &str) -> String {
    utf8_percent_encode(value, URI_COMPONENT).to_string()
}

fn decode_uri_component(value: &str) -> String {
    percent_decode_str(value).decode_utf8_lossy().into_owned()
}

/// A TikTok session made of the `sessionid` cookie and its data center (`tt-target-idc`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CookieSession {
    /// Value of the `sessionid` cookie.
    pub session_id: String,
    /// Value of the `tt-target-idc` cookie (e.g. `useast1a`).
    pub tt_target_idc: String,
}

impl CookieSession {
    /// Creates a new cookie session.
    pub fn new(session_id: impl Into<String>, tt_target_idc: impl Into<String>) -> Self {
        CookieSession { session_id: session_id.into(), tt_target_idc: tt_target_idc.into() }
    }
}

/// Thread-safe cookie store shared by the HTTP client and the WebSocket handshake.
#[derive(Debug)]
pub struct CookieJar {
    store: Mutex<IndexMap<String, String>>,
    session_cookie_names: Vec<String>,
    target_idc_cookie_name: String,
}

impl CookieJar {
    /// Creates a jar seeded from a `Cookie` header string (`a=1; b=2`).
    pub fn new(config: &WebConfig, initial_cookie_header: &str) -> Self {
        CookieJar {
            store: Mutex::new(Self::parse_cookies(initial_cookie_header)),
            session_cookie_names: config.session_cookie_names.clone(),
            target_idc_cookie_name: config.target_idc_cookie_name.clone(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, IndexMap<String, String>> {
        self.store.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Stores the session id under every session cookie name, plus `tt-target-idc`.
    pub fn set_session(&self, session: &CookieSession) {
        let mut store = self.lock();
        for name in &self.session_cookie_names {
            store.insert(name.clone(), session.session_id.clone());
        }
        store.insert(self.target_idc_cookie_name.clone(), session.tt_target_idc.clone());
    }

    /// Returns the session if both a session id and `tt-target-idc` are present.
    pub fn session(&self) -> Option<CookieSession> {
        let store = self.lock();
        let tt_target_idc = store.get(&self.target_idc_cookie_name).filter(|v| !v.is_empty())?;
        let session_id = self.session_cookie_names.iter().find_map(|name| store.get(name).filter(|v| !v.is_empty()))?;
        Some(CookieSession::new(session_id.clone(), tt_target_idc.clone()))
    }

    /// Processes one `Set-Cookie` header: only its leading `name=value` pair is kept.
    pub fn process_set_cookie_header(&self, header: &str) {
        let name_value = header.split(';').next().unwrap_or_default();
        let (name, value) = name_value.split_once('=').unwrap_or((name_value, ""));
        if !name.is_empty() {
            self.lock().insert(decode_uri_component(name), decode_uri_component(value));
        }
    }

    /// Returns a cookie's value.
    pub fn get(&self, name: &str) -> Option<String> {
        self.lock().get(name).cloned()
    }

    /// Sets a cookie.
    pub fn set(&self, name: impl Into<String>, value: impl Into<String>) {
        self.lock().insert(name.into(), value.into());
    }

    /// Removes a cookie, returning its value.
    pub fn remove(&self, name: &str) -> Option<String> {
        self.lock().shift_remove(name)
    }

    /// A snapshot of every cookie.
    pub fn cookies(&self) -> IndexMap<String, String> {
        self.lock().clone()
    }

    /// Serializes every cookie into a `Cookie` header value.
    pub fn cookie_string(&self) -> String {
        Self::serialize(self.lock().iter())
    }

    /// Serializes every cookie except the session ones (session ids and `tt-target-idc`).
    pub fn anonymous_cookie_string(&self) -> String {
        let store = self.lock();
        Self::serialize(
            store.iter().filter(|(name, _)| !self.session_cookie_names.contains(name) && **name != self.target_idc_cookie_name),
        )
    }

    /// Serializes a session into a `Cookie` header value using the names from `config`.
    pub fn serialize_session(config: &WebConfig, session: &CookieSession) -> String {
        let mut cookies: IndexMap<&str, &str> = IndexMap::new();
        for name in &config.session_cookie_names {
            cookies.insert(name, &session.session_id);
        }
        cookies.insert(&config.target_idc_cookie_name, &session.tt_target_idc);
        Self::serialize(cookies.into_iter())
    }

    fn serialize<K: AsRef<str>, V: AsRef<str>>(cookies: impl Iterator<Item = (K, V)>) -> String {
        cookies
            .map(|(k, v)| format!("{}={}", encode_uri_component(k.as_ref()), encode_uri_component(v.as_ref())))
            .collect::<Vec<_>>()
            .join("; ")
    }

    fn parse_cookies(header: &str) -> IndexMap<String, String> {
        header
            .split("; ")
            .filter(|part| !part.is_empty())
            .map(|part| {
                let (name, value) = part.split_once('=').unwrap_or((part, ""));
                (decode_uri_component(name), decode_uri_component(value))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jar(initial: &str) -> CookieJar {
        CookieJar::new(&WebConfig::default(), initial)
    }

    #[test]
    fn parses_initial_header() {
        let jar = jar("tt-target-idc=useast1a; a=b=c");
        assert_eq!(jar.get("tt-target-idc").as_deref(), Some("useast1a"));
        assert_eq!(jar.get("a").as_deref(), Some("b=c"));
    }

    #[test]
    fn session_round_trip() {
        let jar = jar("");
        assert_eq!(jar.session(), None);
        jar.set_session(&CookieSession::new("abc", "useast2a"));
        assert_eq!(jar.session(), Some(CookieSession::new("abc", "useast2a")));
        assert_eq!(jar.cookie_string(), "sessionid=abc; sessionid_ss=abc; sid_tt=abc; sid_guard=abc; tt-target-idc=useast2a");
    }

    #[test]
    fn anonymous_string_strips_session() {
        let jar = jar("tt-target-idc=useast1a; sessionid=x; ttwid=1%7Cabc");
        jar.process_set_cookie_header("msToken=a b; Path=/; Secure");
        assert_eq!(jar.anonymous_cookie_string(), "ttwid=1%7Cabc; msToken=a%20b");
    }

    #[test]
    fn serializes_session_like_node() {
        let session = CookieSession::new("s;1", "useast1a");
        assert_eq!(
            CookieJar::serialize_session(&WebConfig::default(), &session),
            "sessionid=s%3B1; sessionid_ss=s%3B1; sid_tt=s%3B1; sid_guard=s%3B1; tt-target-idc=useast1a"
        );
    }
}
