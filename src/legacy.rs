//! The deprecated `WebcastPushConnection` of the Node library (`tiktok-live-connector/legacy`).
//!
//! Instead of typed protobuf messages, it emits flattened JSON objects produced by
//! [`simplify_object`]: the user attributes (`uniqueId`, `nickname`, `userBadges`, ...) and the
//! common header are merged into the event, the way 1.x versions of the library did.
//!
//! Prefer [`TikTokLiveConnection`] for new code.

use std::ops::Deref;
use std::sync::{Arc, OnceLock, Weak};

use bytes::Bytes;
use regex::Regex;
use serde_json::{Map, Value, json};

use crate::client::{ConnectionOptions, TikTokLiveConnection};
use crate::emitter::ListenerId;
use crate::error::Result;
use crate::events::{Event, EventKind};
use crate::proto::WebcastMessage;

/// An event of the legacy connection.
#[derive(Debug, Clone)]
pub struct LegacyEvent {
    /// Event name (`rawData`, `decodedData`, `chat`, `gift`, `follow`, `streamEnd`, ...).
    pub name: &'static str,
    /// Protobuf message name of the source message.
    pub method: String,
    /// The simplified message (`rawData`: `null`).
    pub data: Value,
    /// The raw protobuf payload of the source message.
    pub payload: Bytes,
}

type Handler = Arc<dyn Fn(&LegacyEvent) + Send + Sync>;

struct Listeners {
    handlers: std::sync::RwLock<Vec<(ListenerId, Option<&'static str>, Handler)>>,
    next_id: std::sync::atomic::AtomicU64,
}

/// The legacy connection. Dereferences to the underlying [`TikTokLiveConnection`] for everything
/// else (connecting, control events, HTTP helpers), like the Node subclass.
#[deprecated = "use TikTokLiveConnection instead"]
pub struct WebcastPushConnection {
    connection: TikTokLiveConnection,
    listeners: Arc<Listeners>,
}

#[allow(deprecated)]
impl Deref for WebcastPushConnection {
    type Target = TikTokLiveConnection;

    fn deref(&self) -> &TikTokLiveConnection {
        &self.connection
    }
}

#[allow(deprecated)]
impl WebcastPushConnection {
    /// Creates a legacy connection.
    pub fn new(unique_id: &str, options: ConnectionOptions) -> Result<Self> {
        let connection = TikTokLiveConnection::with_options(unique_id, options)?;
        let listeners = Arc::new(Listeners { handlers: Default::default(), next_id: Default::default() });

        let weak_listeners: Weak<Listeners> = Arc::downgrade(&listeners);
        let weak_connection = connection.downgrade();
        connection.on(EventKind::RawData, move |event| {
            let (Event::RawData(raw), Some(listeners)) = (event, weak_listeners.upgrade()) else { return };
            let gifts = weak_connection.upgrade().and_then(|c| c.available_gifts());
            process_message(&listeners, &raw.method, &raw.payload, gifts.as_deref());
        });

        Ok(WebcastPushConnection { connection, listeners })
    }

    /// The underlying modern connection.
    pub fn connection(&self) -> &TikTokLiveConnection {
        &self.connection
    }

    /// Calls `handler` for every legacy event named `name` (e.g. `chat`, `gift`, `rawData`).
    pub fn on<F>(&self, name: &'static str, handler: F) -> ListenerId
    where
        F: Fn(&LegacyEvent) + Send + Sync + 'static,
    {
        self.listeners.add(Some(name), Arc::new(handler))
    }

    /// Calls `handler` for every legacy event.
    pub fn on_any_legacy<F>(&self, handler: F) -> ListenerId
    where
        F: Fn(&LegacyEvent) + Send + Sync + 'static,
    {
        self.listeners.add(None, Arc::new(handler))
    }

    /// Removes a legacy listener.
    pub fn off(&self, id: ListenerId) -> bool {
        let mut handlers = self.listeners.handlers.write().unwrap_or_else(|p| p.into_inner());
        let before = handlers.len();
        handlers.retain(|(listener, _, _)| *listener != id);
        before != handlers.len()
    }
}

impl Listeners {
    fn add(&self, name: Option<&'static str>, handler: Handler) -> ListenerId {
        let id = ListenerId::from_raw(self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        self.handlers.write().unwrap_or_else(|p| p.into_inner()).push((id, name, handler));
        id
    }

    fn emit(&self, name: &'static str, method: &str, data: &Value, payload: &Bytes) {
        let handlers: Vec<Handler> = {
            let handlers = self.handlers.read().unwrap_or_else(|p| p.into_inner());
            handlers.iter().filter(|(_, n, _)| n.is_none_or(|n| n == name)).map(|(_, _, h)| h.clone()).collect()
        };
        if handlers.is_empty() {
            return;
        }
        let event = LegacyEvent { name, method: method.to_owned(), data: data.clone(), payload: payload.clone() };
        for handler in handlers {
            handler(&event);
        }
    }
}

fn process_message(listeners: &Listeners, method: &str, payload: &Bytes, gifts: Option<&Value>) {
    listeners.emit("rawData", method, &Value::Null, payload);

    let decoded = match WebcastMessage::decode(method, payload.clone()) {
        Some(Ok(message)) => message.to_json(),
        _ => Value::Object(Map::new()),
    };
    let mut data = simplify_object(method, decoded);
    listeners.emit("decodedData", method, &data, payload);

    let emit = |name: &'static str, data: &Value| listeners.emit(name, method, data, payload);
    let display_type =
        |data: &Value, pointer: &str| data.pointer(pointer).and_then(Value::as_str).unwrap_or_default().to_lowercase();

    match method {
        "WebcastControlMessage" => {
            let action = data.get("action").and_then(Value::as_i64).unwrap_or_default();
            if matches!(action, 3 | 4) {
                emit("streamEnd", &json!({ "action": action }));
            }
        }
        "WebcastRoomUserSeqMessage" => emit("roomUser", &data),
        "WebcastChatMessage" => emit("chat", &data),
        "WebcastMemberMessage" => emit("member", &data),
        "WebcastGiftMessage" => {
            let gift_id = data.get("giftId").and_then(Value::as_i64).unwrap_or_default();
            if gift_id != 0
                && let Some(info) =
                    gifts.and_then(Value::as_array).and_then(|g| g.iter().find(|x| x["id"].as_i64() == Some(gift_id)))
            {
                data["extendedGiftInfo"] = info.clone();
            }
            emit("gift", &data);
        }
        "WebcastSocialMessage" => {
            emit("social", &data);
            let kind = display_type(&data, "/displayType");
            if kind.contains("follow") {
                emit("follow", &data);
            }
            if kind.contains("share") {
                emit("share", &data);
            }
        }
        "WebcastLikeMessage" => emit("like", &data),
        "WebcastQuestionNewMessage" => emit("questionNew", &data),
        "WebcastLinkMicBattle" => emit("linkMicBattle", &data),
        "WebcastLinkMicArmies" => emit("linkMicArmies", &data),
        "WebcastLiveIntroMessage" => emit("liveIntro", &data),
        "WebcastEmoteChatMessage" => emit("emote", &data),
        "WebcastBarrageMessage" => {
            emit("barrage", &data);
            let kinds =
                ["/content/displayType", "/commonBarrageContent/displayType", "/displayType"].map(|p| display_type(&data, p));
            if kinds.iter().any(|k| k.contains("ttlive_superfan_commentnotif_superfanjoined")) {
                emit("superFanJoin", &data);
            } else if kinds.iter().any(|k| k.contains("ttlive_superfan")) {
                emit("superFan", &data);
            }
        }
        "WebcastEnvelopeMessage" => {
            emit("envelope", &data);
            let business_type = data.pointer("/envelopeInfo/businessType").and_then(Value::as_i64);
            if display_type(&data, "/displayType").contains("ttlive_superfanbox") || business_type == Some(19) {
                emit("superFanBox", &data);
            }
        }
        _ => {}
    }
}

fn take(object: &mut Map<String, Value>, key: &str) -> Value {
    object.remove(key).unwrap_or(Value::Null)
}

fn merge(target: &mut Map<String, Value>, source: Value) {
    if let Value::Object(source) = source {
        target.extend(source);
    }
}

fn first_url(image: &Value) -> Value {
    image.pointer("/urlList/0").cloned().unwrap_or(Value::Null)
}

/// Flattens a message (as camelCase JSON) the way the 1.x library did: user attributes and the
/// common header are merged into the top level, `displayText.key` becomes `displayType`, and a few
/// message types get convenience fields (`topViewers`, `battleUsers`, `emotes`, gift details).
pub fn simplify_object(method: &str, value: Value) -> Value {
    let Value::Object(mut object) = value else { return value };

    if let Some(user) = object.remove("user") {
        merge(&mut object, user_attributes(&user));
    }

    if let Some(Value::Object(mut common)) = object.remove("common") {
        if let Some(Value::Object(text)) = common.remove("displayText") {
            object.insert("displayType".to_owned(), text.get("key").cloned().unwrap_or(Value::Null));
            object.insert("label".to_owned(), text.get("defaultPattern").cloned().unwrap_or(Value::Null));
        }
        for key in ["msgId", "createTime"] {
            if let Some(v) = common.get_mut(key) {
                *v = Value::String(v.to_string().trim_matches('"').to_owned());
            }
        }
        object.extend(common);
    }

    match method {
        "WebcastRoomUserSeqMessage" => {
            let ranks = take(&mut object, "ranks");
            let viewers: Vec<Value> = ranks
                .as_array()
                .into_iter()
                .flatten()
                .map(|viewer| {
                    json!({
                        "user": viewer.get("user").filter(|u| !u.is_null()).map(user_attributes),
                        "coinCount": viewer.get("score").and_then(Value::as_i64).unwrap_or(0),
                    })
                })
                .collect();
            object.insert("topViewers".to_owned(), Value::Array(viewers));
        }
        "WebcastLinkMicBattle" => {
            let users: Vec<Value> = object
                .get("anchorsInfo")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|anchor| anchor.get("value").filter(|v| !v.is_null()))
                .map(|value| user_attributes(value.get("user").unwrap_or(&Value::Null)))
                .collect();
            object.insert("battleUsers".to_owned(), Value::Array(users));
        }
        "WebcastGiftMessage" => {
            let repeat_end = object.get("repeatEnd").and_then(Value::as_i64).unwrap_or(0) != 0;
            object.insert("repeatEnd".to_owned(), Value::Bool(repeat_end));
            let details = object.get("gift").cloned().unwrap_or(Value::Null);
            object.insert(
                "gift".to_owned(),
                json!({
                    "gift_id": object.get("giftId"),
                    "repeat_count": object.get("repeatCount"),
                    "repeat_end": u8::from(repeat_end),
                    "gift_type": details.get("type"),
                }),
            );
            if let Some(image) = details.get("image") {
                object.insert("giftPictureUrl".to_owned(), first_url(image));
            }
            object.insert("giftName".to_owned(), details.get("name").cloned().unwrap_or(Value::Null));
            object.insert("diamondCount".to_owned(), details.get("diamondCount").cloned().unwrap_or(Value::Null));
            object.insert("giftType".to_owned(), details.get("type").cloned().unwrap_or(Value::Null));
            if let Some(to_user) = object.get("toUser").filter(|u| !u.is_null()) {
                let receiver = to_user.get("idStr").cloned().unwrap_or(Value::Null);
                object.insert("receiverUserId".to_owned(), receiver);
            }
            if let Some(Value::String(extra)) = object.get("monitorExtra")
                && extra.starts_with('{')
                && let Ok(parsed) = serde_json::from_str::<Value>(extra)
            {
                object.insert("monitorExtra".to_owned(), parsed);
            }
        }
        "WebcastChatMessage" => {
            let emotes: Vec<Value> = object
                .get("emotes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|emote| {
                    json!({
                        "emoteId": emote.pointer("/emote/emoteId"),
                        "emoteImageUrl": emote.pointer("/emote/image/urlList/0"),
                        "placeInComment": emote.get("index"),
                    })
                })
                .collect();
            object.insert("emotes".to_owned(), Value::Array(emotes));
        }
        "WebcastEmoteChatMessage" => {
            let emotes: Vec<Value> = object
                .get("emoteList")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|emote| json!({ "emoteId": emote.get("emoteId"), "emoteImageUrl": emote.pointer("/image/urlList/0") }))
                .collect();
            object.insert("emotes".to_owned(), Value::Array(emotes));
        }
        _ => {}
    }

    Value::Object(object)
}

fn non_empty(value: Option<&Value>) -> Value {
    match value {
        Some(Value::String(s)) if s.is_empty() => Value::Null,
        Some(v) => v.clone(),
        None => Value::Null,
    }
}

/// The flattened attributes of a user (`userId`, `uniqueId`, `nickname`, `userBadges`,
/// `isModerator`, `gifterLevel`, ...).
pub fn user_attributes(user: &Value) -> Value {
    let badges = map_badges(user.get("badgeList"));
    let avatar_urls = user.pointer("/avatarLarge/urlList").cloned().unwrap_or(Value::Array(vec![]));
    let user_id = match user.get("idStr") {
        Some(Value::String(s)) if !s.is_empty() => Value::String(s.clone()),
        _ => user.get("id").map_or(Value::Null, |id| Value::String(id.to_string())),
    };

    let mut attributes = json!({
        "userId": user_id,
        "secUid": non_empty(user.get("secUid")),
        "uniqueId": non_empty(user.get("displayId")),
        "nickname": non_empty(user.get("nickname")),
        "profilePictureUrl": preferred_picture_format(avatar_urls.as_array().map(Vec::as_slice).unwrap_or_default()),
        "followRole": user.pointer("/followInfo/followStatus"),
        "userBadges": badges,
        "userSceneTypes": user.get("badgeList").and_then(Value::as_array).map(|list| {
            list.iter().map(|b| b.get("sceneType").cloned().unwrap_or(json!(0))).collect::<Vec<_>>()
        }),
        "userDetails": {
            "createTime": user.get("createTime").map(std::string::ToString::to_string),
            "bioDescription": user.get("bioDescription"),
            "profilePictureUrls": avatar_urls,
        },
    });

    if let Some(info) = user.get("followInfo").filter(|i| i.is_object()) {
        attributes["followInfo"] = json!({
            "followingCount": info.get("followingCount"),
            "followerCount": info.get("followerCount"),
            "followStatus": info.get("followStatus"),
            "pushStatus": info.get("pushStatus"),
        });
    }

    let badges = attributes["userBadges"].as_array().cloned().unwrap_or_default();
    let text = |badge: &Value, key: &str| badge.get(key).and_then(Value::as_str).unwrap_or_default().to_lowercase();
    let scene = |badge: &Value| badge.get("badgeSceneType").and_then(Value::as_i64).unwrap_or(-1);
    let level_of = |scene_type: i64| {
        badges.iter().find(|b| scene(b) == scene_type).and_then(|b| b.get("level")).and_then(Value::as_i64).unwrap_or(0)
    };

    attributes["isModerator"] = json!(badges.iter().any(|b| text(b, "name").contains("moderator") || scene(b) == 1));
    attributes["isNewGifter"] = json!(badges.iter().any(|b| text(b, "name").contains("live_ng_")));
    attributes["isSubscriber"] = json!(badges.iter().any(|b| text(b, "url").contains("/sub_") || matches!(scene(b), 4 | 7)));
    attributes["topGifterRank"] = badges
        .iter()
        .filter_map(|b| b.get("url").and_then(Value::as_str))
        .find_map(|url| top_gifter_pattern().captures(url).and_then(|c| c[1].parse::<i64>().ok()))
        .map_or(Value::Null, |rank| json!(rank));
    attributes["gifterLevel"] = json!(level_of(8));
    attributes["teamMemberLevel"] = json!(level_of(10));

    attributes
}

fn top_gifter_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"ranklist_top_gifter_(\d+)\.png").expect("valid regex"))
}

/// Flattens a user's badge list into `{ type, badgeSceneType, ... }` entries.
pub fn map_badges(badges: Option<&Value>) -> Value {
    let mut simplified = Vec::new();
    for badge in badges.and_then(Value::as_array).into_iter().flatten() {
        let scene_type = badge.get("sceneType").cloned().unwrap_or(json!(0));
        let display_type = badge.get("displayType").cloned().unwrap_or(Value::Null);

        if let Some(image) = badge.get("image").filter(|i| !i.is_null()) {
            let url = first_url(image.get("image").unwrap_or(&Value::Null));
            if !url.is_null() {
                simplified
                    .push(json!({ "type": "image", "badgeSceneType": scene_type, "displayType": display_type, "url": url }));
            }
        }
        if let Some(text) = badge.get("text").filter(|t| !t.is_null()) {
            simplified.push(json!({
                "type": "text",
                "badgeSceneType": scene_type,
                "displayType": display_type,
                "name": text.get("key"),
                "text": text.get("defaultPattern"),
            }));
        }
        let level = badge.pointer("/privilegeLogExtra/level").and_then(Value::as_str).unwrap_or_default();
        if !level.is_empty() && level != "0" {
            simplified.push(json!({
                "type": "privilege",
                "privilegeId": badge.pointer("/privilegeLogExtra/privilegeId"),
                "level": level.parse::<i64>().unwrap_or(0),
                "badgeSceneType": scene_type,
            }));
        }
    }
    Value::Array(simplified)
}

/// Picks the best avatar URL: 100x100 webp, then 100x100 jpeg, then any unshrunk one.
pub fn preferred_picture_format(urls: &[Value]) -> Value {
    let urls: Vec<&str> = urls.iter().filter_map(Value::as_str).collect();
    urls.iter()
        .find(|u| u.contains("100x100") && u.contains(".webp"))
        .or_else(|| urls.iter().find(|u| u.contains("100x100") && u.contains(".jpeg")))
        .or_else(|| urls.iter().find(|u| !u.contains("shrink")))
        .or_else(|| urls.first())
        .map_or(Value::Null, |u| Value::String((*u).to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::webcast::model::base::ImageModel;
    use crate::proto::webcast::model::base::user::{BadgeStruct, FollowInfo, PrivilegeLogExtra};
    use crate::proto::webcast::model::message::common::Text;
    use crate::proto::{CommonMessageData, User, WebcastChatMessage};
    use prost::Message as _;

    #[test]
    fn simplifies_chat_messages() {
        let message = WebcastChatMessage {
            common: Some(CommonMessageData {
                msg_id: 42,
                display_text: Some(Text { key: "chat_key".to_owned(), ..Default::default() }),
                ..Default::default()
            }),
            user: Some(User {
                id_str: "123".to_owned(),
                display_id: "someone".to_owned(),
                nickname: "Someone".to_owned(),
                avatar_large: Some(ImageModel {
                    url_list: vec!["https://x/shrink.jpeg".to_owned(), "https://x/100x100.webp".to_owned()],
                    ..Default::default()
                }),
                follow_info: Some(FollowInfo { follower_count: 7, ..Default::default() }),
                badge_list: vec![BadgeStruct {
                    scene_type: 8,
                    privilege_log_extra: Some(PrivilegeLogExtra { level: "12".to_owned(), ..Default::default() }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            content: "hello".to_owned(),
            ..Default::default()
        };
        let json = WebcastMessage::decode("WebcastChatMessage", message.encode_to_vec().into()).unwrap().unwrap().to_json();
        let simplified = simplify_object("WebcastChatMessage", json);

        assert_eq!(simplified["content"], "hello");
        assert_eq!(simplified["userId"], "123");
        assert_eq!(simplified["uniqueId"], "someone");
        assert_eq!(simplified["nickname"], "Someone");
        assert_eq!(simplified["profilePictureUrl"], "https://x/100x100.webp");
        assert_eq!(simplified["followInfo"]["followerCount"], 7);
        assert_eq!(simplified["gifterLevel"], 12);
        assert_eq!(simplified["displayType"], "chat_key");
        assert_eq!(simplified["msgId"], "42");
        assert!(simplified.get("user").is_none());
        assert!(simplified.get("common").is_none());
        assert_eq!(simplified["emotes"], json!([]));
    }
}
