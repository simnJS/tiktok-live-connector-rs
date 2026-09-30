//! Events emitted by [`TikTokLiveConnection`].
//!
//! Every event of the Node library's `ClientEventMap` is a variant of [`Event`]. Listen to them
//! with typed callbacks (`connection.on_chat(|msg| ...)`), with [`EventKind`]-filtered callbacks
//! (`connection.on(EventKind::Chat, |event| ...)`), or consume them as an async stream
//! (`connection.subscribe()`).

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use bytes::Bytes;
use serde_json::Value;

use crate::client::TikTokLiveConnection;
use crate::emitter::ListenerId;
use crate::error::Error;
use crate::proto::*;
use crate::ws::WebcastWebSocketClient;

/// Snapshot of the connection state (`TikTokLiveConnectionState`).
#[derive(Debug, Clone, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(rename_all = "camelCase"))]
pub struct ConnectionState {
    /// Whether the connection is established.
    pub is_connected: bool,
    /// Whether a connection attempt is in flight.
    pub is_connecting: bool,
    /// The room id.
    pub room_id: String,
    /// Room info fetched on connect (`fetch_room_info_on_connect`).
    pub room_info: Option<Arc<Value>>,
    /// Gifts fetched on connect (`enable_extended_gift_info`).
    pub available_gifts: Option<Arc<Value>>,
}

/// An error reported through [`Event::Error`]: what was being done, and what went wrong.
#[derive(Debug, Clone)]
pub struct ErrorEvent {
    /// Context of the error (e.g. `Error while connecting`).
    pub info: String,
    /// The error itself.
    pub exception: Arc<Error>,
}

impl fmt::Display for ErrorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.info, self.exception)
    }
}

/// A message as received, before decoding.
#[derive(Debug, Clone)]
pub struct RawDataEvent {
    /// Protobuf message name (e.g. `WebcastChatMessage`).
    pub method: String,
    /// The raw protobuf payload.
    pub payload: Bytes,
}

/// Every decoded message, before it is dispatched to its specific event.
#[derive(Debug, Clone)]
pub struct DecodedDataEvent {
    /// Protobuf message name (e.g. `WebcastChatMessage`).
    pub method: String,
    /// The decoded message.
    pub message: WebcastMessage,
    /// The raw protobuf payload.
    pub payload: Bytes,
}

/// A gift, with the matching entry of the room's gift list when `enable_extended_gift_info` is
/// set. Dereferences to the [`WebcastGiftMessage`].
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize), serde(rename_all = "camelCase"))]
pub struct GiftEvent {
    /// The gift message.
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub message: Arc<WebcastGiftMessage>,
    /// The gift's entry in the room gift list (name, diamond count, image, ...).
    pub extended_gift_info: Option<Arc<Value>>,
}

impl Deref for GiftEvent {
    type Target = WebcastGiftMessage;

    fn deref(&self) -> &WebcastGiftMessage {
        &self.message
    }
}

/// The action of a [`WebcastControlMessage`] that ended the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct StreamEndEvent {
    /// `StreamEnded` (ended by the streamer) or `StreamSuspended` (ended by a moderator).
    pub action: ControlAction,
}

macro_rules! events {
    (
        control { $( $(#[$cmeta:meta])* $cvariant:ident $( ( $cty:ty ) )? => $cname:literal, )* }
        messages { $( $variant:ident, $on:ident => $name:literal, $ty:ident; )* }
    ) => {
        /// An event emitted by the connection.
        #[derive(Debug, Clone)]
        #[non_exhaustive]
        pub enum Event {
            $( $(#[$cmeta])* $cvariant $( ($cty) )?, )*
            $(
                #[doc = concat!("`", $name, "`: [`", stringify!($ty), "`].")]
                $variant(Arc<$ty>),
            )*
        }

        /// The kind of an [`Event`], without payload. [`EventKind::name`] is the Node library's
        /// event name.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[non_exhaustive]
        pub enum EventKind {
            $( $(#[$cmeta])* $cvariant, )*
            $( #[doc = concat!("`", $name, "`")] $variant, )*
        }

        impl EventKind {
            /// Every event kind.
            pub const ALL: &'static [EventKind] = &[$(EventKind::$cvariant,)* $(EventKind::$variant,)*];

            /// The event name used by the Node library (`chat`, `gift`, `streamEnd`, ...).
            pub fn name(self) -> &'static str {
                match self {
                    $( EventKind::$cvariant => $cname, )*
                    $( EventKind::$variant => $name, )*
                }
            }

            /// Looks an event kind up by its Node library name.
            pub fn from_name(name: &str) -> Option<Self> {
                match name {
                    $( $cname => Some(EventKind::$cvariant), )*
                    $( $name => Some(EventKind::$variant), )*
                    _ => None,
                }
            }
        }

        impl Event {
            /// The kind of this event.
            pub fn kind(&self) -> EventKind {
                match self {
                    $( events!(@pattern $cvariant $( $cty )?) => EventKind::$cvariant, )*
                    $( Event::$variant(_) => EventKind::$variant, )*
                }
            }
        }

        /// Maps a message to its event, for messages without special handling.
        pub(crate) fn basic_event(message: &WebcastMessage) -> Option<Event> {
            match message {
                $( WebcastMessage::$ty(m) => Some(Event::$variant(m.clone())), )*
                _ => None,
            }
        }

        impl TikTokLiveConnection {
            $(
                #[doc = concat!("Listens to `", $name, "` events ([`", stringify!($ty), "`]).")]
                pub fn $on<F>(&self, handler: F) -> ListenerId
                where
                    F: Fn(&$ty) + Send + Sync + 'static,
                {
                    self.on(EventKind::$variant, move |event| {
                        if let Event::$variant(message) = event {
                            handler(message)
                        }
                    })
                }
            )*
        }
    };
    (@pattern $variant:ident $ty:ty) => { Event::$variant(..) };
    (@pattern $variant:ident) => { Event::$variant };
}

events! {
    control {
        /// `connected`: the connection is established.
        Connected(ConnectionState) => "connected",
        /// `disconnected`: the WebSocket closed.
        Disconnected(DisconnectedEvent) => "disconnected",
        /// `error`: something went wrong. Only emitted when someone listens to it.
        Error(ErrorEvent) => "error",
        /// `rawData`: every message of a batch, decoded or not (emitted before `decodedData`).
        RawData(RawDataEvent) => "rawData",
        /// `decodedData`: every decoded message, before specific dispatch.
        DecodedData(DecodedDataEvent) => "decodedData",
        /// `websocketConnected`: the WebSocket is open.
        WebsocketConnected(WebcastWebSocketClient) => "websocketConnected",
        /// `websocketData`: every raw binary WebSocket message.
        WebsocketData(Bytes) => "websocketData",
        /// `enterRoom`: TikTok acknowledged the room enter request.
        EnterRoom(Arc<DecodedPushFrame>) => "enterRoom",
        /// `streamEnd`: the stream ended (the connection disconnects).
        StreamEnd(StreamEndEvent) => "streamEnd",
        /// `gift`: a gift was sent.
        Gift(GiftEvent) => "gift",
        /// `follow`: a social message about a follow.
        Follow(Arc<WebcastSocialMessage>) => "follow",
        /// `share`: a social message about a share.
        Share(Arc<WebcastSocialMessage>) => "share",
        /// `superFan`: a super fan barrage.
        SuperFan(Arc<WebcastBarrageMessage>) => "superFan",
        /// `superFanJoin`: someone became a super fan.
        SuperFanJoin(Arc<WebcastBarrageMessage>) => "superFanJoin",
        /// `superFanBox`: a super fan treasure box.
        SuperFanBox(Arc<WebcastEnvelopeMessage>) => "superFanBox",
    }
    messages {
        Chat, on_chat => "chat", WebcastChatMessage;
        Member, on_member => "member", WebcastMemberMessage;
        RoomUser, on_room_user => "roomUser", WebcastRoomUserSeqMessage;
        Social, on_social => "social", WebcastSocialMessage;
        Like, on_like => "like", WebcastLikeMessage;
        QuestionNew, on_question_new => "questionNew", WebcastQuestionNewMessage;
        LinkMicBattle, on_link_mic_battle => "linkMicBattle", WebcastLinkMicBattle;
        LinkMicArmies, on_link_mic_armies => "linkMicArmies", WebcastLinkMicArmies;
        LiveIntro, on_live_intro => "liveIntro", WebcastLiveIntroMessage;
        Emote, on_emote => "emote", WebcastEmoteChatMessage;
        Envelope, on_envelope => "envelope", WebcastEnvelopeMessage;
        Barrage, on_barrage => "barrage", WebcastBarrageMessage;
        ControlMessage, on_control_message => "controlMessage", WebcastControlMessage;
        System, on_system => "system", WebcastSystemMessage;
        GoalUpdate, on_goal_update => "goalUpdate", WebcastGoalUpdateMessage;
        RoomMessage, on_room_message => "roomMessage", WebcastRoomMessage;
        CaptionMessage, on_caption_message => "captionMessage", WebcastCaptionMessage;
        ImDelete, on_im_delete => "imDelete", WebcastImDeleteMessage;
        InRoomBanner, on_in_room_banner => "inRoomBanner", WebcastInRoomBannerMessage;
        RankUpdate, on_rank_update => "rankUpdate", WebcastRankUpdateMessage;
        PollMessage, on_poll_message => "pollMessage", WebcastPollMessage;
        RankText, on_rank_text => "rankText", WebcastRankTextMessage;
        LinkMicBattleItemCard, on_link_mic_battle_item_card => "linkMicBattleItemCard", WebcastLinkMicBattleItemCard;
        LinkMicBattlePunishFinish, on_link_mic_battle_punish_finish => "linkMicBattlePunishFinish", WebcastLinkMicBattlePunishFinish;
        LinkMicBattleTask, on_link_mic_battle_task => "linkMicBattleTask", WebcastLinkmicBattleTaskMessage;
        LinkMicFanTicketMethod, on_link_mic_fan_ticket_method => "linkMicFanTicketMethod", WebcastLinkMicFanTicketMethod;
        LinkMicMethod, on_link_mic_method => "linkMicMethod", WebcastLinkMicMethod;
        UnauthorizedMember, on_unauthorized_member => "unauthorizedMember", WebcastUnauthorizedMemberMessage;
        OecLiveShopping, on_oec_live_shopping => "oecLiveShopping", WebcastOecLiveShoppingMessage;
        MsgDetect, on_msg_detect => "msgDetect", WebcastMsgDetectMessage;
        LinkMessage, on_link_message => "linkMessage", WebcastLinkMessage;
        RoomVerify, on_room_verify => "roomVerify", WebcastRoomVerifyMessage;
        LinkLayer, on_link_layer => "linkLayer", WebcastLinkLayerMessage;
        RoomPin, on_room_pin => "roomPin", WebcastRoomPinMessage;
        AccessControl, on_access_control => "accessControl", WebcastAccessControlMessage;
        AccessRecall, on_access_recall => "accessRecall", WebcastAccessRecallMessage;
        BoostCard, on_boost_card => "boostCard", WebcastBoostCardMessage;
        BottomMessage, on_bottom_message => "bottomMessage", WebcastBottomMessage;
        Capsule, on_capsule => "capsule", WebcastCapsuleMessage;
        GameRankNotify, on_game_rank_notify => "gameRankNotify", WebcastGameRankNotifyMessage;
        GiftBroadcast, on_gift_broadcast => "giftBroadcast", WebcastGiftBroadcastMessage;
        GiftDynamicRestriction, on_gift_dynamic_restriction => "giftDynamicRestriction", WebcastGiftDynamicRestrictionMessage;
        GiftPanelUpdate, on_gift_panel_update => "giftPanelUpdate", WebcastGiftPanelUpdateMessage;
        GiftPrompt, on_gift_prompt => "giftPrompt", WebcastGiftPromptMessage;
        Guide, on_guide => "guide", WebcastGuideMessage;
        HourlyRank, on_hourly_rank => "hourlyRank", WebcastHourlyRankRewardMessage;
        LinkMicLayoutState, on_link_mic_layout_state => "linkMicLayoutState", WebcastLinkMicLayoutStateMessage;
        LinkState, on_link_state => "linkState", WebcastLinkStateMessage;
        LiveGameIntro, on_live_game_intro => "liveGameIntro", WebcastLiveGameIntroMessage;
        MarqueeAnnouncement, on_marquee_announcement => "marqueeAnnouncement", WebcastMarqueeAnnouncementMessage;
        Notice, on_notice => "notice", WebcastNoticeMessage;
        PartnershipDropsUpdate, on_partnership_drops_update => "partnershipDropsUpdate", WebcastPartnershipDropsUpdateMessage;
        PartnershipGameOffline, on_partnership_game_offline => "partnershipGameOffline", WebcastPartnershipGameOfflineMessage;
        PartnershipPunish, on_partnership_punish => "partnershipPunish", WebcastPartnershipPunishMessage;
        Perception, on_perception => "perception", WebcastPerceptionMessage;
        RoomNotify, on_room_notify => "roomNotify", WebcastRoomNotifyMessage;
        Speaker, on_speaker => "speaker", WebcastSpeakerMessage;
        SubNotify, on_sub_notify => "subNotify", WebcastSubNotifyMessage;
        SubPinEvent, on_sub_pin_event => "subPinEvent", WebcastSubPinEventMessage;
        Toast, on_toast => "toast", WebcastToastMessage;
        ViewerPicksUpdate, on_viewer_picks_update => "viewerPicksUpdate", WebcastViewerPicksUpdateMessage;
    }
}

/// Payload of [`Event::Disconnected`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct DisconnectedEvent {
    /// WebSocket close code (`1006` when the connection dropped).
    pub code: u16,
    /// Close reason.
    pub reason: String,
}

macro_rules! control_listeners {
    ($( $on:ident => $variant:ident($ty:ty), )*) => {
        impl TikTokLiveConnection {
            $(
                #[doc = concat!("Listens to [`Event::", stringify!($variant), "`].")]
                pub fn $on<F>(&self, handler: F) -> ListenerId
                where
                    F: Fn(&$ty) + Send + Sync + 'static,
                {
                    self.on(EventKind::$variant, move |event| {
                        if let Event::$variant(payload) = event {
                            handler(payload)
                        }
                    })
                }
            )*
        }
    };
}

control_listeners! {
    on_connected => Connected(ConnectionState),
    on_disconnected => Disconnected(DisconnectedEvent),
    on_error => Error(ErrorEvent),
    on_raw_data => RawData(RawDataEvent),
    on_decoded_data => DecodedData(DecodedDataEvent),
    on_websocket_connected => WebsocketConnected(WebcastWebSocketClient),
    on_websocket_data => WebsocketData(Bytes),
    on_enter_room => EnterRoom(Arc<DecodedPushFrame>),
    on_stream_end => StreamEnd(StreamEndEvent),
    on_gift => Gift(GiftEvent),
    on_follow => Follow(Arc<WebcastSocialMessage>),
    on_share => Share(Arc<WebcastSocialMessage>),
    on_super_fan => SuperFan(Arc<WebcastBarrageMessage>),
    on_super_fan_join => SuperFanJoin(Arc<WebcastBarrageMessage>),
    on_super_fan_box => SuperFanBox(Arc<WebcastEnvelopeMessage>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_names_round_trip() {
        for kind in EventKind::ALL {
            assert_eq!(EventKind::from_name(kind.name()), Some(*kind));
        }
        assert_eq!(EventKind::Chat.name(), "chat");
        assert_eq!(EventKind::StreamEnd.name(), "streamEnd");
        assert_eq!(EventKind::HourlyRank.name(), "hourlyRank");
    }
}
