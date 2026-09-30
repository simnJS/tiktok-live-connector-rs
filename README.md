# tiktok-live-connector (Rust)

[![CI](https://github.com/simnJS/tiktok-live-connector-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/simnJS/tiktok-live-connector-rs/actions/workflows/ci.yml)

A Rust port of [TikTok-Live-Connector](https://github.com/zerodytrash/TikTok-Live-Connector) (Node.js, v2.5.0):
receive real-time events — chat, gifts, likes, follows, shares, viewers, battles, polls, and 60+ more —
from any TikTok LIVE stream.

- **Same protocol, same behavior**: identical HTTP/WebSocket parameters, room id resolution chain
  (HTML → API → Euler Stream), sign server flow, heartbeat, room enter, ACKs, event names and dispatch
  rules (`follow`/`share`, `superFan`, `streamEnd`, ...).
- **Typed**: every Webcast message is a generated [`prost`](https://docs.rs/prost) struct (TikTok Webcast
  protobuf schema v3). Events are a single `Event` enum.
- **Fast**: zero-copy protobuf payloads (`bytes::Bytes`), reference-counted events fanned out without copies,
  optional per-type decode filtering, `tokio` + `rustls`.
- **No `protoc` needed** — bindings are pre-generated (and reproducible with `cargo run -p codegen`).

> This is an unofficial library, not affiliated with TikTok. It relies on the
> [Euler Stream](https://www.eulerstream.com) sign server, like the original.

## Install

```toml
[dependencies]
tiktok-live-connector = { git = "https://github.com/simnJS/tiktok-live-connector-rs" }
tokio = { version = "1", features = ["full"] }
```

Features: `serde` (JSON serialization of every message, camelCase like the Node library), `legacy`
(the deprecated `WebcastPushConnection`).

## Getting started

```rust
use tiktok_live_connector::{Event, TikTokLiveConnection};

#[tokio::main]
async fn main() -> tiktok_live_connector::Result<()> {
    // Username of someone who is currently live (`user`, `@user` or the full live URL)
    let connection = TikTokLiveConnection::new("officialgeilegisela")?;

    // Callbacks, like `connection.on(WebcastEvent.CHAT, ...)`
    connection.on_chat(|msg| {
        let user = msg.user.as_ref().map(|u| u.display_id.as_str()).unwrap_or_default();
        println!("{user} writes: {}", msg.content);
    });
    connection.on_gift(|gift| println!("gift {} x{}", gift.gift_id, gift.repeat_count));

    let state = connection.connect().await?;
    println!("Connected to roomId {}", state.room_id);

    // ...or an async stream of every event
    let mut events = connection.subscribe();
    while let Some(event) = events.recv().await {
        match event {
            Event::Like(like) => println!("+{} likes", like.count),
            Event::Follow(follow) => println!("new follower"),
            Event::StreamEnd(end) => println!("stream ended: {:?}", end.action),
            Event::Disconnected(_) => break,
            _ => {}
        }
    }
    Ok(())
}
```

Run the examples against a live user:

```sh
cargo run --example chat -- <username>
cargo run --example is_live -- <username> <username>...
cargo run --example legacy --features legacy -- <username>
```

## Options

`TikTokLiveConnection::with_options(unique_id, ConnectionOptions { .. })` — mirrors the Node options:

| Node option | Rust field | Default |
|---|---|---|
| `processInitialData` | `process_initial_data` | `true` |
| `fetchRoomInfoOnConnect` | `fetch_room_info_on_connect` | `true` (fails with `Error::UserOffline` when not live) |
| `enableExtendedGiftInfo` | `enable_extended_gift_info` | `false` (fills `GiftEvent::extended_gift_info`) |
| `signApiKey` | `sign_api_key` | `SIGN_API_KEY` env |
| `eulerApiInstance` | `euler_client` | shared client |
| `session.cookie` | `session: Some(CookieSession::new(session_id, tt_target_idc))` | — |
| `session.oAuthToken` | `oauth_token` | — |
| `authenticateWs` / `useMobile` | `authenticate_ws` / `use_mobile` | `false` |
| `clientPresets` | `client_presets` | `ClientPresets::random()` |
| `webClientOptions` (headers, searchParams, agent) | `web_headers`, `web_params`, `http_client`, `proxy` | — |
| `wsClientOptions` (headers, agent) | `ws_headers`, `proxy` | — |
| `webConfigOverrides` / `wsConfigOverrides` | `web_config` / `ws_config` | generated from presets |
| `WebcastDeserializeConfig` (global) | `deserialize` (per connection) | decode everything |
| `WS_CONNECT_TIMEOUT_MS` / `WS_DISCONNECT_TIMEOUT_MS` | `ws_connect_timeout` / `ws_disconnect_timeout` | env or 20 s / 2 s |
| `RouteConfig` (global) | `routes: Some(Arc<dyn Routes>)` | `DefaultRoutes` |

```rust
use std::collections::HashSet;
use tiktok_live_connector::{ConnectionOptions, DeserializeConfig, TikTokLiveConnection};

let options = ConnectionOptions {
    enable_extended_gift_info: true,
    proxy: Some("http://user:pass@127.0.0.1:8080".into()),
    // Only decode what you need on busy streams
    deserialize: DeserializeConfig {
        include_message_types: Some(HashSet::from(["WebcastChatMessage".into(), "WebcastGiftMessage".into()])),
        ..Default::default()
    },
    ..Default::default()
};
let connection = TikTokLiveConnection::with_options("user", options)?;
```

## Methods and properties

| Node | Rust |
|---|---|
| `connect(roomId?)` | `connect()` / `connect_to_room(Some(room_id))` |
| `disconnect()` | `disconnect()` |
| `fetchRoomId(uniqueId?)` | `fetch_room_id(None)` |
| `fetchIsLive(uniqueId?)` | `fetch_is_live(None)` |
| `fetchRoomInfo(roomId?)` | `fetch_room_info(None)` |
| `fetchAvailableGifts()` | `fetch_available_gifts()` |
| `sendMessage(content, roomId?)` | `send_message(content, None)` |
| `waitUntilLive(seconds, signal)` | `wait_until_live(interval)` (drop the future to abort) |
| `state`, `isConnected`, `isConnecting`, `roomId`, `roomInfo`, `availableGifts`, `clientParams`, `uniqueId` | same, as methods |
| `webClient`, `apiClient`, `wsClient` | `web_client()`, `api_client()`, `ws_client()` |
| `on(event, handler)` | `on_<event>(handler)`, `on(EventKind, handler)`, `once`, `on_any`, `off`, `subscribe()` |

Errors from HTTP helpers are also reported through the `error` event, like the Node `@HandleError` decorator.

## Events

Control events: `connected`, `disconnected`, `error`, `rawData`, `decodedData`, `websocketConnected`,
`websocketData`, `enterRoom`, `streamEnd`.

Message events (`Event` variant → `on_*` method → protobuf message):
`chat`, `member`, `gift` (`GiftEvent`, with `extended_gift_info`), `roomUser`, `social`, `like`, `questionNew`,
`linkMicBattle`, `linkMicArmies`, `liveIntro`, `emote`, `envelope`, `follow`, `share`, `controlMessage`,
`barrage`, `superFan`, `superFanJoin`, `superFanBox`, `system`, `goalUpdate`, `roomMessage`, `captionMessage`,
`imDelete`, `inRoomBanner`, `rankUpdate`, `pollMessage`, `rankText`, `linkMicBattleItemCard`,
`linkMicBattlePunishFinish`, `linkMicBattleTask`, `linkMicFanTicketMethod`, `linkMicMethod`,
`unauthorizedMember`, `oecLiveShopping`, `msgDetect`, `linkMessage`, `roomVerify`, `linkLayer`, `roomPin`,
`accessControl`, `accessRecall`, `boostCard`, `bottomMessage`, `capsule`, `gameRankNotify`, `giftBroadcast`,
`giftDynamicRestriction`, `giftPanelUpdate`, `giftPrompt`, `guide`, `hourlyRank`, `linkMicLayoutState`,
`linkState`, `liveGameIntro`, `marqueeAnnouncement`, `notice`, `partnershipDropsUpdate`,
`partnershipGameOffline`, `partnershipPunish`, `perception`, `roomNotify`, `speaker`, `subNotify`,
`subPinEvent`, `toast`, `viewerPicksUpdate`.

`EventKind::name()` returns the Node event name; `EventKind::from_name("chat")` goes the other way.

## Routes

Every HTTP route of the Node `RouteConfig` is a method of the `Routes` trait, with a default implementation.
Override only what you need — overrides are honored everywhere, including by the composite routes:

```rust
use std::sync::Arc;
use async_trait::async_trait;
use tiktok_live_connector::web::{Routes, WebcastHttpClient};

struct MyRoutes;

#[async_trait]
impl Routes for MyRoutes {
    async fn fetch_room_info(&self, web: &WebcastHttpClient, room_id: Option<&str>) -> tiktok_live_connector::Result<serde_json::Value> {
        // your implementation
        todo!()
    }
}
```

`DefaultRoutes { room_id_sources, is_live_sources, html_extraction_pattern }` replaces the Node
`RoomIdRouteConfig`, `IsLiveRouteConfig` and `RoomInfoFromHtmlRouteConfig` globals.

## Authenticated connections and sending messages

```rust
let options = ConnectionOptions {
    session: Some(CookieSession::new("<sessionid>", "<tt-target-idc>")),
    authenticate_ws: true, // requires WHITELIST_AUTHENTICATED_SESSION_ID_HOST=api.eulerstream.com
    ..Default::default()
};
let connection = TikTokLiveConnection::with_options("user", options)?;
connection.connect().await?;
connection.send_message("Hello!", None).await?; // requires an Euler Stream plan with chat
```

## Environment variables

`SIGN_API_KEY`, `SIGN_API_URL`, `WHITELIST_AUTHENTICATED_SESSION_ID_HOST`, `SIGN_SERVER_MESSAGE_DISABLED`,
`TIKTOK_CLIENT_TIMEOUT`, `WS_CONNECT_TIMEOUT_MS`, `WS_DISCONNECT_TIMEOUT_MS`, `RANDOMIZE_TIKTOK_DEVICE`,
`RANDOMIZE_TIKTOK_LOCATION`, `RANDOMIZE_TIKTOK_SCREEN`, `DISABLE_ACK_LOG_WARNING` — same meaning as in Node.
Logs go through [`tracing`](https://docs.rs/tracing).

## Differences with the Node library

Intentional, where the Node behavior is a bug or does not translate to Rust:

- Global mutable config (`RouteConfig`, `SignConfig`, `WebcastDeserializeConfig`, `*RouteConfig`) is per
  connection (`routes`, `euler_client`, `deserialize`).
- `extended_gift_info` actually matches (Node compares a string id with a numeric one and never matches).
- HTTPS is used for TikTok hosts and plain HTTP only for loopback hosts (the Node check is inverted).
- Heartbeats use the current room after `switch_rooms` (Node keeps the initial one).
- Frames that are not message batches (heartbeat echoes) are ignored instead of failing to decode — the
  Node decoder silently yields an empty batch for them.
- Dropping a `connect()` future resets the state instead of leaving it `CONNECTING`.
- A panicking listener is logged and does not kill the connection.
- `legacy::simplify_object` is adapted to the v3 schema (the Node version expects v2 field names).

## Regenerating the protobuf bindings

The schema lives in `proto/` (from [TikTok-Webcast-Protobuf](https://github.com/isaackogan/TikTok-Webcast-Protobuf)).
After updating it:

```sh
cargo run -p codegen
```

## License

AGPL-3.0-only, like the original project. The protobuf schema is subject to its own license (`proto/LICENSE`).
