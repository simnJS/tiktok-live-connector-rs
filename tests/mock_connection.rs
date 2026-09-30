//! End-to-end test of the connection flow against local mock servers: sign server, TikTok webcast
//! API and push WebSocket.

use std::io::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flate2::Compression;
use flate2::write::GzEncoder;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tiktok_live_connector::proto::webcast::model::message::common::Text;
use tiktok_live_connector::proto::*;
use tiktok_live_connector::web::WebConfig;
use tiktok_live_connector::{ConnectionOptions, EulerStreamApiClient, Event, EventKind, SignConfig, TikTokLiveConnection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

const ROOM_ID: &str = "7000000000000000001";

fn common(method: &str, key: &str) -> Option<CommonMessageData> {
    Some(CommonMessageData {
        method: method.to_owned(),
        display_text: Some(Text { key: key.to_owned(), ..Default::default() }),
        ..Default::default()
    })
}

fn base_message(method: &str, payload: Vec<u8>) -> BaseProtoMessage {
    BaseProtoMessage { method: method.to_owned(), payload: payload.into(), ..Default::default() }
}

fn chat(content: &str) -> BaseProtoMessage {
    let message =
        WebcastChatMessage { common: common("WebcastChatMessage", ""), content: content.to_owned(), ..Default::default() };
    base_message("WebcastChatMessage", message.encode_to_vec())
}

/// Minimal HTTP/1.1 server answering the sign server and TikTok API routes.
async fn http_server(listener: TcpListener, ws_port: u16, requests: Arc<Mutex<Vec<String>>>) {
    loop {
        let Ok((mut socket, _)) = listener.accept().await else { return };
        let requests = requests.clone();
        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut chunk = [0u8; 4096];
            while !buffer.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    return;
                }
                buffer.extend_from_slice(&chunk[..n]);
            }
            let head = String::from_utf8_lossy(&buffer).into_owned();
            let target = head.split_whitespace().nth(1).unwrap_or_default().to_owned();
            requests.lock().unwrap().push(head.clone());

            let (content_type, extra_headers, body): (&str, String, Vec<u8>) = if target.starts_with("/webcast/room/info/") {
                ("application/json", String::new(), br#"{"data":{"status":2,"title":"mock"}}"#.to_vec())
            } else if target.starts_with(&format!("/webcast/rooms/{ROOM_ID}/connect")) {
                let result = ProtoMessageFetchResult {
                    messages: vec![chat("initial message")],
                    cursor: "cursor-1".to_owned(),
                    internal_ext: "ext-1".to_owned(),
                    push_server: format!("ws://127.0.0.1:{ws_port}/webcast/im/ws_proxy/ws_reuse_supplement/"),
                    route_params: [("im_path".to_owned(), "/webcast/im/fetch/".to_owned()), ("empty".to_owned(), String::new())]
                        .into(),
                    ..Default::default()
                };
                let headers = format!("x-set-tt-cookie: ttwid=abc%7C123; Path=/\r\nx-room-id: {ROOM_ID}\r\n");
                ("application/protobuf", headers, result.encode_to_vec())
            } else {
                ("application/json", String::new(), br#"{"error":"not found"}"#.to_vec())
            };

            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n{extra_headers}connection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            socket.shutdown().await.ok();
        });
    }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

struct WsObservations {
    url: String,
    cookie: String,
    client_frames: Vec<WebcastPushFrame>,
}

/// Mock push server: answers the room enter, pushes a gzip batch requiring an ACK, then ends the
/// stream.
async fn ws_server(listener: TcpListener, observations: Arc<Mutex<Option<WsObservations>>>) {
    let (socket, _) = listener.accept().await.unwrap();
    let mut url = String::new();
    let mut cookie = String::new();
    #[allow(clippy::result_large_err)]
    let callback = |request: &Request, response: Response| {
        url = request.uri().to_string();
        cookie = request.headers().get("cookie").and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
        Ok(response)
    };
    let mut ws = tokio_tungstenite::accept_hdr_async(socket, callback).await.unwrap();
    let mut client_frames = Vec::new();

    // 1. The client enters the room.
    let enter = loop {
        if let Some(Ok(Message::Binary(data))) = ws.next().await {
            let frame = WebcastPushFrame::decode(data).unwrap();
            client_frames.push(frame.clone());
            if frame.payload_type == "im_enter_room" {
                break frame;
            }
        }
    };
    let enter_message = WebcastImEnterRoomMessage::decode(enter.payload).unwrap();
    assert_eq!(enter_message.room_id.to_string(), ROOM_ID);
    assert_eq!(enter_message.live_id, 12);
    assert_eq!(enter_message.identity, "audience");
    assert!(enter_message.enter_unique_id > 0);

    let enter_response = WebcastPushFrame {
        payload_type: "im_enter_room_resp".to_owned(),
        payload_encoding: "pb".to_owned(),
        ..Default::default()
    };
    ws.send(Message::Binary(enter_response.encode_to_vec().into())).await.unwrap();

    // 2. A gzip-compressed batch that needs an ACK.
    let gift =
        WebcastGiftMessage { common: common("WebcastGiftMessage", ""), gift_id: 5655, repeat_count: 3, ..Default::default() };
    let follow =
        WebcastSocialMessage { common: common("WebcastSocialMessage", "pm_mt_guidance_viewer_follow"), ..Default::default() };
    let control = WebcastControlMessage {
        common: common("WebcastControlMessage", ""),
        action: ControlAction::StreamEnded as i32,
        ..Default::default()
    };
    let batch = ProtoMessageFetchResult {
        messages: vec![
            base_message("WebcastGiftMessage", gift.encode_to_vec()),
            base_message("WebcastSocialMessage", follow.encode_to_vec()),
            base_message("WebcastUnknownFutureMessage", vec![1, 2, 3]),
            base_message("WebcastControlMessage", control.encode_to_vec()),
        ],
        internal_ext: "ext-2".to_owned(),
        need_ack: true,
        ..Default::default()
    };
    let frame = WebcastPushFrame {
        log_id: 424242,
        payload_encoding: "pb".to_owned(),
        payload_type: "msg".to_owned(),
        payload: gzip(&batch.encode_to_vec()).into(),
        ..Default::default()
    };
    ws.send(Message::Binary(frame.encode_to_vec().into())).await.unwrap();

    // 3. Read until the client closes (the stream end triggers a disconnect).
    // Keep reading after the close frame so that tungstenite flushes the close reply.
    while let Some(Ok(message)) = ws.next().await {
        if let Message::Binary(data) = message {
            client_frames.push(WebcastPushFrame::decode(data).unwrap());
        }
    }

    *observations.lock().unwrap() = Some(WsObservations { url, cookie, client_frames });
}

/// Minimal HTTP `CONNECT` proxy. Records the requested authorities.
async fn connect_proxy(listener: TcpListener, tunnels: Arc<Mutex<Vec<String>>>) {
    loop {
        let Ok((mut client, _)) = listener.accept().await else { return };
        let tunnels = tunnels.clone();
        tokio::spawn(async move {
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if client.read(&mut byte).await.unwrap() == 0 {
                    return;
                }
                head.push(byte[0]);
            }
            let head = String::from_utf8(head).unwrap();
            let authority = head.split_whitespace().nth(1).unwrap().to_owned();
            assert!(head.starts_with("CONNECT "));
            assert!(head.contains("Proxy-Authorization: Basic dXNlcjpwYXNz"), "missing credentials: {head}");
            tunnels.lock().unwrap().push(authority.clone());
            let mut upstream = tokio::net::TcpStream::connect(&authority).await.unwrap();
            client.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await.unwrap();
            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
        });
    }
}

#[tokio::test]
async fn full_connection_flow() {
    run_flow(false).await;
}

#[tokio::test]
async fn full_connection_flow_through_proxy() {
    run_flow(true).await;
}

async fn run_flow(use_proxy: bool) {
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();
    let ws_port = ws_listener.local_addr().unwrap().port();

    let requests = Arc::new(Mutex::new(Vec::new()));
    let observations = Arc::new(Mutex::new(None));
    tokio::spawn(http_server(http_listener, ws_port, requests.clone()));
    let ws_task = tokio::spawn(ws_server(ws_listener, observations.clone()));

    let web_config = WebConfig {
        tiktok_host_webcast: format!("127.0.0.1:{http_port}"),
        tiktok_host_web: format!("127.0.0.1:{http_port}"),
        ..WebConfig::default()
    };

    let tunnels = Arc::new(Mutex::new(Vec::new()));
    let proxy = if use_proxy {
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = proxy_listener.local_addr().unwrap().port();
        tokio::spawn(connect_proxy(proxy_listener, tunnels.clone()));
        Some(format!("http://user:pass@127.0.0.1:{proxy_port}"))
    } else {
        None
    };

    let euler = EulerStreamApiClient::new(SignConfig {
        base_path: format!("http://127.0.0.1:{http_port}"),
        api_key: Some("test-key".to_owned()),
        ..SignConfig::default()
    });

    let options = ConnectionOptions {
        web_config: Some(web_config),
        euler_client: Some(euler),
        proxy,
        // Plain HTTP goes straight to the mock; only the WebSocket is tunneled.
        http_client: Some(tiktok_live_connector::web::default_http_client()),
        ..Default::default()
    };
    let connection = TikTokLiveConnection::with_options("https://www.tiktok.com/@mock_user/live", options).unwrap();

    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let events = events.clone();
        connection.on_any(move |event| {
            if !matches!(event.kind(), EventKind::WebsocketData | EventKind::DecodedData | EventKind::RawData) {
                events.lock().unwrap().push(event.kind().name().to_owned());
            }
        });
    }
    let chats = Arc::new(Mutex::new(Vec::new()));
    {
        let chats = chats.clone();
        connection.on_chat(move |msg| chats.lock().unwrap().push(msg.content.clone()));
    }
    let gifts = Arc::new(Mutex::new(Vec::new()));
    {
        let gifts = gifts.clone();
        connection.on_gift(move |gift| gifts.lock().unwrap().push((gift.gift_id, gift.repeat_count)));
    }
    let mut stream = connection.subscribe();

    let state = connection.connect_to_room(Some(ROOM_ID)).await.unwrap();
    assert!(state.is_connected);
    assert_eq!(state.room_id, ROOM_ID);
    assert_eq!(state.room_info.as_ref().unwrap()["data"]["title"], "mock");
    assert!(matches!(connection.connect().await, Err(tiktok_live_connector::Error::AlreadyConnected)));

    // Wait for the disconnect triggered by the stream end.
    let disconnected = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(event) = stream.recv().await {
            if let Event::Disconnected(disconnected) = event {
                return disconnected;
            }
        }
        panic!("stream ended without disconnect");
    })
    .await
    .expect("disconnect within 10s");
    // Like the Node `ws` library, a close without status code is reported as 1005.
    assert_eq!(disconnected.code, 1005);
    ws_task.await.unwrap();

    assert!(!connection.is_connected());
    assert_eq!(connection.room_id(), "");
    assert!(connection.ws_client().is_none());

    let events = events.lock().unwrap().clone();
    assert_eq!(
        events,
        ["chat", "websocketConnected", "connected", "enterRoom", "gift", "follow", "controlMessage", "streamEnd", "disconnected"],
        "unexpected event order"
    );
    assert_eq!(*chats.lock().unwrap(), ["initial message"]);
    assert_eq!(*gifts.lock().unwrap(), [(5655, 3)]);

    // The sign server was called with the API key, the library identity and the cursor.
    let requests = requests.lock().unwrap().clone();
    let connect_request = requests.iter().find(|r| r.contains("/connect")).expect("sign request");
    assert!(connect_request.contains("apiKey=test-key"));
    assert!(connect_request.contains("client=ttlive-node"));
    assert!(connect_request.contains("client_enter=true"));
    assert!(connect_request.contains("platform=web"));
    assert!(connect_request.to_lowercase().contains("x-api-key: test-key"));
    let room_info_request = requests.iter().find(|r| r.contains("/webcast/room/info/")).expect("room info request");
    assert!(room_info_request.contains(&format!("room_id={ROOM_ID}")));
    assert!(room_info_request.contains("aid=1988"));

    let observations = observations.lock().unwrap().take().expect("ws observations");
    assert!(observations.url.contains("compress=gzip"));
    assert!(observations.url.contains(&format!("room_id={ROOM_ID}")));
    assert!(observations.url.contains("cursor=cursor-1"));
    assert!(observations.url.contains("internal_ext=ext-1"));
    assert!(observations.url.contains("im_path=%2Fwebcast%2Fim%2Ffetch%2F"));
    assert!(!observations.url.contains("empty="));
    assert!(observations.url.ends_with("&version_code=270000"));
    // The cookie set by the sign server is forwarded, the session cookies are not.
    assert!(observations.cookie.contains("ttwid=abc%7C123"), "cookie: {}", observations.cookie);
    assert!(!observations.cookie.contains("tt-target-idc"));

    if use_proxy {
        assert_eq!(*tunnels.lock().unwrap(), [format!("127.0.0.1:{ws_port}")]);
    }

    let ack = observations.client_frames.iter().find(|f| f.payload_type == "ack").expect("ack frame");
    assert_eq!(ack.log_id, 424242);
    assert_eq!(&ack.payload[..], b"ext-2");
}

#[tokio::test]
async fn connect_failure_resets_state_and_reports_error() {
    let euler = EulerStreamApiClient::new(SignConfig { base_path: "http://127.0.0.1:9".to_owned(), ..SignConfig::default() });
    let options = ConnectionOptions { euler_client: Some(euler), fetch_room_info_on_connect: false, ..Default::default() };
    let connection = TikTokLiveConnection::with_options("mock_user", options).unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));
    {
        let errors = errors.clone();
        connection.on_error(move |error| errors.lock().unwrap().push(error.info.clone()));
    }

    let err = connection.connect_to_room(Some("123")).await.unwrap_err();
    let sign_error = err.as_sign_api_error().expect("sign api error");
    assert_eq!(sign_error.reason, tiktok_live_connector::ErrorReason::ConnectError);
    assert!(err.to_string().starts_with("[Connect Error] Failed to connect to sign server."));

    assert!(!connection.is_connecting());
    assert!(!connection.is_connected());
    assert_eq!(*errors.lock().unwrap(), ["Error while connecting"]);
}
