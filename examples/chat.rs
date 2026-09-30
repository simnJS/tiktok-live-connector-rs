//! Prints chat messages, gifts, likes and follows of a live stream.
//!
//! ```sh
//! cargo run --example chat -- <username>
//! ```

use tiktok_live_connector::{ConnectionOptions, TikTokLiveConnection};

#[tokio::main]
async fn main() -> tiktok_live_connector::Result<()> {
    let username = std::env::args().nth(1).unwrap_or_else(|| "officialgeilegisela".to_owned());

    // Gift names need the signed gift list, which requires an Euler Stream Business plan.
    let enable_extended_gift_info = std::env::var_os("EXTENDED_GIFT_INFO").is_some();
    let options = ConnectionOptions { enable_extended_gift_info, ..Default::default() };
    let connection = TikTokLiveConnection::with_options(&username, options)?;

    let name = |user: Option<&tiktok_live_connector::proto::User>| user.map(|u| u.display_id.clone()).unwrap_or_default();

    connection.on_chat(move |msg| println!("[chat] {}: {}", name(msg.user.as_ref()), msg.content));
    connection.on_gift(move |gift| {
        let gift_name = gift
            .extended_gift_info
            .as_ref()
            .and_then(|info| info["name"].as_str().map(str::to_owned))
            .unwrap_or_else(|| gift.gift_id.to_string());
        println!("[gift] {} sent {gift_name} x{}", name(gift.user.as_ref()), gift.repeat_count);
    });
    connection.on_like(move |like| println!("[like] {} +{} (total {})", name(like.user.as_ref()), like.count, like.total));
    connection.on_follow(move |follow| println!("[follow] {}", name(follow.user.as_ref())));
    connection.on_member(move |member| println!("[join] {}", name(member.user.as_ref())));
    connection.on_room_user(|seq| println!("[viewers] {}", seq.total));
    connection.on_stream_end(|end| println!("[stream end] {:?}", end.action));
    connection.on_error(|error| eprintln!("[error] {error}"));

    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    connection.on_disconnected(move |event| {
        println!("[disconnected] code {} {}", event.code, event.reason);
        if let Some(tx) = tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    });

    let state = connection.connect().await?;
    println!("Connected to room {} ({})", state.room_id, username);

    tokio::select! {
        _ = rx => {}
        _ = tokio::signal::ctrl_c() => connection.disconnect().await,
    }
    Ok(())
}
