//! The deprecated `WebcastPushConnection`, emitting flattened JSON like the 1.x library.
//!
//! ```sh
//! cargo run --example legacy --features legacy -- <username>
//! ```

#![allow(deprecated)]

use tiktok_live_connector::ConnectionOptions;
use tiktok_live_connector::legacy::WebcastPushConnection;

#[tokio::main]
async fn main() -> tiktok_live_connector::Result<()> {
    let username = std::env::args().nth(1).unwrap_or_else(|| "officialgeilegisela".to_owned());
    let connection = WebcastPushConnection::new(&username, ConnectionOptions::default())?;

    connection.on("chat", |event| {
        println!("{} (userId:{}) writes: {}", event.data["uniqueId"], event.data["userId"], event.data["content"])
    });
    connection
        .on("gift", |event| println!("{} sends {} ({})", event.data["uniqueId"], event.data["giftName"], event.data["giftId"]));
    connection.on("roomUser", |event| {
        println!("viewers: {}, top: {}", event.data["total"], event.data["topViewers"][0]["user"]["uniqueId"])
    });

    let state = connection.connect().await?;
    println!("Connected to room {}", state.room_id);
    tokio::signal::ctrl_c().await?;
    connection.disconnect().await;
    Ok(())
}
