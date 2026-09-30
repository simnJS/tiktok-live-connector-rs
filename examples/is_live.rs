//! Checks whether users are live and prints their room id.
//!
//! ```sh
//! cargo run --example is_live -- <username> [<username>...]
//! ```

use tiktok_live_connector::TikTokLiveConnection;

#[tokio::main]
async fn main() -> tiktok_live_connector::Result<()> {
    for username in std::env::args().skip(1) {
        let connection = TikTokLiveConnection::new(&username)?;
        match connection.fetch_is_live(None).await {
            Ok(true) => match connection.fetch_room_id(None).await {
                Ok(room_id) => println!("{username}: live (room {room_id})"),
                Err(err) => println!("{username}: live, room id unavailable: {err}"),
            },
            Ok(false) => println!("{username}: offline"),
            Err(err) => println!("{username}: error: {err}"),
        }
    }
    Ok(())
}
