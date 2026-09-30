//! Device, screen and location presets used to build a believable browser fingerprint.

use rand::RngExt as _;

/// Browser language, country and time zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationPreset {
    /// Language (`en`).
    pub lang: String,
    /// Browser language (`en-GB`).
    pub lang_country: String,
    /// Country / region (`GB`).
    pub country: String,
    /// Time zone (`Europe/London`).
    pub tz_name: String,
}

/// Browser identity derived from a User-Agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DevicePreset {
    /// Everything after the first `/` of the User-Agent.
    pub browser_version: String,
    /// Everything before the first `/` of the User-Agent.
    pub browser_name: String,
    /// `Win32` or `MacIntel`.
    pub browser_platform: String,
    /// The User-Agent.
    pub user_agent: String,
    /// `windows` or `mac`.
    pub os: String,
}

/// Screen resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScreenPreset {
    /// Width in pixels.
    pub screen_width: u32,
    /// Height in pixels.
    pub screen_height: u32,
}

/// Everything needed to generate the HTTP and WebSocket client configurations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientPresets {
    /// Screen resolution.
    pub screen: ScreenPreset,
    /// Language and time zone.
    pub location: LocationPreset,
    /// Browser.
    pub device: DevicePreset,
}

impl ClientPresets {
    /// Picks presets the same way the Node library does: fixed defaults, randomized per category
    /// when `RANDOMIZE_TIKTOK_DEVICE`, `RANDOMIZE_TIKTOK_LOCATION` or `RANDOMIZE_TIKTOK_SCREEN` is `true`.
    pub fn random() -> Self {
        ClientPresets { device: random_device_preset(), screen: random_screen_preset(), location: random_location_preset() }
    }
}

impl Default for ClientPresets {
    fn default() -> Self {
        Self::random()
    }
}

const LOCATIONS: &[(&str, &str, &str, &str)] = &[
    ("en-GB", "en", "GB", "Europe/London"),
    ("en-CA", "en", "CA", "America/Toronto"),
    ("en-AU", "en", "AU", "Australia/Sydney"),
    ("en-NZ", "en", "NZ", "Pacific/Auckland"),
    ("en-ZA", "en", "ZA", "Africa/Johannesburg"),
    ("en-IE", "en", "IE", "Europe/Dublin"),
    ("en-JM", "en", "JM", "America/Jamaica"),
    ("en-BZ", "en", "BZ", "America/Belize"),
    ("en-TT", "en", "TT", "America/Port_of_Spain"),
];

const SCREENS: &[(u32, u32)] = &[
    (1920, 1080),
    (2560, 1440),
    (3840, 2160),
    (4096, 2160),
    (5120, 2880),
    (7680, 4320),
    (1152, 2048),
    (1440, 2560),
    (2160, 3840),
    (4320, 7680),
];

/// Recent desktop browser User-Agents.
pub const USER_AGENTS: &[&str] = &[
    // Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    // Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    // Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:130.0) Gecko/20100101 Firefox/130.0",
    // Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/128.0.2739.79",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/128.0.2739.79",
    // Opera
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/113.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 OPR/113.0.0.0",
];

/// Index into [`USER_AGENTS`] used when device randomization is disabled.
const DEFAULT_USER_AGENT: usize = 6;

/// Derives the browser fields TikTok expects from a User-Agent string.
pub fn user_agent_to_device_preset(user_agent: &str) -> DevicePreset {
    let (browser_name, browser_version) = user_agent.split_once('/').unwrap_or((user_agent, ""));
    let mac = user_agent.contains("Macintosh");
    DevicePreset {
        user_agent: user_agent.to_owned(),
        browser_name: browser_name.to_owned(),
        browser_version: browser_version.to_owned(),
        browser_platform: if mac { "MacIntel" } else { "Win32" }.to_owned(),
        os: if mac { "mac" } else { "windows" }.to_owned(),
    }
}

/// All device presets, one per entry of [`USER_AGENTS`].
pub fn devices() -> Vec<DevicePreset> {
    USER_AGENTS.iter().map(|ua| user_agent_to_device_preset(ua)).collect()
}

/// All location presets.
pub fn locations() -> Vec<LocationPreset> {
    LOCATIONS.iter().map(|&(lc, l, c, tz)| location(lc, l, c, tz)).collect()
}

/// All screen presets.
pub fn screens() -> Vec<ScreenPreset> {
    SCREENS.iter().map(|&(w, h)| ScreenPreset { screen_width: w, screen_height: h }).collect()
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v.eq_ignore_ascii_case("true"))
}

fn location(lang_country: &str, lang: &str, country: &str, tz_name: &str) -> LocationPreset {
    LocationPreset {
        lang_country: lang_country.to_owned(),
        lang: lang.to_owned(),
        country: country.to_owned(),
        tz_name: tz_name.to_owned(),
    }
}

/// A random device when `RANDOMIZE_TIKTOK_DEVICE=true`, otherwise a fixed Edge/macOS device.
pub fn random_device_preset() -> DevicePreset {
    let index =
        if env_flag("RANDOMIZE_TIKTOK_DEVICE") { rand::rng().random_range(0..USER_AGENTS.len()) } else { DEFAULT_USER_AGENT };
    user_agent_to_device_preset(USER_AGENTS[index])
}

/// A random location when `RANDOMIZE_TIKTOK_LOCATION=true`, otherwise `en-DE` / `Europe/Berlin`.
pub fn random_location_preset() -> LocationPreset {
    if env_flag("RANDOMIZE_TIKTOK_LOCATION") {
        let (lc, l, c, tz) = LOCATIONS[rand::rng().random_range(0..LOCATIONS.len())];
        location(lc, l, c, tz)
    } else {
        location("en-DE", "en", "DE", "Europe/Berlin")
    }
}

/// A random screen when `RANDOMIZE_TIKTOK_SCREEN=true`, otherwise 1920x1080.
pub fn random_screen_preset() -> ScreenPreset {
    let (w, h) =
        if env_flag("RANDOMIZE_TIKTOK_SCREEN") { SCREENS[rand::rng().random_range(0..SCREENS.len())] } else { (1920, 1080) };
    ScreenPreset { screen_width: w, screen_height: h }
}

/// A random 19-digit device id.
pub fn generate_device_id() -> String {
    let mut rng = rand::rng();
    (0..19).map(|_| char::from(b'0' + rng.random_range(0..10u8))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_preset_from_user_agent() {
        let preset = user_agent_to_device_preset(USER_AGENTS[DEFAULT_USER_AGENT]);
        assert_eq!(preset.browser_name, "Mozilla");
        assert!(preset.browser_version.starts_with("5.0 (Macintosh;"));
        assert_eq!(preset.browser_platform, "MacIntel");
        assert_eq!(preset.os, "mac");

        let windows = user_agent_to_device_preset(USER_AGENTS[0]);
        assert_eq!(windows.browser_platform, "Win32");
        assert_eq!(windows.os, "windows");
    }

    #[test]
    fn device_id_has_19_digits() {
        let id = generate_device_id();
        assert_eq!(id.len(), 19);
        assert!(id.bytes().all(|b| b.is_ascii_digit()));
    }
}
