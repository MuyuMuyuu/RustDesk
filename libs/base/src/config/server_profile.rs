use std::collections::HashMap;

use super::keys::{
    OPTION_API_SERVER, OPTION_CUSTOM_RENDEZVOUS_SERVER, OPTION_INTRANET_API_SERVER,
    OPTION_INTRANET_KEY, OPTION_INTRANET_RELAY_SERVER, OPTION_INTRANET_RENDEZVOUS_SERVER,
    OPTION_KEY, OPTION_RELAY_SERVER,
};

pub const SERVER_PROFILE_OFFICIAL: &str = "official";
pub const SERVER_PROFILE_INTRANET: &str = "intranet";

const ACTIVE_KEYS: [&str; 4] = [
    OPTION_CUSTOM_RENDEZVOUS_SERVER,
    OPTION_RELAY_SERVER,
    OPTION_API_SERVER,
    OPTION_KEY,
];

fn stored(options: &HashMap<String, String>, key: &str) -> String {
    options
        .get(key)
        .map(|value| value.trim().to_owned())
        .unwrap_or_default()
}

fn store(options: &mut HashMap<String, String>, key: &str, value: String) {
    if value.is_empty() {
        options.remove(key);
    } else {
        options.insert(key.to_owned(), value);
    }
}

/// Point the active ID/rendezvous host, relay, API server, and key at one profile.
///
/// `official` removes those options so the public defaults apply. `intranet`
/// copies the saved preset. The preset itself is left in place. An intranet
/// preset with no host is refused and the map is unchanged.
pub fn apply_server_profile(
    options: &mut HashMap<String, String>,
    profile: &str,
) -> Result<(), &'static str> {
    match profile {
        SERVER_PROFILE_OFFICIAL => {
            for key in ACTIVE_KEYS {
                options.remove(key);
            }
            Ok(())
        }
        SERVER_PROFILE_INTRANET => {
            let host = stored(options, OPTION_INTRANET_RENDEZVOUS_SERVER);
            if host.is_empty() {
                return Err("Intranet server is not set");
            }
            store(options, OPTION_CUSTOM_RENDEZVOUS_SERVER, host);
            store(
                options,
                OPTION_RELAY_SERVER,
                stored(options, OPTION_INTRANET_RELAY_SERVER),
            );
            store(
                options,
                OPTION_API_SERVER,
                stored(options, OPTION_INTRANET_API_SERVER),
            );
            store(options, OPTION_KEY, stored(options, OPTION_INTRANET_KEY));
            Ok(())
        }
        _ => Err("Unknown server profile"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn preset(options: &mut HashMap<String, String>) {
        options.insert(
            OPTION_INTRANET_RENDEZVOUS_SERVER.to_owned(),
            "id.example.test".to_owned(),
        );
        options.insert(
            OPTION_INTRANET_RELAY_SERVER.to_owned(),
            "relay.example.test".to_owned(),
        );
        options.insert(
            OPTION_INTRANET_API_SERVER.to_owned(),
            "https://api.example.test".to_owned(),
        );
        options.insert(OPTION_INTRANET_KEY.to_owned(), "test-public-key".to_owned());
    }

    fn assert_preset_unchanged(options: &HashMap<String, String>) {
        assert_eq!(
            options
                .get(OPTION_INTRANET_RENDEZVOUS_SERVER)
                .map(String::as_str),
            Some("id.example.test")
        );
        assert_eq!(
            options
                .get(OPTION_INTRANET_RELAY_SERVER)
                .map(String::as_str),
            Some("relay.example.test")
        );
        assert_eq!(
            options.get(OPTION_INTRANET_API_SERVER).map(String::as_str),
            Some("https://api.example.test")
        );
        assert_eq!(
            options.get(OPTION_INTRANET_KEY).map(String::as_str),
            Some("test-public-key")
        );
    }

    fn assert_active_cleared(options: &HashMap<String, String>) {
        for key in ACTIVE_KEYS {
            assert!(
                options.get(key).map(|v| v.is_empty()).unwrap_or(true),
                "active server option still set: {}",
                key
            );
        }
    }

    #[test]
    fn official_clears_custom_server() {
        let mut options = HashMap::new();
        options.insert(
            OPTION_CUSTOM_RENDEZVOUS_SERVER.to_owned(),
            "id.example.test".to_owned(),
        );
        options.insert(
            OPTION_RELAY_SERVER.to_owned(),
            "relay.example.test".to_owned(),
        );
        options.insert(
            OPTION_API_SERVER.to_owned(),
            "https://api.example.test".to_owned(),
        );
        options.insert(OPTION_KEY.to_owned(), "test-public-key".to_owned());
        options.insert("enable-keyboard".to_owned(), "Y".to_owned());
        preset(&mut options);

        apply_server_profile(&mut options, SERVER_PROFILE_OFFICIAL).unwrap();

        assert_active_cleared(&options);
        assert_preset_unchanged(&options);
        assert_eq!(
            options.get("enable-keyboard").map(String::as_str),
            Some("Y")
        );
    }

    #[test]
    fn intranet_applies_saved_preset() {
        let mut options = HashMap::new();
        options.insert(
            OPTION_CUSTOM_RENDEZVOUS_SERVER.to_owned(),
            "other.example.test".to_owned(),
        );
        options.insert("enable-keyboard".to_owned(), "Y".to_owned());
        preset(&mut options);

        apply_server_profile(&mut options, SERVER_PROFILE_INTRANET).unwrap();

        assert_eq!(
            options
                .get(OPTION_CUSTOM_RENDEZVOUS_SERVER)
                .map(String::as_str),
            Some("id.example.test")
        );
        assert_eq!(
            options.get(OPTION_RELAY_SERVER).map(String::as_str),
            Some("relay.example.test")
        );
        assert_eq!(
            options.get(OPTION_API_SERVER).map(String::as_str),
            Some("https://api.example.test")
        );
        assert_eq!(
            options.get(OPTION_KEY).map(String::as_str),
            Some("test-public-key")
        );
        assert_preset_unchanged(&options);
        assert_eq!(
            options.get("enable-keyboard").map(String::as_str),
            Some("Y")
        );
    }

    #[test]
    fn intranet_blank_preset_does_not_apply() {
        let mut options = HashMap::new();
        options.insert("enable-keyboard".to_owned(), "Y".to_owned());
        options.insert(
            OPTION_INTRANET_RENDEZVOUS_SERVER.to_owned(),
            "  ".to_owned(),
        );
        let before = options.clone();

        let err = apply_server_profile(&mut options, SERVER_PROFILE_INTRANET).unwrap_err();

        assert_eq!(err, "Intranet server is not set");
        assert_eq!(options, before);
        assert_active_cleared(&options);
    }
}
