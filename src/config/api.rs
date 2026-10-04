use serde::{Deserialize, Serialize};

fn default_bind() -> String {
    "127.0.0.1:8765".into()
}
fn default_max_session_secs() -> u32 {
    60
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_max_session_secs")]
    pub max_session_secs: u32,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_bind(),
            max_session_secs: default_max_session_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ApiConfig;

    #[test]
    fn defaults_are_safe_and_disabled() {
        let config = ApiConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bind, "127.0.0.1:8765");
        assert_eq!(config.max_session_secs, 60);
    }
}
