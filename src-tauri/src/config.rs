use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tauri::{AppHandle, Manager};

const APP_DIRECTORY: &str = "GPTVoice";
const CONFIG_FILE: &str = "config.json";
const MASKED_TOKEN: &str = "********";

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSnapshot {
    pub config_path: String,
    pub data_dir: String,
    pub browser_profile: String,
    pub discord_token_configured: bool,
    pub discord_token_masked: String,
    pub discord_guild_id: String,
    pub chatgpt_url: String,
    pub browser_executable: String,
    pub browser_hide_when_ready: bool,
    pub audio_capture_mode: String,
    pub audio_input_volume: f64,
    pub audio_output_volume: f64,
    pub audio_earcons_enabled: bool,
    pub audio_earcon_volume: f64,
    pub audio_sample_rate: u64,
    pub audio_channels: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConfigPatch {
    pub discord_token: Option<String>,
    pub discord_guild_id: Option<String>,
    pub chatgpt_url: Option<String>,
    pub browser_executable: Option<String>,
    pub browser_hide_when_ready: Option<bool>,
    pub audio_capture_mode: Option<String>,
    pub audio_input_volume: Option<f64>,
    pub audio_output_volume: Option<f64>,
    pub audio_earcons_enabled: Option<bool>,
    pub audio_earcon_volume: Option<f64>,
    pub audio_sample_rate: Option<u64>,
    pub audio_channels: Option<u64>,
}

pub fn load_discord_credentials(app: &AppHandle) -> Result<(String, Option<String>), String> {
    let (_path, environment) = merged_environment(app)?;
    let token = string_value(&environment, "DISCORD_TOKEN", "");
    if token.is_empty() {
        return Err("Discord bot token is not configured".to_owned());
    }
    let guild_id = string_value(&environment, "DISCORD_GUILD_ID", "");
    Ok((
        token,
        if guild_id.is_empty() {
            None
        } else {
            Some(guild_id)
        },
    ))
}

fn local_data_dir(app: &AppHandle) -> Result<PathBuf, String> {
    if cfg!(windows) {
        if let Some(path) = env::var_os("LOCALAPPDATA") {
            return Ok(PathBuf::from(path).join(APP_DIRECTORY));
        }
    }

    app.path()
        .app_local_data_dir()
        .map_err(|error| format!("Could not locate GPTVoice data directory: {error}"))
}

fn read_environment(path: &Path) -> Result<Map<String, Value>, String> {
    if !path.exists() {
        return Ok(Map::new());
    }

    let contents = fs::read_to_string(path)
        .map_err(|error| format!("Could not read saved GPTVoice setup: {error}"))?;
    let value: Value = serde_json::from_str(&contents)
        .map_err(|error| format!("Saved GPTVoice setup is not valid JSON: {error}"))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| "Saved GPTVoice setup must be a JSON object".to_owned())
}

fn string_value(environment: &Map<String, Value>, key: &str, fallback: &str) -> String {
    environment
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or(fallback)
        .trim()
        .to_owned()
}

fn bool_value(environment: &Map<String, Value>, key: &str, fallback: bool) -> bool {
    match environment.get(key) {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => value.trim().eq_ignore_ascii_case("true"),
        _ => fallback,
    }
}

fn number_value(environment: &Map<String, Value>, key: &str, fallback: f64) -> f64 {
    match environment.get(key) {
        Some(Value::Number(value)) => value.as_f64().unwrap_or(fallback),
        Some(Value::String(value)) => value.parse::<f64>().unwrap_or(fallback),
        _ => fallback,
    }
}

fn integer_value(environment: &Map<String, Value>, key: &str, fallback: u64) -> u64 {
    number_value(environment, key, fallback as f64).max(0.0) as u64
}

fn ensure_range(name: &str, value: f64, minimum: f64, maximum: f64) -> Result<(), String> {
    if !value.is_finite() || value < minimum || value > maximum {
        return Err(format!("{name} must be between {minimum} and {maximum}"));
    }
    Ok(())
}

fn ensure_positive(name: &str, value: u64) -> Result<(), String> {
    if value == 0 {
        return Err(format!("{name} must be greater than zero"));
    }
    Ok(())
}

fn validate_environment(environment: &Map<String, Value>) -> Result<(), String> {
    if let Some(token) = environment.get("DISCORD_TOKEN").and_then(Value::as_str) {
        let token = token.trim();
        if !token.is_empty() && token.len() < 8 {
            return Err("Discord token looks too short".to_owned());
        }
        if token.chars().all(|character| character == '*') {
            return Err(
                "Enter the complete Discord bot token, not the masked placeholder".to_owned(),
            );
        }
    }

    let chatgpt_url = string_value(environment, "CHATGPT_URL", "https://chatgpt.com/");
    if !(chatgpt_url.starts_with("https://") || chatgpt_url.starts_with("http://"))
        || chatgpt_url.chars().any(char::is_whitespace)
    {
        return Err("ChatGPT URL must be an http(s) URL without spaces".to_owned());
    }

    let mode = string_value(environment, "AUDIO_CAPTURE_MODE", "browser-media").to_lowercase();
    if !matches!(
        mode.as_str(),
        "device" | "browser-process" | "browser-media"
    ) {
        return Err(
            "Audio capture mode must be device, browser-process, or browser-media".to_owned(),
        );
    }

    ensure_range(
        "Audio input volume",
        number_value(environment, "AUDIO_INPUT_VOLUME", 1.0),
        0.0,
        4.0,
    )?;
    ensure_range(
        "Audio output volume",
        number_value(environment, "AUDIO_OUTPUT_VOLUME", 1.0),
        0.0,
        4.0,
    )?;
    ensure_range(
        "Audio earcon volume",
        number_value(environment, "AUDIO_EARCON_VOLUME", 0.18),
        0.0,
        1.0,
    )?;
    ensure_positive(
        "Audio sample rate",
        integer_value(environment, "AUDIO_SAMPLE_RATE", 48_000),
    )?;
    ensure_positive(
        "Audio channels",
        integer_value(environment, "AUDIO_CHANNELS", 2),
    )?;
    Ok(())
}

fn default_environment(data_dir: &Path) -> Map<String, Value> {
    let mut environment = Map::new();
    environment.insert(
        "CHATGPT_URL".to_owned(),
        Value::String("https://chatgpt.com/".to_owned()),
    );
    environment.insert(
        "CHATGPT_BROWSER_PROFILE".to_owned(),
        Value::String(
            data_dir
                .join("browser-profile")
                .to_string_lossy()
                .into_owned(),
        ),
    );
    environment.insert(
        "CHATGPT_BROWSER_HIDE_WHEN_READY".to_owned(),
        Value::String("true".to_owned()),
    );
    environment.insert(
        "AUDIO_CAPTURE_MODE".to_owned(),
        Value::String("browser-media".to_owned()),
    );
    environment.insert(
        "AUDIO_INPUT_VOLUME".to_owned(),
        Value::String("1".to_owned()),
    );
    environment.insert(
        "AUDIO_OUTPUT_VOLUME".to_owned(),
        Value::String("1".to_owned()),
    );
    environment.insert(
        "AUDIO_EARCONS_ENABLED".to_owned(),
        Value::String("true".to_owned()),
    );
    environment.insert(
        "AUDIO_EARCON_VOLUME".to_owned(),
        Value::String("0.18".to_owned()),
    );
    environment.insert(
        "AUDIO_SAMPLE_RATE".to_owned(),
        Value::String("48000".to_owned()),
    );
    environment.insert("AUDIO_CHANNELS".to_owned(), Value::String("2".to_owned()));
    environment
}

fn merged_environment(app: &AppHandle) -> Result<(PathBuf, Map<String, Value>), String> {
    let data_dir = local_data_dir(app)?;
    let path = data_dir.join(CONFIG_FILE);
    let stored = read_environment(&path)?;
    let mut environment = default_environment(&data_dir);
    environment.extend(stored);
    validate_environment(&environment)?;
    Ok((path, environment))
}

fn string_config_value(environment: &Map<String, Value>, key: &str, fallback: &str) -> String {
    string_value(environment, key, fallback)
}

fn snapshot_from_environment(path: &Path, environment: &Map<String, Value>) -> ConfigSnapshot {
    let token = string_value(environment, "DISCORD_TOKEN", "");
    let data_dir = path.parent().unwrap_or_else(|| Path::new("."));
    let default_browser_profile = data_dir.join("browser-profile");
    ConfigSnapshot {
        config_path: path.to_string_lossy().into_owned(),
        data_dir: data_dir.to_string_lossy().into_owned(),
        browser_profile: string_config_value(
            environment,
            "CHATGPT_BROWSER_PROFILE",
            default_browser_profile.to_string_lossy().as_ref(),
        ),
        discord_token_configured: !token.is_empty(),
        discord_token_masked: if token.is_empty() {
            String::new()
        } else {
            MASKED_TOKEN.to_owned()
        },
        discord_guild_id: string_value(environment, "DISCORD_GUILD_ID", ""),
        chatgpt_url: string_value(environment, "CHATGPT_URL", "https://chatgpt.com/"),
        browser_executable: string_value(environment, "CHATGPT_BROWSER_EXECUTABLE", ""),
        browser_hide_when_ready: bool_value(environment, "CHATGPT_BROWSER_HIDE_WHEN_READY", true),
        audio_capture_mode: string_value(environment, "AUDIO_CAPTURE_MODE", "browser-media"),
        audio_input_volume: number_value(environment, "AUDIO_INPUT_VOLUME", 1.0),
        audio_output_volume: number_value(environment, "AUDIO_OUTPUT_VOLUME", 1.0),
        audio_earcons_enabled: bool_value(environment, "AUDIO_EARCONS_ENABLED", true),
        audio_earcon_volume: number_value(environment, "AUDIO_EARCON_VOLUME", 0.18),
        audio_sample_rate: integer_value(environment, "AUDIO_SAMPLE_RATE", 48_000),
        audio_channels: integer_value(environment, "AUDIO_CHANNELS", 2),
    }
}

fn write_environment(path: &Path, environment: &Map<String, Value>) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "GPTVoice config path has no parent directory".to_owned())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("Could not create GPTVoice data directory: {error}"))?;

    let temporary_path = parent.join(format!("{CONFIG_FILE}.{}.tmp", std::process::id()));
    let serialized = serde_json::to_vec_pretty(environment)
        .map_err(|error| format!("Could not serialize GPTVoice setup: {error}"))?;
    let write_result = (|| -> Result<(), String> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary_path)
            .map_err(|error| format!("Could not create temporary GPTVoice setup: {error}"))?;
        file.write_all(&serialized)
            .and_then(|_| file.write_all(b"\n"))
            .and_then(|_| file.sync_all())
            .map_err(|error| format!("Could not write GPTVoice setup: {error}"))?;
        Ok(())
    })();

    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }

    if let Err(rename_error) = fs::rename(&temporary_path, path) {
        if path.exists() {
            fs::remove_file(path)
                .and_then(|_| fs::rename(&temporary_path, path))
                .map_err(|error| {
                    let _ = fs::remove_file(&temporary_path);
                    format!("Could not replace GPTVoice setup: {rename_error}; {error}")
                })?;
        } else {
            let _ = fs::remove_file(&temporary_path);
            return Err(format!("Could not save GPTVoice setup: {rename_error}"));
        }
    }
    Ok(())
}

pub fn load_snapshot(app: &AppHandle) -> Result<ConfigSnapshot, String> {
    let (path, environment) = merged_environment(app)?;
    Ok(snapshot_from_environment(&path, &environment))
}

pub fn save_patch(app: &AppHandle, patch: ConfigPatch) -> Result<ConfigSnapshot, String> {
    let data_dir = local_data_dir(app)?;
    let path = data_dir.join(CONFIG_FILE);
    let mut environment = default_environment(&data_dir);
    environment.extend(read_environment(&path)?);

    if let Some(value) = patch.discord_token {
        let value = value.trim().to_owned();
        if value.is_empty() {
            return Err("Discord token cannot be empty".to_owned());
        }
        if value.chars().all(|character| character == '*') {
            return Err(
                "Enter the complete Discord bot token, not the masked placeholder".to_owned(),
            );
        }
        environment.insert("DISCORD_TOKEN".to_owned(), Value::String(value));
    }
    if let Some(value) = patch.discord_guild_id {
        environment.insert(
            "DISCORD_GUILD_ID".to_owned(),
            Value::String(value.trim().to_owned()),
        );
    }
    if let Some(value) = patch.chatgpt_url {
        environment.insert(
            "CHATGPT_URL".to_owned(),
            Value::String(value.trim().to_owned()),
        );
    }
    if let Some(value) = patch.browser_executable {
        environment.insert(
            "CHATGPT_BROWSER_EXECUTABLE".to_owned(),
            Value::String(value.trim().to_owned()),
        );
    }
    if let Some(value) = patch.browser_hide_when_ready {
        environment.insert(
            "CHATGPT_BROWSER_HIDE_WHEN_READY".to_owned(),
            Value::String(value.to_string()),
        );
    }
    if let Some(value) = patch.audio_capture_mode {
        environment.insert(
            "AUDIO_CAPTURE_MODE".to_owned(),
            Value::String(value.trim().to_lowercase()),
        );
    }
    if let Some(value) = patch.audio_input_volume {
        environment.insert(
            "AUDIO_INPUT_VOLUME".to_owned(),
            Value::String(value.to_string()),
        );
    }
    if let Some(value) = patch.audio_output_volume {
        environment.insert(
            "AUDIO_OUTPUT_VOLUME".to_owned(),
            Value::String(value.to_string()),
        );
    }
    if let Some(value) = patch.audio_earcons_enabled {
        environment.insert(
            "AUDIO_EARCONS_ENABLED".to_owned(),
            Value::String(value.to_string()),
        );
    }
    if let Some(value) = patch.audio_earcon_volume {
        environment.insert(
            "AUDIO_EARCON_VOLUME".to_owned(),
            Value::String(value.to_string()),
        );
    }
    if let Some(value) = patch.audio_sample_rate {
        environment.insert(
            "AUDIO_SAMPLE_RATE".to_owned(),
            Value::String(value.to_string()),
        );
    }
    if let Some(value) = patch.audio_channels {
        environment.insert(
            "AUDIO_CHANNELS".to_owned(),
            Value::String(value.to_string()),
        );
    }

    validate_environment(&environment)?;
    write_environment(&path, &environment)?;
    Ok(snapshot_from_environment(&path, &environment))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_use_browser_media_and_persist_string_values() {
        let environment = default_environment(Path::new(r"C:\GPTVoice"));
        assert_eq!(
            string_value(&environment, "AUDIO_CAPTURE_MODE", ""),
            "browser-media"
        );
        assert_eq!(number_value(&environment, "AUDIO_OUTPUT_VOLUME", 0.0), 1.0);
        assert!(validate_environment(&environment).is_ok());
    }

    #[test]
    fn validation_rejects_masked_tokens_and_invalid_modes() {
        let mut environment = default_environment(Path::new(r"C:\GPTVoice"));
        environment.insert(
            "DISCORD_TOKEN".to_owned(),
            Value::String(MASKED_TOKEN.to_owned()),
        );
        assert!(validate_environment(&environment).is_err());

        environment.insert(
            "DISCORD_TOKEN".to_owned(),
            Value::String("valid-token".to_owned()),
        );
        environment.insert(
            "AUDIO_CAPTURE_MODE".to_owned(),
            Value::String("invalid".to_owned()),
        );
        assert!(validate_environment(&environment).is_err());
    }
}
