mod audio;
mod browser;
mod config;
mod discord;

use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    env,
    process::Command,
    sync::OnceLock,
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter, Manager, RunEvent};

static APP_STARTED: OnceLock<Instant> = OnceLock::new();
const UPDATE_REPOSITORY: &str = "mutedvector/gptvoice-discord";
const UPDATE_RELEASES_URL: &str = "https://github.com/mutedvector/gptvoice-discord/releases";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SystemStatus {
    version: &'static str,
    process_id: u32,
    uptime_seconds: u64,
    platform: &'static str,
    architecture: &'static str,
    cpu_count: usize,
    memory: String,
    browser_executable: String,
    browser_profile: String,
    browser_url: String,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct UpdateStatus {
    checked: bool,
    current_version: String,
    latest_version: Option<String>,
    release_url: String,
    release_name: Option<String>,
    published_at: Option<String>,
    available: bool,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: Option<String>,
    html_url: Option<String>,
    name: Option<String>,
    published_at: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
struct ReleaseVersion {
    major: u64,
    minor: u64,
    patch: u64,
    prerelease: String,
}

fn parse_release_version(value: &str) -> Option<ReleaseVersion> {
    let value = value.trim().trim_start_matches(['v', 'V']);
    let (core, prerelease) = value.split_once('-').unwrap_or((value, ""));
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(ReleaseVersion {
        major,
        minor,
        patch,
        prerelease: prerelease.to_owned(),
    })
}

fn is_newer_release(latest: &str, current: &str) -> bool {
    let Some(latest) = parse_release_version(latest) else {
        return false;
    };
    let Some(current) = parse_release_version(current) else {
        return false;
    };
    match (latest.major, latest.minor, latest.patch).cmp(&(
        current.major,
        current.minor,
        current.patch,
    )) {
        Ordering::Greater => true,
        Ordering::Less => false,
        Ordering::Equal => match (latest.prerelease.is_empty(), current.prerelease.is_empty()) {
            (true, false) => true,
            (false, true) => false,
            _ => latest.prerelease > current.prerelease,
        },
    }
}

fn unavailable_update(error: impl Into<String>) -> UpdateStatus {
    UpdateStatus {
        current_version: env!("CARGO_PKG_VERSION").to_owned(),
        release_url: UPDATE_RELEASES_URL.to_owned(),
        error: Some(error.into()),
        ..UpdateStatus::default()
    }
}

#[tauri::command]
async fn check_for_update() -> UpdateStatus {
    let current_version = env!("CARGO_PKG_VERSION").to_owned();
    let endpoint = format!("https://api.github.com/repos/{UPDATE_REPOSITORY}/releases/latest");
    let client = match reqwest::Client::builder()
        .user_agent("GPTVoice-update-check")
        .timeout(Duration::from_secs(4))
        .build()
    {
        Ok(client) => client,
        Err(error) => return unavailable_update(error.to_string()),
    };

    let response = match client
        .get(endpoint)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => return unavailable_update(error.to_string()),
    };
    if !response.status().is_success() {
        return unavailable_update(format!("GitHub returned HTTP {}.", response.status()));
    }

    let release = match response.json::<GitHubRelease>().await {
        Ok(release) => release,
        Err(error) => return unavailable_update(error.to_string()),
    };
    let Some(latest_version) = release
        .tag_name
        .as_deref()
        .map(str::trim)
        .map(|tag| tag.trim_start_matches(['v', 'V']).to_owned())
        .filter(|tag| parse_release_version(tag).is_some())
    else {
        return unavailable_update(
            "The latest GitHub release did not have a semantic version tag.",
        );
    };
    let release_url = release
        .html_url
        .filter(|url| url.starts_with("https://github.com/"))
        .unwrap_or_else(|| UPDATE_RELEASES_URL.to_owned());

    UpdateStatus {
        checked: true,
        current_version: current_version.clone(),
        latest_version: Some(latest_version.clone()),
        release_url,
        release_name: release.name,
        published_at: release.published_at,
        available: is_newer_release(&latest_version, &current_version),
        error: None,
    }
}

#[tauri::command]
fn open_release_url(url: String) -> Result<(), String> {
    let parsed = reqwest::Url::parse(&url).map_err(|_| "Invalid release URL.".to_owned())?;
    if parsed.scheme() != "https" || parsed.host_str() != Some("github.com") {
        return Err("Only GitHub release links can be opened.".to_owned());
    }

    #[cfg(target_os = "windows")]
    let mut command = {
        let mut command = Command::new("rundll32.exe");
        command.args(["url.dll,FileProtocolHandler", &url]);
        command
    };
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut command = Command::new("open");
        command.arg(&url);
        command
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut command = Command::new("xdg-open");
        command.arg(&url);
        command
    };

    command
        .spawn()
        .map(|_| ())
        .map_err(|error| format!("Could not open the release page: {error}"))
}

#[tauri::command]
fn system_status(app: AppHandle) -> Result<SystemStatus, String> {
    let started = APP_STARTED.get_or_init(Instant::now);
    let config = config::load_snapshot(&app)?;
    Ok(SystemStatus {
        version: env!("CARGO_PKG_VERSION"),
        process_id: std::process::id(),
        uptime_seconds: started.elapsed().as_secs(),
        platform: env::consts::OS,
        architecture: env::consts::ARCH,
        cpu_count: std::thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1),
        memory: "Native host".to_owned(),
        browser_executable: config.browser_executable,
        browser_profile: config.browser_profile,
        browser_url: config.chatgpt_url,
    })
}

#[tauri::command]
fn close_app(app: AppHandle) {
    app.exit(0);
}

#[tauri::command]
fn get_config(app: AppHandle) -> Result<config::ConfigSnapshot, String> {
    config::load_snapshot(&app)
}

fn spawn_runtime_start(app: &AppHandle) {
    let app = app.clone();
    let discord = app.state::<discord::DiscordRuntime>().inner().clone();
    let browsers = app.state::<browser::BrowserRuntime>().inner().clone();
    tauri::async_runtime::spawn(async move {
        let config = match config::load_snapshot(&app) {
            Ok(config) => config,
            Err(error) => {
                let _ = app.emit(
                    "runtime-log",
                    format!("Could not load startup configuration: {error}"),
                );
                return;
            }
        };
        if !config.discord_token_configured {
            let _ = app.emit(
                "runtime-log",
                "Discord is waiting for a bot token. Save one in Config to start automatically.",
            );
            return;
        }
        if let Err(error) = discord::start_runtime(app.clone(), discord, browsers.clone()).await {
            let _ = app.emit("runtime-log", format!("Discord auto-start failed: {error}"));
            return;
        }

        let guild_id = config.discord_guild_id.trim().to_owned();
        if guild_id.is_empty() {
            let _ = app.emit(
                "runtime-log",
                "Discord is running. Set a Discord guild ID in Config to prefetch ChatGPT threads at startup.",
            );
            return;
        }
        match browsers.start_guild(&app, &guild_id).await {
            Ok(status) if status.logged_in => {
                let _ = app.emit(
                    "runtime-log",
                    "Dedicated ChatGPT browser is ready; recent threads were prefetched.",
                );
            }
            Ok(_) => {
                let _ = app.emit(
                    "runtime-log",
                    "Dedicated ChatGPT browser is waiting for sign-in before threads can be prefetched.",
                );
            }
            Err(error) => {
                let _ = app.emit(
                    "runtime-log",
                    format!("ChatGPT browser auto-start failed: {error}"),
                );
            }
        }
    });
}

#[tauri::command]
async fn save_config(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    patch: config::ConfigPatch,
) -> Result<config::ConfigSnapshot, String> {
    let snapshot = config::save_patch(&app, patch)?;
    runtime
        .apply_audio_gains(
            snapshot.audio_input_volume as f32,
            snapshot.audio_output_volume as f32,
        )
        .await;
    spawn_runtime_start(&app);
    Ok(snapshot)
}

#[tauri::command]
async fn browser_status(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
) -> Result<browser::BrowserStatus, String> {
    browser::status(app, runtime, guild_id).await
}

#[tauri::command]
async fn browser_start(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
) -> Result<browser::BrowserStatus, String> {
    browser::start(app, runtime, guild_id).await
}

#[tauri::command]
async fn browser_stop(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
) -> Result<browser::BrowserStatus, String> {
    browser::stop(app, runtime, guild_id).await
}

#[tauri::command]
async fn browser_new_thread(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
) -> Result<browser::BrowserStatus, String> {
    browser::new_thread(app, runtime, guild_id).await
}

#[tauri::command]
async fn browser_resume_thread(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
    thread_id: String,
) -> Result<browser::BrowserStatus, String> {
    browser::resume_thread(app, runtime, guild_id, thread_id).await
}

#[tauri::command]
async fn browser_reconnect_voice(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
) -> Result<browser::BrowserStatus, String> {
    browser::reconnect_voice(app, runtime, guild_id).await
}

#[tauri::command]
async fn browser_set_visibility(
    app: AppHandle,
    runtime: tauri::State<'_, browser::BrowserRuntime>,
    guild_id: String,
    hidden: bool,
) -> Result<browser::BrowserStatus, String> {
    browser::set_visibility(app, runtime, guild_id, hidden).await
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let app = tauri::Builder::default()
        .manage(discord::DiscordRuntime::default())
        .manage(browser::BrowserRuntime::default())
        .invoke_handler(tauri::generate_handler![
            system_status,
            check_for_update,
            open_release_url,
            close_app,
            get_config,
            save_config,
            discord::status,
            browser_status,
            browser_start,
            browser_stop,
            browser_new_thread,
            browser_resume_thread,
            browser_reconnect_voice,
            browser_set_visibility,
            browser::open_voice_settings,
            browser::close_voice_settings,
            browser::set_voice,
            browser::set_intelligence,
            browser::set_language,
            browser::set_mic_muted
        ])
        .setup(|app| {
            if cfg!(debug_assertions) {
                app.handle().plugin(
                    tauri_plugin_log::Builder::default()
                        .level(log::LevelFilter::Info)
                        .build(),
                )?;
            }
            spawn_runtime_start(app.handle());
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while running tauri application");
    app.run(|app, event| {
        if matches!(event, RunEvent::ExitRequested { .. }) {
            discord::stop_runtime(app, app.state::<discord::DiscordRuntime>().inner());
            app.state::<browser::BrowserRuntime>().stop_all();
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{is_newer_release, parse_release_version};

    #[test]
    fn compares_release_versions() {
        assert!(is_newer_release("v0.2.0", "0.1.9"));
        assert!(!is_newer_release("0.1.2", "0.1.2"));
        assert!(!is_newer_release("0.1.2-beta.1", "0.1.2"));
        assert!(is_newer_release("0.1.2", "0.1.2-beta.1"));
        assert!(!is_newer_release("not-a-version", "0.1.2"));
    }

    #[test]
    fn parses_optional_v_prefix_and_prerelease() {
        let version = parse_release_version("v1.2.3-beta.1").expect("valid release version");
        assert_eq!(version.major, 1);
        assert_eq!(version.minor, 2);
        assert_eq!(version.patch, 3);
        assert_eq!(version.prerelease, "beta.1");
    }
}
