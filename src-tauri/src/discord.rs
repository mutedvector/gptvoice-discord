use crate::{audio::BrowserMediaTransport, browser, config};
use serde::Serialize;
use serenity::all::{
    async_trait, ButtonStyle, Command, ComponentInteraction, Context, CreateActionRow,
    CreateButton, CreateCommand, CreateEmbed, CreateInteractionResponse,
    CreateInteractionResponseMessage, EditInteractionResponse, EditMessage, EventHandler,
    GatewayIntents, GuildId, Interaction, MessageId, Ready, UserId,
};
use serenity::gateway::ShardManager;
use serenity::http::Http;
use serenity::Client;
use songbird::{
    events::{CoreEvent, Event, EventContext, TrackEvent},
    input::RawAdapter,
    tracks::Track,
    SerenityInit,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tauri::{AppHandle, Emitter, State};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscordStatus {
    pub state: String,
    pub connected: bool,
    pub user_name: Option<String>,
    pub guild_count: usize,
    pub voice_count: usize,
    pub last_error: Option<String>,
}

impl Default for DiscordStatus {
    fn default() -> Self {
        Self {
            state: "stopped".to_owned(),
            connected: false,
            user_name: None,
            guild_count: 0,
            voice_count: 0,
            last_error: None,
        }
    }
}

#[derive(Clone, Default)]
pub struct DiscordRuntime {
    task: Arc<Mutex<Option<tauri::async_runtime::JoinHandle<()>>>>,
    shard_manager: Arc<Mutex<Option<Arc<ShardManager>>>>,
    status: Arc<Mutex<DiscordStatus>>,
    voice_guilds: Arc<Mutex<HashSet<u64>>>,
    member_panels: Arc<Mutex<HashMap<u64, (serenity::all::ChannelId, MessageId)>>>,
    member_panel_monitor: Arc<Mutex<Option<tauri::async_runtime::JoinHandle<()>>>>,
    http: Arc<Mutex<Option<Arc<Http>>>>,
}

impl DiscordRuntime {
    fn status(&self) -> DiscordStatus {
        let mut status = self
            .status
            .lock()
            .map(|status| status.clone())
            .unwrap_or_else(|_| DiscordStatus {
                state: "error".to_owned(),
                last_error: Some("Discord runtime state lock was poisoned".to_owned()),
                ..DiscordStatus::default()
            });
        status.voice_count = self
            .voice_guilds
            .lock()
            .map(|guilds| guilds.len())
            .unwrap_or_default();
        status
    }

    fn set_status(&self, next: DiscordStatus) {
        if let Ok(mut status) = self.status.lock() {
            *status = next;
        }
    }

    fn emit_status(&self, app: &AppHandle) {
        let mut status = self.status();
        status.voice_count = self
            .voice_guilds
            .lock()
            .map(|guilds| guilds.len())
            .unwrap_or_default();
        let _ = app.emit("discord-status", status);
    }

    fn set_state(&self, app: &AppHandle, state: &str) {
        let mut next = self.status();
        next.state = state.to_owned();
        next.connected = state == "connected";
        next.voice_count = self
            .voice_guilds
            .lock()
            .map(|guilds| guilds.len())
            .unwrap_or_default();
        self.set_status(next);
        self.emit_status(app);
    }

    fn set_error(&self, app: &AppHandle, error: impl Into<String>) {
        let message = error.into();
        let mut next = self.status();
        next.state = "error".to_owned();
        next.connected = false;
        next.last_error = Some(message.clone());
        self.set_status(next);
        let _ = app.emit("runtime-log", format!("Discord: {message}"));
        self.emit_status(app);
    }

    fn voice_connected(&self, guild_id: GuildId) -> bool {
        self.voice_guilds
            .lock()
            .map(|guilds| guilds.contains(&guild_id.get()))
            .unwrap_or(false)
    }

    fn start_member_panel_monitor(&self, app: &AppHandle, browsers: browser::BrowserRuntime) {
        let Ok(mut monitor) = self.member_panel_monitor.lock() else {
            return;
        };
        if let Some(existing) = monitor.as_ref() {
            if !existing.inner().is_finished() {
                return;
            }
        }

        let runtime = self.clone();
        let app = app.clone();
        *monitor = Some(tauri::async_runtime::spawn(async move {
            let mut snapshots = HashMap::<u64, MemberPanelSnapshot>::new();
            loop {
                tokio::time::sleep(Duration::from_millis(750)).await;

                let panels = runtime
                    .member_panels
                    .lock()
                    .map(|panels| {
                        panels
                            .iter()
                            .map(|(guild_id, (channel_id, message_id))| {
                                (*guild_id, (*channel_id, *message_id))
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let panel_guilds = panels
                    .iter()
                    .map(|(guild_id, _)| *guild_id)
                    .collect::<HashSet<_>>();
                snapshots.retain(|guild_id, _| panel_guilds.contains(guild_id));

                for (guild_id, (channel_id, message_id)) in panels {
                    let discord_guild_id = GuildId::new(guild_id);
                    let connected = runtime.voice_connected(discord_guild_id);
                    let browser = if connected {
                        match browsers.status_guild(&app, &guild_id.to_string()).await {
                            Ok(status) => Some(status),
                            Err(error) => {
                                let _ = app.emit(
                                    "runtime-log",
                                    format!(
                                        "Discord panel status refresh failed for guild {guild_id}: {error}"
                                    ),
                                );
                                continue;
                            }
                        }
                    } else {
                        None
                    };
                    let snapshot = MemberPanelSnapshot::new(connected, browser.as_ref());
                    let Some(previous) = snapshots.get(&guild_id).cloned() else {
                        snapshots.insert(guild_id, snapshot);
                        continue;
                    };
                    if previous == snapshot {
                        continue;
                    }

                    let Some(http) = runtime.http.lock().ok().and_then(|http| http.clone()) else {
                        continue;
                    };
                    let voice_limit_started =
                        !previous.voice_limit_reached && snapshot.voice_limit_reached;
                    let voice_ended = previous.voice_mode_active
                        && !snapshot.voice_mode_active
                        && !snapshot.voice_limit_reached;
                    let last_action = if voice_limit_started {
                        Some("ChatGPT Voice reached its daily limit. Reconnect after the reset time.")
                    } else if voice_ended {
                        Some(
                            "ChatGPT Voice ended by itself. Press Reconnect to resume this thread.",
                        )
                    } else {
                        None
                    };
                    let builder = EditMessage::new()
                        .embed(member_panel_embed(
                            discord_guild_id,
                            connected,
                            browser.as_ref(),
                            last_action,
                        ))
                        .components(member_panel_components(
                            discord_guild_id,
                            connected,
                            browser.as_ref().and_then(|status| status.chatgpt_mic_muted),
                        ));
                    match channel_id
                        .edit_message(http.as_ref(), message_id, builder)
                        .await
                    {
                        Ok(_) => {
                            snapshots.insert(guild_id, snapshot);
                            if voice_ended {
                                let _ = app.emit(
                                    "runtime-log",
                                    format!(
                                        "ChatGPT Voice ended for guild {guild_id}; Discord member panel refreshed."
                                    ),
                                );
                            }
                        }
                        Err(error) => {
                            let _ = app.emit(
                                "runtime-log",
                                format!(
                                    "Discord member panel refresh failed for guild {guild_id}: {error}"
                                ),
                            );
                        }
                    }
                }
            }
        }));
    }

    async fn remember_member_panel(
        &self,
        guild_id: GuildId,
        channel_id: serenity::all::ChannelId,
        message_id: MessageId,
        http: &Http,
    ) {
        let previous = self
            .member_panels
            .lock()
            .ok()
            .and_then(|mut panels| panels.insert(guild_id.get(), (channel_id, message_id)));
        if let Some((previous_channel, previous_message)) = previous {
            if previous_message != message_id || previous_channel != channel_id {
                let _ = previous_channel
                    .delete_message(http, previous_message)
                    .await;
            }
        }
    }

    fn delete_member_panels(&self) {
        let panels = self
            .member_panels
            .lock()
            .map(|mut panels| std::mem::take(&mut *panels))
            .unwrap_or_default();
        let http = self.http.lock().ok().and_then(|http| http.clone());
        let Some(http) = http else {
            return;
        };
        tauri::async_runtime::block_on(async move {
            for (_, (channel_id, message_id)) in panels {
                let _ = channel_id.delete_message(http.as_ref(), message_id).await;
            }
        });
    }

    pub fn stop_all(&self, app: &AppHandle) {
        if let Ok(mut monitor) = self.member_panel_monitor.lock() {
            if let Some(monitor) = monitor.take() {
                monitor.abort();
            }
        }
        self.delete_member_panels();
        let shard_manager = self
            .shard_manager
            .lock()
            .ok()
            .and_then(|mut manager| manager.take());
        if let Some(shard_manager) = shard_manager {
            tauri::async_runtime::block_on(async move {
                let _ = tokio::time::timeout(Duration::from_secs(3), shard_manager.shutdown_all())
                    .await;
            });
        }
        if let Ok(mut task) = self.task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
        if let Ok(mut guilds) = self.voice_guilds.lock() {
            guilds.clear();
        }
        if let Ok(mut http) = self.http.lock() {
            *http = None;
        }
        self.set_state(app, "stopped");
    }
}

struct Handler {
    app: AppHandle,
    runtime: DiscordRuntime,
    browsers: browser::BrowserRuntime,
    guild_id: Option<GuildId>,
}

struct ReceiveToBrowser {
    media: Arc<BrowserMediaTransport>,
}

struct RelayTrackEventLogger {
    app: AppHandle,
    guild_id: GuildId,
    event_name: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberPanelAction {
    Join,
    Leave,
    Mic,
    Reconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemberPanelSnapshot {
    connected: bool,
    browser_open: bool,
    logged_in: bool,
    auth_required: bool,
    voice_mode_active: bool,
    voice_limit_reached: bool,
    voice_limit_reset: Option<String>,
    microphone_permission_required: bool,
    chatgpt_mic_muted: Option<bool>,
    active_thread: Option<String>,
    voice: Option<String>,
    voice_description: Option<String>,
    intelligence: Option<String>,
    language: Option<String>,
}

impl MemberPanelSnapshot {
    fn new(connected: bool, browser: Option<&browser::BrowserStatus>) -> Self {
        Self {
            connected,
            browser_open: browser.map(|status| status.open).unwrap_or(false),
            logged_in: browser.map(|status| status.logged_in).unwrap_or(false),
            auth_required: browser.map(|status| status.auth_required).unwrap_or(false),
            voice_mode_active: browser
                .map(|status| status.voice_mode_active)
                .unwrap_or(false),
            voice_limit_reached: browser
                .map(|status| status.voice_limit_reached)
                .unwrap_or(false),
            voice_limit_reset: browser.and_then(|status| status.voice_limit_reset.clone()),
            microphone_permission_required: browser
                .map(|status| status.microphone_permission_required)
                .unwrap_or(false),
            chatgpt_mic_muted: browser.and_then(|status| status.chatgpt_mic_muted),
            active_thread: browser.and_then(|status| {
                status
                    .active_thread
                    .as_ref()
                    .map(|thread| thread.title.clone())
            }),
            voice: browser.and_then(|status| status.voice.clone()),
            voice_description: browser.and_then(|status| status.voice_description.clone()),
            intelligence: browser.and_then(|status| status.intelligence.clone()),
            language: browser.and_then(|status| status.language.clone()),
        }
    }
}

fn member_panel_custom_id(action: &str, guild_id: GuildId) -> String {
    format!("gptvoice:member:{action}:{}", guild_id.get())
}

fn parse_member_panel_action(custom_id: &str) -> Option<(MemberPanelAction, u64)> {
    let mut parts = custom_id.split(':');
    if parts.next() != Some("gptvoice") || parts.next() != Some("member") {
        return None;
    }
    let action = match parts.next()? {
        "join" => MemberPanelAction::Join,
        "leave" => MemberPanelAction::Leave,
        "mic" => MemberPanelAction::Mic,
        "reconnect" => MemberPanelAction::Reconnect,
        _ => return None,
    };
    let guild_id = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((action, guild_id))
}

fn panel_value(value: Option<&str>, fallback: &str, maximum: usize) -> String {
    let value = value.map(str::trim).filter(|value| !value.is_empty());
    let value = value.unwrap_or(fallback);
    if value.chars().count() <= maximum {
        return value.to_owned();
    }
    let truncated = value
        .chars()
        .take(maximum.saturating_sub(3))
        .collect::<String>();
    format!("{truncated}...")
}

fn member_panel_embed(
    guild_id: GuildId,
    connected: bool,
    browser: Option<&browser::BrowserStatus>,
    last_action: Option<&str>,
) -> CreateEmbed {
    let browser_open = browser.map(|status| status.open).unwrap_or(false);
    let logged_in = browser.map(|status| status.logged_in).unwrap_or(false);
    let voice_active = browser
        .map(|status| status.voice_mode_active)
        .unwrap_or(false);
    let limit_reached = browser
        .map(|status| status.voice_limit_reached)
        .unwrap_or(false);
    let mic_muted = browser.and_then(|status| status.chatgpt_mic_muted);

    let (state, color) = if limit_reached {
        ("ChatGPT Voice daily limit reached", 0xED4245)
    } else if !connected {
        ("Not connected to a Discord voice channel", 0x5865F2)
    } else if !browser_open || !logged_in {
        ("Waiting for the dedicated ChatGPT browser", 0xFEE75C)
    } else if voice_active {
        (
            if mic_muted == Some(true) {
                "Connected · ChatGPT mic muted"
            } else {
                "Connected · ChatGPT Voice active"
            },
            0x57F287,
        )
    } else {
        ("Connected · ChatGPT Voice is not active", 0xFEE75C)
    };

    let thread = browser
        .and_then(|status| status.active_thread.as_ref())
        .map(|thread| thread.title.as_str());
    let voice = browser.and_then(|status| status.voice.as_deref());
    let voice_description = browser.and_then(|status| status.voice_description.as_deref());
    let voice_value = match (voice, voice_description) {
        (Some(voice), Some(description)) if !description.trim().is_empty() => {
            format!(
                "{} — {}",
                panel_value(Some(voice), "Unknown", 80),
                panel_value(Some(description), "", 150)
            )
        }
        (voice, _) => panel_value(voice, "Unknown", 150),
    };
    let intelligence = browser.and_then(|status| status.intelligence.as_deref());
    let language = browser.and_then(|status| status.language.as_deref());
    let mut description =
        format!("**{state}**\nThis panel controls the shared GPTVoice session for this server.");
    if let Some(last_action) = last_action.map(str::trim).filter(|value| !value.is_empty()) {
        description.push_str(&format!("\n\n_{last_action}_"));
    }
    if browser.map(|status| status.auth_required).unwrap_or(false) {
        description.push_str("\n\nThe server's dedicated ChatGPT browser needs sign-in.");
    }
    if let Some(status) = browser {
        if status.microphone_permission_required {
            description.push_str("\n\nChatGPT needs microphone permission; an administrator can open the browser and allow it.");
        }
        if let Some(reset) = status
            .voice_limit_reset
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            if status.voice_limit_reached {
                description.push_str(&format!("\nVoice limit resets at **{reset}**."));
            }
        }
    }

    CreateEmbed::new()
        .title("🎙️ GPTVoice")
        .description(description)
        .color(color)
        .field(
            "Thread",
            panel_value(thread, "No thread selected", 200),
            true,
        )
        .field("Voice", voice_value, true)
        .field(
            "Intelligence",
            panel_value(intelligence, "Unknown", 100),
            true,
        )
        .field("Language", panel_value(language, "Unknown", 100), true)
        .footer(serenity::all::CreateEmbedFooter::new(format!(
            "Server {}",
            guild_id.get()
        )))
}

fn member_panel_components(
    guild_id: GuildId,
    connected: bool,
    mic_muted: Option<bool>,
) -> Vec<CreateActionRow> {
    let mic_label = match mic_muted {
        Some(true) => "Unmute input",
        Some(false) => "Mute input",
        None => "Mute input",
    };
    vec![CreateActionRow::Buttons(vec![
        CreateButton::new(member_panel_custom_id("join", guild_id))
            .label("Join voice")
            .style(ButtonStyle::Success)
            .disabled(connected),
        CreateButton::new(member_panel_custom_id("leave", guild_id))
            .label("Leave voice")
            .style(ButtonStyle::Danger)
            .disabled(!connected),
        CreateButton::new(member_panel_custom_id("mic", guild_id))
            .label(mic_label)
            .style(if mic_muted == Some(true) {
                ButtonStyle::Danger
            } else {
                ButtonStyle::Secondary
            })
            .disabled(!connected || mic_muted.is_none()),
        CreateButton::new(member_panel_custom_id("reconnect", guild_id))
            .label("Reconnect")
            .style(ButtonStyle::Primary)
            .disabled(!connected),
    ])]
}

#[async_trait]
impl songbird::EventHandler for RelayTrackEventLogger {
    async fn act(&self, context: &EventContext<'_>) -> Option<songbird::Event> {
        let EventContext::Track(&[(state, _)]) = context else {
            return None;
        };
        let message = format!(
            "Discord ChatGPT output track {} for guild {}: {:?}",
            self.event_name,
            self.guild_id.get(),
            state.playing
        );
        if self.event_name == "error" || self.event_name == "ended" {
            log::warn!("{message}");
        } else {
            log::info!("{message}");
        }
        let _ = self.app.emit("runtime-log", message);
        None
    }
}

#[async_trait]
impl songbird::EventHandler for ReceiveToBrowser {
    async fn act(&self, context: &EventContext<'_>) -> Option<songbird::Event> {
        let EventContext::VoiceTick(tick) = context else {
            return None;
        };
        let frame_len = tick
            .speaking
            .values()
            .filter_map(|voice| voice.decoded_voice.as_ref().map(Vec::len))
            .max()
            .unwrap_or(1_920);
        let mut mixed = vec![0_i16; frame_len];
        let mut counts = vec![0_u32; frame_len];
        for voice in tick.speaking.values() {
            let Some(samples) = voice.decoded_voice.as_ref() else {
                continue;
            };
            for (index, sample) in samples.iter().copied().enumerate().take(frame_len) {
                mixed[index] = mixed[index].saturating_add(sample);
                counts[index] += 1;
            }
        }
        for (sample, count) in mixed.iter_mut().zip(counts) {
            if count > 1 {
                *sample = (*sample as i32 / count as i32) as i16;
            }
        }
        let _ = self.media.send_pcm_i16(&mixed);
        None
    }
}

fn command_definitions() -> Vec<CreateCommand> {
    vec![
        CreateCommand::new("join").description("Join your current Discord voice channel"),
        CreateCommand::new("leave").description("Leave the current Discord voice channel"),
        CreateCommand::new("status").description("Show GPTVoice status"),
    ]
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, context: Context, ready: Ready) {
        if let Ok(mut http) = self.runtime.http.lock() {
            *http = Some(context.http.clone());
        }
        self.runtime
            .start_member_panel_monitor(&self.app, self.browsers.clone());
        let mut status = self.runtime.status();
        status.state = "connected".to_owned();
        status.connected = true;
        status.user_name = Some(ready.user.name.clone());
        status.guild_count = ready.guilds.len();
        status.last_error = None;
        self.runtime.set_status(status);
        self.runtime.emit_status(&self.app);
        let _ = self.app.emit(
            "runtime-log",
            format!("Logged in to Discord as {}.", ready.user.name),
        );

        let commands = command_definitions();
        if let Some(guild_id) = self.guild_id {
            match guild_id.set_commands(&context.http, commands.clone()).await {
                Ok(commands) => {
                    let _ = self.app.emit(
                        "runtime-log",
                        format!(
                            "Registered {} Discord slash commands in the configured guild.",
                            commands.len()
                        ),
                    );
                }
                Err(error) => {
                    let _ = self.app.emit(
                        "runtime-log",
                        format!("Could not register configured-guild slash commands: {error}"),
                    );
                }
            }
        }
        match Command::set_global_commands(&context.http, commands).await {
            Ok(commands) => {
                let _ = self.app.emit(
                    "runtime-log",
                    format!(
                        "Registered {} Discord slash commands globally for all visible servers.",
                        commands.len()
                    ),
                );
            }
            Err(error) => {
                let _ = self.app.emit(
                    "runtime-log",
                    format!("Could not register global slash commands: {error}"),
                );
            }
        }
    }

    async fn interaction_create(&self, context: Context, interaction: Interaction) {
        match interaction {
            Interaction::Command(command) => match command.data.name.as_str() {
                "join" => {
                    // `/join` intentionally creates a public message: it is the shared member
                    // control panel for this server. All later button presses edit this same
                    // message in place.
                    let _ = command.defer(&context).await;
                    let command_guild = command.guild_id;
                    let reply = self
                        .join_voice(&context, command_guild, command.user.id)
                        .await;
                    let connected = command_guild
                        .map(|guild_id| self.runtime.voice_connected(guild_id))
                        .unwrap_or(false);
                    let response = if let Some(guild_id) = command_guild.filter(|_| connected) {
                        self.member_panel_response(guild_id, Some(&reply)).await
                    } else {
                        EditInteractionResponse::new().content(reply)
                    };
                    match command.edit_response(&context, response).await {
                        Ok(message) if connected => {
                            if let Some(guild_id) = command_guild {
                                self.runtime
                                    .remember_member_panel(
                                        guild_id,
                                        command.channel_id,
                                        message.id,
                                        context.http.as_ref(),
                                    )
                                    .await;
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = self.app.emit(
                                "runtime-log",
                                format!("Discord join response failed: {error}"),
                            );
                        }
                    }
                }
                "leave" => {
                    let _ = command.defer_ephemeral(&context).await;
                    let reply = self.leave_voice(&context, command.guild_id).await;
                    let _ = command
                        .edit_response(&context, EditInteractionResponse::new().content(reply))
                        .await;
                }
                "status" => {
                    let content = if self.runtime.status().connected {
                        "GPTVoice is connected to Discord."
                    } else {
                        "GPTVoice is not connected to Discord."
                    };
                    let response = CreateInteractionResponse::Message(
                        CreateInteractionResponseMessage::new()
                            .content(content)
                            .ephemeral(true),
                    );
                    if let Err(error) = command.create_response(&context, response).await {
                        let _ = self.app.emit(
                            "runtime-log",
                            format!("Discord interaction failed: {error}"),
                        );
                    }
                }
                _ => {}
            },
            Interaction::Component(component) => {
                let Some((_, panel_guild_id)) =
                    parse_member_panel_action(&component.data.custom_id)
                else {
                    return;
                };
                let Some(guild_id) = component.guild_id else {
                    return;
                };
                if panel_guild_id != guild_id.get() {
                    return;
                }
                if let Err(error) = component.defer(&context).await {
                    let _ = self.app.emit(
                        "runtime-log",
                        format!("Discord panel acknowledgement failed: {error}"),
                    );
                    return;
                }
                let response = match self
                    .handle_member_panel_component(&context, &component)
                    .await
                {
                    Ok(response) => response,
                    Err(error) => self.member_panel_response(guild_id, Some(&error)).await,
                };
                match component.edit_response(&context, response).await {
                    Ok(_) => {
                        self.runtime
                            .remember_member_panel(
                                guild_id,
                                component.channel_id,
                                component.message.id,
                                context.http.as_ref(),
                            )
                            .await;
                    }
                    Err(error) => {
                        let _ = self.app.emit(
                            "runtime-log",
                            format!("Discord member panel update failed: {error}"),
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

impl Handler {
    async fn member_panel_response(
        &self,
        guild_id: GuildId,
        last_action: Option<&str>,
    ) -> EditInteractionResponse {
        let connected = self.runtime.voice_connected(guild_id);
        let browser = if connected {
            self.browsers
                .status_guild(&self.app, &guild_id.to_string())
                .await
                .ok()
        } else {
            None
        };
        let mic_muted = browser.as_ref().and_then(|status| status.chatgpt_mic_muted);
        EditInteractionResponse::new()
            .embed(member_panel_embed(
                guild_id,
                connected,
                browser.as_ref(),
                last_action,
            ))
            .components(member_panel_components(guild_id, connected, mic_muted))
    }

    async fn handle_member_panel_component(
        &self,
        context: &Context,
        component: &ComponentInteraction,
    ) -> Result<EditInteractionResponse, String> {
        let guild_id = component
            .guild_id
            .ok_or_else(|| "This panel can only be used inside a Discord server.".to_owned())?;
        let (action, panel_guild_id) = parse_member_panel_action(&component.data.custom_id)
            .ok_or_else(|| "This GPTVoice panel button is no longer valid.".to_owned())?;
        if panel_guild_id != guild_id.get() {
            return Err("This panel belongs to a different Discord server.".to_owned());
        }

        let reply = match action {
            MemberPanelAction::Join => {
                self.join_voice(context, Some(guild_id), component.user.id)
                    .await
            }
            MemberPanelAction::Leave => self.leave_voice(context, Some(guild_id)).await,
            MemberPanelAction::Mic => {
                let status = self
                    .browsers
                    .status_guild(&self.app, &guild_id.to_string())
                    .await?;
                let muted = status.chatgpt_mic_muted.ok_or_else(|| {
                    "ChatGPT's microphone state is unavailable. Reconnect Voice and try again."
                        .to_owned()
                })?;
                let next_muted = !muted;
                let updated = self
                    .browsers
                    .set_mic_muted_guild(&self.app, &guild_id.to_string(), next_muted)
                    .await?;
                if updated.chatgpt_mic_muted != Some(next_muted) {
                    return Err("ChatGPT's microphone state did not update.".to_owned());
                }
                if next_muted {
                    "Input to ChatGPT is muted for this server."
                } else {
                    "Input to ChatGPT is live again for this server."
                }
                .to_owned()
            }
            MemberPanelAction::Reconnect => {
                let status = self
                    .browsers
                    .reconnect_voice_guild(&self.app, &guild_id.to_string())
                    .await?;
                if status.voice_limit_reached {
                    if let Some(reset) = status
                        .voice_limit_reset
                        .as_deref()
                        .filter(|value| !value.is_empty())
                    {
                        format!("ChatGPT Voice reached its daily limit; it resets at {reset}.")
                    } else {
                        "ChatGPT Voice reached its daily limit.".to_owned()
                    }
                } else if status.voice_mode_active {
                    "ChatGPT Voice reconnected to the active thread.".to_owned()
                } else {
                    "The browser refreshed, but ChatGPT Voice is not active. An administrator can start or resume a thread from the desktop panel."
                        .to_owned()
                }
            }
        };

        Ok(self.member_panel_response(guild_id, Some(&reply)).await)
    }

    async fn join_voice(
        &self,
        context: &Context,
        guild_id: Option<GuildId>,
        user_id: UserId,
    ) -> String {
        let Some(guild_id) = guild_id else {
            return "Use this control inside a Discord server.".to_owned();
        };
        if self
            .runtime
            .voice_guilds
            .lock()
            .map(|guilds| guilds.contains(&guild_id.get()))
            .unwrap_or(false)
        {
            return "GPTVoice is already connected to this server's voice channel.".to_owned();
        }
        let channel_id = context.cache.guild(guild_id).and_then(|guild| {
            guild
                .voice_states
                .get(&user_id)
                .and_then(|state| state.channel_id)
        });
        let Some(channel_id) = channel_id else {
            return "Join a voice channel first, then press /join again.".to_owned();
        };
        let Some(songbird) = songbird::get(context).await else {
            return "The Discord voice driver is not available yet.".to_owned();
        };
        if let Err(error) = songbird.join(guild_id, channel_id).await {
            return format!("I could not join that voice channel: {error}");
        }
        match self
            .browsers
            .start_guild(&self.app, &guild_id.to_string())
            .await
        {
            Ok(browser) if browser.logged_in => {
                let media = match self
                    .browsers
                    .media_for_guild(&self.app, &guild_id.to_string())
                    .await
                {
                    Ok(media) => media,
                    Err(error) => {
                        let _ = songbird.leave(guild_id).await;
                        return format!("I could not start the browser audio transport: {error}");
                    }
                };
                let reader = match self
                    .browsers
                    .take_media_reader(&self.app, &guild_id.to_string())
                    .await
                {
                    Ok(reader) => reader,
                    Err(error) => {
                        let _ = songbird.leave(guild_id).await;
                        return format!("I could not attach ChatGPT audio to Discord: {error}");
                    }
                };
                let Some(call) = songbird.get(guild_id) else {
                    let _ = songbird.leave(guild_id).await;
                    return "Discord did not keep the voice connection open.".to_owned();
                };
                {
                    let mut call = call.lock().await;
                    call.add_global_event(CoreEvent::VoiceTick.into(), ReceiveToBrowser { media });
                    let output_track = Track::from(RawAdapter::new(
                        reader,
                        crate::audio::PCM_SAMPLE_RATE,
                        crate::audio::PCM_CHANNELS,
                    ));
                    let output_handle = call.play_only(output_track);
                    for (event, event_name) in [
                        (TrackEvent::Playable, "ready"),
                        (TrackEvent::Error, "error"),
                        (TrackEvent::End, "ended"),
                    ] {
                        if let Err(error) = output_handle.add_event(
                            Event::Track(event),
                            RelayTrackEventLogger {
                                app: self.app.clone(),
                                guild_id,
                                event_name,
                            },
                        ) {
                            log::warn!(
                                "Could not attach Discord output track diagnostics for guild {}: {error}",
                                guild_id.get()
                            );
                        }
                    }
                }
                if let Ok(mut guilds) = self.runtime.voice_guilds.lock() {
                    guilds.insert(guild_id.get());
                }
                let _ = self.app.emit(
                    "runtime-log",
                    format!(
                        "Joined Discord voice and opened ChatGPT for guild {}.",
                        guild_id.get()
                    ),
                );
                "Joined your voice channel and connected the continuous audio relay. The administrator can control ChatGPT from the GPTVoice desktop Status panel.".to_owned()
            }
            Ok(_) => {
                let _ = songbird.leave(guild_id).await;
                let _ = self
                    .browsers
                    .stop_guild(&self.app, &guild_id.to_string())
                    .await;
                "The dedicated ChatGPT profile needs sign-in. Complete the browser setup, then press /join again.".to_owned()
            }
            Err(error) => {
                let _ = songbird.leave(guild_id).await;
                format!("I could not start the dedicated ChatGPT browser: {error}")
            }
        }
    }

    async fn leave_voice(&self, context: &Context, guild_id: Option<GuildId>) -> String {
        let Some(guild_id) = guild_id else {
            return "Use this control inside a Discord server.".to_owned();
        };
        if let Some(songbird) = songbird::get(context).await {
            let _ = songbird.leave(guild_id).await;
        }
        if let Ok(mut guilds) = self.runtime.voice_guilds.lock() {
            guilds.remove(&guild_id.get());
        }
        let _ = self
            .browsers
            .stop_guild(&self.app, &guild_id.to_string())
            .await;
        "Left the Discord voice channel and stopped this guild's ChatGPT browser session."
            .to_owned()
    }
}

#[tauri::command]
pub async fn status(runtime: State<'_, DiscordRuntime>) -> Result<DiscordStatus, String> {
    Ok(runtime.status())
}

pub async fn start_runtime(
    app: AppHandle,
    runtime: DiscordRuntime,
    browsers: browser::BrowserRuntime,
) -> Result<DiscordStatus, String> {
    {
        let mut task = runtime
            .task
            .lock()
            .map_err(|_| "Discord runtime task lock was poisoned".to_owned())?;
        if let Some(existing) = task.as_ref() {
            if !existing.inner().is_finished() {
                return Ok(runtime.status());
            }
        }

        let (token, guild_id) = config::load_discord_credentials(&app)?;
        let parsed_guild_id = guild_id
            .as_deref()
            .map(str::parse::<u64>)
            .transpose()
            .map_err(|_| "Configured Discord guild ID must be numeric".to_owned())?
            .map(GuildId::new);
        runtime.set_state(&app, "connecting");
        let app_for_task = app.clone();
        let runtime_for_task = runtime.clone();
        let task_handle = tauri::async_runtime::spawn(async move {
            let intents = GatewayIntents::GUILDS | GatewayIntents::GUILD_VOICE_STATES;
            let handler = Handler {
                app: app_for_task.clone(),
                runtime: runtime_for_task.clone(),
                browsers,
                guild_id: parsed_guild_id,
            };
            let songbird_config = songbird::Config::default().decode_mode(
                songbird::driver::DecodeMode::Decode(songbird::driver::DecodeConfig::default()),
            );
            let client = Client::builder(token, intents)
                .event_handler(handler)
                .register_songbird_from_config(songbird_config)
                .await;
            let mut client = match client {
                Ok(client) => client,
                Err(error) => {
                    runtime_for_task
                        .set_error(&app_for_task, format!("Could not create client: {error}"));
                    return;
                }
            };
            if let Ok(mut shard_manager) = runtime_for_task.shard_manager.lock() {
                *shard_manager = Some(client.shard_manager.clone());
            }
            if let Err(error) = client.start().await {
                runtime_for_task.set_error(
                    &app_for_task,
                    format!("Discord connection stopped: {error}"),
                );
            } else {
                runtime_for_task.set_state(&app_for_task, "stopped");
            }
            if let Ok(mut shard_manager) = runtime_for_task.shard_manager.lock() {
                *shard_manager = None;
            }
        });
        *task = Some(task_handle);
    }
    Ok(runtime.status())
}

pub fn stop_runtime(app: &AppHandle, runtime: &DiscordRuntime) {
    runtime.stop_all(app);
    let _ = app.emit("runtime-log", "Discord connection stopped.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_panel_action_ids_are_server_scoped() {
        let guild_id = GuildId::new(964597969980620820);
        let custom_id = member_panel_custom_id("reconnect", guild_id);
        assert_eq!(
            parse_member_panel_action(&custom_id),
            Some((MemberPanelAction::Reconnect, guild_id.get()))
        );
        assert_eq!(
            parse_member_panel_action("gptvoice:member:reconnect:123:extra"),
            None
        );
        assert_eq!(
            parse_member_panel_action("gptvoice:member:unknown:123"),
            None
        );
    }

    #[test]
    fn member_panel_has_compact_controls_and_updates_mic_label() {
        let guild_id = GuildId::new(964597969980620820);
        let components = member_panel_components(guild_id, true, Some(false));
        let value = serde_json::to_value(components).expect("components should serialize");
        let buttons = value[0]["components"]
            .as_array()
            .expect("panel row should contain buttons");
        assert_eq!(buttons.len(), 4);
        assert_eq!(buttons[0]["label"], "Join voice");
        assert_eq!(buttons[1]["label"], "Leave voice");
        assert_eq!(buttons[2]["label"], "Mute input");
        assert_eq!(buttons[3]["label"], "Reconnect");
        assert_eq!(buttons[0]["disabled"], true);
        assert_eq!(buttons[1]["disabled"], false);

        let muted = serde_json::to_value(member_panel_components(guild_id, true, Some(true)))
            .expect("muted components should serialize");
        assert_eq!(muted[0]["components"][2]["label"], "Unmute input");
    }

    #[test]
    fn member_panel_embed_exposes_current_voice_state() {
        let guild_id = GuildId::new(964597969980620820);
        let mut browser = browser::BrowserStatus::closed(&guild_id.to_string());
        browser.open = true;
        browser.logged_in = true;
        browser.voice_mode_active = true;
        browser.voice = Some("spruce".to_owned());
        browser.voice_description = Some("Calm and affirming".to_owned());
        browser.intelligence = Some("Instant".to_owned());
        browser.language = Some("Arabic".to_owned());
        browser.active_thread = Some(browser::ThreadSummary {
            id: "thread-1".to_owned(),
            title: "Friends voice chat".to_owned(),
            url: "https://chatgpt.com/c/thread-1".to_owned(),
        });
        let embed = member_panel_embed(guild_id, true, Some(&browser), None);
        let value = serde_json::to_value(embed).expect("embed should serialize");
        let fields = value["fields"]
            .as_array()
            .expect("embed should have fields");
        assert_eq!(fields[0]["value"], "Friends voice chat");
        assert_eq!(fields[1]["value"], "spruce — Calm and affirming");
        assert_eq!(fields[2]["value"], "Instant");
        assert_eq!(fields[3]["value"], "Arabic");
    }
}
