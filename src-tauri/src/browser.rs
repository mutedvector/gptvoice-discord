use crate::{
    audio::BrowserMediaTransport,
    config::{self, ConfigSnapshot},
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tauri::{AppHandle, Emitter, State};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex as AsyncMutex},
    time::{sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

const START_TIMEOUT: Duration = Duration::from_secs(30);
const EVALUATE_TIMEOUT: Duration = Duration::from_secs(15);
const STATUS_PROBE_MIN_INTERVAL: Duration = Duration::from_secs(1);
const CDP_FAILURES_BEFORE_RESTART: u32 = 3;
const VOICE_END_CONFIRMATION_CHECKS: u32 = 2;
const VOICE_MEDIA_END_CONFIRMATION_CHECKS: u32 = 3;
const AUTH_MARKER: &str = ".gptvoice-authenticated";
const OPTIONS_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_DISCOVERED_VOICES: usize = 32;
const PAGE_READY_SETTLE: Duration = Duration::from_millis(100);
const SIDEBAR_SETTLE: Duration = Duration::from_millis(120);
const THREAD_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(3);
const THREAD_DISCOVERY_INTERVAL: Duration = Duration::from_millis(75);
const THREAD_CACHE_TTL: Duration = Duration::from_secs(30);
const VOICE_SETTINGS_POLL_INTERVAL: Duration = Duration::from_millis(50);
const VOICE_SETTINGS_WAIT_TIMEOUT: Duration = Duration::from_secs(3);

type BrowserWebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type BrowserWriter = SplitSink<BrowserWebSocket, Message>;
type PendingCdpRequests = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, String>>>>>;

#[derive(Clone)]
struct CdpClient {
    writer: Arc<AsyncMutex<BrowserWriter>>,
    pending: PendingCdpRequests,
    next_id: Arc<AtomicU64>,
}

impl CdpClient {
    async fn connect(websocket_url: &str) -> Result<Self, String> {
        let (socket, _) = connect_async(websocket_url)
            .await
            .map_err(|error| format!("Could not connect to browser CDP: {error}"))?;
        let (writer, mut reader) = socket.split();
        let client = Self {
            writer: Arc::new(AsyncMutex::new(writer)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
        };
        let pending = Arc::clone(&client.pending);
        tauri::async_runtime::spawn(async move {
            while let Some(message) = reader.next().await {
                let Ok(Message::Text(text)) = message else {
                    continue;
                };
                let Ok(value) = serde_json::from_str::<Value>(text.as_ref()) else {
                    continue;
                };
                let Some(id) = value.get("id").and_then(Value::as_u64) else {
                    continue;
                };
                let result = if let Some(error) = value.get("error") {
                    Err(error.to_string())
                } else {
                    Ok(value.get("result").cloned().unwrap_or(Value::Null))
                };
                if let Ok(mut pending) = pending.lock() {
                    if let Some(sender) = pending.remove(&id) {
                        let _ = sender.send(result);
                    }
                }
            }
            if let Ok(mut pending) = pending.lock() {
                for (_, sender) in pending.drain() {
                    let _ = sender.send(Err("Browser CDP connection closed".to_owned()));
                }
            }
        });
        Ok(client)
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| "Browser CDP pending-request lock was poisoned".to_owned())?
            .insert(id, sender);
        let payload = json!({"id": id, "method": method, "params": params}).to_string();
        if let Err(error) = self
            .writer
            .lock()
            .await
            .send(Message::Text(payload.into()))
            .await
        {
            self.pending
                .lock()
                .ok()
                .and_then(|mut pending| pending.remove(&id));
            return Err(format!("Could not send browser CDP command: {error}"));
        }
        let result = timeout(EVALUATE_TIMEOUT, receiver).await;
        if result.is_err() {
            if let Ok(mut pending) = self.pending.lock() {
                pending.remove(&id);
            }
            return Err(format!("Browser CDP command {method} timed out"));
        }
        result
            .expect("CDP timeout result was checked")
            .map_err(|_| "Browser CDP response channel closed".to_owned())?
    }

    async fn evaluate(&self, expression: &str) -> Result<Value, String> {
        let result = self
            .call(
                "Runtime.evaluate",
                json!({
                    "expression": expression,
                    "awaitPromise": true,
                    "returnByValue": true,
                    "userGesture": true
                }),
            )
            .await?;
        if let Some(exception) = result.get("exceptionDetails") {
            return Err(format!("Browser page evaluation failed: {exception}"));
        }
        Ok(result
            .get("result")
            .and_then(|remote| remote.get("value"))
            .cloned()
            .unwrap_or(Value::Null))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSummary {
    pub id: String,
    pub title: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct VoiceOption {
    pub value: String,
    pub label: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct VoiceSettingOptions {
    voices: Vec<VoiceOption>,
    intelligence: Vec<String>,
    languages: Vec<String>,
    current_voice: Option<String>,
    current_voice_description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserStatus {
    pub guild_id: String,
    pub open: bool,
    pub logged_in: bool,
    pub auth_required: bool,
    pub url: Option<String>,
    pub title: Option<String>,
    pub voice_control_visible: bool,
    pub voice_mode_active: bool,
    pub voice_settings_open: bool,
    pub voice: Option<String>,
    pub voice_description: Option<String>,
    pub intelligence: Option<String>,
    pub language: Option<String>,
    pub available_voices: Vec<VoiceOption>,
    pub available_intelligence: Vec<String>,
    pub available_languages: Vec<String>,
    pub chatgpt_mic_muted: Option<bool>,
    pub microphone_permission_required: bool,
    pub voice_limit_reached: bool,
    pub voice_limit_reset: Option<String>,
    pub active_thread: Option<ThreadSummary>,
    pub recent_threads: Vec<ThreadSummary>,
    pub browser_process_id: Option<u32>,
    pub window_hidden: bool,
    pub media_connected: bool,
    pub media_packets_sent: u64,
    pub media_packets_dropped: u64,
    pub media_input_queue_depth: u64,
    pub media_input_queue_peak_depth: u64,
    pub media_input_queue_dropped: u64,
    pub media_packets_received: u64,
    pub media_reader_reads: u64,
    pub media_reader_source_bytes: u64,
    pub media_reader_output_bytes: u64,
    pub media_reader_silence_bytes: u64,
    pub media_output_capture_mode: Option<String>,
    pub media_output_track_count: u64,
    pub media_output_callbacks: u64,
    pub media_output_dropped_callbacks: u64,
    pub media_output_attach_errors: u64,
    pub media_output_worklet_captures: u64,
    pub media_output_worklet_fallbacks: u64,
    pub media_output_worklet_frames: u64,
    pub media_output_worklet_peak: f64,
    pub media_output_worklet_non_silent_frames: u64,
    pub media_output_worklet_max_gap_ms: f64,
    pub media_input_frames: u64,
    pub media_input_silence_frames: u64,
    pub media_browser_input_queue_samples: u64,
    pub media_browser_input_queue_peak_samples: u64,
    pub media_browser_input_queue_depth: u64,
    pub media_browser_input_dropped_messages: u64,
    pub media_browser_input_last_frame_at: u64,
    pub cdp_status_failures: u32,
    pub cdp_restarts: u64,
    pub error: Option<String>,
}

impl BrowserStatus {
    pub(crate) fn closed(guild_id: &str) -> Self {
        Self {
            guild_id: guild_id.to_owned(),
            open: false,
            logged_in: false,
            auth_required: false,
            url: None,
            title: None,
            voice_control_visible: false,
            voice_mode_active: false,
            voice_settings_open: false,
            voice: None,
            voice_description: None,
            intelligence: None,
            language: None,
            available_voices: Vec::new(),
            available_intelligence: Vec::new(),
            available_languages: Vec::new(),
            chatgpt_mic_muted: None,
            microphone_permission_required: false,
            voice_limit_reached: false,
            voice_limit_reset: None,
            active_thread: None,
            recent_threads: Vec::new(),
            browser_process_id: None,
            window_hidden: false,
            media_connected: false,
            media_packets_sent: 0,
            media_packets_dropped: 0,
            media_input_queue_depth: 0,
            media_input_queue_peak_depth: 0,
            media_input_queue_dropped: 0,
            media_packets_received: 0,
            media_reader_reads: 0,
            media_reader_source_bytes: 0,
            media_reader_output_bytes: 0,
            media_reader_silence_bytes: 0,
            media_output_capture_mode: None,
            media_output_track_count: 0,
            media_output_callbacks: 0,
            media_output_dropped_callbacks: 0,
            media_output_attach_errors: 0,
            media_output_worklet_captures: 0,
            media_output_worklet_fallbacks: 0,
            media_output_worklet_frames: 0,
            media_output_worklet_peak: 0.0,
            media_output_worklet_non_silent_frames: 0,
            media_output_worklet_max_gap_ms: 0.0,
            media_input_frames: 0,
            media_input_silence_frames: 0,
            media_browser_input_queue_samples: 0,
            media_browser_input_queue_peak_samples: 0,
            media_browser_input_queue_depth: 0,
            media_browser_input_dropped_messages: 0,
            media_browser_input_last_frame_at: 0,
            cdp_status_failures: 0,
            cdp_restarts: 0,
            error: None,
        }
    }

    fn sign_in_required(guild_id: &str) -> Self {
        let mut status = Self::closed(guild_id);
        status.open = true;
        status.auth_required = true;
        status.error =
            Some("Sign in to ChatGPT in the dedicated GPTVoice browser window.".to_owned());
        status
    }
}

#[derive(Debug, Default, Deserialize)]
struct CdpTarget {
    id: String,
    #[serde(rename = "type")]
    target_type: String,
    #[serde(rename = "webSocketDebuggerUrl")]
    websocket_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PageState {
    url: String,
    title: String,
    logged_in: bool,
    voice_control_visible: bool,
    voice_mode_active: bool,
    voice_settings_open: bool,
    voice: Option<String>,
    voice_description: Option<String>,
    intelligence: Option<String>,
    language: Option<String>,
    chatgpt_mic_muted: Option<bool>,
    microphone_permission_required: bool,
    voice_limit_reached: bool,
    voice_limit_reset: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct PageMediaState {
    installed: bool,
    output_track_count: u64,
}

const DISCOVER_OPTIONS_SCRIPT: &str = r#"(async () => {
  const wait = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));
  const visible = (element) => {
    if (!element || element.disabled) return false;
    const style = getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
  };
  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
  const metadata = (element) => clean(`${element?.getAttribute('aria-label') || ''} ${element?.getAttribute('title') || ''} ${element?.textContent || ''}`);
  const genericSettingText = /^(voice settings|voice customization|intelligence|language|close|cancel|dismiss|instant|medium|high|auto-detect)$/i;
  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
  const hasSettingsText = (element) => {
    if (!element || element.closest('nav,aside')) return false;
    const text = clean(element.innerText);
    return text.length <= 5000 && /\blanguage\b/i.test(text) &&
      [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, 'Language')) &&
      ( /\bintelligence\b/i.test(text) || /\bvoice\b/i.test(text) ||
        element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 ||
        element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 );
  };
  const settingsSurface = () => {
    const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
    const marked = surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0];
    if (marked) return marked;
    const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
    const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
    const anchor = intelligence || language;
    if (!anchor || !language) return null;
    for (let candidate = anchor, depth = 0; candidate && depth < 20; candidate = candidate.parentElement, depth += 1) {
      if (visible(candidate) && !candidate.closest('nav,aside') && clean(candidate.innerText).length <= 5000 &&
        /\blanguage\b/i.test(candidate.innerText || '') && [...candidate.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, 'Language')) &&
        (candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(candidate.innerText || ''))) return candidate;
    }
    return null;
  };
  const readVoice = () => {
    const surface = settingsSurface();
    if (!surface) return null;
    const box = surface.getBoundingClientRect();
    const selectedRadio = [...surface.querySelectorAll('[role="radio"],input[type="radio"]')].find((element) => visible(element) && (element.getAttribute('aria-checked') === 'true' || element.getAttribute('data-state') === 'checked' || element.checked));
    const selectedRadioText = clean(selectedRadio?.getAttribute('aria-label') || selectedRadio?.textContent || '');
    const candidates = [...surface.querySelectorAll('h1,h2,h3,h4,h5,[role="heading"],p,span,div')]
      .filter((element) => visible(element) && (element.matches('h1,h2,h3,h4,h5,[role="heading"]') || element.children.length === 0))
      .map((element) => {
        const text = clean(element.textContent);
        const rect = element.getBoundingClientRect();
        return { element, text, rect, centerX: rect.left + rect.width / 2, centerY: rect.top + rect.height / 2, heading: element.matches('h1,h2,h3,h4,h5,[role="heading"]') };
      })
      .filter((item) => item.text && item.text.length <= 60 && !genericSettingText.test(item.text) && !item.element.closest('button,[role="button"]'))
      .filter((item) => item.centerY > box.top + box.height * .18 && item.centerY < box.top + box.height * .66)
      .sort((a, b) => {
        const score = (item) => (item.heading ? 0 : 1) + Math.abs(item.centerX - (box.left + box.width / 2)) / Math.max(box.width, 1) + Math.abs(item.centerY - (box.top + box.height * .43)) / Math.max(box.height, 1);
        return score(a) - score(b);
      });
    const selected = candidates.find((candidate) => selectedRadioText && candidate.text.toLowerCase() === selectedRadioText.toLowerCase()) || candidates[0];
    if (!selected && !selectedRadioText) return null;
    const description = selected ? [...surface.querySelectorAll('p,span,div')]
      .filter((element) => visible(element) && element.children.length === 0 && !element.closest('button,[role="button"]'))
      .map((element) => ({ text: clean(element.textContent), rect: element.getBoundingClientRect() }))
      .filter((item) => item.text && item.text !== selected.text && item.text.length <= 100 && !genericSettingText.test(item.text) && item.rect.top >= selected.rect.bottom && item.rect.top < selected.rect.bottom + 70)
      .sort((a, b) => a.rect.top - b.rect.top)[0]?.text || null : null;
    return { value: selectedRadioText || selected.text, label: selectedRadioText || selected.text, description };
  };
  const radioVoices = () => {
    const surface = settingsSurface();
    if (!surface) return [];
    const group = [...surface.querySelectorAll('[role="radiogroup"]')].find(visible) || surface;
    const seen = new Set();
    return [...group.querySelectorAll('[role="radio"],input[type="radio"]')]
      .filter(visible)
      .map((element) => ({
        value: clean(element.getAttribute('aria-label') || element.getAttribute('data-value') || element.textContent),
        selected: element.getAttribute('aria-checked') === 'true' || element.getAttribute('data-state') === 'checked' || element.checked
      }))
      .filter((voice) => voice.value && voice.value.length <= 60 && !genericSettingText.test(voice.value) && !seen.has(voice.value.toLowerCase()) && (seen.add(voice.value.toLowerCase()), true));
  };
  const readSetting = (label) => {
    const surface = settingsSurface();
    if (!surface) return null;
    const labelNode = [...surface.querySelectorAll('*')].find((element) => visible(element) && hasExactText(element, label));
    if (!labelNode) return null;
    const withoutLabel = (value) => clean(value).replace(new RegExp(`^${label}\\s*`, 'i'), '').trim();
    for (let ancestor = labelNode, depth = 0; ancestor && depth < 7; ancestor = ancestor.parentElement, depth += 1) {
      if (clean(ancestor.innerText).length > 500) continue;
      const controls = [...ancestor.querySelectorAll('button,[role="button"],[role="combobox"],select')]
        .filter(visible)
        .map((element) => withoutLabel(element.innerText || element.textContent))
        .filter((text) => text && text.toLowerCase() !== label.toLowerCase() && text.length <= 80);
      if (controls.length) return controls[controls.length - 1];
      const leaves = [...ancestor.querySelectorAll('*')]
        .filter((element) => visible(element) && element.children.length === 0)
        .map((element) => withoutLabel(element.textContent))
        .filter((text) => text && text.toLowerCase() !== label.toLowerCase() && text.length <= 80);
      if (leaves.length) return leaves[leaves.length - 1];
    }
    return null;
  };
  const rowTrigger = (label) => {
    const surface = settingsSurface();
    if (!surface) return null;
    const labelNode = [...surface.querySelectorAll('*')].find((element) => visible(element) && hasExactText(element, label));
    if (!labelNode) return null;
    const direct = labelNode.closest('button,[role="button"],[role="combobox"],select');
    if (direct && visible(direct) && clean(direct.innerText || direct.textContent).length <= 160) return direct;
    for (let ancestor = labelNode, depth = 0; ancestor && depth < 7; ancestor = ancestor.parentElement, depth += 1) {
      if (clean(ancestor.innerText).length > 500) continue;
      const controls = [...ancestor.querySelectorAll('button,[role="button"],[role="combobox"],select')]
        .filter(visible)
        .filter((element) => !/close|cancel|dismiss|voice settings|voice customization/i.test(metadata(element)))
        .filter((element) => clean(element.innerText || element.textContent).length <= 160);
      if (controls.length) return controls[controls.length - 1];
    }
    return null;
  };
  const popupRoots = (trigger) => {
    const surface = settingsSurface();
    const controlledId = trigger?.getAttribute('aria-controls') || trigger?.getAttribute('aria-owns');
    const controlled = controlledId ? document.getElementById(controlledId) : null;
    if (controlled && visible(controlled)) return [controlled];
    const triggerRect = trigger?.getBoundingClientRect();
    const roots = [...document.querySelectorAll('[role="listbox"],[role="menu"],[data-radix-popper-content-wrapper],[data-radix-select-content],[data-radix-menu-content],[data-radix-dropdown-menu-content]')]
      .filter(visible)
      .filter((root) => root !== surface && !hasSettingsText(root) && !root.closest('nav,aside') && clean(root.innerText).length <= 2500)
      .map((root) => {
        const rect = root.getBoundingClientRect();
        const centerX = rect.left + rect.width / 2;
        const centerY = rect.top + rect.height / 2;
        const triggerX = triggerRect ? triggerRect.left + triggerRect.width / 2 : centerX;
        const triggerY = triggerRect ? triggerRect.top + triggerRect.height / 2 : centerY;
        const roleScore = root.matches('[role="listbox"],[role="menu"]') ? 0 : 1;
        return { root, roleScore, distance: Math.hypot(centerX - triggerX, centerY - triggerY) };
      })
      .sort((a, b) => a.distance - b.distance || a.roleScore - b.roleScore);
    return roots.length ? [roots[0].root] : [];
  };
  const closePopup = async (trigger) => {
    const escape = new KeyboardEvent('keydown', { key: 'Escape', code: 'Escape', bubbles: true, cancelable: true });
    document.activeElement?.dispatchEvent(escape);
    document.dispatchEvent(escape);
    const deadline = performance.now() + 500;
    while (popupRoots(trigger).length && performance.now() < deadline) await wait(20);
    if (popupRoots(trigger).length && trigger && visible(trigger)) {
      trigger.click();
      while (popupRoots(trigger).length && performance.now() < deadline + 500) await wait(20);
    }
  };
  const menuValues = (root) => {
    const semanticNodes = [...root.querySelectorAll('[role="option"],[role="menuitem"]')].filter(visible);
    const fallbackNodes = semanticNodes.length
      ? semanticNodes
      : [...root.querySelectorAll(':scope > button,:scope > li,:scope > [data-value]')].filter(visible);
    const nodes = root.matches('[role="option"],[role="menuitem"]')
      ? [root]
      : fallbackNodes;
    const values = [];
    for (const node of nodes) {
      if (!visible(node)) continue;
      const lines = (node.innerText || node.textContent || '').split(/\n/).map(clean).filter(Boolean);
      for (const line of lines) {
        if (line.length > 80 || /^(intelligence|language|voice settings|voice customization|close|cancel|dismiss|search|pinned|recents?|new chat|chats?|projects?|show more|more)$/i.test(line)) continue;
        if (!values.some((value) => value.toLowerCase() === line.toLowerCase())) values.push(line);
      }
    }
    return values;
  };
  const discoverRow = async (label) => {
    const trigger = rowTrigger(label);
    const current = readSetting(label);
    if (!trigger) return current ? [current] : [];
    trigger.click();
    const deadline = performance.now() + 900;
    let values = [];
    while (performance.now() < deadline) {
      values = popupRoots(trigger).flatMap(menuValues);
      if (values.length) break;
      await wait(20);
    }
    await closePopup(trigger);
    if (current && !values.some((value) => value.toLowerCase() === current.toLowerCase())) values.unshift(current);
    return [...new Map(values.map((value) => [value.toLowerCase(), value])).values()];
  };
  const clickArrow = (direction) => {
    const surface = settingsSurface();
    if (!surface) return false;
    const controls = [...surface.querySelectorAll('button,[role="button"]')].filter(visible).filter((element) => !/close|cancel|dismiss|mute|unmute|intelligence|language|voice settings|customize|sample|play/i.test(metadata(element)));
    const wantsNext = direction === 'next';
    const arrow = controls.find((element) => {
      const label = metadata(element);
      const forward = /next voice|next|forward|right|chevron-right|arrow-right/i.test(label) && !/previous|back|left|chevron-left|arrow-left/i.test(label);
      const backward = /previous voice|previous|back|left|chevron-left|arrow-left/i.test(label) && !/next|forward|right|chevron-right|arrow-right/i.test(label);
      return wantsNext ? forward : backward;
    });
    if (arrow) { arrow.click(); return true; }
    const box = surface.getBoundingClientRect();
    const midpoint = box.left + box.width / 2;
    const targetY = box.top + box.height * .56;
    const candidates = controls.map((element) => { const rect = element.getBoundingClientRect(); return { element, rect, x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 }; })
      .filter((item) => item.rect.width >= 18 && item.rect.height >= 18 && item.y > box.top + box.height * .30 && item.y < box.top + box.height * .78 && (wantsNext ? item.x > midpoint : item.x < midpoint))
      .sort((a, b) => Math.abs(a.y - targetY) - Math.abs(b.y - targetY));
    if (!candidates[0]) return false;
    candidates[0].element.click();
    return true;
  };
  const waitForVoiceUi = async () => {
    const deadline = performance.now() + 2500;
    let current = readVoice();
    let radios = radioVoices();
    while (!current && !radios.length && performance.now() < deadline) {
      await wait(60);
      current = readVoice();
      radios = radioVoices();
    }
    return { current, radios };
  };
  const voices = [];
  const seenVoices = new Set();
  const addVoice = (voice) => {
    const key = voice?.value?.trim().toLowerCase();
    if (!key || seenVoices.has(key)) return;
    seenVoices.add(key);
    voices.push({ value: voice.value.trim(), label: voice.label?.trim() || voice.value.trim(), description: voice.description || null });
  };
  // The Voice dialog paints the carousel after its setting rows. Wait for the
  // voice UI itself before discovering those rows so fast prefetches do not
  // close the dialog before the voice options are available.
  const voiceUi = await waitForVoiceUi();
  const radioOptions = voiceUi.radios;
  const selectedRadio = radioOptions.find((option) => option.selected);
  const initial = voiceUi.current || (selectedRadio ? {
    value: selectedRadio.value,
    label: selectedRadio.value,
    description: null
  } : null);
  if (radioOptions.length) {
    for (const option of radioOptions) {
      addVoice({
        value: option.value,
        label: option.value,
        description: initial && initial.value.toLowerCase() === option.value.toLowerCase() ? initial.description : null
      });
    }
  } else {
    addVoice(initial);
    let current = initial;
    for (let step = 0; step < 32 && current; step += 1) {
      const before = current.value.toLowerCase();
      if (!clickArrow('next')) break;
      await wait(300);
      const next = readVoice();
      if (!next || next.value.toLowerCase() === before || seenVoices.has(next.value.toLowerCase())) break;
      addVoice(next);
      current = next;
    }
    if (initial && current && current.value.toLowerCase() !== initial.value.toLowerCase()) {
      for (let step = 0; step < 32; step += 1) {
        if (!clickArrow('previous')) break;
        await wait(220);
        const restored = readVoice();
        if (!restored || restored.value.toLowerCase() === initial.value.toLowerCase()) break;
      }
    }
  }
  const intelligence = await discoverRow('Intelligence');
  const languages = await discoverRow('Language');
  return {
    voices,
    intelligence,
    languages,
    currentVoice: initial?.value || null,
    currentVoiceDescription: initial?.description || null
  };
})()"#;

const VOICE_SETTINGS_VISIBLE_SCRIPT: &str = r#"(() => {
  const visible = (element) => {
    if (!element || element.disabled) return false;
    const style = getComputedStyle(element);
    const rect = element.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
  };
  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
  const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
  const hasSettingsText = (element) => {
    if (!element || element.closest('nav,aside')) return false;
    const text = clean(element.innerText);
    return text.length <= 5000 && /\blanguage\b/i.test(text) &&
      hasExactDescendant(element, 'Language') &&
      ( /\bintelligence\b/i.test(text) || /\bvoice\b/i.test(text) ||
        element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 ||
        element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 );
  };
  const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
  if (surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0]) return true;
  const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
  const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
  const anchor = intelligence || language;
  if (!anchor || !language) return false;
  for (let candidate = anchor, depth = 0; candidate && depth < 20; candidate = candidate.parentElement, depth += 1) {
    if (visible(candidate) && !candidate.closest('nav,aside') && clean(candidate.innerText).length <= 5000 &&
      /\blanguage\b/i.test(candidate.innerText || '') && hasExactDescendant(candidate, 'Language') &&
      (candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(candidate.innerText || ''))) return true;
  }
  return false;
})()"#;

struct BrowserSession {
    guild_id: String,
    url: String,
    profile_dir: PathBuf,
    executable: Option<PathBuf>,
    hide_when_ready: bool,
    manual_visibility_override: Option<bool>,
    visibility_hold: bool,
    child: Option<Child>,
    browser_process_id: Option<u32>,
    cdp: Option<CdpClient>,
    page_target_id: Option<String>,
    media: Option<Arc<BrowserMediaTransport>>,
    options_fetched_at: Option<Instant>,
    recent_threads_fetched_at: Option<Instant>,
    last_status_refresh_at: Option<Instant>,
    cdp_status_failures: u32,
    cdp_restarts: u64,
    voice_inactive_checks: u32,
    voice_media_seen: bool,
    voice_media_missing_checks: u32,
    status: BrowserStatus,
}

impl BrowserSession {
    fn new(guild_id: &str, config: &ConfigSnapshot, profile_dir: PathBuf) -> Self {
        Self {
            guild_id: guild_id.to_owned(),
            url: config.chatgpt_url.clone(),
            profile_dir,
            executable: if config.browser_executable.trim().is_empty() {
                None
            } else {
                Some(PathBuf::from(&config.browser_executable))
            },
            hide_when_ready: config.browser_hide_when_ready,
            manual_visibility_override: None,
            visibility_hold: false,
            child: None,
            browser_process_id: None,
            cdp: None,
            page_target_id: None,
            media: None,
            options_fetched_at: None,
            recent_threads_fetched_at: None,
            last_status_refresh_at: None,
            cdp_status_failures: 0,
            cdp_restarts: 0,
            voice_inactive_checks: 0,
            voice_media_seen: false,
            voice_media_missing_checks: 0,
            status: BrowserStatus::closed(guild_id),
        }
    }

    async fn start(&mut self, app: &AppHandle) -> Result<BrowserStatus, String> {
        let result = self.start_inner(app).await;
        if result.is_err() {
            self.stop();
        }
        result
    }

    async fn start_inner(&mut self, app: &AppHandle) -> Result<BrowserStatus, String> {
        fs::create_dir_all(&self.profile_dir)
            .map_err(|error| format!("Could not create browser profile: {error}"))?;
        let marker = self.profile_dir.join(AUTH_MARKER);
        let first_run = !marker.exists();
        if self.cdp.is_some() {
            let process_running = match self.child.as_mut() {
                Some(child) => child
                    .try_wait()
                    .map(|exit_status| exit_status.is_none())
                    .unwrap_or(false),
                None => self.browser_process_id.is_some(),
            };
            if process_running {
                emit_browser_log(
                    app,
                    &self.guild_id,
                    "Refreshing the existing browser session.",
                );
                match self.refresh_status().await {
                    Ok(()) => {
                        if self.status.logged_in && !marker.exists() {
                            let _ = fs::write(&marker, b"authenticated\n");
                            emit_browser_log(
                                app,
                                &self.guild_id,
                                "ChatGPT sign-in detected; the session is ready to use.",
                            );
                        }
                        return Ok(self.status.clone());
                    }
                    Err(error) => {
                        self.cdp_status_failures = self.cdp_status_failures.saturating_add(1);
                        self.status.cdp_status_failures = self.cdp_status_failures;
                        if self.cdp_status_failures < CDP_FAILURES_BEFORE_RESTART {
                            emit_browser_log(
                                app,
                                &self.guild_id,
                                &format!(
                                    "Browser status probe failed; keeping the dedicated browser alive (failure {}/{}): {error}",
                                    self.cdp_status_failures,
                                    CDP_FAILURES_BEFORE_RESTART
                                ),
                            );
                            return Ok(self.status.clone());
                        }
                        emit_browser_log(
                            app,
                            &self.guild_id,
                            &format!(
                                "The browser session was unreachable after {} consecutive status probe failures; restarting it ({error}).",
                                self.cdp_status_failures
                            ),
                        );
                        self.cdp_restarts = self.cdp_restarts.saturating_add(1);
                        self.stop();
                    }
                }
            } else {
                emit_browser_log(
                    app,
                    &self.guild_id,
                    "The browser window was closed; restarting the dedicated session.",
                );
                self.stop();
            }
        }
        emit_browser_log(app, &self.guild_id, "Looking for the configured browser.");
        let executable = find_browser_executable(self.executable.as_deref()).ok_or_else(|| {
            "Could not find Brave, Chrome, Edge, or a managed Chromium browser".to_owned()
        })?;
        if first_run {
            emit_browser_log(
                app,
                &self.guild_id,
                "First-run setup opened the dedicated browser. Sign in to your ChatGPT account at chatgpt.com; GPTVoice will detect the completed sign-in automatically.",
            );
        }

        emit_browser_log(
            app,
            &self.guild_id,
            "Launching the isolated browser with CDP enabled.",
        );
        let port = reserve_port().await?;
        let child = launch_browser(&executable, &self.profile_dir, &self.url, port, true)?;
        self.browser_process_id = Some(child.id());
        self.child = Some(child);
        let version_url = format!("http://127.0.0.1:{port}/json/version");
        let _version = wait_for_json::<Value>(
            &version_url,
            self.child
                .as_mut()
                .ok_or_else(|| "Browser process handle was lost".to_owned())?,
        )
        .await?;
        emit_browser_log(app, &self.guild_id, "Browser control endpoint is ready.");
        let targets_url = format!("http://127.0.0.1:{port}/json/list");
        let targets = wait_for_json::<Vec<CdpTarget>>(
            &targets_url,
            self.child
                .as_mut()
                .ok_or_else(|| "Browser process handle was lost".to_owned())?,
        )
        .await?;
        let target = targets
            .iter()
            .find(|target| target.target_type == "page" && target.websocket_url.is_some())
            .ok_or_else(|| "Browser CDP exposed no controllable page".to_owned())?;
        let websocket_url = target
            .websocket_url
            .as_deref()
            .ok_or_else(|| "Browser page did not expose a CDP WebSocket".to_owned())?;
        let cdp = CdpClient::connect(websocket_url).await?;
        emit_browser_log(app, &self.guild_id, "Connected to the browser page.");
        cdp.call("Runtime.enable", json!({})).await?;
        cdp.call("Page.enable", json!({})).await?;
        cdp.call("Page.setBypassCSP", json!({"enabled": true}))
            .await?;
        let media = BrowserMediaTransport::start().await?;
        media.set_latency_context(app, &self.guild_id);
        emit_browser_log(
            app,
            &self.guild_id,
            "Started the local browser audio transport.",
        );
        let bridge_script = media.browser_script();
        if bridge_script.is_empty() {
            return Err("The embedded browser media bridge could not be loaded".to_owned());
        }
        cdp.call(
            "Page.addScriptToEvaluateOnNewDocument",
            json!({"source": bridge_script}),
        )
        .await?;
        emit_browser_log(
            app,
            &self.guild_id,
            "Installed the continuous media bridge.",
        );
        self.page_target_id = Some(target.id.clone());
        self.cdp = Some(cdp);
        self.media = Some(media);
        emit_browser_log(
            app,
            &self.guild_id,
            "Opening ChatGPT in the dedicated profile.",
        );
        self.navigate(&self.url.clone()).await?;
        if let (Some(cdp), Some(media)) = (self.cdp.as_ref(), self.media.as_ref()) {
            let _ = cdp.evaluate(&media.browser_script()).await;
        }
        self.refresh_status().await?;
        emit_browser_log(
            app,
            &self.guild_id,
            if self.status.logged_in {
                "ChatGPT session is ready."
            } else {
                "ChatGPT sign-in is required. Sign in to your account in the dedicated GPTVoice browser window; GPTVoice will continue automatically."
            },
        );
        if self.status.logged_in {
            let _ = fs::write(marker, b"authenticated\n");
            self.set_window_hidden(self.hide_when_ready);
        }
        Ok(self.status.clone())
    }

    async fn navigate(&mut self, target_url: &str) -> Result<(), String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser CDP is not connected".to_owned())?;
        cdp.call("Page.navigate", json!({"url": target_url}))
            .await?;
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if let Ok(Value::String(state)) = cdp.evaluate("document.readyState").await {
                if state == "complete" || state == "interactive" {
                    sleep(PAGE_READY_SETTLE).await;
                    return Ok(());
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        Err("ChatGPT page did not finish loading in time".to_owned())
    }

    async fn evaluate_state(&self) -> Result<PageState, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser CDP is not connected".to_owned())?;
        let value = cdp
            .evaluate(
                r#"(() => {
                  const visible = (element) => {
                    if (!element) return false;
                    const style = getComputedStyle(element);
                    return style.display !== 'none' && style.visibility !== 'hidden' &&
                      element.getAttribute('aria-hidden') !== 'true';
                  };
                  const labelOf = (element) => `${element?.getAttribute('aria-label') || ''} ${element?.getAttribute('title') || ''} ${element?.textContent || ''}`.replace(/\s+/g, ' ').trim();
                  const controls = [...document.querySelectorAll('button,[role="button"]')];
                  const labels = controls.map(labelOf);
                  const login = [...document.querySelectorAll('a,button')].some((element) => visible(element) && /\blog\s*in\b|\bsign\s*in\b/i.test(labelOf(element)));
                  const composer = [...document.querySelectorAll('textarea,[contenteditable="true"]')].some(visible);
                  const microphonePermissionRequired = [...document.querySelectorAll('p,div,span,button')].some((element) => {
                    if (!visible(element)) return false;
                    const text = (element.textContent || '').replace(/\s+/g, ' ').trim();
                    return text.length <= 180 && /enable microphone access in settings/i.test(text);
                  });
                  const voiceControlVisible = controls.some((element, index) => visible(element) && /voice|microphone|mic/i.test(labels[index]) && !/settings|mute|unmute/i.test(labels[index]));
                  const activeVoiceControl = controls.some((element, index) => visible(element) && /\b(?:end|stop|leave|exit|close|quit)\b.{0,32}\b(?:voice|call|conversation)\b|\b(?:voice|call|conversation)\b.{0,32}\b(?:end|stop|leave|exit|close|quit)\b/i.test(labels[index]));
                  const activeMicrophoneControl = controls.some((element, index) => visible(element) && /\b(?:mute|unmute|turn off|turn on)(?: the)?\s+(?:microphone|mic)\b/i.test(labels[index]));
                  const startVoiceControl = controls.some((element, index) => visible(element) && ( /\b(?:start|open|begin|talk to|use)\b.{0,24}\bvoice\b/i.test(labels[index]) || /^(?:voice|voice mode|talk to chatgpt)$/i.test(labels[index]) ) && !/settings|customize/i.test(labels[index]));
                  const voiceModeActive = activeVoiceControl || (activeMicrophoneControl && !startVoiceControl);
                  const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
                  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
                  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
                  const hasSettingsText = (element) => {
                    if (!element || element.closest('nav,aside')) return false;
                    const text = clean(element.innerText);
                    return text.length <= 5000 && /\blanguage\b/i.test(text) &&
                      [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, 'Language')) &&
                      ( /\bintelligence\b/i.test(text) || /\bvoice\b/i.test(text) ||
                        element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 ||
                        element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 );
                  };
                  const findSettingsSurface = () => {
                    const markedSurface = surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0];
                    if (markedSurface) return markedSurface;
                    const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
                    const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
                    const anchor = intelligence || language;
                    if (anchor && language) {
                      let surface = anchor;
                      for (let depth = 0; depth < 20 && surface; depth += 1, surface = surface.parentElement) {
                        if (!visible(surface)) continue;
                        const text = surface.innerText || '';
                        if (!surface.closest('nav,aside') && clean(text).length <= 5000 && /\blanguage\b/i.test(text) &&
                          [...surface.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, 'Language')) &&
                          (surface.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(text))) return surface;
                      }
                      return null;
                    }
                    return null;
                  };
                  const settingsSurface = findSettingsSurface();
                  const genericSettingText = /^(voice settings|voice customization|intelligence|language|close|cancel|dismiss|instant|medium|high|auto-detect)$/i;
                  const voiceInfo = () => {
                    if (!settingsSurface) return null;
                    const box = settingsSurface.getBoundingClientRect();
                    const selectedRadio = [...settingsSurface.querySelectorAll('[role="radio"],input[type="radio"]')].find((element) => visible(element) && (element.getAttribute('aria-checked') === 'true' || element.getAttribute('data-state') === 'checked' || element.checked));
                    const selectedRadioText = clean(selectedRadio?.getAttribute('aria-label') || selectedRadio?.textContent || '');
                    const candidates = [...settingsSurface.querySelectorAll('h1,h2,h3,h4,h5,[role="heading"],p,span,div')]
                      .filter((element) => visible(element) && (element.matches('h1,h2,h3,h4,h5,[role="heading"]') || element.children.length === 0))
                      .map((element) => {
                        const text = clean(element.textContent);
                        const rect = element.getBoundingClientRect();
                        const centerX = rect.left + rect.width / 2;
                        const centerY = rect.top + rect.height / 2;
                        return { element, text, rect, centerX, centerY, heading: element.matches('h1,h2,h3,h4,h5,[role="heading"]') };
                      })
                      .filter((item) => item.text && item.text.length <= 60 && !genericSettingText.test(item.text) && !item.element.closest('button,[role="button"]'))
                      .filter((item) => item.centerY > box.top + box.height * .18 && item.centerY < box.top + box.height * .66)
                      .sort((a, b) => {
                        const score = (item) => (item.heading ? 0 : 1) + Math.abs(item.centerX - (box.left + box.width / 2)) / Math.max(box.width, 1) + Math.abs(item.centerY - (box.top + box.height * .43)) / Math.max(box.height, 1);
                        return score(a) - score(b);
                      });
                    const selected = candidates.find((candidate) => selectedRadioText && candidate.text.toLowerCase() === selectedRadioText.toLowerCase()) || candidates[0];
                    if (!selected && !selectedRadioText) return null;
                    const description = selected ? [...settingsSurface.querySelectorAll('p,span,div')]
                      .filter((element) => visible(element) && element.children.length === 0 && !element.closest('button,[role="button"]'))
                      .map((element) => ({ text: clean(element.textContent), rect: element.getBoundingClientRect() }))
                      .filter((item) => item.text && item.text !== selected.text && item.text.length <= 100 && !genericSettingText.test(item.text) && item.rect.top >= selected.rect.bottom && item.rect.top < selected.rect.bottom + 70)
                      .sort((a, b) => a.rect.top - b.rect.top)[0]?.text || null : null;
                    return { value: selectedRadioText || selected.text, description };
                  };
                  const settingValue = (label) => {
                    if (!settingsSurface) return null;
                    const labelNode = [...settingsSurface.querySelectorAll('*')].find((element) => visible(element) && hasExactText(element, label));
                    if (!labelNode) return null;
                    const withoutLabel = (value) => clean(value).replace(new RegExp(`^${label}\\s*`, 'i'), '').trim();
                    for (let ancestor = labelNode, depth = 0; ancestor && depth < 7; ancestor = ancestor.parentElement, depth += 1) {
                      if (clean(ancestor.innerText).length > 500) continue;
                      const controls = [...ancestor.querySelectorAll('button,[role="button"],[role="combobox"],select')]
                        .filter(visible)
                        .map((element) => withoutLabel(element.innerText || element.textContent))
                        .filter((text) => text && text.toLowerCase() !== label.toLowerCase() && text.length <= 80);
                      if (controls.length) return controls[controls.length - 1];
                      const leaves = [...ancestor.querySelectorAll('*')]
                        .filter((element) => visible(element) && element.children.length === 0)
                        .map((element) => withoutLabel(element.textContent))
                        .filter((text) => text && text.toLowerCase() !== label.toLowerCase() && text.length <= 80);
                      if (leaves.length) return leaves[leaves.length - 1];
                    }
                    return null;
                  };
                  const voice = voiceInfo();
                  const intelligence = settingValue('Intelligence');
                  const language = settingValue('Language');
                  const muted = controls.some((element, index) => visible(element) && /turn on microphone|turn on mic|unmute microphone|^unmute$/i.test(labels[index]));
                  const live = controls.some((element, index) => visible(element) && /turn off microphone|turn off mic|mute microphone|^mute$/i.test(labels[index]));
                  const limitSurface = surfaces.find((element) => /daily.*limit.*reached|reached.*daily.*limit|limit with voice/i.test(element.innerText || ''));
                  const limitText = limitSurface?.innerText || '';
                  const reset = limitText.match(/reset(?:s)?\s+(?:at|on)\s+([^.!?\n]+)/i)?.[1]?.trim() || null;
                  return { url: location.href, title: document.title, logged_in: !login && (composer || voiceControlVisible), voice_control_visible: voiceControlVisible, voice_mode_active: voiceModeActive, voice_settings_open: Boolean(settingsSurface), voice: voice?.value || null, voice_description: voice?.description || null, intelligence, language, chatgpt_mic_muted: muted ? true : live ? false : null, microphone_permission_required: microphonePermissionRequired, voice_limit_reached: Boolean(limitSurface), voice_limit_reset: reset };
                })()"#,
            )
            .await?;
        serde_json::from_value(value)
            .map_err(|error| format!("Could not read ChatGPT page state: {error}"))
    }

    async fn evaluate_media_state(&self) -> Result<Option<PageMediaState>, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser CDP is not connected".to_owned())?;
        let value = cdp
            .evaluate(
                r#"(() => {
                  const bridge = window.__gptVoiceMediaBridge;
                  if (!bridge?.getDiagnostics) return null;
                  const diagnostics = bridge.getDiagnostics() || {};
                  return {
                    installed: true,
                    outputTrackCount: Number(diagnostics.outputTrackCount) || 0
                  };
                })()"#,
            )
            .await?;
        if value.is_null() {
            return Ok(None);
        }
        serde_json::from_value(value)
            .map(Some)
            .map_err(|error| format!("Could not read ChatGPT media bridge state: {error}"))
    }

    async fn refresh_status(&mut self) -> Result<(), String> {
        let state = self.evaluate_state().await?;
        let (media_connected, transport_output_track_count) = self
            .media
            .as_ref()
            .map(|media| {
                let diagnostics = media.diagnostics();
                (diagnostics.connected, diagnostics.output_track_count)
            })
            .unwrap_or((false, 0));
        let page_media_state = self
            .evaluate_media_state()
            .await
            .ok()
            .flatten()
            .filter(|media| media.installed);
        let media_output_track_count = page_media_state
            .as_ref()
            .map(|media| media.output_track_count)
            .unwrap_or(transport_output_track_count);
        let media_signal_available = page_media_state.is_some() || media_connected;
        let media_voice_active = media_signal_available && media_output_track_count > 0;
        if media_voice_active {
            self.voice_media_seen = true;
            self.voice_media_missing_checks = 0;
        } else if self.voice_media_seen && !state.voice_settings_open {
            self.voice_media_missing_checks = self.voice_media_missing_checks.saturating_add(1);
        } else {
            self.voice_media_missing_checks = 0;
        }
        let media_end_confirmed = self.voice_media_seen
            && !media_voice_active
            && !state.voice_settings_open
            && self.voice_media_missing_checks >= VOICE_MEDIA_END_CONFIRMATION_CHECKS;
        let observed_voice_active = state.voice_mode_active || media_voice_active;
        self.last_status_refresh_at = Some(Instant::now());
        self.cdp_status_failures = 0;
        self.status.open = true;
        self.status.url = Some(state.url);
        self.status.title = Some(state.title);
        self.status.logged_in = state.logged_in;
        self.status.auth_required = !state.logged_in;
        self.status.voice_control_visible = state.voice_control_visible;
        if media_end_confirmed {
            self.voice_inactive_checks = 0;
            self.voice_media_seen = false;
            self.voice_media_missing_checks = 0;
            self.status.voice_mode_active = false;
        } else if observed_voice_active {
            self.voice_inactive_checks = 0;
            self.status.voice_mode_active = true;
        } else if self.status.voice_mode_active && state.voice_settings_open {
            // Opening Voice settings can temporarily hide the live controls. Keep the
            // previous active state until the settings surface is closed and probed again.
            self.voice_inactive_checks = 0;
        } else if self.status.voice_mode_active {
            self.voice_inactive_checks = self.voice_inactive_checks.saturating_add(1);
            if self.voice_inactive_checks >= VOICE_END_CONFIRMATION_CHECKS {
                self.voice_inactive_checks = 0;
                self.status.voice_mode_active = false;
            }
        } else {
            self.voice_inactive_checks = 0;
            self.status.voice_mode_active = false;
        }
        self.status.voice_settings_open = state.voice_settings_open;
        if let Some(voice) = state.voice {
            self.status.voice_description = state
                .voice_description
                .or_else(|| {
                    self.status
                        .available_voices
                        .iter()
                        .find(|candidate| candidate.value.eq_ignore_ascii_case(&voice))
                        .and_then(|candidate| candidate.description.clone())
                })
                .or_else(|| {
                    let description = voice_description(&voice);
                    (!description.is_empty()).then(|| description.to_owned())
                });
            self.status.voice = Some(voice);
        }
        if state.intelligence.is_some() {
            self.status.intelligence = state.intelligence;
        } else if self.options_fetched_at.is_some() && self.status.available_intelligence.is_empty()
        {
            self.status.intelligence = None;
        }
        if state.language.is_some() {
            self.status.language = state.language;
        }
        self.status.chatgpt_mic_muted = state.chatgpt_mic_muted;
        self.status.microphone_permission_required = state.microphone_permission_required;
        self.status.voice_limit_reached = state.voice_limit_reached;
        self.status.voice_limit_reset = state.voice_limit_reset;
        self.status.cdp_status_failures = self.cdp_status_failures;
        self.status.cdp_restarts = self.cdp_restarts;
        self.status.browser_process_id = self.browser_process_id;
        if let Some(media) = &self.media {
            let diagnostics = media.diagnostics();
            self.status.media_connected = diagnostics.connected;
            self.status.media_packets_sent = diagnostics.packets_sent;
            self.status.media_packets_dropped = diagnostics.packets_dropped;
            self.status.media_input_queue_depth = diagnostics.input_queue_depth;
            self.status.media_input_queue_peak_depth = diagnostics.input_queue_peak_depth;
            self.status.media_input_queue_dropped = diagnostics.input_queue_dropped;
            self.status.media_packets_received = diagnostics.packets_received;
            self.status.media_reader_reads = diagnostics.reader_reads;
            self.status.media_reader_source_bytes = diagnostics.reader_source_bytes;
            self.status.media_reader_output_bytes = diagnostics.reader_output_bytes;
            self.status.media_reader_silence_bytes = diagnostics.reader_silence_bytes;
            self.status.media_output_capture_mode = diagnostics.output_capture_mode;
            self.status.media_output_track_count = media_output_track_count;
            self.status.media_output_callbacks = diagnostics.output_callbacks;
            self.status.media_output_dropped_callbacks = diagnostics.output_dropped_callbacks;
            self.status.media_output_attach_errors = diagnostics.output_attach_errors;
            self.status.media_output_worklet_captures = diagnostics.output_worklet_captures;
            self.status.media_output_worklet_fallbacks = diagnostics.output_worklet_fallbacks;
            self.status.media_output_worklet_frames = diagnostics.output_worklet_frames;
            self.status.media_output_worklet_peak = diagnostics.output_worklet_peak;
            self.status.media_output_worklet_non_silent_frames =
                diagnostics.output_worklet_non_silent_frames;
            self.status.media_output_worklet_max_gap_ms = diagnostics.output_worklet_max_gap_ms;
            self.status.media_input_frames = diagnostics.input_frames;
            self.status.media_input_silence_frames = diagnostics.input_silence_frames;
            self.status.media_browser_input_queue_samples = diagnostics.browser_input_queue_samples;
            self.status.media_browser_input_queue_peak_samples =
                diagnostics.browser_input_queue_peak_samples;
            self.status.media_browser_input_queue_depth = diagnostics.browser_input_queue_depth;
            self.status.media_browser_input_dropped_messages =
                diagnostics.browser_input_dropped_messages;
            self.status.media_browser_input_last_frame_at = diagnostics.browser_input_last_frame_at;
        }
        if self.status.logged_in
            && self.manual_visibility_override.is_none()
            && self.hide_when_ready
            && !self.visibility_hold
            && !self.status.window_hidden
        {
            self.set_window_hidden(true);
        }
        Ok(())
    }

    async fn status_probe(&mut self, app: &AppHandle) -> Result<(), String> {
        if self.cdp.is_none() {
            return Ok(());
        }

        let process_running = match self.child.as_mut() {
            Some(child) => child
                .try_wait()
                .map(|exit_status| exit_status.is_none())
                .unwrap_or(false),
            None => self.browser_process_id.is_some(),
        };
        if !process_running {
            emit_browser_log(
                app,
                &self.guild_id,
                "The browser window was closed; restarting the dedicated session.",
            );
            self.stop();
            return self.start(app).await.map(|_| ());
        }

        if self.status.window_hidden {
            if let Some(cdp) = self.cdp.as_ref() {
                // Keep Chromium's renderer active while its Windows window is hidden. Without
                // this nudge, ChatGPT can defer Voice UI and media state changes until the
                // browser is shown again.
                let _ = cdp.call("Page.bringToFront", json!({})).await;
                let _ = cdp
                    .call(
                        "Emulation.setFocusEmulationEnabled",
                        json!({"enabled": true}),
                    )
                    .await;
                let _ = cdp
                    .call("Page.setWebLifecycleState", json!({"state": "active"}))
                    .await;
            }
        }

        if self
            .last_status_refresh_at
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < STATUS_PROBE_MIN_INTERVAL)
        {
            return Ok(());
        }

        let was_voice_active = self.status.voice_mode_active;
        let was_output_track_count = self.status.media_output_track_count;
        match self.refresh_status().await {
            Ok(()) => {
                if was_output_track_count > 0 && self.status.media_output_track_count == 0 {
                    emit_browser_log(
                        app,
                        &self.guild_id,
                        "ChatGPT Voice output track ended; checking the hidden Voice session state.",
                    );
                }
                if was_voice_active
                    && !self.status.voice_mode_active
                    && !self.status.voice_limit_reached
                {
                    emit_browser_log(
                        app,
                        &self.guild_id,
                        "ChatGPT Voice ended; the active thread is available for recovery.",
                    );
                }
                Ok(())
            }
            Err(error) => {
                self.cdp_status_failures = self.cdp_status_failures.saturating_add(1);
                self.status.cdp_status_failures = self.cdp_status_failures;
                if self.cdp_status_failures < CDP_FAILURES_BEFORE_RESTART {
                    emit_browser_log(
                        app,
                        &self.guild_id,
                        &format!(
                            "Browser status probe failed; keeping the dedicated browser and Voice session alive (failure {}/{}): {error}",
                            self.cdp_status_failures,
                            CDP_FAILURES_BEFORE_RESTART
                        ),
                    );
                    return Ok(());
                }

                emit_browser_log(
                    app,
                    &self.guild_id,
                    &format!(
                        "The browser session was unreachable after {} consecutive status probe failures; restarting it ({error}).",
                        self.cdp_status_failures
                    ),
                );
                self.cdp_restarts = self.cdp_restarts.saturating_add(1);
                self.stop();
                self.start(app).await.map(|_| ())
            }
        }
    }

    fn set_window_hidden(&mut self, hidden: bool) {
        let Some(process_id) = self.child.as_ref().map(Child::id) else {
            return;
        };
        if set_process_windows_visible(process_id, !hidden) > 0 {
            self.status.window_hidden = hidden;
        }
    }

    async fn set_visibility(&mut self, hidden: bool) -> BrowserStatus {
        self.manual_visibility_override = Some(hidden);
        self.set_window_hidden(hidden);
        if !hidden {
            if let Some(cdp) = self.cdp.as_ref() {
                let _ = cdp.call("Page.bringToFront", json!({})).await;
                let _ = cdp
                    .call("Page.setWebLifecycleState", json!({"state": "active"}))
                    .await;
            }
        }
        self.status.clone()
    }

    async fn new_thread(&mut self) -> Result<BrowserStatus, String> {
        let home_url = self.url.clone();
        self.navigate(&home_url).await?;
        sleep(Duration::from_millis(500)).await;
        let cdp = self
            .cdp
            .clone()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        let mut clicked = false;
        while tokio::time::Instant::now() < deadline {
            let result = cdp
                .evaluate(
                    r#"(() => {
                      const visible = (element) => {
                        if (!element || element.disabled) return false;
                        const style = getComputedStyle(element);
                        const rect = element.getBoundingClientRect();
                        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                      };
                      const labelOf = (element) => `${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''} ${element.textContent || ''}`.replace(/\s+/g, ' ').trim();
                      const controls = [...document.querySelectorAll('button,[role="button"],a')];
                      const target = controls.find((element) => visible(element) && (element.matches('[data-testid="create-new-chat-button"]') || (element.matches('a[href="/"]') && /new chat|new conversation|new thread/i.test(labelOf(element))) || /new chat|new conversation|new thread/i.test(labelOf(element))) && !/voice|settings/i.test(labelOf(element)));
                      if (target) {
                        target.click();
                        return true;
                      }
                      const composer = [...document.querySelectorAll('textarea,[contenteditable="true"]')].some(visible);
                      if (location.pathname === '/' && composer) return 'ready';
                      const sidebar = controls.find((element) => visible(element) && /open sidebar|show sidebar|toggle sidebar|navigation|hamburger/i.test(labelOf(element)) && !/voice|settings|mute|microphone/i.test(labelOf(element)));
                      if (sidebar) {
                        sidebar.click();
                        return 'sidebar';
                      }
                      return false;
                    })()"#,
                )
                .await?;
            if result == Value::Bool(true) || result == Value::String("ready".to_owned()) {
                clicked = true;
                break;
            }
            if result == Value::String("sidebar".to_owned()) {
                sleep(Duration::from_millis(350)).await;
            } else {
                sleep(Duration::from_millis(200)).await;
            }
        }
        if !clicked {
            return Err("ChatGPT's New chat control is not ready after opening the home page. Show the browser once and retry.".to_owned());
        }
        self.status.active_thread = None;
        self.recent_threads_fetched_at = None;
        sleep(Duration::from_millis(800)).await;
        self.start_voice().await
    }

    async fn recent_threads(&mut self) -> Result<Vec<ThreadSummary>, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let read_threads = r#"(() => { const seen = new Set(); return [...document.querySelectorAll('a[href*="/c/"]')].map((element) => { const match = (element.href || '').match(/\/c\/([^/?#]+)/i); if (!match) return null; const id = decodeURIComponent(match[1]); if (seen.has(id)) return null; seen.add(id); return { id, title: (element.textContent || element.getAttribute('aria-label') || '').replace(/\s+/g, ' ').trim() || 'Untitled conversation', url: element.href }; }).filter(Boolean).slice(0, 5); })()"#;
        let mut value = cdp.evaluate(read_threads).await?;
        if !value.as_array().is_some_and(|threads| !threads.is_empty()) {
            let _ = cdp
                .evaluate(
                    r#"(() => { const visible = (element) => { if (!element || element.disabled) return false; const style = getComputedStyle(element); const rect = element.getBoundingClientRect(); return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0; }; const controls = [...document.querySelectorAll('button,[role="button"]')]; const target = controls.find((element) => { if (!visible(element)) return false; const label = `${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''} ${element.textContent || ''}`; return /open|show|toggle|hamburger|navigation|sidebar|menu/i.test(label) && !/voice|settings|mute/i.test(label); }); if (target) { target.click(); return true; } return false; })()"#,
                )
                .await?;
            sleep(SIDEBAR_SETTLE).await;
            let deadline = tokio::time::Instant::now() + THREAD_DISCOVERY_TIMEOUT;
            while tokio::time::Instant::now() < deadline {
                value = cdp.evaluate(read_threads).await?;
                if value.as_array().is_some_and(|threads| !threads.is_empty()) {
                    break;
                }
                sleep(THREAD_DISCOVERY_INTERVAL).await;
            }
        }
        serde_json::from_value(value)
            .map_err(|error| format!("Could not read recent ChatGPT threads: {error}"))
    }

    async fn resume_thread(&mut self, thread_id: &str) -> Result<BrowserStatus, String> {
        let threads = self.recent_threads().await?;
        let thread = threads
            .iter()
            .find(|thread| thread.id == thread_id)
            .cloned()
            .ok_or_else(|| "That ChatGPT thread is not visible in the recent list".to_owned())?;
        self.navigate(&thread.url).await?;
        self.status.active_thread = Some(thread);
        self.start_voice().await
    }

    async fn start_voice(&mut self) -> Result<BrowserStatus, String> {
        // Chromium can keep a background page alive while refusing the user-gesture/media
        // work that ChatGPT Voice needs. Give the dedicated window a short visible activation
        // opportunity, then restore the user's automatic-hidden preference.
        let temporarily_show = self.status.window_hidden;
        if temporarily_show {
            self.visibility_hold = true;
            log::info!(
                "[browser:{}] Temporarily showing the dedicated browser for Voice activation.",
                self.guild_id
            );
            self.set_window_hidden(false);
            if let Some(cdp) = self.cdp.as_ref() {
                let _ = cdp.call("Page.bringToFront", json!({})).await;
                let _ = cdp
                    .call("Page.setWebLifecycleState", json!({"state": "active"}))
                    .await;
                let visibility_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                while tokio::time::Instant::now() < visibility_deadline {
                    if cdp
                        .evaluate("document.visibilityState === 'visible' && !document.hidden")
                        .await
                        == Ok(Value::Bool(true))
                    {
                        break;
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
            sleep(Duration::from_millis(700)).await;
        }

        let result = self.start_voice_inner().await;
        if temporarily_show {
            self.visibility_hold = false;
            self.set_window_hidden(true);
            if let Some(cdp) = self.cdp.as_ref() {
                let _ = cdp
                    .call("Page.setWebLifecycleState", json!({"state": "active"}))
                    .await;
            }
            let _ = self.recover_media_output().await;
            log::info!(
                "[browser:{}] Voice activation finished; dedicated browser hidden again.",
                self.guild_id
            );
            return result.map(|_| self.status.clone());
        }
        result
    }

    async fn start_voice_inner(&mut self) -> Result<BrowserStatus, String> {
        self.refresh_status().await?;
        if self.status.microphone_permission_required {
            return Err(self.microphone_permission_error());
        }
        let cdp = self
            .cdp
            .clone()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut clicked = false;
        while tokio::time::Instant::now() < deadline {
            if self.status.microphone_permission_required {
                return Err(self.microphone_permission_error());
            }
            let result = cdp
                .evaluate(
                    r#"(() => {
                      const visible = (element) => {
                        if (!element || element.disabled) return false;
                        const style = getComputedStyle(element);
                        const rect = element.getBoundingClientRect();
                        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                      };
                      const controls = [...document.querySelectorAll('button,[role="button"]')];
                      const target = controls.find((element) => {
                        if (!visible(element)) return false;
                        const label = `${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''} ${element.textContent || ''}`.replace(/\s+/g, ' ').trim();
                        return /voice|microphone/i.test(label) && !/end|stop|leave|exit|mute|unmute|settings|close|cancel|disable/i.test(label);
                      });
                      if (!target) return false;
                      const rect = target.getBoundingClientRect();
                      return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
                    })()"#,
                )
                .await?;
            let Some(point) = result.as_object() else {
                sleep(Duration::from_millis(250)).await;
                continue;
            };
            let Some(x) = point.get("x").and_then(Value::as_f64) else {
                sleep(Duration::from_millis(250)).await;
                continue;
            };
            let Some(y) = point.get("y").and_then(Value::as_f64) else {
                sleep(Duration::from_millis(250)).await;
                continue;
            };
            cdp.call(
                "Input.dispatchMouseEvent",
                json!({"type": "mouseMoved", "x": x, "y": y, "button": "none", "pointerType": "mouse"}),
            )
            .await?;
            cdp.call(
                "Input.dispatchMouseEvent",
                json!({"type": "mousePressed", "x": x, "y": y, "button": "left", "clickCount": 1, "pointerType": "mouse"}),
            )
            .await?;
            cdp.call(
                "Input.dispatchMouseEvent",
                json!({"type": "mouseReleased", "x": x, "y": y, "button": "left", "clickCount": 1, "pointerType": "mouse"}),
            )
            .await?;
            clicked = true;
            break;
        }
        if !clicked {
            return Err("ChatGPT's Voice control is not visible. Show the browser and open a logged-in conversation.".to_owned());
        }
        let active_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while tokio::time::Instant::now() < active_deadline {
            self.refresh_status().await?;
            if self.status.microphone_permission_required {
                return Err(self.microphone_permission_error());
            }
            if self.status.voice_mode_active {
                sleep(Duration::from_millis(500)).await;
                if let Err(error) = self.prefetch_voice_settings().await {
                    log::warn!("Could not prefetch ChatGPT Voice settings: {error}");
                }
                self.ensure_chatgpt_mic_live().await;
                if let Err(error) = self.recover_media_output().await {
                    log::warn!(
                        "[browser:{}] Could not refresh ChatGPT output capture after Voice activation: {error}",
                        self.guild_id
                    );
                }
                return Ok(self.status.clone());
            }
            sleep(Duration::from_millis(250)).await;
        }
        self.refresh_status().await?;
        if self.status.microphone_permission_required {
            return Err(self.microphone_permission_error());
        }
        if !self.status.voice_mode_active {
            return Err("ChatGPT Voice was clicked, but the voice session did not become active. Use Show browser once to allow the browser microphone, then retry.".to_owned());
        }
        Ok(self.status.clone())
    }

    fn microphone_permission_error(&self) -> String {
        "ChatGPT needs microphone access. Click Show browser, enable microphone access in Settings, then press Reconnect Voice.".to_owned()
    }

    async fn recover_media_output(&self) -> Result<Value, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        cdp.evaluate(
            r#"(() => {
              const bridge = window.__gptVoiceMediaBridge;
              if (!bridge) return { installed: false, reason: 'media bridge is not installed' };
              bridge.reconnect?.();
              bridge.recoverOutput?.();
              return bridge.getDiagnostics?.() || { installed: true };
            })()"#,
        )
        .await
    }

    async fn prefetch_voice_settings(&mut self) -> Result<BrowserStatus, String> {
        let started_at = Instant::now();
        let refresh_available_options = self
            .options_fetched_at
            .map(|fetched_at| fetched_at.elapsed() >= OPTIONS_CACHE_TTL)
            .unwrap_or(true);
        // Use one open -> read -> close transaction so a thread start does not
        // visibly reopen Voice settings or toggle the microphone twice.
        self.open_voice_settings().await?;

        if refresh_available_options {
            match self.discover_available_options().await {
                Ok(options) => {
                    let discovered = !options.voices.is_empty()
                        || !options.intelligence.is_empty()
                        || !options.languages.is_empty();
                    let current_voice = options.current_voice.clone();
                    let current_voice_description = options.current_voice_description.clone();
                    self.status.available_voices = options.voices;
                    self.status.available_intelligence = options.intelligence;
                    self.status.available_languages = options.languages;
                    if self.status.available_intelligence.is_empty() {
                        self.status.intelligence = None;
                    }
                    if current_voice.is_some() {
                        self.status.voice = current_voice;
                        self.status.voice_description = current_voice_description;
                    }
                    if discovered {
                        self.options_fetched_at = Some(Instant::now());
                        log::info!(
                            "[browser:{}] Discovered {} voice(s), {} intelligence option(s), and {} language option(s) in {} ms.",
                            self.guild_id,
                            self.status.available_voices.len(),
                            self.status.available_intelligence.len(),
                            self.status.available_languages.len(),
                            started_at.elapsed().as_millis()
                        );
                    }
                }
                Err(error) => {
                    log::warn!(
                        "[browser:{}] Could not discover live Voice options: {error}",
                        self.guild_id
                    );
                }
            }
        }

        self.close_voice_settings().await?;
        if self.status.voice.is_none() {
            // Closing the dialog can race the page's Voice UI state update. A
            // refresh is enough to settle that state; reopening the dialog is
            // intentionally avoided because it causes the duplicate-dialog
            // behavior this prefetch is meant to prevent.
            sleep(Duration::from_millis(100)).await;
            self.refresh_status().await?;
        }
        log::info!(
            "[browser:{}] Voice settings prefetched in one dialog cycle ({} ms).",
            self.guild_id,
            started_at.elapsed().as_millis()
        );
        Ok(self.status.clone())
    }

    async fn discover_available_options(&self) -> Result<VoiceSettingOptions, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let value = cdp.evaluate(DISCOVER_OPTIONS_SCRIPT).await?;
        serde_json::from_value(value)
            .map_err(|error| format!("Could not read live ChatGPT Voice options: {error}"))
    }

    async fn reload_page(&mut self) -> Result<BrowserStatus, String> {
        let cdp = self
            .cdp
            .clone()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        cdp.call("Page.reload", json!({"ignoreCache": true}))
            .await?;
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if let Ok(Value::String(state)) = cdp.evaluate("document.readyState").await {
                if state == "complete" || state == "interactive" {
                    sleep(Duration::from_millis(500)).await;
                    if let (Some(cdp), Some(media)) = (self.cdp.as_ref(), self.media.as_ref()) {
                        let _ = cdp.evaluate(&media.browser_script()).await;
                    }
                    self.options_fetched_at = None;
                    self.refresh_status().await?;
                    return Ok(self.status.clone());
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        Err("ChatGPT page did not finish refreshing in time".to_owned())
    }

    async fn reconnect_voice(&mut self) -> Result<BrowserStatus, String> {
        self.reload_page().await?;
        self.start_voice().await
    }

    async fn open_voice_settings(&mut self) -> Result<BrowserStatus, String> {
        let cdp = self
            .cdp
            .clone()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let clicked = cdp
            .evaluate(
                r#"(() => {
                  const visible = (element) => { if (!element) return false; const style = getComputedStyle(element); const rect = element.getBoundingClientRect(); return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0; };
                  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
                  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
                  const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
                  const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
                  const hasSettingsText = (element) => !element.closest('nav,aside') && clean(element.innerText || '').length <= 5000 && /\blanguage\b/i.test(element.innerText || '') && hasExactDescendant(element, 'Language') && (/\bintelligence\b/i.test(element.innerText || '') || /\bvoice\b/i.test(element.innerText || '') || element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 || element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2);
                  if (surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0]) return true;
                    const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
                    const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
                    const anchor = intelligence || language;
                    if (anchor && language) {
                      let surface = anchor;
                    for (let depth = 0; depth < 20 && surface; depth += 1, surface = surface.parentElement) {
                      if (!visible(surface)) continue;
                      const text = surface.innerText || '';
                      if (!surface.closest('nav,aside') && clean(text).length <= 5000 && /\blanguage\b/i.test(text) && hasExactDescendant(surface, 'Language') && (surface.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(text))) return true;
                    }
                  }
                  const controls = [...document.querySelectorAll('button,[role="button"],a')];
                  const target = controls.find((element) => visible(element) && /voice settings|voice customization|customize voice|settings.*voice/i.test(`${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''} ${element.textContent || ''}`));
                  if (!target) return false;
                  target.click();
                  return true;
                })()"#,
            )
            .await?;
        if clicked != Value::Bool(true) {
            return Err(
                "ChatGPT's Voice settings control is not visible. Start Voice and try again."
                    .to_owned(),
            );
        }
        let deadline = tokio::time::Instant::now() + VOICE_SETTINGS_WAIT_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if cdp.evaluate(VOICE_SETTINGS_VISIBLE_SCRIPT).await? == Value::Bool(true) {
                self.refresh_status().await?;
                if self.status.voice_settings_open {
                    return Ok(self.status.clone());
                }
            }
            sleep(VOICE_SETTINGS_POLL_INTERVAL).await;
        }
        Err(
            "ChatGPT Voice settings did not become visible. Show the browser once and retry."
                .to_owned(),
        )
    }

    async fn close_voice_settings(&mut self) -> Result<BrowserStatus, String> {
        let cdp = self
            .cdp
            .clone()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let clicked = cdp
            .evaluate(
                r#"(() => {
                  const visible = (element) => { if (!element) return false; const style = getComputedStyle(element); const rect = element.getBoundingClientRect(); return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0; };
                  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
                  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
                  const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
                  const hasSettingsText = (element) => !element.closest('nav,aside') && clean(element.innerText || '').length <= 5000 && /\blanguage\b/i.test(element.innerText || '') && hasExactDescendant(element, 'Language') && (/\bintelligence\b/i.test(element.innerText || '') || /\bvoice\b/i.test(element.innerText || '') || element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 || element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2);
                  const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible).filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length);
                  let surface = surfaces[0];
                  if (!surface) {
                    const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
                    const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
                    const anchor = intelligence || language;
                    if (anchor && language) {
                      for (let depth = 0, candidate = anchor; depth < 20 && candidate; depth += 1, candidate = candidate.parentElement) {
                        if (!visible(candidate)) continue;
                        const text = candidate.innerText || '';
                        if (!candidate.closest('nav,aside') && clean(text).length <= 5000 && /\blanguage\b/i.test(text) && hasExactDescendant(candidate, 'Language') && (candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(text))) { surface = candidate; break; }
                      }
                    }
                  }
                  if (!surface) return true;
                  const controls = [...surface.querySelectorAll('button,[role="button"]')];
                  const target = controls.find((element) => visible(element) && /close|cancel|dismiss/i.test(`${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''} ${element.textContent || ''}`)) || controls.find((element) => visible(element) && /^[×✕✖]$/.test((element.textContent || '').trim()));
                  if (!target) return false;
                  target.click();
                  return true;
                })()"#,
            )
            .await?;
        if clicked != Value::Bool(true) {
            return Err(
                "ChatGPT Voice settings is open, but its close button could not be found."
                    .to_owned(),
            );
        }
        let deadline = tokio::time::Instant::now() + VOICE_SETTINGS_WAIT_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if cdp.evaluate(VOICE_SETTINGS_VISIBLE_SCRIPT).await? == Value::Bool(false) {
                self.refresh_status().await?;
                if !self.status.voice_settings_open {
                    return Ok(self.status.clone());
                }
            }
            sleep(VOICE_SETTINGS_POLL_INTERVAL).await;
        }
        Err("ChatGPT Voice settings did not close.".to_owned())
    }

    async fn set_chatgpt_mic_muted(&mut self, muted: bool) -> Result<BrowserStatus, String> {
        if !self.status.voice_mode_active {
            self.refresh_status().await?;
        }
        if !self.status.voice_mode_active {
            return Err("ChatGPT Voice is not active.".to_owned());
        }
        if self.status.voice_settings_open {
            self.close_voice_settings().await?;
        }
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let desired = if muted { "mute" } else { "unmute" };
        let desired_literal = serde_json::to_string(desired).unwrap_or_default();
        let clicked = cdp
            .evaluate(&format!(
                r#"(() => {{
                  const visible = (element) => {{ if (!element || element.disabled) return false; const style = getComputedStyle(element); return style.display !== 'none' && style.visibility !== 'hidden' && element.getAttribute('aria-hidden') !== 'true'; }};
                  const controls = [...document.querySelectorAll('button,[role="button"]')];
                  const target = controls.find((element) => {{ if (!visible(element)) return false; const label = `${{element.getAttribute('aria-label') || ''}} ${{element.getAttribute('title') || ''}} ${{element.textContent || ''}}`; return {desired_literal} === 'mute' ? /turn off microphone|turn off mic|mute microphone|^mute$/i.test(label) : /turn on microphone|turn on mic|unmute microphone|^unmute$/i.test(label); }});
                  if (!target) return false;
                  target.click();
                  return true;
                }})()"#
                , desired_literal = desired_literal
            ))
            .await?;
        if clicked != Value::Bool(true) {
            return Err(format!("ChatGPT's microphone could not be {}d.", desired));
        }
        sleep(Duration::from_millis(250)).await;
        self.refresh_status().await?;
        if self.status.chatgpt_mic_muted != Some(muted) {
            return Err(format!(
                "ChatGPT's microphone did not become {}.",
                if muted { "muted" } else { "live" }
            ));
        }
        Ok(self.status.clone())
    }

    async fn ensure_chatgpt_mic_live(&mut self) -> BrowserStatus {
        // ChatGPT may mute the Voice microphone while opening, closing, or applying Voice
        // settings. Retry the state read and unmute click while the Voice controls settle.
        for attempt in 0..5 {
            if let Err(error) = self.refresh_status().await {
                log::warn!(
                    "[browser:{}] Could not refresh ChatGPT microphone state during automatic unmute (attempt {}): {error}",
                    self.guild_id,
                    attempt + 1
                );
                sleep(Duration::from_millis(250)).await;
                continue;
            }
            if !self.status.voice_mode_active {
                sleep(Duration::from_millis(250)).await;
                continue;
            }
            if self.status.chatgpt_mic_muted == Some(false) {
                return self.status.clone();
            }
            match self.set_chatgpt_mic_muted(false).await {
                Ok(status) if status.chatgpt_mic_muted == Some(false) => return status,
                Ok(_) => {}
                Err(error) => {
                    log::warn!(
                        "[browser:{}] Automatic ChatGPT Voice unmute attempt {} did not complete: {error}",
                        self.guild_id,
                        attempt + 1
                    );
                }
            }
            sleep(Duration::from_millis(250)).await;
        }
        if self.status.voice_mode_active && self.status.chatgpt_mic_muted != Some(false) {
            log::warn!(
                "[browser:{}] ChatGPT Voice microphone is still not confirmed live after automatic unmute retries.",
                self.guild_id
            );
        }
        self.status.clone()
    }

    async fn restore_chatgpt_mic_after_voice_setting(&mut self) -> BrowserStatus {
        // Give ChatGPT's settings transition a moment to settle before retrying the live mic.
        sleep(Duration::from_millis(150)).await;
        self.ensure_chatgpt_mic_live().await
    }

    async fn set_voice_setting_row(
        &mut self,
        label: &str,
        target: &str,
    ) -> Result<BrowserStatus, String> {
        if label.eq_ignore_ascii_case("Intelligence")
            && self.options_fetched_at.is_some()
            && self.status.available_intelligence.is_empty()
        {
            return Err(
                "ChatGPT Intelligence is not available on this ChatGPT account.".to_owned(),
            );
        }
        self.open_voice_settings().await?;
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let label_literal = serde_json::to_string(label).unwrap_or_default();
        let target_literal = serde_json::to_string(target).unwrap_or_default();
        let expression = r#"(() => {
          const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
          const visible = (element) => {
            if (!element || element.disabled) return false;
            const style = getComputedStyle(element);
            const rect = element.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
          };
          const wanted = __LABEL__.toLowerCase();
          const matchesLabel = (element) => clean(element.textContent).toLowerCase() === wanted || clean(element.getAttribute('aria-label')).toLowerCase() === wanted;
          const metadata = (element) => clean(`${element?.getAttribute('aria-label') || ''} ${element?.getAttribute('title') || ''} ${element?.textContent || ''}`);
          const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
          const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
          const hasSettingsText = (element) => !element.closest('nav,aside') && clean(element.innerText || '').length <= 5000 && /\blanguage\b/i.test(element.innerText || '') && hasExactDescendant(element, 'Language') && (/\bintelligence\b/i.test(element.innerText || '') || /\bvoice\b/i.test(element.innerText || '') || element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 || element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2);
          const settingsSurface = () => {
            const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
            const marked = surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0];
            if (marked) return marked;
            const labelNode = [...document.querySelectorAll('*')].find((element) => visible(element) && matchesLabel(element) && !element.closest('nav,aside'));
            if (!labelNode) return null;
            for (let candidate = labelNode, depth = 0; candidate && depth < 20; candidate = candidate.parentElement, depth += 1) {
              const text = candidate.innerText || '';
              const controls = candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length;
              if (visible(candidate) && !candidate.closest('nav,aside') && clean(text).length <= 5000 && /\blanguage\b/i.test(text) && hasExactDescendant(candidate, 'Language') && (controls >= 2 || /voice settings|voice customization/i.test(text))) return candidate;
            }
            return null;
          };
          const surface = settingsSurface();
          if (!surface) return false;
          const controls = [...surface.querySelectorAll('button,[role="button"],[role="combobox"],select')].filter(visible);
          const direct = controls.find((element) => {
            const text = metadata(element).toLowerCase();
            return text === wanted || text.startsWith(`${wanted} `);
          });
          if (direct) {
            direct.click();
            return true;
          }
          const labels = [...surface.querySelectorAll('*')].filter((element) => visible(element) && matchesLabel(element));
          for (const labelNode of labels) {
            for (let ancestor = labelNode, depth = 0; ancestor && depth < 7; ancestor = ancestor.parentElement, depth += 1) {
              const rowControls = [...ancestor.querySelectorAll('button,[role="button"],[role="combobox"],select')].filter(visible).filter((element) => !/close|cancel|dismiss/i.test(metadata(element)));
              if (rowControls.length) {
                rowControls[rowControls.length - 1].click();
                return true;
              }
            }
          }
          return false;
        })()"#.replace("__LABEL__", &label_literal);
        let result = async {
            let clicked = cdp.evaluate(&expression).await?;
            if clicked != Value::Bool(true) {
                return Err(format!("The ChatGPT {label} setting could not be found."));
            }
            let option_expression = r#"(() => {
          const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
          const visible = (element) => {
            if (!element || element.disabled) return false;
            const style = getComputedStyle(element);
            const rect = element.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
          };
          const wanted = __LABEL__.toLowerCase();
          const matchesLabel = (element) => clean(element.textContent).toLowerCase() === wanted || clean(element.getAttribute('aria-label')).toLowerCase() === wanted;
          const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
          const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
          const hasSettingsText = (element) => !element.closest('nav,aside') && clean(element.innerText || '').length <= 5000 && /\blanguage\b/i.test(element.innerText || '') && hasExactDescendant(element, 'Language') && (/\bintelligence\b/i.test(element.innerText || '') || /\bvoice\b/i.test(element.innerText || '') || element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 || element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2);
          const settingsSurface = () => {
            const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
            const marked = surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0];
            if (marked) return marked;
            const labelNode = [...document.querySelectorAll('*')].find((element) => visible(element) && matchesLabel(element) && !element.closest('nav,aside'));
            if (!labelNode) return null;
            for (let candidate = labelNode, depth = 0; candidate && depth < 20; candidate = candidate.parentElement, depth += 1) {
              const text = candidate.innerText || '';
              const controls = candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length;
              if (visible(candidate) && !candidate.closest('nav,aside') && clean(text).length <= 5000 && /\blanguage\b/i.test(text) && hasExactDescendant(candidate, 'Language') && (controls >= 2 || /voice settings|voice customization/i.test(text))) return candidate;
            }
            return null;
          };
          const surface = settingsSurface();
          const scope = surface || document.body;
          const labelNode = [...scope.querySelectorAll('*')].find((element) => visible(element) && matchesLabel(element) && !element.closest('nav,aside'));
          if (!labelNode) return false;
          let trigger = null;
          for (let ancestor = labelNode, depth = 0; ancestor && depth < 7; ancestor = ancestor.parentElement, depth += 1) {
            const controls = [...ancestor.querySelectorAll('button,[role="button"],[role="combobox"],select')].filter(visible).filter((element) => !/close|cancel|dismiss/i.test(`${element.getAttribute('aria-label') || ''} ${element.getAttribute('title') || ''} ${element.textContent || ''}`));
            if (controls.length) {
              trigger = controls[controls.length - 1];
              break;
            }
          }
          const controlledId = trigger?.getAttribute('aria-controls') || trigger?.getAttribute('aria-owns');
          const controlled = controlledId ? document.getElementById(controlledId) : null;
          const triggerRect = trigger?.getBoundingClientRect();
          const roots = controlled && visible(controlled) ? [controlled] : [...document.querySelectorAll('[role="listbox"],[role="menu"],[data-radix-popper-content-wrapper],[data-radix-select-content],[data-radix-menu-content],[data-radix-dropdown-menu-content]')]
            .filter(visible)
            .filter((root) => root !== surface && !hasSettingsText(root) && !root.closest('nav,aside'))
            .map((root) => {
              const rect = root.getBoundingClientRect();
              const centerX = rect.left + rect.width / 2;
              const centerY = rect.top + rect.height / 2;
              const triggerX = triggerRect ? triggerRect.left + triggerRect.width / 2 : centerX;
              const triggerY = triggerRect ? triggerRect.top + triggerRect.height / 2 : centerY;
              const roleScore = root.matches('[role="listbox"],[role="menu"]') ? 0 : 1;
              return { root, roleScore, distance: Math.hypot(centerX - triggerX, centerY - triggerY) };
            })
            .sort((a, b) => a.distance - b.distance || a.roleScore - b.roleScore)
            .slice(0, 1)
            .map((entry) => entry.root);
          const expected = __TARGET__.toLowerCase();
          const nodes = roots.flatMap((root) => {
            const semantic = [...root.querySelectorAll('[role="option"],[role="menuitem"],button,li,[data-radix-collection-item]')].filter(visible);
            return root.matches('[role="option"],[role="menuitem"],button,li,[data-radix-collection-item]') ? [root, ...semantic] : semantic;
          });
          const match = nodes.find((element) => (element.innerText || element.textContent || '').split(/\n/).map(clean).some((line) => line.toLowerCase() === expected));
          if (!match) return false;
          match.click();
          return true;
            })()"#.replace("__LABEL__", &label_literal).replace("__TARGET__", &target_literal);
            let mut selected = false;
            for _ in 0..20 {
                if cdp.evaluate(&option_expression).await? == Value::Bool(true) {
                    selected = true;
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
            if !selected {
                return Err(format!(
                    "The ChatGPT {label} option {target} was not visible."
                ));
            }
            sleep(Duration::from_millis(150)).await;
            Ok::<(), String>(())
        }
        .await;
        let close_result = self.close_voice_settings().await;
        match (result, close_result) {
            (Err(error), _) => Err(error),
            (Ok(()), Ok(_status)) => {
                if label.eq_ignore_ascii_case("Intelligence") {
                    self.status.intelligence = Some(target.to_owned());
                } else if label.eq_ignore_ascii_case("Language") {
                    self.status.language = Some(target.to_owned());
                }
                Ok(self.restore_chatgpt_mic_after_voice_setting().await)
            }
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn click_voice_option(&self, target: &str) -> Result<bool, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let target_literal = serde_json::to_string(target).unwrap_or_default();
        let result = cdp
            .evaluate(&format!(
                r#"(() => {{
                  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
                  const visible = (element) => {{
                    if (!element || element.disabled) return false;
                    const style = getComputedStyle(element);
                    const rect = element.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                  }};
                  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
                  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
                  const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
                  const hasSettingsText = (element) => !element.closest('nav,aside') && clean(element.innerText || '').length <= 5000 && /\blanguage\b/i.test(element.innerText || '') && hasExactDescendant(element, 'Language') && (/\bintelligence\b/i.test(element.innerText || '') || /\bvoice\b/i.test(element.innerText || '') || element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 || element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2);
                  const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
                  let surface = surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0];
                  if (!surface) {{
                    const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
                    const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
                    const anchor = intelligence || language;
                    if (anchor && language) {{
                      for (let candidate = anchor, depth = 0; candidate && depth < 20; candidate = candidate.parentElement, depth += 1) {{
                        if (visible(candidate) && !candidate.closest('nav,aside') && clean(candidate.innerText).length <= 5000 && /\blanguage\b/i.test(candidate.innerText || '') && hasExactDescendant(candidate, 'Language') && (candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(candidate.innerText || ''))) {{
                          surface = candidate;
                          break;
                        }}
                      }}
                    }}
                  }}
                  if (!surface) return false;
                  const requested = clean({target_literal}).toLowerCase();
                  const voices = [...surface.querySelectorAll('[role="radio"],input[type="radio"]')]
                    .filter(visible)
                    .map((element) => {{
                      const label = clean(element.getAttribute('aria-label') || element.getAttribute('data-value') || element.textContent);
                      return {{ element, label }};
                    }});
                  const match = voices.find((voice) => voice.label.toLowerCase() === requested);
                  if (!match) return false;
                  match.element.click();
                  return true;
                }})()"#,
                target_literal = target_literal
            ))
            .await?;
        Ok(result == Value::Bool(true))
    }

    async fn click_voice_arrow(&self, direction: &str) -> Result<bool, String> {
        let cdp = self
            .cdp
            .as_ref()
            .ok_or_else(|| "Browser is not running".to_owned())?;
        let direction_literal = serde_json::to_string(direction).unwrap_or_default();
        let result = cdp
            .evaluate(&format!(
                r#"(() => {{
                  const visible = (element) => {{
                    if (!element || element.disabled) return false;
                    const style = getComputedStyle(element);
                    const rect = element.getBoundingClientRect();
                    return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0;
                  }};
                  const clean = (value) => String(value || '').replace(/\s+/g, ' ').trim();
                  const hasExactText = (element, value) => clean(element?.textContent).toLowerCase() === value.toLowerCase();
                  const hasExactDescendant = (element, value) => hasExactText(element, value) || [...element.querySelectorAll('*')].some((child) => visible(child) && hasExactText(child, value));
                  const hasSettingsText = (element) => !element.closest('nav,aside') && clean(element.innerText || '').length <= 5000 && /\blanguage\b/i.test(element.innerText || '') && hasExactDescendant(element, 'Language') && (/\bintelligence\b/i.test(element.innerText || '') || /\bvoice\b/i.test(element.innerText || '') || element.querySelectorAll('[role="radio"],input[type="radio"]').length > 0 || element.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2);
                  const surfaces = [...document.querySelectorAll('dialog[open],[role="dialog"],[aria-modal="true"],[data-state="open"]')].filter(visible);
                  let surface = surfaces.filter(hasSettingsText).sort((a, b) => clean(a.innerText).length - clean(b.innerText).length)[0];
                  if (!surface) {{
                    const intelligence = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Intelligence'));
                    const language = [...document.querySelectorAll('*')].find((element) => visible(element) && !element.closest('nav,aside') && hasExactText(element, 'Language'));
                    const anchor = intelligence || language;
                    if (anchor && language) {{
                      for (let candidate = anchor, depth = 0; candidate && depth < 20; candidate = candidate.parentElement, depth += 1) {{
                        if (visible(candidate) && !candidate.closest('nav,aside') && clean(candidate.innerText).length <= 5000 && /\blanguage\b/i.test(candidate.innerText || '') && hasExactDescendant(candidate, 'Language') && (candidate.querySelectorAll('button,[role="button"],[role="combobox"],select').length >= 2 || /voice settings|voice customization/i.test(candidate.innerText || ''))) {{
                          surface = candidate;
                          break;
                        }}
                      }}
                    }}
                  }}
                  if (!surface) return false;
                  const metadata = (element) => {{
                    const icon = [...element.querySelectorAll('svg use')].map((use) => use.getAttribute('href') || use.getAttribute('xlink:href') || '').join(' ');
                    return `${{element.getAttribute('aria-label') || ''}} ${{element.getAttribute('title') || ''}} ${{element.textContent || ''}} ${{icon}}`.replace(/\s+/g, ' ').trim();
                  }};
                  const controls = [...surface.querySelectorAll('button,[role="button"]')].filter(visible).filter((element) => !/close|cancel|dismiss|mute|unmute|intelligence|language|voice settings|customize|sample|play/i.test(metadata(element)));
                  const wantsNext = {direction_literal} === 'next';
                  const arrow = controls.find((element) => {{
                    const label = metadata(element);
                    const forward = /next voice|next|forward|right|chevron-right|arrow-right/i.test(label) && !/previous|back|left|chevron-left|arrow-left/i.test(label);
                    const backward = /previous voice|previous|back|left|chevron-left|arrow-left/i.test(label) && !/next|forward|right|chevron-right|arrow-right/i.test(label);
                    return wantsNext ? forward : backward;
                  }});
                  if (arrow) {{
                    arrow.click();
                    return true;
                  }}
                  const box = surface.getBoundingClientRect();
                  const midpoint = box.left + box.width / 2;
                  const targetY = box.top + box.height * .56;
                  const candidates = controls.map((element) => {{
                    const rect = element.getBoundingClientRect();
                    return {{ element, rect, x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 }};
                  }}).filter((item) => item.rect.width >= 18 && item.rect.height >= 18 && item.y > box.top + box.height * .30 && item.y < box.top + box.height * .78 && (wantsNext ? item.x > box.left + box.width * .65 : item.x < box.left + box.width * .35)).sort((a, b) => Math.abs(a.y - targetY) - Math.abs(b.y - targetY));
                  if (!candidates[0]) return false;
                  candidates[0].element.click();
                  return true;
                }})()"#,
                direction_literal = direction_literal
            ))
            .await?;
        Ok(result == Value::Bool(true))
    }

    async fn set_voice(&mut self, target: &str) -> Result<BrowserStatus, String> {
        let requested = target.trim();
        if requested.is_empty() {
            return Err("A ChatGPT Voice name is required".to_owned());
        }
        self.open_voice_settings().await?;
        let result = async {
            self.refresh_status().await?;
            let mut target_name = self
                .status
                .available_voices
                .iter()
                .find(|voice| voice.value.eq_ignore_ascii_case(requested))
                .map(|voice| voice.value.clone());
            if target_name.is_none() {
                match self.discover_available_options().await {
                    Ok(options) => {
                        let current_voice = options.current_voice.clone();
                        let current_voice_description = options.current_voice_description.clone();
                        self.status.available_voices = options.voices;
                        self.status.available_intelligence = options.intelligence;
                        self.status.available_languages = options.languages;
                        if self.status.available_intelligence.is_empty() {
                            self.status.intelligence = None;
                        }
                        if current_voice.is_some() {
                            self.status.voice = current_voice;
                            self.status.voice_description = current_voice_description;
                        }
                        self.options_fetched_at = Some(Instant::now());
                        target_name = self
                            .status
                            .available_voices
                            .iter()
                            .find(|voice| voice.value.eq_ignore_ascii_case(requested))
                            .map(|voice| voice.value.clone());
                    }
                    Err(error) => {
                        log::warn!(
                            "[browser:{}] Could not refresh live Voice options before selection: {error}",
                            self.guild_id
                        );
                    }
                }
            }
            let Some(target_name) = target_name else {
                let available = self
                    .status
                    .available_voices
                    .iter()
                    .map(|voice| voice.label.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(if available.is_empty() {
                    format!("ChatGPT Voice {requested} was not found; live Voice options are not available yet.")
                } else {
                    format!("ChatGPT Voice {requested} was not found. Available voices: {available}")
                });
            };
            let mut current =
                self.status.voice.clone().ok_or_else(|| {
                    "The current ChatGPT voice could not be identified.".to_owned()
                })?;
            if current.eq_ignore_ascii_case(&target_name) {
                self.status.voice_description = self
                    .status
                    .available_voices
                    .iter()
                    .find(|voice| voice.value.eq_ignore_ascii_case(&target_name))
                    .and_then(|voice| voice.description.clone())
                    .or_else(|| {
                        let description = voice_description(&target_name);
                        (!description.is_empty()).then(|| description.to_owned())
                    });
                return Ok(self.status.clone());
            }

            // Select an accessible radio option directly when available; use the
            // carousel fallback for layouts without a selectable voice list.
            if self.click_voice_option(&target_name).await? {
                sleep(Duration::from_millis(350)).await;
                self.refresh_status().await?;
                current = self.status.voice.clone().ok_or_else(|| {
                    "ChatGPT Voice changed, but the selected voice could not be read."
                        .to_owned()
                })?;
                if current.eq_ignore_ascii_case(&target_name) {
                    self.status.voice_description = self
                        .status
                        .available_voices
                        .iter()
                        .find(|voice| voice.value.eq_ignore_ascii_case(&target_name))
                        .and_then(|voice| voice.description.clone())
                        .or_else(|| {
                            let description = voice_description(&target_name);
                            (!description.is_empty()).then(|| description.to_owned())
                        });
                    return Ok(self.status.clone());
                }
            }

            // Do not infer direction from the voice list order. Walk each arrow
            // direction and verify the displayed name after every change.
            let maximum_steps = self
                .status
                .available_voices
                .len()
                .clamp(2, MAX_DISCOVERED_VOICES)
                + 1;
            for direction in ["next", "previous"] {
                for _ in 0..maximum_steps {
                    let before = current.clone();
                    if !self.click_voice_arrow(direction).await? {
                        break;
                    }
                    sleep(Duration::from_millis(450)).await;
                    self.refresh_status().await?;
                    current = self.status.voice.clone().ok_or_else(|| {
                        "ChatGPT Voice changed, but the selected voice could not be read."
                            .to_owned()
                    })?;
                    if current.eq_ignore_ascii_case(&target_name) {
                        self.status.voice_description = self
                            .status
                            .available_voices
                            .iter()
                            .find(|voice| voice.value.eq_ignore_ascii_case(&target_name))
                            .and_then(|voice| voice.description.clone())
                            .or_else(|| {
                                let description = voice_description(&target_name);
                                (!description.is_empty()).then(|| description.to_owned())
                            });
                        return Ok(self.status.clone());
                    }
                    if current.eq_ignore_ascii_case(&before) {
                        break;
                    }
                }
            }
            Err(format!("ChatGPT Voice did not switch to {target_name}."))
        }
        .await;
        let close_result = self.close_voice_settings().await;
        match (result, close_result) {
            (Err(error), _) => Err(error),
            (Ok(_), Ok(_status)) => Ok(self.restore_chatgpt_mic_after_voice_setting().await),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    fn stop(&mut self) {
        if let Some(media) = self.media.take() {
            media.stop();
        }
        self.cdp = None;
        let process_id = self.browser_process_id.take();
        if let Some(mut child) = self.child.take() {
            terminate_process_tree(child.id());
            let _ = child.wait();
        } else if let Some(process_id) = process_id {
            terminate_process_tree(process_id);
        }
        self.page_target_id = None;
        self.options_fetched_at = None;
        self.recent_threads_fetched_at = None;
        self.last_status_refresh_at = None;
        self.cdp_status_failures = 0;
        self.voice_inactive_checks = 0;
        self.voice_media_seen = false;
        self.voice_media_missing_checks = 0;
        self.manual_visibility_override = None;
        self.visibility_hold = false;
        self.status = BrowserStatus::closed(&self.guild_id);
        self.status.cdp_restarts = self.cdp_restarts;
    }
}

impl Drop for BrowserSession {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Default)]
pub struct BrowserRuntime {
    sessions: Arc<Mutex<HashMap<String, Arc<AsyncMutex<BrowserSession>>>>>,
}

impl BrowserRuntime {
    pub fn stop_all(&self) {
        let sessions = self
            .sessions
            .lock()
            .map(|sessions| sessions.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for session in sessions {
            // ExitRequested is synchronous, but a browser start/status call may still hold the
            // async session mutex. Waiting briefly here prevents a clean panel close from
            // leaving the dedicated Chromium process tree behind.
            tauri::async_runtime::block_on(async {
                if let Ok(mut session) = timeout(Duration::from_secs(5), session.lock()).await {
                    session.stop();
                }
            });
        }
    }

    fn session(
        &self,
        guild_id: &str,
        config: &ConfigSnapshot,
    ) -> Result<Arc<AsyncMutex<BrowserSession>>, String> {
        if !guild_id.chars().all(|character| character.is_ascii_digit()) || guild_id.len() < 15 {
            return Err("A valid Discord guild ID is required".to_owned());
        }
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| "Browser session lock was poisoned".to_owned())?;
        if let Some(session) = sessions.get(guild_id) {
            return Ok(Arc::clone(session));
        }
        let profile = Self::profile_dir(guild_id, config);
        let session = Arc::new(AsyncMutex::new(BrowserSession::new(
            guild_id, config, profile,
        )));
        sessions.insert(guild_id.to_owned(), Arc::clone(&session));
        Ok(session)
    }

    fn profile_dir(guild_id: &str, config: &ConfigSnapshot) -> PathBuf {
        if config.discord_guild_id.trim() == guild_id {
            PathBuf::from(&config.browser_profile)
        } else {
            PathBuf::from(&config.browser_profile)
                .join("guilds")
                .join(guild_id)
        }
    }

    pub async fn start_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        let status = session.start(app).await?;
        if status.logged_in {
            match session.recent_threads().await {
                Ok(threads) => {
                    let count = threads.len();
                    session.status.recent_threads = threads;
                    if count > 0 {
                        session.recent_threads_fetched_at = Some(Instant::now());
                    } else {
                        session.recent_threads_fetched_at = None;
                    }
                    emit_browser_log(
                        app,
                        &session.guild_id,
                        &format!("Prefetched {count} recent ChatGPT thread(s)."),
                    );
                }
                Err(error) => {
                    emit_browser_log(
                        app,
                        &session.guild_id,
                        &format!("Could not prefetch recent ChatGPT threads: {error}"),
                    );
                }
            }
        }
        Ok(session.status.clone())
    }

    pub async fn status_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let setup_required = !Self::profile_dir(guild_id, &config)
            .join(AUTH_MARKER)
            .exists();
        // First-run setup may still be establishing its control endpoint. Report the missing
        // authentication state before taking the session lock so the panel can explain what is
        // happening while the setup flow is still opening.
        if setup_required {
            let browser_is_controlled = session
                .try_lock()
                .map(|session| session.cdp.is_some())
                .unwrap_or(false);
            if !browser_is_controlled {
                return Ok(BrowserStatus::sign_in_required(guild_id));
            }
        }
        let mut session = session.lock().await;
        if session.cdp.is_some() {
            session.status_probe(app).await?;
            let refresh_threads = session
                .recent_threads_fetched_at
                .map(|fetched_at| fetched_at.elapsed() >= THREAD_CACHE_TTL)
                .unwrap_or(true)
                && !session.status.voice_mode_active;
            if refresh_threads {
                let threads = session.recent_threads().await.unwrap_or_default();
                if !threads.is_empty() || session.status.recent_threads.is_empty() {
                    session.status.recent_threads = threads;
                }
                session.recent_threads_fetched_at = if session.status.recent_threads.is_empty() {
                    None
                } else {
                    Some(Instant::now())
                };
            }
        }
        Ok(session.status.clone())
    }

    pub async fn stop_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.stop();
        Ok(session.status.clone())
    }

    pub async fn take_media_reader(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<crate::audio::PcmReader, String> {
        let config = config::load_snapshot(app)?;
        let input_gain = config.audio_input_volume as f32;
        let output_gain = config.audio_output_volume as f32;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        let media = session
            .media
            .as_ref()
            .ok_or_else(|| "Browser media transport is not available".to_owned())?
            .clone();
        media.set_input_gain(input_gain);
        media.take_pcm_reader_with_gain(output_gain)
    }

    pub async fn apply_audio_gains(&self, input_gain: f32, output_gain: f32) {
        let sessions = self
            .sessions
            .lock()
            .map(|sessions| sessions.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for session in sessions {
            let session = session.lock().await;
            if let Some(media) = session.media.as_ref() {
                media.set_input_gain(input_gain);
                media.set_output_gain(output_gain);
            }
        }
    }

    pub async fn media_for_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<Arc<BrowserMediaTransport>, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session
            .media
            .as_ref()
            .cloned()
            .ok_or_else(|| "Browser media transport is not available".to_owned())
    }

    pub async fn set_visibility_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
        hidden: bool,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        Ok(session.set_visibility(hidden).await)
    }

    pub async fn open_voice_settings_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session.open_voice_settings().await
    }

    pub async fn close_voice_settings_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session.close_voice_settings().await
    }

    pub async fn set_voice_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
        voice: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session.set_voice(voice).await
    }

    pub async fn set_intelligence_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
        intelligence: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session
            .set_voice_setting_row("Intelligence", intelligence)
            .await
    }

    pub async fn set_language_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
        language: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session.set_voice_setting_row("Language", language).await
    }

    pub async fn set_mic_muted_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
        muted: bool,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session.set_chatgpt_mic_muted(muted).await
    }

    pub async fn reconnect_voice_guild(
        &self,
        app: &AppHandle,
        guild_id: &str,
    ) -> Result<BrowserStatus, String> {
        let config = config::load_snapshot(app)?;
        let session = self.session(guild_id, &config)?;
        let mut session = session.lock().await;
        session.start(app).await?;
        session.reconnect_voice().await
    }
}

fn find_browser_executable(configured: Option<&Path>) -> Option<PathBuf> {
    let local_app_data = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_default();
    let program_files = std::env::var_os("ProgramFiles")
        .map(PathBuf::from)
        .unwrap_or_default();
    let program_files_x86 = std::env::var_os("ProgramFiles(x86)")
        .map(PathBuf::from)
        .unwrap_or_default();
    let candidates = [
        configured.map(Path::to_path_buf),
        Some(local_app_data.join("BraveSoftware/Brave-Browser/Application/brave.exe")),
        Some(program_files.join("BraveSoftware/Brave-Browser/Application/brave.exe")),
        Some(program_files_x86.join("BraveSoftware/Brave-Browser/Application/brave.exe")),
        Some(local_app_data.join("Google/Chrome/Application/chrome.exe")),
        Some(program_files.join("Google/Chrome/Application/chrome.exe")),
        Some(program_files_x86.join("Google/Chrome/Application/chrome.exe")),
        Some(local_app_data.join("Microsoft/Edge/Application/msedge.exe")),
    ];
    candidates.into_iter().flatten().find(|path| path.is_file())
}

async fn reserve_port() -> Result<u16, String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| format!("Could not reserve browser CDP port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("Could not read browser CDP port: {error}"))
}

fn launch_browser(
    executable: &Path,
    profile_dir: &Path,
    url: &str,
    port: u16,
    remote_debugging: bool,
) -> Result<Child, String> {
    let mut command = Command::new(executable);
    command
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg("--new-window")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if remote_debugging {
        command
            .arg(format!("--remote-debugging-port={port}"))
            .arg("--remote-debugging-address=127.0.0.1")
            .arg("about:blank");
    } else {
        command.arg(url);
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    command
        .spawn()
        .map_err(|error| format!("Could not launch browser {}: {error}", executable.display()))
}

fn terminate_process_tree(process_id: u32) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        let _ = Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .creation_flags(0x08000000)
            .output();
    }
    #[cfg(not(windows))]
    {
        let _ = process_id;
    }
}

#[cfg(windows)]
fn set_process_windows_visible(process_id: u32, visible: bool) -> usize {
    use std::mem::MaybeUninit;
    use windows_sys::{
        core::BOOL,
        Win32::{
            Foundation::{HWND, LPARAM},
            System::Threading::{AttachThreadInput, GetCurrentThreadId},
            UI::WindowsAndMessaging::{
                BringWindowToTop, EnumWindows, GetForegroundWindow, GetWindowTextLengthW,
                GetWindowTextW, GetWindowThreadProcessId, SetForegroundWindow, ShowWindow, SW_HIDE,
                SW_RESTORE,
            },
        },
    };

    #[repr(C)]
    struct Context {
        process_id: u32,
        visible: bool,
        changed: usize,
        activated: bool,
    }

    unsafe extern "system" fn callback(hwnd: HWND, parameter: LPARAM) -> BOOL {
        let context = &mut *(parameter as *mut Context);
        let mut owner = MaybeUninit::<u32>::zeroed();
        GetWindowThreadProcessId(hwnd, owner.as_mut_ptr());
        if owner.assume_init() == context.process_id {
            if context.visible {
                ShowWindow(hwnd, SW_RESTORE);
                if !context.activated {
                    let title_length = GetWindowTextLengthW(hwnd);
                    let mut title = vec![0u16; title_length.max(0) as usize + 1];
                    let copied = GetWindowTextW(hwnd, title.as_mut_ptr(), title.len() as i32);
                    let title = String::from_utf16_lossy(&title[..copied.max(0) as usize]);
                    let normalized_title = title.to_ascii_lowercase();
                    let is_utility_window = normalized_title.is_empty()
                        || normalized_title.contains("restore pages")
                        || normalized_title.contains("default ime")
                        || normalized_title.contains("msctfime");
                    if !is_utility_window {
                        let current_thread = GetCurrentThreadId();
                        let foreground = GetForegroundWindow();
                        let mut foreground_process = 0u32;
                        let foreground_thread = if foreground.is_null() {
                            0
                        } else {
                            GetWindowThreadProcessId(foreground, &mut foreground_process)
                        };
                        let attached =
                            foreground_thread != 0 && foreground_thread != current_thread;
                        if attached {
                            AttachThreadInput(current_thread, foreground_thread, 1);
                        }
                        BringWindowToTop(hwnd);
                        SetForegroundWindow(hwnd);
                        if attached {
                            AttachThreadInput(current_thread, foreground_thread, 0);
                        }
                        context.activated = true;
                    }
                }
            } else {
                ShowWindow(hwnd, SW_HIDE);
            }
            context.changed += 1;
        }
        1
    }

    let mut context = Context {
        process_id,
        visible,
        changed: 0,
        activated: false,
    };
    unsafe {
        EnumWindows(Some(callback), &mut context as *mut Context as LPARAM);
    }
    context.changed
}

#[cfg(not(windows))]
fn set_process_windows_visible(_process_id: u32, _visible: bool) -> usize {
    0
}

async fn wait_for_json<T: for<'de> Deserialize<'de>>(
    endpoint: &str,
    child: &mut Child,
) -> Result<T, String> {
    let client = HttpClient::new();
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    let mut last_error = None;
    while tokio::time::Instant::now() < deadline {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("Could not inspect browser process: {error}"))?
        {
            return Err(format!(
                "The browser exited before CDP was ready ({status})"
            ));
        }
        match client
            .get(endpoint)
            .timeout(Duration::from_millis(750))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                return response
                    .json::<T>()
                    .await
                    .map_err(|error| format!("Could not parse browser CDP response: {error}"));
            }
            Ok(response) => last_error = Some(format!("HTTP {}", response.status())),
            Err(error) => last_error = Some(error.to_string()),
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(format!(
        "Browser did not expose CDP within {} seconds{}",
        START_TIMEOUT.as_secs(),
        last_error
            .map(|error| format!(": {error}"))
            .unwrap_or_default()
    ))
}

fn voice_description(voice: &str) -> &'static str {
    match voice.to_ascii_lowercase().as_str() {
        "arbor" => "Easygoing and versatile",
        "breeze" => "Animated and earnest",
        "cove" => "Composed and direct",
        "ember" => "Confident and optimistic",
        "juniper" => "Open and upbeat",
        "maple" => "Cheerful and candid",
        "sol" => "Savvy and relaxed",
        "spruce" => "Calm and affirming",
        "vale" => "Bright and inquisitive",
        _ => "",
    }
}

fn emit_browser_log(app: &AppHandle, guild_id: &str, message: &str) {
    log::info!("[browser:{guild_id}] {message}");
    let _ = app.emit("runtime-log", format!("Browser: {message}"));
}

pub async fn status(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    runtime.status_guild(&app, guild_id.trim()).await
}

pub async fn start(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    runtime.start_guild(&app, guild_id.trim()).await
}

pub async fn stop(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    runtime.stop_guild(&app, guild_id.trim()).await
}

pub async fn new_thread(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    let config = config::load_snapshot(&app)?;
    let session = runtime.session(guild_id.trim(), &config)?;
    let mut session = session.lock().await;
    session.start(&app).await?;
    session.new_thread().await
}

pub async fn resume_thread(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
    thread_id: String,
) -> Result<BrowserStatus, String> {
    let config = config::load_snapshot(&app)?;
    let session = runtime.session(guild_id.trim(), &config)?;
    let mut session = session.lock().await;
    session.start(&app).await?;
    session.resume_thread(thread_id.trim()).await
}

pub async fn reconnect_voice(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    runtime.reconnect_voice_guild(&app, guild_id.trim()).await
}

pub async fn set_visibility(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
    hidden: bool,
) -> Result<BrowserStatus, String> {
    runtime
        .set_visibility_guild(&app, guild_id.trim(), hidden)
        .await
}

#[tauri::command]
pub async fn open_voice_settings(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    runtime
        .open_voice_settings_guild(&app, guild_id.trim())
        .await
}

#[tauri::command]
pub async fn close_voice_settings(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
) -> Result<BrowserStatus, String> {
    runtime
        .close_voice_settings_guild(&app, guild_id.trim())
        .await
}

#[tauri::command]
pub async fn set_voice(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
    voice: String,
) -> Result<BrowserStatus, String> {
    runtime
        .set_voice_guild(&app, guild_id.trim(), voice.trim())
        .await
}

#[tauri::command]
pub async fn set_intelligence(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
    intelligence: String,
) -> Result<BrowserStatus, String> {
    runtime
        .set_intelligence_guild(&app, guild_id.trim(), intelligence.trim())
        .await
}

#[tauri::command]
pub async fn set_language(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
    language: String,
) -> Result<BrowserStatus, String> {
    runtime
        .set_language_guild(&app, guild_id.trim(), language.trim())
        .await
}

#[tauri::command]
pub async fn set_mic_muted(
    app: AppHandle,
    runtime: State<'_, BrowserRuntime>,
    guild_id: String,
    muted: bool,
) -> Result<BrowserStatus, String> {
    runtime
        .set_mic_muted_guild(&app, guild_id.trim(), muted)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_in_required_status_exposes_onboarding_state() {
        let status = BrowserStatus::sign_in_required("964597969980620820");
        assert!(status.open);
        assert!(!status.logged_in);
        assert!(status.auth_required);
        assert_eq!(
            status.error.as_deref(),
            Some("Sign in to ChatGPT in the dedicated GPTVoice browser window.")
        );
    }

    #[tokio::test]
    async fn cdp_client_smoke_test_when_endpoint_is_provided() {
        let Ok(endpoint) = std::env::var("GPTVOICE_CDP_WS") else {
            return;
        };
        let client = CdpClient::connect(&endpoint)
            .await
            .expect("CDP should accept a websocket connection");
        client
            .call("Runtime.enable", json!({}))
            .await
            .expect("Runtime.enable should complete");
        client
            .call("Page.enable", json!({}))
            .await
            .expect("Page.enable should complete");
        client
            .call(
                "Page.addScriptToEvaluateOnNewDocument",
                json!({"source": "(() => { window.__gptVoiceTest = true; })();"}),
            )
            .await
            .expect("CDP should accept a page init script");
        let value = client
            .evaluate("document.readyState")
            .await
            .expect("Runtime.evaluate should complete");
        assert!(value.is_string());
    }
}
