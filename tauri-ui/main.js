const invoke = window.__TAURI__?.core?.invoke;

const state = {
  config: null,
  discord: null,
  browser: null,
  system: null,
  logs: [],
  activeTab: localStorage.getItem("gptvoice-tab") || "console",
  chatgptSignInRequired: false,
  inputMuted: false,
  lastBrowserPackets: { sent: 0, dropped: 0, received: 0 }
};
let settingsSaveTimer = null;

const $ = (id) => document.getElementById(id);

function text(id, value) {
  const element = $(id);
  if (element) element.textContent = value == null || value === "" ? "—" : String(value);
}

function formatBytes(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  if (number < 1024) return `${number} B`;
  if (number < 1024 ** 2) return `${(number / 1024).toFixed(1)} KB`;
  return `${(number / (1024 ** 2)).toFixed(1)} MB`;
}

function formatDuration(seconds) {
  const value = Math.max(0, Number(seconds) || 0);
  const hours = Math.floor(value / 3600);
  const minutes = Math.floor((value % 3600) / 60);
  const remaining = Math.floor(value % 60);
  return hours ? `${hours}h ${minutes}m` : `${minutes}m ${remaining}s`;
}

function log(message, level = "INFO") {
  const messageText = String(message);
  if (/first-run browser setup is open|chatgpt needs sign-in/i.test(messageText)) state.chatgptSignInRequired = true;
  if (/chatgpt session is ready/i.test(messageText)) state.chatgptSignInRequired = false;
  const line = `[${new Date().toLocaleTimeString()}] [${level}] ${String(message)}`;
  state.logs.push(line);
  if (state.logs.length > 1_000) state.logs.shift();
  renderConsole();
  updateAuthBanner(state.browser);
}

function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

function renderConsole() {
  const output = $("console-output");
  if (!output) return;
  const query = String($("console-search")?.value || "").trim().toLowerCase();
  const lines = query ? state.logs.filter((line) => line.toLowerCase().includes(query)) : state.logs;
  const wasAtBottom = output.scrollHeight - output.scrollTop - output.clientHeight < 8;
  output.value = lines.join("\n");
  text("console-match-count", query ? `${lines.length} match${lines.length === 1 ? "" : "es"}` : "0 matches");
  if ($("console-autoscroll")?.checked && (wasAtBottom || !output.value)) output.scrollTop = output.scrollHeight;
}

function switchTab(tab) {
  const allowed = new Set(["console", "status", "performance", "system", "settings", "config"]);
  state.activeTab = allowed.has(tab) ? tab : "console";
  localStorage.setItem("gptvoice-tab", state.activeTab);
  document.querySelectorAll(".tab-button").forEach((button) => button.classList.toggle("active", button.dataset.tab === state.activeTab));
  document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.dataset.panel === state.activeTab));
}

function setFeedback(message, target = "status-feedback", level = "") {
  const element = $(target);
  if (!element) return;
  element.textContent = message;
  element.className = `form-feedback ${level}`.trim();
}

function updateVolumeLabels() {
  const input = Number($("setting-input-volume")?.value || 1);
  const output = Number($("setting-output-volume")?.value || 1);
  text("setting-input-volume-label", `${Math.round(input * 100)}%`);
  text("setting-output-volume-label", `${Math.round(output * 100)}%`);
}

function populateConfig(config) {
  state.config = config;
  $("config-guild-id").value = config.discordGuildId || "";
  $("config-chatgpt-url").value = config.chatgptUrl || "";
  $("config-browser-executable").value = config.browserExecutable || "";
  $("config-sample-rate").value = config.audioSampleRate || 48000;
  $("config-channels").value = config.audioChannels || 2;
  $("setting-input-volume").value = config.audioInputVolume ?? 1;
  $("setting-output-volume").value = config.audioOutputVolume ?? 1;
  $("setting-earcons").checked = config.audioEarconsEnabled !== false;
  $("setting-earcon-volume").value = config.audioEarconVolume ?? 0.18;
  $("setting-audio-mode").value = config.audioCaptureMode || "browser-media";
  $("setting-browser-hidden").checked = config.browserHideWhenReady !== false;
  text("config-path", config.configPath);
  $("config-preview").textContent = JSON.stringify({
    discordToken: config.discordTokenConfigured ? config.discordTokenMasked : "not configured",
    discordGuildId: config.discordGuildId,
    chatgptUrl: config.chatgptUrl,
    browserProfile: config.browserProfile,
    browserExecutable: config.browserExecutable || "automatic",
    audioCaptureMode: config.audioCaptureMode,
    audioInputVolume: config.audioInputVolume,
    audioOutputVolume: config.audioOutputVolume
  }, null, 2);
  updateVolumeLabels();
  log(config.discordTokenConfigured ? "Loaded saved configuration and masked Discord token." : "Loaded configuration; Discord token is not configured.");
  updateSetupGuide();
}

function updateDiscordStatus(status) {
  state.discord = status || null;
  const connected = status?.connected === true;
  text("status-discord", connected ? "Connected" : status?.state === "connecting" ? "Connecting" : "Disconnected");
  text("status-bot", status?.userName || "—");
  text("guild-count", status?.guildCount ?? 0);
  text("footer-guilds", status?.guildCount ?? 0);
  text("footer-voice", status?.voiceCount ?? 0);
  const footer = $("footer-connection");
  if (footer) {
    footer.textContent = connected ? "Connected" : "Disconnected";
    footer.className = `connection-state ${connected ? "connected" : "disconnected"}`;
  }
  $("guild-list").innerHTML = connected
    ? `<div class="list-item selected"><strong>Discord gateway</strong><small>${status.guildCount || 0} server(s) visible</small></div>`
    : `<div class="empty-state">${status?.lastError || "Waiting for Discord..."}</div>`;
  if (status?.lastError) setFeedback(status.lastError, "status-feedback", "error-text");
}

function updateAuthBanner(browser) {
  if (browser?.loggedIn) state.chatgptSignInRequired = false;
  const required = Boolean(state.chatgptSignInRequired || browser?.authRequired);
  const banner = $("auth-banner");
  if (banner) banner.classList.toggle("hidden", !required);
  const button = $("auth-banner-show-browser");
  if (!button) return;
  const browserOpen = browser?.open === true;
  button.disabled = !browserOpen || browser?.windowHidden !== true;
  button.textContent = browserOpen && browser?.windowHidden === true
    ? "Show sign-in window"
    : browserOpen
      ? "Sign-in window is open"
      : "Sign-in window opening…";
  updateSetupGuide();
}

function updateSetupGuide() {
  const banner = $("setup-banner");
  if (!banner) return;
  const config = state.config;
  if (!config) {
    banner.classList.add("hidden");
    return;
  }
  const tokenReady = Boolean(config.discordTokenConfigured);
  const guildReady = Boolean(String(config.discordGuildId || "").trim());
  const browserOpen = state.browser?.open === true;
  if (browserOpen) {
    banner.classList.add("hidden");
    return;
  }

  const title = $("setup-banner-title");
  const copy = $("setup-banner-copy");
  const configButton = $("setup-banner-config");
  if (!tokenReady) {
    if (title) title.textContent = "Welcome to GPTVoice";
    if (copy) copy.textContent = "Start in Config: paste your Discord bot token, add your server ID, and save the changes.";
    if (configButton) configButton.classList.remove("hidden");
  } else if (!guildReady) {
    if (title) title.textContent = "Finish Discord setup";
    if (copy) copy.textContent = "Open Config and add your Discord guild ID so GPTVoice knows which server to start first.";
    if (configButton) configButton.classList.remove("hidden");
  } else {
    if (title) title.textContent = "Start the dedicated ChatGPT browser";
    if (copy) copy.textContent = "Your configuration is saved. Open Status to monitor the dedicated browser; it starts automatically. If it is closed, choose Start browser, then sign in to ChatGPT when the window appears.";
    if (configButton) configButton.classList.add("hidden");
  }
  banner.classList.remove("hidden");
}

function populateDynamicSelect(id, placeholder, values, current, mapValue = (value) => ({ value, label: value })) {
  const select = $(id);
  if (!select) return;
  const items = (Array.isArray(values) ? values : [])
    .map(mapValue)
    .filter((item) => item?.value && item?.label)
    .filter((item, index, all) => all.findIndex((candidate) => candidate.value.toLowerCase() === item.value.toLowerCase()) === index);
  const currentValue = String(current || "").trim();
  if (currentValue && !items.some((item) => item.value.toLowerCase() === currentValue.toLowerCase())) {
    items.unshift({ value: currentValue, label: currentValue + " (current)", description: "Current value; refresh Voice options to discover the latest choices." });
  }
  select.replaceChildren(new Option(placeholder, ""));
  for (const item of items) {
    const option = new Option(item.label, item.value);
    if (item.description) option.title = item.description;
    select.add(option);
  }
  const canChange = Boolean(state.browser?.open && state.browser?.loggedIn && items.length > 0);
  select.disabled = !canChange;
  const selected = [...select.options].find((option) => option.value.toLowerCase() === currentValue.toLowerCase());
  select.value = selected?.value || "";
}

function updateBrowserStatus(browser) {
  state.browser = browser || null;
  updateAuthBanner(state.browser);
  text("status-browser", browser?.open ? "Open" : "Closed");
  text("status-login", browser?.loggedIn ? "Detected" : browser?.authRequired ? "Sign-in needed" : "Not detected");
  text("status-voice", browser?.voiceModeActive ? "Active" : "Inactive");
  text("status-current-voice", browser?.voice || "Not prefetched");
  const intelligenceUnavailable = Boolean(
    browser?.voiceModeActive &&
    !browser?.intelligence &&
    Array.isArray(browser?.availableIntelligence) &&
    browser.availableIntelligence.length === 0 &&
    (browser?.voice || browser?.language)
  );
  text("status-current-intelligence", browser?.intelligence || (intelligenceUnavailable ? "Unavailable on this account" : "Not prefetched"));
  text("status-current-language", browser?.language || "Not prefetched");
  text("status-thread", browser?.activeThread?.title || "—");
  text("status-mic", browser?.chatgptMicMuted === true ? "Muted" : browser?.chatgptMicMuted === false ? "Live" : "—");
  text("status-window", browser?.open ? (browser.windowHidden ? "Hidden" : "Visible") : "—");
  text("status-media", browser?.mediaConnected ? "Connected" : browser?.open ? "Waiting" : "—");
  text("status-audio-mode", state.config?.audioCaptureMode || "browser-media");
  text("status-channel", "Managed by Discord");
  const permissionNotice = $("status-mic-permission");
  if (permissionNotice) {
    permissionNotice.textContent = browser?.microphonePermissionRequired
      ? "ChatGPT needs microphone access. Show the browser, enable microphone access in Settings, then press Reconnect Voice."
      : "";
    permissionNotice.classList.toggle("hidden", !browser?.microphonePermissionRequired);
  }
  const select = $("status-thread-select");
  const selected = select.value;
  select.replaceChildren(new Option("Select a recent thread…", ""));
  for (const thread of browser?.recentThreads || []) select.add(new Option(thread.title, thread.id));
  if ([...select.options].some((option) => option.value === selected)) select.value = selected;
  $("status-show-browser").disabled = !browser?.open || !browser.windowHidden;
  $("status-hide-browser").disabled = !browser?.open || browser.windowHidden;
  const browserConfigReady = Boolean(
    state.config?.discordTokenConfigured &&
    String(state.config?.discordGuildId || "").trim()
  );
  $("status-start-browser").disabled = !browserConfigReady || Boolean(browser?.open);
  $("status-start-browser").title = browserConfigReady
    ? ""
    : !state.config?.discordTokenConfigured
      ? "Save the Discord bot token in Config before starting the browser."
      : "Save a Discord guild ID in Config before starting the browser.";
  $("status-reconnect").disabled = !browser?.open;
  $("status-toggle-mic").disabled = !browser?.voiceModeActive;
  $("status-toggle-mic").textContent = browser?.chatgptMicMuted === true ? "Unmute input" : "Mute input";
  populateDynamicSelect(
    "status-voice-select",
    browser?.voice ? "Change voice…" : "Voice options appear after Voice starts…",
    browser?.availableVoices,
    browser?.voice,
    (voice) => ({ value: String(voice.value || "").trim(), label: String(voice.label || voice.value || "").trim(), description: voice.description })
  );
  populateDynamicSelect(
    "status-intelligence-select",
    intelligenceUnavailable
      ? "Intelligence is not available for this account"
      : browser?.intelligence
        ? "Change intelligence…"
        : "Intelligence options appear after Voice starts…",
    browser?.availableIntelligence,
    browser?.intelligence
  );
  populateDynamicSelect(
    "status-language-select",
    browser?.language ? "Change language…" : "Language options appear after Voice starts…",
    browser?.availableLanguages,
    browser?.language
  );
  text("metric-duration", browser?.open ? (browser.voiceModeActive ? "Voice active" : "Ready") : "Closed");
  text("metric-browser-gap", browser?.mediaConnected ? "Yes" : "No");
  text("metric-transport-gap", browser?.mediaPacketsSent ?? 0);
  text("metric-underruns", browser?.mediaPacketsDropped ?? 0);
  text("metric-writes", browser?.mediaPacketsReceived ?? 0);
  text("metric-reader", browser?.mediaReaderOutputBytes > 0
    ? `${Math.round(browser.mediaReaderOutputBytes / 1024)} KB`
    : "Waiting");
  text("metric-output-capture", browser?.mediaOutputTrackCount > 0
    ? `${browser.mediaOutputTrackCount} track${browser.mediaOutputTrackCount === 1 ? "" : "s"}`
    : browser?.mediaOutputCaptureMode || "Waiting");
  text("metric-output-signal", browser?.mediaOutputWorkletFrames > 0
    ? `${Math.round((browser.mediaOutputWorkletPeak || 0) * 100)}% / ${browser.mediaOutputWorkletNonSilentFrames || 0} frames`
    : "Waiting");
  text("metric-input-queue", browser?.mediaConnected
    ? `${browser.mediaInputQueueDepth || 0}/${browser.mediaInputQueuePeakDepth || 0} frames`
    : "Waiting");
  text("metric-worklet-gap", browser?.mediaOutputWorkletFrames > 0
    ? `${Math.round(browser.mediaOutputWorkletMaxGapMs || 0)} ms`
    : "Waiting");
  text("metric-decoded", state.discord?.guildCount ?? 0);
  text("metric-users", browser?.browserProcessId || "—");
  text("metric-speakers", statusVoiceCount());
  const quality = browser?.mediaConnected ? "Quality: stable" : browser?.open ? "Quality: waiting" : "Quality: idle";
  text("performance-quality", quality);
  const health = $("performance-health");
  if (health) health.style.color = browser?.mediaConnected ? "var(--success)" : "var(--warning)";
}

function statusVoiceCount() {
  return state.discord?.connected ? "Discord ready" : "—";
}

function updateSystem(system) {
  state.system = system;
  text("system-version", system?.version);
  text("system-pid", system?.processId);
  text("system-uptime", formatDuration(system?.uptimeSeconds));
  text("system-platform", system?.platform);
  text("system-arch", system?.architecture);
  text("system-cpus", system?.cpuCount);
  text("system-memory", system?.memory);
  text("system-browser-executable", system?.browserExecutable || "Automatic");
  text("system-browser-profile", system?.browserProfile);
  text("system-browser-url", system?.browserUrl);
  text("system-browser-process", state.browser?.browserProcessId || "—");
  text("footer-uptime", formatDuration(system?.uptimeSeconds));
}

function drawChart(id, values, color = "#4c9be8") {
  const canvas = $(id);
  if (!canvas) return;
  const context = canvas.getContext("2d");
  const width = canvas.clientWidth || 560;
  const height = 190;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = width * ratio;
  canvas.height = height * ratio;
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  context.fillStyle = getComputedStyle(document.documentElement).getPropertyValue("--console").trim();
  context.fillRect(0, 0, width, height);
  context.strokeStyle = getComputedStyle(document.documentElement).getPropertyValue("--border").trim();
  context.beginPath();
  for (let i = 1; i < 5; i++) { const y = (height * i) / 5; context.moveTo(0, y); context.lineTo(width, y); }
  context.stroke();
  const points = values.length ? values : [0];
  const max = Math.max(1, ...points);
  context.strokeStyle = color;
  context.lineWidth = 2;
  context.beginPath();
  points.forEach((value, index) => {
    const x = points.length === 1 ? width / 2 : (index / (points.length - 1)) * width;
    const y = height - (Number(value) / max) * (height - 12) - 6;
    if (index === 0) context.moveTo(x, y); else context.lineTo(x, y);
  });
  context.stroke();
}

async function refreshStatus() {
  if (typeof invoke !== "function") return;
  try {
    const [discord, system] = await Promise.all([invoke("status"), invoke("system_status")]);
    updateDiscordStatus(discord);
    updateSystem(system);
    if (state.config?.discordGuildId) updateBrowserStatus(await invoke("browser_status", { guildId: state.config.discordGuildId }));
    else updateBrowserStatus(null);
    const sent = Number(state.browser?.mediaPacketsSent || 0);
    const received = Number(state.browser?.mediaPacketsReceived || 0);
    drawChart("audio-chart", [state.browser?.mediaConnected ? 1 : 0, state.browser?.voiceModeActive ? 1 : 0], "#46c77a");
    drawChart("cpu-chart", [state.lastBrowserPackets.sent, sent, state.lastBrowserPackets.received, received], "#4c9be8");
    state.lastBrowserPackets = { sent, received, dropped: Number(state.browser?.mediaPacketsDropped || 0) };
  } catch (error) {
    log(errorMessage(error), "ERROR");
  }
}

function updateUpdateStatus(update) {
  const banner = $("update-banner");
  if (!banner) return;
  banner.replaceChildren();
  if (update?.available && update.latestVersion && update.releaseUrl) {
    const message = document.createElement("strong");
    message.textContent = `GPTVoice v${update.latestVersion} is available. `;
    const link = document.createElement("a");
    link.href = update.releaseUrl;
    link.textContent = "Open the release page";
    link.rel = "noreferrer noopener";
    link.addEventListener("click", (event) => {
      event.preventDefault();
      void run("open_release_url", { url: update.releaseUrl }, "status-feedback");
    });
    banner.append(message, link, " to download the latest installer.");
    banner.classList.remove("hidden");
    text("status-update", `v${update.latestVersion} available`);
    return;
  }
  banner.classList.add("hidden");
  text("status-update", update?.checked ? `v${update.currentVersion} up to date` : "Unavailable");
}

async function checkForUpdate() {
  if (typeof invoke !== "function") return;
  try {
    updateUpdateStatus(await invoke("check_for_update"));
  } catch (error) {
    updateUpdateStatus({ checked: false });
    log(`Update check failed: ${errorMessage(error)}`, "ERROR");
  }
}

async function run(command, payload = {}, feedbackTarget = "status-feedback") {
  if (typeof invoke !== "function") {
    setFeedback("Open GPTVoice through the Tauri desktop app.", feedbackTarget, "error-text");
    return null;
  }
  try {
    const result = await invoke(command, payload);
    log(`${command} completed.`);
    await refreshStatus();
    return result;
  } catch (error) {
    const message = errorMessage(error);
    log(message, "ERROR");
    setFeedback(message, feedbackTarget, "error-text");
    return null;
  }
}

async function saveConfig({ quiet = false } = {}) {
  const token = $("config-discord-token").value.trim();
  const patch = {
    discordToken: token || null,
    discordGuildId: $("config-guild-id").value.trim(),
    chatgptUrl: $("config-chatgpt-url").value.trim(),
    browserExecutable: $("config-browser-executable").value.trim(),
    browserHideWhenReady: $("setting-browser-hidden").checked,
    audioCaptureMode: $("setting-audio-mode").value,
    audioInputVolume: Number($("setting-input-volume").value),
    audioOutputVolume: Number($("setting-output-volume").value),
    audioEarconsEnabled: $("setting-earcons").checked,
    audioEarconVolume: Number($("setting-earcon-volume").value),
    audioSampleRate: Number($("config-sample-rate").value),
    audioChannels: Number($("config-channels").value)
  };
  try {
    const config = await invoke("save_config", { patch });
    $("config-discord-token").value = "";
    populateConfig(config);
    if (!quiet) {
      setFeedback("Configuration saved.", "config-feedback");
      setFeedback("Configuration saved. Discord starts automatically when a valid token is configured.", "status-feedback");
      log("Saved configuration.");
    } else {
      setFeedback("Changes saved automatically.", "settings-feedback");
    }
    await refreshStatus();
  } catch (error) {
    setFeedback(errorMessage(error), quiet ? "settings-feedback" : "config-feedback", "error-text");
    log(errorMessage(error), "ERROR");
  }
}

function scheduleSettingsSave() {
  if (settingsSaveTimer !== null) window.clearTimeout(settingsSaveTimer);
  settingsSaveTimer = window.setTimeout(() => {
    settingsSaveTimer = null;
    void saveConfig({ quiet: true });
  }, 350);
}

function applyTheme() {
  const theme = $("setting-theme").value || "dark";
  document.documentElement.dataset.theme = theme;
  localStorage.setItem("gptvoice-theme", theme);
  const size = Math.min(20, Math.max(11, Number($("setting-font-size").value) || 13));
  document.documentElement.style.setProperty("--font-size", `${size}px`);
  localStorage.setItem("gptvoice-font-size", String(size));
}

function wireEvents() {
  document.querySelectorAll(".tab-button").forEach((button) => button.addEventListener("click", () => switchTab(button.dataset.tab)));
  $("console-clear").addEventListener("click", () => { state.logs = []; renderConsole(); });
  $("console-copy").addEventListener("click", () => navigator.clipboard?.writeText($("console-output").value));
  $("console-copy-selection").addEventListener("click", () => { const output = $("console-output"); navigator.clipboard?.writeText(output.value.slice(output.selectionStart, output.selectionEnd)); });
  $("console-search").addEventListener("input", renderConsole);
  $("console-autoscroll").addEventListener("change", renderConsole);
  $("setting-input-volume").addEventListener("input", updateVolumeLabels);
  $("setting-output-volume").addEventListener("input", updateVolumeLabels);
  [
    "setting-input-volume",
    "setting-output-volume",
    "setting-earcons",
    "setting-earcon-volume",
    "setting-browser-hidden",
    "setting-audio-mode"
  ].forEach((id) => $(id).addEventListener("change", scheduleSettingsSave));
  $("setting-theme").addEventListener("change", applyTheme);
  $("setting-font-size").addEventListener("input", applyTheme);
  $("settings-reset").addEventListener("click", () => { if (state.config) populateConfig(state.config); setFeedback("Settings reset.", "settings-feedback"); });
  $("config-save").addEventListener("click", () => void saveConfig());
  $("config-reset").addEventListener("click", () => { if (state.config) populateConfig(state.config); setFeedback("Configuration reset.", "config-feedback"); });
  $("setup-banner-config").addEventListener("click", () => {
    switchTab("config");
    if (!state.config?.discordTokenConfigured) $("config-discord-token")?.focus();
    else $("config-guild-id")?.focus();
  });
  $("setup-banner-status").addEventListener("click", () => switchTab("status"));
  $("status-start-browser").addEventListener("click", () => void run("browser_start", { guildId: state.config?.discordGuildId || "" }));
  $("auth-banner-show-browser").addEventListener("click", () => {
    if (!state.browser?.open) return setFeedback("The dedicated sign-in window is still starting. Complete sign-in there when it appears.", "status-feedback", "warning-text");
    void run("browser_set_visibility", { guildId: state.config.discordGuildId, hidden: false });
  });
  $("status-show-browser").addEventListener("click", () => void run("browser_set_visibility", { guildId: state.config.discordGuildId, hidden: false }));
  $("status-hide-browser").addEventListener("click", () => void run("browser_set_visibility", { guildId: state.config.discordGuildId, hidden: true }));
  $("status-refresh").addEventListener("click", () => void refreshStatus());
  $("status-reconnect").addEventListener("click", () => void run("browser_reconnect_voice", { guildId: state.config.discordGuildId }));
  $("performance-refresh").addEventListener("click", () => void refreshStatus());
  $("status-new-thread").addEventListener("click", () => void run("browser_new_thread", { guildId: state.config.discordGuildId }));
  $("status-resume-thread").addEventListener("click", () => {
    const threadId = $("status-thread-select").value;
    if (!threadId) return setFeedback("Select a recent thread first.", "status-feedback", "error-text");
    void run("browser_resume_thread", { guildId: state.config.discordGuildId, threadId });
  });
  $("status-toggle-mic").addEventListener("click", () => {
    const muted = state.browser?.chatgptMicMuted !== true;
    void run("set_mic_muted", { guildId: state.config.discordGuildId, muted });
  });
  $("status-voice-select").addEventListener("change", (event) => {
    if (event.target.value) void run("set_voice", { guildId: state.config.discordGuildId, voice: event.target.value });
  });
  $("status-intelligence-select").addEventListener("change", (event) => {
    if (event.target.value) void run("set_intelligence", { guildId: state.config.discordGuildId, intelligence: event.target.value });
  });
  $("status-language-select").addEventListener("change", (event) => {
    if (event.target.value) void run("set_language", { guildId: state.config.discordGuildId, language: event.target.value });
  });
}

async function initialize() {
  switchTab(state.activeTab);
  const storedTheme = localStorage.getItem("gptvoice-theme") || "dark";
  $("setting-theme").value = storedTheme;
  $("setting-font-size").value = localStorage.getItem("gptvoice-font-size") || "13";
  applyTheme();
  wireEvents();
  if (typeof invoke !== "function") return log("Tauri API unavailable; open the packaged desktop app.", "ERROR");
  if (typeof window.__TAURI__?.event?.listen === "function") {
    await window.__TAURI__.event.listen("runtime-log", (event) => log(event.payload));
    await window.__TAURI__.event.listen("discord-status", (event) => updateDiscordStatus(event.payload));
  }
  try {
    populateConfig(await invoke("get_config"));
    await refreshStatus();
    log("Native GPTVoice panel is ready.");
    void checkForUpdate();
  } catch (error) {
    log(errorMessage(error), "ERROR");
  }
  setInterval(() => void refreshStatus(), 5_000);
}

void initialize();
