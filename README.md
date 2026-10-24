# GPTVoice

## Talk with ChatGPT Voice in Discord

GPTVoice is a small native desktop app for macOS and Windows that brings a
ChatGPT Voice conversation into a Discord voice channel, so you and your
friends can talk with ChatGPT together.

It uses the normal ChatGPT website and your own ChatGPT account. You do not
need an OpenAI API key or a Realtime API subscription.

Each Discord server gets its own persistent ChatGPT browser profile. You can
use the same ChatGPT account in every server or sign into a different account
per server. The configured guild uses the main profile directory; additional
guilds receive isolated subprofiles automatically.

## What you need

- macOS on Apple Silicon (including M4) or Intel, or Windows 10 or newer.
  Tauri uses the native WebKit WebView on macOS and WebView2 on Windows.
- A Discord bot token.
- A ChatGPT account with Voice available.
- Brave, Chrome, Chromium, or Edge installed for the dedicated ChatGPT browser
  session. On macOS, GPTVoice detects the standard `.app` installations in
  `/Applications` and `~/Applications`.

On macOS, the browser is a separate Chromium process with its own profile and
CDP endpoint. The first time GPTVoice hides or shows that browser, macOS may
ask for Accessibility access. Grant it to GPTVoice in **System Settings →
Privacy & Security → Accessibility** (or to the launcher used during local
development) if you want the **Hide browser** and **Show browser** controls to work. The relay itself does not need a native
microphone recording permission: Discord receives audio through the bot, and
ChatGPT receives the mixed stream through the signed-in browser page.
- A Discord bot invitation created with the `bot` and `applications.commands`
  OAuth2 scopes. Give the bot these permissions: **View Channels**, **Send
  Messages**, **Embed Links**, **Read Message History**, **Connect**, and
  **Speak**. Administrator permission is not required.

## First-time setup

1. Download the latest `GPTVoice_<version>_x64-setup.exe` from Releases and
   install it.
2. Open GPTVoice and select the **Config** tab.
3. If you do not already have a Discord bot token, open the [Discord Developer
   Portal](https://discord.com/developers/applications), choose **New
   Application**, name it, and create it. Open the app's **Bot** page in the
   left sidebar. Under **Build-a-Bot**, choose **Reset Token**, confirm, and
   copy the new token immediately. Discord only shows a token when it is
   generated, and resetting it invalidates the previous token. Treat it like a
   password and never share it.
4. Invite the bot to your server. In the Developer Portal, open **OAuth2** →
   **URL Generator**, select the `bot` and `applications.commands` scopes,
   select **View Channels**, **Send Messages**, **Embed Links**, **Read Message
   History**, **Connect**, and **Speak** under Bot Permissions, then open the
   generated URL and authorize the bot for your server.
   Members who use `/join` also need **Use Application Commands** allowed in
   that text channel.
5. In GPTVoice, open **Config** and paste the bot token. To find the Discord
   server ID, open **User Settings** (the gear beside your username), scroll
   the settings list until you reach **Developer**, open it, and turn on
   **Developer Mode**. Then right-click the server name or icon and choose
   **Copy Server ID**. Paste that value into GPTVoice and choose **Save
   changes**. A saved token is shown only as stars. GPTVoice starts the Discord
   connection automatically. The guild ID selects the server used for startup
   status and thread prefetching; other servers can still use the global slash
   commands.
6. Join a Discord voice channel and run `/join` in a text channel where the
   bot can send messages.
7. On the first run, GPTVoice opens its dedicated browser profile. Sign into
   ChatGPT and complete any human verification yourself. Then run `/join`
   again.
8. GPTVoice prefetches the five most recent ChatGPT threads automatically when
   the dedicated browser session starts. In the desktop **Status** tab, choose
   **New thread + Voice** or select a recent thread and choose **Resume + Voice**.

After Voice starts, the Status tab shows the active Voice, Intelligence, and
Language values prefetched from ChatGPT. If ChatGPT reports that microphone
access must be enabled in Settings, choose **Show browser**, grant the
permission, then choose **Reconnect Voice**. Reconnect refreshes the ChatGPT
page before starting Voice again.

Some free ChatGPT accounts do not expose an **Intelligence** setting in Voice.
GPTVoice treats that setting as unavailable and still discovers and controls
the available Voice and Language options.

GPTVoice can hide the dedicated browser after login. Use **Show browser** in
the desktop Status tab whenever you need to inspect it. The bot owner does not
need to remain in the Discord voice channel after the bot has joined.

## Desktop panel

GPTVoice opens a native desktop control panel with:

- Console logs with search, copy, and auto-scroll.
- Discord, browser, Voice, thread, and media status.
- Discord starts automatically when GPTVoice launches and stops cleanly when the
  desktop window closes.
- Audio and performance information.
- System and browser-profile information.
- Appearance, audio, browser, and masked-token configuration. Audio and browser
  settings are saved automatically when changed; volume changes also apply to
  an active relay immediately.

Closing the GPTVoice window stops GPTVoice and its dedicated browser sessions.
It does not close the user's normal Brave, Chrome, or Edge windows.

## Discord member panel

After a successful `/join`, GPTVoice posts one shared panel in the server. The
panel is intentionally small so members do not need access to the desktop app:

- **Join voice** and **Leave voice** control this server's Discord voice relay.
- **Mute input** and **Unmute input** control the ChatGPT microphone stream.
- **Reconnect** refreshes the ChatGPT Voice connection for the active thread.
- The panel shows the active thread, current Voice, Intelligence, and Language.

Button presses edit the same message instead of posting a new message each
time. GPTVoice removes the tracked panel when the app shuts down normally.
The local desktop panel remains the administrator's place for ChatGPT threads,
Voice settings, browser visibility, and configuration.

## Audio and privacy

GPTVoice is a local, user-operated relay. It does not send voice data to a
GPTVoice server or store a central copy. When a guild session is active,
Discord voice audio is received by the local app, mixed into one ChatGPT
microphone stream, and sent through the signed-in ChatGPT web session to
ChatGPT/OpenAI.

GPTVoice does not currently include an in-app consent-management system. The
person operating GPTVoice must inform everyone in the voice channel, before
enabling the relay, that their voice may be sent to ChatGPT/OpenAI. They are
responsible for obtaining any permission required by applicable law, Discord
rules, or server rules.

Do not use GPTVoice to relay audio from people who have not been informed.
Participants who do not want to take part should leave or mute the voice
channel, or ask the operator to stop the relay.

GPTVoice does not identify speakers to ChatGPT. Participating voices are mixed
into one shared conversation. The app may use temporary in-memory audio
buffers for real-time processing, but it does not intentionally save voice
recordings.

OpenAI's processing is governed by the signed-in ChatGPT account and OpenAI's
policies. GPTVoice cannot control OpenAI's retention, training, or regional
processing. This section is a technical disclosure, not legal advice.

GPTVoice stores its settings here:

```text
Windows: %LOCALAPPDATA%\GPTVoice\config.json
macOS:   ~/Library/Application Support/com.gptvoice.desktop.tauri/config.json
```

The file contains the Discord bot token. The dedicated browser profiles under
the same folder contain browser login data. Protect both locations like a
password and never upload them. The path is resolved through Tauri's
platform-local application-data directory; do not hard-code it in scripts or
backup tooling.

## Slash commands

- `/join` - join the current voice channel and start the guild relay.
- `/leave` - leave the voice channel and stop the guild session.
- `/status` - show whether the Discord gateway is connected.

`/join` creates the public member panel after the relay connects. Members can
then use its voice, input-mute, and reconnect controls. ChatGPT threads, Voice
settings, browser visibility, and administrator configuration remain managed
from the local desktop Status and Config tabs.

## For developers

Building requires a current Rust toolchain and the Tauri CLI. Windows builds
also need the WebView2 runtime and Windows C++ build tools. macOS builds use
the system WebKit WebView and Xcode Command Line Tools; no extra audio driver
or Node.js runtime is required by the packaged app. On Apple Silicon, install
the arm64 Rust target and build natively to produce a native M4-compatible
bundle.

```sh
cargo test --manifest-path src-tauri/Cargo.toml
cargo tauri dev
cargo tauri build
```

If Node.js is available, `npm test` runs the small browser-asset check as well.

The current app is organized as:

```text
src-tauri/            Rust/Tauri desktop host, browser relay, and Discord runtime
tauri-ui/             Native control panel frontend
src/browser/          Browser-injected media bridge asset used by the Rust host
test/                 Browser-asset checks; native tests live with the Rust code
src-tauri/icons/      Cross-platform application icons
```

On Windows, `cargo tauri build` produces the configured Windows bundles. On
macOS, it produces an `.app` and `.dmg` under
`src-tauri/target/release/bundle/`. The Tauri configuration selects the
platform-appropriate targets while retaining the Windows icon and installer
metadata.

### Porting notes

- The Discord/Songbird relay, PCM queues, WebSocket transport, and browser
  media bridge are platform-neutral; they do not call WASAPI or CoreAudio.
- Browser discovery is platform-specific. macOS resolves the executable inside
  the standard Brave, Chrome, Chromium, or Edge application bundle and also
  accepts an explicit executable path in Config.
- Browser launch uses a dedicated Unix process group on macOS so Chromium
  helper processes are terminated when GPTVoice exits. Windows retains its
  existing process-tree cleanup.
- Browser visibility uses macOS System Events through `osascript`. If
  Accessibility access is denied, the browser remains usable and only the
  hide/show convenience controls are unavailable.
- ChatGPT Web Voice is controlled through the public browser page and CDP. A
  future ChatGPT UI change can still require updates to the DOM discovery
  scripts; this is independent of the host operating system.

## License

GPTVoice is released under the MIT License. See [LICENSE](LICENSE).

The MIT License allows people to use, modify, and redistribute GPTVoice as
long as they keep the copyright and license notice.

## Trademarks

GPTVoice is an independent hobby project. It is not affiliated with or
endorsed by OpenAI, ChatGPT, Discord, Brave, Chrome, or Edge.
