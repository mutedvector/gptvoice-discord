# GPTVoice

## Talk with ChatGPT Voice in Discord

GPTVoice is a small native Windows app that brings a ChatGPT Voice conversation
into a Discord voice channel, so you and your friends can talk with ChatGPT
together.

It uses the normal ChatGPT website and your own ChatGPT account. You do not
need an OpenAI API key or a Realtime API subscription.

Each Discord server gets its own persistent ChatGPT browser profile. You can
use the same ChatGPT account in every server or sign into a different account
per server. The configured guild uses the main profile directory; additional
guilds receive isolated subprofiles automatically.

## What you need

- Windows 10 or newer with WebView2 installed (it is already present on most
  Windows systems).
- A Discord bot token.
- A ChatGPT account with Voice available.
- Brave, Chrome, or Edge installed for the dedicated ChatGPT browser session.
- A Discord bot invitation with permission to view channels, send messages,
  connect to voice channels, and speak.

## First-time setup

1. Download the latest `GPTVoice_<version>_x64-setup.exe` from Releases and
   install it.
2. Open GPTVoice and select the **Config** tab.
3. Paste the Discord bot token, enter the Discord server ID for the desktop
   session, and choose **Save changes**. A saved token is shown only as stars.
   GPTVoice starts the Discord connection automatically. The guild ID selects
   the server used for startup status and thread prefetching; other servers can
   still use the global slash commands.
4. Join a Discord voice channel and run `/join` in a text channel where the
   bot can send messages.
5. On the first run, GPTVoice opens its dedicated browser profile. Sign into
   ChatGPT and complete any human verification yourself. Then run `/join`
   again.
6. GPTVoice prefetches the five most recent ChatGPT threads automatically when
   the dedicated browser session starts. In the desktop **Status** tab, choose
   **New thread + Voice** or select a recent thread and choose **Resume + Voice**.

After Voice starts, the Status tab shows the active Voice, Intelligence, and
Language values prefetched from ChatGPT. If ChatGPT reports that microphone
access must be enabled in Settings, choose **Show browser**, grant the
permission, then choose **Reconnect Voice**. Reconnect refreshes the ChatGPT
page before starting Voice again.

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

GPTVoice mixes Discord voices into one ChatGPT microphone stream. ChatGPT is
not told which person is speaking, and simultaneous speech can overlap.

GPTVoice stores its settings here:

```text
%LOCALAPPDATA%\GPTVoice\config.json
```

The file contains the Discord bot token. The dedicated browser profiles under
the same folder contain browser login data. Protect both locations like a
password and never upload them.

## Slash commands

- `/join` - join the current voice channel and start the guild relay.
- `/leave` - leave the voice channel and stop the guild session.
- `/status` - show whether the Discord gateway is connected.

`/join` creates the public member panel after the relay connects. Members can
then use its voice, input-mute, and reconnect controls. ChatGPT threads, Voice
settings, browser visibility, and administrator configuration remain managed
from the local desktop Status and Config tabs.

## For developers

Building the Windows app requires a current Rust toolchain, the Tauri CLI,
WebView2, and the Windows C++ build tools. The packaged app does not require a
Node.js runtime, Playwright download, or extra Windows audio driver.

```powershell
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
src-tauri/icons/      Windows application icons
```

The release installer is produced in `src-tauri/target/release/bundle/nsis/`.

## License

GPTVoice is released under the MIT License. See [LICENSE](LICENSE).

The MIT License allows people to use, modify, and redistribute GPTVoice as
long as they keep the copyright and license notice.

## Trademarks

GPTVoice is an independent hobby project. It is not affiliated with or
endorsed by OpenAI, ChatGPT, Discord, Brave, Chrome, or Edge.
