use futures_util::{SinkExt, StreamExt};
use std::{
    collections::VecDeque,
    io::{self, Read, Seek, SeekFrom},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
use symphonia_core::io::MediaSource;
use tauri::Emitter;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        http::{Response as HttpResponse, StatusCode},
        Message,
    },
};

// Keep the browser input live instead of allowing stale speech to sit in a
// multi-second FIFO when Chromium is briefly scheduled late. At 20 ms per
// packet this is an 80 ms real-time handoff buffer.
const BROWSER_PACKET_QUEUE: usize = 4;
// Keep output live too. Eight browser packets are about 160 ms at the
// browser's normal 20 ms cadence; stale packets are discarded if a renderer
// releases a stale burst after being backgrounded.
const DISCORD_PACKET_QUEUE: usize = 8;
const PCM_READ_WAIT: Duration = Duration::from_millis(30);
pub const PCM_SAMPLE_RATE: u32 = 48_000;
pub const PCM_CHANNELS: u32 = 2;
const PCM_FRAMES_PER_PACKET: usize = PCM_SAMPLE_RATE as usize / 50;
const PCM_INPUT_BYTES_PER_FRAME: usize =
    PCM_FRAMES_PER_PACKET * PCM_CHANNELS as usize * std::mem::size_of::<i16>();
const PCM_OUTPUT_BYTES_PER_PACKET: usize =
    PCM_FRAMES_PER_PACKET * PCM_CHANNELS as usize * std::mem::size_of::<f32>();
const PCM_JITTER_MIN_FRAMES: usize = 3;
const PCM_JITTER_MAX_FRAMES: usize = 8;
const PCM_JITTER_UNDERRUNS_BEFORE_RESYNC: usize = 3;
const PCM_JITTER_STABLE_FRAMES_BEFORE_RELAX: usize = 750;
const LATENCY_NEW_TURN_GAP: Duration = Duration::from_millis(350);
const LATENCY_RESPONSE_QUIET_GAP: Duration = Duration::from_millis(1_000);
const LATENCY_RESPONSE_MATCH_WINDOW: Duration = Duration::from_millis(15_000);
const LATENCY_SIGNAL_THRESHOLD: i32 = 256;

struct RealtimePacketQueue {
    packets: Mutex<VecDeque<Vec<u8>>>,
    notify: Notify,
    closed: AtomicBool,
    capacity: usize,
    peak_depth: AtomicU64,
    dropped_oldest: AtomicU64,
}

struct RealtimePcmQueue {
    packets: Mutex<VecDeque<Vec<u8>>>,
    available: Condvar,
    closed: AtomicBool,
    capacity: usize,
    peak_depth: AtomicU64,
    dropped_oldest: AtomicU64,
}

impl RealtimePcmQueue {
    fn new(capacity: usize) -> Self {
        Self {
            packets: Mutex::new(VecDeque::with_capacity(capacity)),
            available: Condvar::new(),
            closed: AtomicBool::new(false),
            capacity,
            peak_depth: AtomicU64::new(0),
            dropped_oldest: AtomicU64::new(0),
        }
    }

    fn push(&self, packet: Vec<u8>) -> Result<bool, ()> {
        let mut packets = self.packets.lock().map_err(|_| ())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(());
        }
        let dropped = if packets.len() >= self.capacity {
            packets.pop_front();
            self.dropped_oldest.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        };
        packets.push_back(packet);
        let depth = packets.len() as u64;
        self.peak_depth.fetch_max(depth, Ordering::Relaxed);
        drop(packets);
        self.available.notify_one();
        Ok(dropped)
    }

    fn receive_timeout(&self, timeout: Duration) -> Option<Vec<u8>> {
        let deadline = Instant::now() + timeout;
        let mut packets = self.packets.lock().ok()?;
        loop {
            if let Some(packet) = packets.pop_front() {
                return Some(packet);
            }
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, result) = self.available.wait_timeout(packets, remaining).ok()?;
            packets = next;
            if result.timed_out() && packets.is_empty() {
                return None;
            }
        }
    }

    fn clear(&self) {
        if let Ok(mut packets) = self.packets.lock() {
            packets.clear();
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.available.notify_all();
    }
}

impl RealtimePacketQueue {
    fn new(capacity: usize) -> Self {
        Self {
            packets: Mutex::new(VecDeque::with_capacity(capacity)),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
            capacity,
            peak_depth: AtomicU64::new(0),
            dropped_oldest: AtomicU64::new(0),
        }
    }

    fn push(&self, packet: Vec<u8>) -> Result<bool, ()> {
        let mut packets = self.packets.lock().map_err(|_| ())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(());
        }
        let dropped = if packets.len() >= self.capacity {
            packets.pop_front();
            self.dropped_oldest.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        };
        packets.push_back(packet);
        let depth = packets.len() as u64;
        self.peak_depth.fetch_max(depth, Ordering::Relaxed);
        drop(packets);
        self.notify.notify_one();
        Ok(dropped)
    }

    async fn pop(&self) -> Option<Vec<u8>> {
        loop {
            let notified = self.notify.notified();
            if let Ok(mut packets) = self.packets.lock() {
                if let Some(packet) = packets.pop_front() {
                    return Some(packet);
                }
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
            } else {
                return None;
            }
            notified.await;
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn depth(&self) -> u64 {
        self.packets
            .lock()
            .map(|packets| packets.len() as u64)
            .unwrap_or_default()
    }
}

struct TransportInner {
    endpoint: String,
    token: String,
    to_browser: Mutex<Option<Arc<RealtimePacketQueue>>>,
    from_browser: Arc<RealtimePcmQueue>,
    reader: Mutex<Option<Arc<RealtimePcmQueue>>>,
    shutdown: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    next_connection_id: AtomicU64,
    active_connection_id: AtomicU64,
    connected: AtomicBool,
    packets_sent: AtomicU64,
    packets_dropped: AtomicU64,
    packets_received: AtomicU64,
    reader_reads: AtomicU64,
    reader_source_bytes: AtomicU64,
    reader_output_bytes: AtomicU64,
    reader_silence_bytes: AtomicU64,
    input_gain_bits: AtomicU32,
    output_gain_bits: AtomicU32,
    browser_diagnostics: Mutex<BrowserBridgeDiagnostics>,
    latency: Mutex<LatencyState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatencyOutputKind {
    Response,
    Autonomous,
}

struct LatencyInputTurn {
    turn: u64,
    first_at: Instant,
    last_at: Instant,
    response_started: bool,
    interrupted: bool,
}

struct LatencyOutput {
    kind: LatencyOutputKind,
    turn: Option<u64>,
    first_input_at: Option<Instant>,
    last_input_at: Option<Instant>,
    started_at: Instant,
    last_at: Instant,
    discord_output_at: Option<Instant>,
    interrupted: bool,
    interruption_turn: Option<u64>,
}

#[derive(Default)]
struct LatencyState {
    app: Option<tauri::AppHandle>,
    guild_id: String,
    turn: u64,
    input: Option<LatencyInputTurn>,
    output: Option<LatencyOutput>,
}

impl TransportInner {
    fn input_gain(&self) -> f32 {
        f32::from_bits(self.input_gain_bits.load(Ordering::Relaxed)).clamp(0.0, 4.0)
    }

    fn output_gain(&self) -> f32 {
        f32::from_bits(self.output_gain_bits.load(Ordering::Relaxed)).clamp(0.0, 4.0)
    }

    fn set_input_gain(&self, gain: f32) {
        self.input_gain_bits
            .store(gain.clamp(0.0, 4.0).to_bits(), Ordering::Relaxed);
    }

    fn set_output_gain(&self, gain: f32) {
        self.output_gain_bits
            .store(gain.clamp(0.0, 4.0).to_bits(), Ordering::Relaxed);
    }

    fn emit_latency_report(&self, message: String) {
        let (app, guild_id) = self
            .latency
            .lock()
            .map(|latency| (latency.app.clone(), latency.guild_id.clone()))
            .unwrap_or((None, String::new()));
        let message = format!(
            "[latency:{}] {message}",
            if guild_id.is_empty() {
                "unknown"
            } else {
                &guild_id
            }
        );
        log::info!("{message}");
        if let Some(app) = app {
            let _ = app.emit("runtime-log", message);
        }
    }

    fn record_input_pcm(&self, samples: &[i16]) {
        if !has_i16_signal(samples) {
            return;
        }
        let now = Instant::now();
        let mut event = None;
        if let Ok(mut latency) = self.latency.lock() {
            let output_active = latency
                .output
                .as_ref()
                .map(|output| now.duration_since(output.last_at) <= LATENCY_RESPONSE_QUIET_GAP)
                .unwrap_or(false);

            if output_active {
                let already_interrupted = latency
                    .output
                    .as_ref()
                    .and_then(|output| output.interruption_turn)
                    .is_some();
                if !already_interrupted {
                    latency.turn += 1;
                    let turn = latency.turn;
                    latency.input = Some(LatencyInputTurn {
                        turn,
                        first_at: now,
                        last_at: now,
                        response_started: false,
                        interrupted: true,
                    });
                    if let Some(output) = latency.output.as_mut() {
                        output.interruption_turn = Some(turn);
                        output.interrupted = true;
                    }
                    event = Some(format!(
                        "turn {turn}: Discord speech interrupted ChatGPT output"
                    ));
                } else if let Some(input) = latency.input.as_mut() {
                    input.last_at = now;
                }
            } else {
                let starts_new_turn = latency
                    .input
                    .as_ref()
                    .map(|input| now.duration_since(input.last_at) > LATENCY_NEW_TURN_GAP)
                    .unwrap_or(true);
                if starts_new_turn {
                    latency.turn += 1;
                    let turn = latency.turn;
                    latency.input = Some(LatencyInputTurn {
                        turn,
                        first_at: now,
                        last_at: now,
                        response_started: false,
                        interrupted: false,
                    });
                } else if let Some(input) = latency.input.as_mut() {
                    input.last_at = now;
                }
            }
        }
        if let Some(event) = event {
            self.emit_latency_report(event);
        }
    }

    fn record_browser_output_packet(&self, packet: &[u8]) {
        if !has_i16_byte_signal(packet) {
            return;
        }
        let now = Instant::now();
        let mut event = None;
        if let Ok(mut latency) = self.latency.lock() {
            if let Some(output) = latency.output.as_mut() {
                if now.duration_since(output.last_at) <= LATENCY_RESPONSE_QUIET_GAP {
                    output.last_at = now;
                    return;
                }
            }

            let input = latency.input.as_mut().and_then(|input| {
                if !input.response_started
                    && now.duration_since(input.last_at) <= LATENCY_RESPONSE_MATCH_WINDOW
                {
                    input.response_started = true;
                    Some((input.turn, input.first_at, input.last_at, input.interrupted))
                } else {
                    None
                }
            });
            let (kind, turn, first_input_at, last_input_at, interrupted) = match input {
                Some((turn, first, last, interrupted)) => (
                    LatencyOutputKind::Response,
                    Some(turn),
                    Some(first),
                    Some(last),
                    interrupted,
                ),
                None => (LatencyOutputKind::Autonomous, None, None, None, false),
            };
            latency.output = Some(LatencyOutput {
                kind,
                turn,
                first_input_at,
                last_input_at,
                started_at: now,
                last_at: now,
                discord_output_at: None,
                interrupted,
                interruption_turn: None,
            });
            if kind == LatencyOutputKind::Autonomous {
                event = Some("ChatGPT initiated autonomous speech".to_owned());
            }
        }
        if let Some(event) = event {
            self.emit_latency_report(event);
        }
    }

    fn record_discord_output(&self) {
        let report = if let Ok(mut latency) = self.latency.lock() {
            let Some(output) = latency.output.as_mut() else {
                return;
            };
            let now = Instant::now();
            if output.discord_output_at.is_some() {
                return;
            }
            output.discord_output_at = Some(now);
            let relay_ms = now.duration_since(output.started_at).as_millis();
            match output.kind {
                LatencyOutputKind::Response => {
                    let Some(first_input_at) = output.first_input_at else {
                        return;
                    };
                    let Some(last_input_at) = output.last_input_at else {
                        return;
                    };
                    let label = if output.interrupted {
                        "response after interruption"
                    } else {
                        "response"
                    };
                    Some(format!(
                        "turn {} ({label}): first Discord speech -> ChatGPT audio {} ms; last Discord speech -> ChatGPT audio {} ms; ChatGPT audio -> Discord relay handoff {} ms; first speech -> Discord relay handoff {} ms",
                        output.turn.unwrap_or_default(),
                        output.started_at.duration_since(first_input_at).as_millis(),
                        output.started_at.duration_since(last_input_at).as_millis(),
                        relay_ms,
                        now.duration_since(first_input_at).as_millis(),
                    ))
                }
                LatencyOutputKind::Autonomous => Some(format!(
                    "autonomous ChatGPT speech: ChatGPT audio -> Discord relay handoff {} ms",
                    relay_ms
                )),
            }
        } else {
            None
        };

        if let Some(report) = report {
            self.emit_latency_report(report);
        }
    }
}

#[derive(Clone)]
pub struct BrowserMediaTransport {
    inner: Arc<TransportInner>,
}

impl BrowserMediaTransport {
    pub async fn start() -> Result<Arc<Self>, String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|error| format!("Could not start browser media transport: {error}"))?;
        let port = listener
            .local_addr()
            .map_err(|error| format!("Could not read browser media transport port: {error}"))?
            .port();
        let token = make_transport_token();
        let from_browser = Arc::new(RealtimePcmQueue::new(DISCORD_PACKET_QUEUE));
        let (shutdown, shutdown_receiver) = tokio::sync::oneshot::channel();
        let transport = Arc::new(Self {
            inner: Arc::new(TransportInner {
                endpoint: format!("ws://127.0.0.1:{port}"),
                token,
                to_browser: Mutex::new(None),
                from_browser: Arc::clone(&from_browser),
                reader: Mutex::new(Some(from_browser)),
                shutdown: Mutex::new(Some(shutdown)),
                next_connection_id: AtomicU64::new(1),
                active_connection_id: AtomicU64::new(0),
                connected: AtomicBool::new(false),
                packets_sent: AtomicU64::new(0),
                packets_dropped: AtomicU64::new(0),
                packets_received: AtomicU64::new(0),
                reader_reads: AtomicU64::new(0),
                reader_source_bytes: AtomicU64::new(0),
                reader_output_bytes: AtomicU64::new(0),
                reader_silence_bytes: AtomicU64::new(0),
                input_gain_bits: AtomicU32::new(1.0_f32.to_bits()),
                output_gain_bits: AtomicU32::new(1.0_f32.to_bits()),
                browser_diagnostics: Mutex::new(BrowserBridgeDiagnostics::default()),
                latency: Mutex::new(LatencyState::default()),
            }),
        });
        let server_transport = Arc::clone(&transport);
        tauri::async_runtime::spawn(async move {
            run_transport_server(listener, shutdown_receiver, server_transport).await;
        });
        Ok(transport)
    }

    pub fn set_latency_context(&self, app: &tauri::AppHandle, guild_id: &str) {
        if let Ok(mut latency) = self.inner.latency.lock() {
            latency.app = Some(app.clone());
            latency.guild_id = guild_id.to_owned();
        }
    }

    pub fn browser_script(&self) -> String {
        let source = include_str!("../../src/browser/media-bridge.js");
        let start_marker = "const BROWSER_INIT_SCRIPT = String.raw`";
        let Some(start) = source
            .find(start_marker)
            .map(|index| index + start_marker.len())
        else {
            return String::new();
        };
        let Some(template) = source[start..].trim().strip_suffix("`;") else {
            return String::new();
        };
        template
            .replace(
                "__GPTVOICE_TRANSPORT_URL__",
                &serde_json::to_string(&self.inner.endpoint).unwrap_or_default(),
            )
            .replace(
                "__GPTVOICE_TRANSPORT_TOKEN__",
                &serde_json::to_string(&self.inner.token).unwrap_or_default(),
            )
    }

    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Relaxed)
    }

    pub fn take_pcm_reader_with_gain(&self, gain: f32) -> Result<PcmReader, String> {
        self.set_output_gain(gain);
        let receiver = self
            .inner
            .reader
            .lock()
            .map_err(|_| "Browser media reader lock was poisoned".to_owned())?
            .take()
            .ok_or_else(|| "The browser media output is already attached to Discord".to_owned())?;
        Ok(PcmReader::new(receiver, gain, Arc::clone(&self.inner)))
    }

    pub fn set_input_gain(&self, gain: f32) {
        self.inner.set_input_gain(gain);
    }

    pub fn set_output_gain(&self, gain: f32) {
        self.inner.set_output_gain(gain);
    }

    pub fn send_pcm_i16(&self, samples: &[i16]) -> bool {
        if samples.is_empty() {
            return true;
        }
        let gain = self.inner.input_gain();
        let mut bytes = Vec::with_capacity(samples.len() * 2);
        for sample in samples {
            let scaled = ((*sample as f32) * gain)
                .round()
                .clamp(i16::MIN as f32, i16::MAX as f32) as i16;
            bytes.extend_from_slice(&scaled.to_le_bytes());
        }
        let queue = self
            .inner
            .to_browser
            .lock()
            .ok()
            .and_then(|queue| queue.clone());
        let Some(queue) = queue else {
            return false;
        };
        match queue.push(bytes) {
            Ok(dropped_oldest) => {
                if dropped_oldest {
                    self.inner.packets_dropped.fetch_add(1, Ordering::Relaxed);
                }
                self.inner.record_input_pcm(samples);
                self.inner.packets_sent.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.inner.packets_dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub fn stop(&self) {
        if let Ok(mut shutdown) = self.inner.shutdown.lock() {
            if let Some(sender) = shutdown.take() {
                let _ = sender.send(());
            }
        }
        if let Ok(mut queue) = self.inner.to_browser.lock() {
            if let Some(queue) = queue.take() {
                queue.close();
            }
        }
        self.inner.from_browser.close();
        self.inner.connected.store(false, Ordering::Relaxed);
    }

    pub fn diagnostics(&self) -> BrowserMediaDiagnostics {
        let browser = self
            .inner
            .browser_diagnostics
            .lock()
            .map(|diagnostics| diagnostics.clone())
            .unwrap_or_default();
        let input_queue = self
            .inner
            .to_browser
            .lock()
            .ok()
            .and_then(|queue| queue.clone());
        BrowserMediaDiagnostics {
            connected: self.is_connected(),
            packets_sent: self.inner.packets_sent.load(Ordering::Relaxed),
            packets_dropped: self.inner.packets_dropped.load(Ordering::Relaxed),
            input_queue_depth: input_queue
                .as_ref()
                .map(|queue| queue.depth())
                .unwrap_or_default(),
            input_queue_peak_depth: input_queue
                .as_ref()
                .map(|queue| queue.peak_depth.load(Ordering::Relaxed))
                .unwrap_or_default(),
            input_queue_dropped: input_queue
                .as_ref()
                .map(|queue| queue.dropped_oldest.load(Ordering::Relaxed))
                .unwrap_or_default(),
            packets_received: self.inner.packets_received.load(Ordering::Relaxed),
            reader_reads: self.inner.reader_reads.load(Ordering::Relaxed),
            reader_source_bytes: self.inner.reader_source_bytes.load(Ordering::Relaxed),
            reader_output_bytes: self.inner.reader_output_bytes.load(Ordering::Relaxed),
            reader_silence_bytes: self.inner.reader_silence_bytes.load(Ordering::Relaxed),
            output_capture_mode: browser.output_capture_mode,
            output_track_count: browser.output_track_count,
            output_callbacks: browser.output_callbacks,
            output_dropped_callbacks: browser.output_dropped_callbacks,
            output_attach_errors: browser.output_attach_errors,
            output_worklet_captures: browser.output_worklet_captures,
            output_worklet_fallbacks: browser.output_worklet_fallbacks,
            output_worklet_frames: browser.output_worklet_frames,
            output_worklet_peak: browser.output_worklet_peak,
            output_worklet_non_silent_frames: browser.output_worklet_non_silent_frames,
            output_worklet_max_gap_ms: browser.output_worklet_max_gap_ms,
            input_frames: browser.input_frames,
            input_silence_frames: browser.input_silence_frames,
            browser_input_queue_samples: browser.input_queue_samples,
            browser_input_queue_peak_samples: browser.input_queue_peak_samples,
            browser_input_queue_depth: browser.input_queue_depth,
            browser_input_dropped_messages: browser.input_dropped_messages,
            browser_input_last_frame_at: browser.input_last_frame_at,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserMediaDiagnostics {
    pub connected: bool,
    pub packets_sent: u64,
    pub packets_dropped: u64,
    pub input_queue_depth: u64,
    pub input_queue_peak_depth: u64,
    pub input_queue_dropped: u64,
    pub packets_received: u64,
    pub reader_reads: u64,
    pub reader_source_bytes: u64,
    pub reader_output_bytes: u64,
    pub reader_silence_bytes: u64,
    pub output_capture_mode: Option<String>,
    pub output_track_count: u64,
    pub output_callbacks: u64,
    pub output_dropped_callbacks: u64,
    pub output_attach_errors: u64,
    pub output_worklet_captures: u64,
    pub output_worklet_fallbacks: u64,
    pub output_worklet_frames: u64,
    pub output_worklet_peak: f64,
    pub output_worklet_non_silent_frames: u64,
    pub output_worklet_max_gap_ms: f64,
    pub input_frames: u64,
    pub input_silence_frames: u64,
    pub browser_input_queue_samples: u64,
    pub browser_input_queue_peak_samples: u64,
    pub browser_input_queue_depth: u64,
    pub browser_input_dropped_messages: u64,
    pub browser_input_last_frame_at: u64,
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct BrowserBridgeDiagnostics {
    output_capture_mode: Option<String>,
    output_track_count: u64,
    output_callbacks: u64,
    output_dropped_callbacks: u64,
    output_attach_errors: u64,
    output_worklet_captures: u64,
    output_worklet_fallbacks: u64,
    output_worklet_frames: u64,
    output_worklet_peak: f64,
    output_worklet_non_silent_frames: u64,
    output_worklet_max_gap_ms: f64,
    input_frames: u64,
    input_silence_frames: u64,
    input_queue_samples: u64,
    input_queue_peak_samples: u64,
    input_queue_depth: u64,
    input_dropped_messages: u64,
    input_last_frame_at: u64,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserBridgeStatsMessage {
    kind: String,
    diagnostics: BrowserBridgeDiagnostics,
}

async fn run_transport_server(
    listener: TcpListener,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    transport: Arc<BrowserMediaTransport>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { break; };
                let connection_transport = Arc::clone(&transport);
                tauri::async_runtime::spawn(async move {
                    let _ = handle_transport_connection(stream, connection_transport).await;
                });
            }
        }
    }
    transport.inner.connected.store(false, Ordering::Relaxed);
}

#[allow(clippy::result_large_err)]
async fn handle_transport_connection(
    stream: TcpStream,
    transport: Arc<BrowserMediaTransport>,
) -> Result<(), String> {
    let expected_token = transport.inner.token.clone();
    let socket = accept_hdr_async(stream, move |request: &Request, response: Response| {
        let token = request.uri().query().and_then(|query| {
            query
                .split('&')
                .find_map(|part| part.strip_prefix("token="))
        });
        if token == Some(expected_token.as_str()) {
            Ok(response)
        } else {
            Err(HttpResponse::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body(Some(
                    "GPTVoice browser transport rejected this connection".to_owned(),
                ))
                .expect("valid websocket rejection response"))
        }
    })
    .await
    .map_err(|error| format!("Browser media WebSocket handshake failed: {error}"))?;
    let (mut writer, mut reader) = socket.split();
    let connection_id = transport
        .inner
        .next_connection_id
        .fetch_add(1, Ordering::Relaxed);
    let queue = Arc::new(RealtimePacketQueue::new(BROWSER_PACKET_QUEUE));
    if let Ok(mut active) = transport.inner.to_browser.lock() {
        if let Some(previous) = active.replace(Arc::clone(&queue)) {
            previous.close();
        }
    }
    transport
        .inner
        .active_connection_id
        .store(connection_id, Ordering::Relaxed);
    transport.inner.from_browser.clear();
    transport.inner.connected.store(true, Ordering::Relaxed);

    let writer_queue = Arc::clone(&queue);
    let writer_task = tauri::async_runtime::spawn(async move {
        while let Some(packet) = writer_queue.pop().await {
            if writer.send(Message::Binary(packet.into())).await.is_err() {
                break;
            }
        }
        let _ = writer.close().await;
    });

    while let Some(message) = reader.next().await {
        match message {
            Ok(Message::Binary(packet)) => {
                transport
                    .inner
                    .packets_received
                    .fetch_add(1, Ordering::Relaxed);
                match transport.inner.from_browser.push(packet.to_vec()) {
                    Ok(true) => {
                        transport
                            .inner
                            .packets_dropped
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(false) | Err(()) => {}
                }
            }
            Ok(Message::Text(text)) => {
                if let Ok(stats) = serde_json::from_str::<BrowserBridgeStatsMessage>(text.as_ref())
                {
                    if stats.kind == "gpt-voice-stats" {
                        if let Ok(mut diagnostics) = transport.inner.browser_diagnostics.lock() {
                            *diagnostics = stats.diagnostics;
                        }
                    }
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(Message::Frame(_)) => {}
        }
    }

    queue.close();
    if transport.inner.active_connection_id.load(Ordering::Relaxed) == connection_id {
        transport.inner.from_browser.clear();
        if let Ok(mut active) = transport.inner.to_browser.lock() {
            if active
                .as_ref()
                .map(|current| Arc::ptr_eq(current, &queue))
                .unwrap_or(false)
            {
                active.take();
            }
        }
        transport.inner.connected.store(false, Ordering::Relaxed);
    }
    writer_task.abort();
    Ok(())
}

fn has_i16_signal(samples: &[i16]) -> bool {
    samples
        .iter()
        .any(|sample| (*sample as i32).abs() >= LATENCY_SIGNAL_THRESHOLD)
}

fn has_i16_byte_signal(bytes: &[u8]) -> bool {
    bytes.chunks_exact(2).any(|sample| {
        let value = i16::from_le_bytes([sample[0], sample[1]]);
        (value as i32).abs() >= LATENCY_SIGNAL_THRESHOLD
    })
}

fn make_transport_token() -> String {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{time:x}{:x}", std::process::id())
}

pub struct PcmReader {
    receiver: Arc<RealtimePcmQueue>,
    transport: Arc<TransportInner>,
    pending: Vec<u8>,
    offset: usize,
    packet_remaining: usize,
    packet_had_underflow: bool,
    jitter_target_frames: usize,
    jitter_primed: bool,
    jitter_has_started: bool,
    jitter_underflows: usize,
    jitter_stable_frames: usize,
    jitter_upshifts: u64,
    jitter_downshifts: u64,
}

impl PcmReader {
    fn new(receiver: Arc<RealtimePcmQueue>, gain: f32, transport: Arc<TransportInner>) -> Self {
        transport.set_output_gain(gain);
        Self {
            receiver,
            transport,
            pending: Vec::new(),
            offset: 0,
            packet_remaining: 0,
            packet_had_underflow: false,
            jitter_target_frames: PCM_JITTER_MIN_FRAMES,
            jitter_primed: false,
            jitter_has_started: false,
            jitter_underflows: 0,
            jitter_stable_frames: 0,
            jitter_upshifts: 0,
            jitter_downshifts: 0,
        }
    }

    fn pending_len(&self) -> usize {
        self.pending.len().saturating_sub(self.offset)
    }

    fn compact(&mut self) {
        if self.offset == 0 {
            return;
        }
        if self.offset >= self.pending.len() {
            self.pending.clear();
            self.offset = 0;
            return;
        }
        self.pending.drain(..self.offset);
        self.offset = 0;
    }

    fn receive_until(&mut self, bytes_needed: usize) {
        while self.pending_len() < bytes_needed {
            // Browser worklet packets are 1024 stereo frames (about 21.3 ms),
            // while Songbird normally requests 20 ms. A packet can therefore
            // leave a small partial remainder. Keep waiting for the next packet
            // in that case; using try_recv here made every partial read turn the
            // rest of the Discord frame into silence and let playback run ahead.
            let Some(packet) = self.receiver.receive_timeout(PCM_READ_WAIT) else {
                break;
            };
            if !packet.is_empty() {
                self.compact();
                self.transport
                    .reader_source_bytes
                    .fetch_add(packet.len() as u64, Ordering::Relaxed);
                self.transport.record_browser_output_packet(&packet);
                self.pending.extend(packet);
            }
        }
    }

    fn prime_jitter_buffer(&mut self) {
        if self.jitter_primed {
            return;
        }

        let target_bytes = PCM_INPUT_BYTES_PER_FRAME * self.jitter_target_frames;
        if self.pending_len() == 0 {
            self.receive_until(PCM_INPUT_BYTES_PER_FRAME);
        }
        if self.pending_len() > 0 {
            self.receive_until(target_bytes);
        }
        // Do not lose a short Voice reply just because it ended before the
        // full target arrived. One complete input frame is enough to start;
        // the larger target is used whenever the live transport can provide
        // it within the normal receive wait.
        if self.pending_len() >= PCM_INPUT_BYTES_PER_FRAME {
            self.jitter_primed = true;
            self.jitter_has_started = true;
        }
    }

    fn finish_output_packet(&mut self, had_underflow: bool) {
        if had_underflow && self.jitter_has_started {
            self.jitter_stable_frames = 0;
            self.jitter_underflows += 1;
            if self.jitter_underflows >= PCM_JITTER_UNDERRUNS_BEFORE_RESYNC {
                self.jitter_underflows = 0;
                self.jitter_primed = false;
                if self.jitter_target_frames < PCM_JITTER_MAX_FRAMES {
                    self.jitter_target_frames += 1;
                    self.jitter_upshifts += 1;
                }
            }
        } else if !had_underflow && self.jitter_has_started {
            self.jitter_underflows = 0;
            self.jitter_stable_frames += 1;
            if self.jitter_stable_frames >= PCM_JITTER_STABLE_FRAMES_BEFORE_RELAX
                && self.jitter_target_frames > PCM_JITTER_MIN_FRAMES
            {
                self.jitter_stable_frames = 0;
                self.jitter_target_frames -= 1;
                self.jitter_downshifts += 1;
            }
        }
        self.packet_had_underflow = false;
    }
}

impl Read for PcmReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }

        // Songbird's raw PCM reader asks for one 20 ms packet at a time, but
        // Symphonia reads from its MediaSourceStream using exponentially larger
        // chunks. Keep the source-side read cycle aligned to one output packet;
        // otherwise a single native read can pull future browser audio into
        // Symphonia's read-ahead buffer and add an audible response delay.
        if self.packet_remaining == 0 {
            self.packet_remaining = PCM_OUTPUT_BYTES_PER_PACKET;
        }
        let max_read_len = output.len().min(self.packet_remaining);
        let read_len = max_read_len.saturating_sub(max_read_len % 4);
        if read_len == 0 {
            return Ok(0);
        }
        self.transport.reader_reads.fetch_add(1, Ordering::Relaxed);

        self.prime_jitter_buffer();
        if !self.jitter_primed {
            output[..read_len].fill(0);
            self.packet_had_underflow = true;
            self.packet_remaining = self.packet_remaining.saturating_sub(read_len);
            self.transport
                .reader_output_bytes
                .fetch_add(read_len as u64, Ordering::Relaxed);
            self.transport
                .reader_silence_bytes
                .fetch_add(read_len as u64, Ordering::Relaxed);
            if self.packet_remaining == 0 {
                self.finish_output_packet(true);
            }
            return Ok(read_len);
        }

        let sample_bytes = read_len / 4 * 2;
        self.receive_until(sample_bytes.max(2));
        if self.pending_len() < sample_bytes {
            self.packet_had_underflow = true;
        }
        let mut output_offset = 0;
        let mut output_has_signal = false;
        while output_offset + 4 <= read_len && self.pending_len() >= 2 {
            let sample =
                i16::from_le_bytes([self.pending[self.offset], self.pending[self.offset + 1]]);
            self.offset += 2;
            output_has_signal |= (sample as i32).abs() >= LATENCY_SIGNAL_THRESHOLD;
            let value = ((sample as f32 / 32_768.0) * self.transport.output_gain())
                .clamp(-1.0, 1.0)
                .to_ne_bytes();
            output[output_offset..output_offset + 4].copy_from_slice(&value);
            output_offset += 4;
        }
        output[output_offset..read_len].fill(0);
        self.packet_remaining = self.packet_remaining.saturating_sub(read_len);
        self.transport
            .reader_output_bytes
            .fetch_add(read_len as u64, Ordering::Relaxed);
        self.transport
            .reader_silence_bytes
            .fetch_add((read_len - output_offset) as u64, Ordering::Relaxed);
        self.compact();
        if output_has_signal {
            self.transport.record_discord_output();
        }
        if self.packet_remaining == 0 {
            self.finish_output_packet(self.packet_had_underflow);
        }
        Ok(read_len)
    }
}

impl Seek for PcmReader {
    fn seek(&mut self, _position: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Live browser media cannot seek",
        ))
    }
}

impl MediaSource for PcmReader {
    fn is_seekable(&self) -> bool {
        false
    }

    fn byte_len(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_transport() -> Arc<TransportInner> {
        let from_browser = Arc::new(RealtimePcmQueue::new(1));
        Arc::new(TransportInner {
            endpoint: String::new(),
            token: String::new(),
            to_browser: Mutex::new(None),
            from_browser,
            reader: Mutex::new(None),
            shutdown: Mutex::new(None),
            next_connection_id: AtomicU64::new(0),
            active_connection_id: AtomicU64::new(0),
            connected: AtomicBool::new(false),
            packets_sent: AtomicU64::new(0),
            packets_dropped: AtomicU64::new(0),
            packets_received: AtomicU64::new(0),
            reader_reads: AtomicU64::new(0),
            reader_source_bytes: AtomicU64::new(0),
            reader_output_bytes: AtomicU64::new(0),
            reader_silence_bytes: AtomicU64::new(0),
            input_gain_bits: AtomicU32::new(1.0_f32.to_bits()),
            output_gain_bits: AtomicU32::new(1.0_f32.to_bits()),
            browser_diagnostics: Mutex::new(BrowserBridgeDiagnostics::default()),
            latency: Mutex::new(LatencyState::default()),
        })
    }

    #[test]
    fn pcm_reader_converts_i16_to_native_f32_and_keeps_streaming() {
        let receiver = Arc::new(RealtimePcmQueue::new(2));
        receiver
            .push(vec![0, 64, 0, 192])
            .expect("test packet should be accepted");
        let mut reader = PcmReader::new(receiver, 1.0, test_transport());
        reader.jitter_primed = true;
        reader.jitter_has_started = true;
        let mut output = [0_u8; 8];
        reader
            .read_exact(&mut output)
            .expect("live reader should read");
        let left = f32::from_ne_bytes(output[0..4].try_into().expect("left sample"));
        let right = f32::from_ne_bytes(output[4..8].try_into().expect("right sample"));
        assert!((left - 0.5).abs() < 0.001);
        assert!((right + 0.5).abs() < 0.001);
    }

    #[test]
    fn pcm_reader_waits_for_a_partial_packet_before_filling_silence() {
        let receiver = Arc::new(RealtimePcmQueue::new(2));
        receiver
            .push(vec![0, 64])
            .expect("first partial packet should be accepted");
        let sender = Arc::clone(&receiver);
        let sender_thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(5));
            sender
                .push(vec![0, 192])
                .expect("second partial packet should be accepted");
        });
        let mut reader = PcmReader::new(receiver, 1.0, test_transport());
        reader.jitter_primed = true;
        reader.jitter_has_started = true;
        let mut output = [0_u8; 4];
        reader
            .read_exact(&mut output)
            .expect("reader should wait for the rest of the stereo sample");
        sender_thread.join().expect("sender thread should finish");
        let sample = f32::from_ne_bytes(output);
        assert!((sample - 0.5).abs() < 0.001);
    }

    #[test]
    fn transport_uses_expected_audio_format() {
        assert_eq!(PCM_SAMPLE_RATE, 48_000);
        assert_eq!(PCM_CHANNELS, 2);
    }

    #[test]
    fn raw_adapter_has_a_registered_pcm_decoder() {
        let receiver = Arc::new(RealtimePcmQueue::new(1));
        let reader = PcmReader::new(receiver, 1.0, test_transport());
        let input: songbird::input::Input =
            songbird::input::RawAdapter::new(reader, PCM_SAMPLE_RATE, PCM_CHANNELS).into();
        let songbird::input::Input::Live(live, _) = input else {
            panic!("raw adapter should create a live input");
        };
        live.promote(
            songbird::input::codecs::get_codec_registry(),
            songbird::input::codecs::get_probe(),
        )
        .expect("Songbird should recognize the raw PCM relay track");
    }

    #[test]
    fn pcm_reader_limits_source_reads_to_one_songbird_packet() {
        let receiver = Arc::new(RealtimePcmQueue::new(4));
        receiver
            .push(vec![0x00; 4_096])
            .expect("first browser packet should be accepted");
        receiver
            .push(vec![0x00; 4_096])
            .expect("second browser packet should be accepted");
        receiver
            .push(vec![0x00; 4_096])
            .expect("third browser packet should be accepted");
        receiver
            .push(vec![0x00; 4_096])
            .expect("fourth browser packet should be accepted");
        let transport = test_transport();
        let mut reader = PcmReader::new(receiver, 1.0, Arc::clone(&transport));
        let mut output = vec![0_u8; PCM_OUTPUT_BYTES_PER_PACKET + 512];

        let bytes_read = reader
            .read(&mut output)
            .expect("reader should return a packet");

        assert_eq!(bytes_read, PCM_OUTPUT_BYTES_PER_PACKET);
        assert_eq!(
            transport.reader_source_bytes.load(Ordering::Relaxed),
            12_288
        );
    }

    #[test]
    fn raw_reader_primes_a_bounded_jitter_buffer() {
        use songbird::input::codecs::RawReader;
        use symphonia_core::{
            formats::{FormatOptions, FormatReader},
            io::{MediaSourceStream, MediaSourceStreamOptions},
        };

        let receiver = Arc::new(RealtimePcmQueue::new(4));
        receiver
            .push(vec![0_u8; 4_096])
            .expect("first browser packet should be accepted");
        receiver
            .push(vec![0_u8; 4_096])
            .expect("second browser packet should be accepted");
        receiver
            .push(vec![0_u8; 4_096])
            .expect("third browser packet should be accepted");
        receiver
            .push(vec![0_u8; 4_096])
            .expect("fourth browser packet should be accepted");
        let transport = test_transport();
        let reader = PcmReader::new(receiver, 1.0, Arc::clone(&transport));
        let source = MediaSourceStream::new(
            Box::new(songbird::input::RawAdapter::new(
                reader,
                PCM_SAMPLE_RATE,
                PCM_CHANNELS,
            )),
            MediaSourceStreamOptions::default(),
        );
        let mut raw = RawReader::try_new(source, &FormatOptions::default())
            .expect("Songbird's raw reader should parse the relay header");

        let packet = raw
            .next_packet()
            .expect("raw reader should return one packet");

        assert_eq!(packet.data.len(), PCM_OUTPUT_BYTES_PER_PACKET);
        assert_eq!(
            transport.reader_source_bytes.load(Ordering::Relaxed),
            12_288
        );
    }

    #[test]
    fn pcm_reader_adapts_jitter_target_after_underflows_and_stability() {
        let receiver = Arc::new(RealtimePcmQueue::new(1));
        let mut reader = PcmReader::new(receiver, 1.0, test_transport());
        reader.jitter_primed = true;
        reader.jitter_has_started = true;

        for _ in 0..PCM_JITTER_UNDERRUNS_BEFORE_RESYNC {
            reader.finish_output_packet(true);
        }
        assert_eq!(reader.jitter_target_frames, PCM_JITTER_MIN_FRAMES + 1);
        assert_eq!(reader.jitter_upshifts, 1);
        assert!(!reader.jitter_primed);

        reader.jitter_stable_frames = PCM_JITTER_STABLE_FRAMES_BEFORE_RELAX - 1;
        reader.finish_output_packet(false);
        assert_eq!(reader.jitter_target_frames, PCM_JITTER_MIN_FRAMES);
        assert_eq!(reader.jitter_downshifts, 1);
    }

    #[tokio::test]
    async fn browser_script_extracts_the_embedded_media_bridge() {
        let transport = BrowserMediaTransport::start()
            .await
            .expect("browser media transport should start");
        let script = transport.browser_script();
        assert!(!script.is_empty());
        assert!(script.contains("recoverOutputStreams"));
        assert!(!script.contains("__GPTVOICE_TRANSPORT_URL__"));
        assert!(!script.contains("__GPTVOICE_TRANSPORT_TOKEN__"));
        transport.stop();
    }
}
