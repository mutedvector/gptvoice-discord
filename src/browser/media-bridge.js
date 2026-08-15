const BROWSER_INIT_SCRIPT = String.raw`(() => {
  // Do not modify OpenAI's authentication or identity-provider pages. The
  // bridge is only needed by the ChatGPT app, and touching auth.openai.com
  // can interfere with its browser verification flow.
  const host = window.location.hostname.toLowerCase();
  const isChatGptHost = host === "chatgpt.com" || host.endsWith(".chatgpt.com") ||
    host === "chat.openai.com" || host.endsWith(".chat.openai.com");
  const isChatGptAuthPath = isChatGptHost && /^\/auth(?:\/|$)/i.test(window.location.pathname);
  if (!isChatGptHost || isChatGptAuthPath) {
    return;
  }

  const TRANSPORT_URL = __GPTVOICE_TRANSPORT_URL__;
  const TRANSPORT_TOKEN = __GPTVOICE_TRANSPORT_TOKEN__;
  const existingBridge = window.__gptVoiceMediaBridge;
  if (existingBridge?.token === TRANSPORT_TOKEN) {
    existingBridge.reconnect?.();
    return;
  }
  const OUTPUT_WORKLET_SOURCE = [
    "class GptVoiceCaptureProcessor extends AudioWorkletProcessor {",
    "  constructor() {",
    "    super();",
    "    this.buffer = new Int16Array(2048);",
    "    this.offset = 0;",
    "    this.outputPort = null;",
    "    this.processedFrames = 0;",
    "    this.lastFrame = -1;",
    "    this.maxGapFrames = 0;",
    "    this.peak = 0;",
    "    this.nonSilentFrames = 0;",
    "    this.lastStatsFrame = 0;",
    "    this.port.onmessage = (event) => {",
    "      if (event.data?.kind === 'attach-output-port') {",
    "        this.outputPort = event.data.port;",
    "        this.outputPort.start?.();",
    "      }",
    "    };",
    "  }",
    "  process(inputs, outputs) {",
    "    const input = inputs[0] || [];",
    "    const left = input[0] || null;",
    "    const right = input[1] || left;",
    "    const output = outputs[0] || [];",
    "    const frameCount = left ? left.length : (output[0] ? output[0].length : 128);",
    "    const frame = typeof currentFrame === 'number' ? currentFrame : this.processedFrames;",
    "    if (this.lastFrame >= 0) {",
    "      this.maxGapFrames = Math.max(this.maxGapFrames, frame - this.lastFrame - frameCount);",
    "    }",
    "    this.lastFrame = frame;",
    "    for (let index = 0; index < frameCount; index += 1) {",
      "      const leftSample = Math.max(-1, Math.min(1, left ? left[index] : 0));",
      "      const rightSample = Math.max(-1, Math.min(1, right ? right[index] : leftSample));",
      "      const amplitude = Math.max(Math.abs(leftSample), Math.abs(rightSample));",
      "      this.peak = Math.max(this.peak, amplitude);",
      "      if (amplitude > 0.0005) this.nonSilentFrames += 1;",
      "      this.buffer[this.offset] = leftSample < 0 ? leftSample * 32768 : leftSample * 32767;",
    "      this.buffer[this.offset + 1] = rightSample < 0 ? rightSample * 32768 : rightSample * 32767;",
    "      this.offset += 2;",
    "      if (this.offset >= this.buffer.length) {",
    "        const payload = this.buffer;",
    "        this.buffer = new Int16Array(2048);",
    "        this.offset = 0;",
    "        if (this.outputPort) {",
    "          this.outputPort.postMessage(payload.buffer, [payload.buffer]);",
    "        }",
    "      }",
    "    }",
    "    for (let channelIndex = 0; channelIndex < output.length; channelIndex += 1) {",
    "      output[channelIndex].fill(0);",
    "    }",
    "    this.processedFrames += frameCount;",
    "    if (this.processedFrames - this.lastStatsFrame >= sampleRate) {",
      "      this.port.postMessage({ kind: 'output-worklet-stats', processedFrames: this.processedFrames, maxGapMs: this.maxGapFrames * 1000 / sampleRate, peak: this.peak, nonSilentFrames: this.nonSilentFrames });",
    "      this.lastStatsFrame = this.processedFrames;",
    "    }",
    "    return true;",
    "  }",
    "}",
    "registerProcessor('gpt-voice-capture', GptVoiceCaptureProcessor);",
    "class GptVoiceInputProcessor extends AudioWorkletProcessor {",
    "  constructor() {",
    "    super();",
    "    this.queue = [];",
    "    this.queueSamples = 0;",
    // Four 20 ms stereo frames keep the browser microphone stream live
    // without replaying stale speech after a backgrounded renderer catches up.
    "    this.maxSamples = 48000 * 2 * 4 / 50;",
    "    this.processedFrames = 0;",
    "    this.silenceFrames = 0;",
    "    this.queuePeakSamples = 0;",
    "    this.lastStatsFrame = 0;",
    "    this.port.onmessage = (event) => {",
    "      if (event.data?.kind !== 'attach-input-port') {",
    "        return;",
    "      }",
    "      const inputPort = event.data.port;",
    "      inputPort.onmessage = (message) => {",
    "        if (!(message.data instanceof ArrayBuffer) || message.data.byteLength < 4) {",
    "          return;",
    "        }",
    "        const samples = new Int16Array(message.data);",
    "        this.queue.push({ samples, offset: 0 });",
    "        this.queueSamples += samples.length;",
    "        this.queuePeakSamples = Math.max(this.queuePeakSamples, this.queueSamples);",
    "        while (this.queueSamples > this.maxSamples && this.queue.length > 1) {",
    "          const dropped = this.queue.shift();",
    "          this.queueSamples -= dropped.samples.length - dropped.offset;",
    "        }",
    "      };",
    "      inputPort.start?.();",
    "    };",
    "  }",
    "  process(inputs, outputs) {",
    "    const output = outputs[0] || [];",
    "    const left = output[0] || [];",
    "    const right = output[1] || left;",
    "    for (let index = 0; index < left.length; index += 1) {",
    "      const item = this.queue[0];",
    "      if (!item) {",
    "        left[index] = 0;",
    "        if (right !== left) right[index] = 0;",
    "        this.silenceFrames += 1;",
    "        continue;",
    "      }",
    "      const sampleIndex = item.offset;",
    "      const leftSample = item.samples[sampleIndex] / 32768;",
    "      const rightSample = item.samples[sampleIndex + 1] / 32768;",
    "      left[index] = leftSample;",
    "      if (right !== left) right[index] = rightSample;",
    "      item.offset += 2;",
    "      this.queueSamples -= 2;",
    "      if (item.offset >= item.samples.length) this.queue.shift();",
    "    }",
    "    this.processedFrames += left.length;",
    "    if (this.processedFrames - this.lastStatsFrame >= sampleRate) {",
      "      this.port.postMessage({ kind: 'input-worklet-stats', processedFrames: this.processedFrames, silenceFrames: this.silenceFrames, queueSamples: this.queueSamples, queueDepth: this.queue.length, queuePeakSamples: this.queuePeakSamples });",
    "      this.lastStatsFrame = this.processedFrames;",
    "    }",
    "    return true;",
    "  }",
    "}",
    "registerProcessor('gpt-voice-input', GptVoiceInputProcessor);"
  ].join("\n");
  const TRANSPORT_WORKER_SOURCE = [
    "let transportUrl = '';",
    "let transportToken = '';",
    "let socket = null;",
    "let reconnectTimer = null;",
    "let outputPort = null;",
    "let inputPort = null;",
    "let inputPending = [];",
    "let outputPending = [];",
    "let outputPendingBytes = 0;",
    "let outputTrackCount = 0;",
    "const diagnostics = { outputCaptureMode: 'audio-worklet-worker', outputMessagesReceived: 0, outputMessagesSent: 0, outputBytesSent: 0, outputMaxGapMs: 0, outputDroppedCallbacks: 0, outputQueuePeakBytes: 0, inputMessages: 0, inputBytes: 0, inputDroppedMessages: 0, transportReconnects: 0, lastOutputAt: 0, lastSendAt: 0 };",
    "const workletStats = { outputWorkletMaxGapMs: 0, outputWorkletFrames: 0, outputWorkletPeak: 0, outputWorkletNonSilentFrames: 0, inputFrames: 0, inputSilenceFrames: 0, inputQueueSamples: 0, inputQueueDepth: 0, inputQueuePeakSamples: 0 };",
    "function scheduleReconnect() {",
    "  if (reconnectTimer !== null) return;",
    "  reconnectTimer = setTimeout(() => { reconnectTimer = null; openTransport(); }, 250);",
    "}",
    "function sendStats() {",
    "  if (!socket || socket.readyState !== WebSocket.OPEN) return;",
    "  try {",
    "    socket.send(JSON.stringify({ kind: 'gpt-voice-stats', diagnostics: { ...diagnostics, ...workletStats, outputTrackCount, outputQueueBytes: outputPendingBytes, inputPendingMessages: inputPending.length, reportedAt: Date.now() } }));",
    "  } catch {}",
    "}",
    "function flushOutput() {",
    "  if (!socket || socket.readyState !== WebSocket.OPEN) return;",
    "  while (outputPending.length > 0 && socket.bufferedAmount <= 256 * 1024) {",
    "    const data = outputPending.shift();",
    "    outputPendingBytes -= data.byteLength;",
    "    try {",
    "      socket.send(data);",
    "      const now = Date.now();",
    "      if (diagnostics.lastSendAt > 0) diagnostics.outputMaxGapMs = Math.max(diagnostics.outputMaxGapMs, now - diagnostics.lastSendAt);",
    "      diagnostics.lastSendAt = now;",
    "      diagnostics.outputMessagesSent += 1;",
    "      diagnostics.outputBytesSent += data.byteLength;",
    "    } catch {",
    "      outputPending.unshift(data);",
    "      outputPendingBytes += data.byteLength;",
    "      break;",
    "    }",
    "  }",
    "}",
    "function queueOutput(data) {",
    "  if (!(data instanceof ArrayBuffer) || data.byteLength < 4) return;",
    "  diagnostics.outputMessagesReceived += 1;",
    "  const now = Date.now();",
    "  if (diagnostics.lastOutputAt > 0) diagnostics.outputMaxGapMs = Math.max(diagnostics.outputMaxGapMs, now - diagnostics.lastOutputAt);",
    "  diagnostics.lastOutputAt = now;",
    "  if (outputPending.length >= 64) {",
    "    const dropped = outputPending.shift();",
    "    outputPendingBytes -= dropped.byteLength;",
    "    diagnostics.outputDroppedCallbacks += 1;",
    "  }",
    "  outputPending.push(data);",
    "  outputPendingBytes += data.byteLength;",
    "  diagnostics.outputQueuePeakBytes = Math.max(diagnostics.outputQueuePeakBytes, outputPendingBytes);",
    "  flushOutput();",
    "}",
    "function flushInput() {",
    "  if (!inputPort) return;",
    "  while (inputPending.length > 0) {",
    "    const data = inputPending.shift();",
    "    try { inputPort.postMessage(data, [data]); } catch { diagnostics.inputDroppedMessages += 1; }",
    "  }",
    "}",
    "function queueInput(data) {",
    "  if (!(data instanceof ArrayBuffer) || data.byteLength < 4) return;",
    "  diagnostics.inputMessages += 1;",
    "  diagnostics.inputBytes += data.byteLength;",
    "  if (inputPort) {",
    "    try { inputPort.postMessage(data, [data]); return; } catch {}",
    "  }",
    "  if (inputPending.length >= 4) { inputPending.shift(); diagnostics.inputDroppedMessages += 1; }",
    "  inputPending.push(data);",
    "}",
    "function openTransport() {",
    "  if (!transportUrl || !transportToken || (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING))) return;",
    "  const current = new WebSocket(transportUrl + '?token=' + encodeURIComponent(transportToken));",
    "  current.binaryType = 'arraybuffer';",
    "  current.onopen = () => { if (socket !== current) return; flushOutput(); flushInput(); sendStats(); };",
    "  current.onmessage = (event) => { if (socket === current && event.data instanceof ArrayBuffer) queueInput(event.data); };",
    "  current.onclose = () => { if (socket === current) { socket = null; diagnostics.transportReconnects += 1; scheduleReconnect(); } };",
    "  current.onerror = () => {};",
    "  socket = current;",
    "}",
    "setInterval(() => { flushOutput(); flushInput(); sendStats(); }, 1000);",
    "self.onmessage = (event) => {",
    "  const message = event.data || {};",
    "  if (message.kind === 'configure') { transportUrl = message.url; transportToken = message.token; openTransport(); return; }",
    "  if (message.kind === 'attach-output-port') { outputPort = message.port; outputPort.onmessage = (item) => queueOutput(item.data); outputPort.start?.(); flushOutput(); return; }",
    "  if (message.kind === 'attach-input-port') { inputPort = message.port; inputPort.start?.(); flushInput(); return; }",
    "  if (message.kind === 'set-output-track-count') { outputTrackCount = message.count; sendStats(); return; }",
    "  if (message.kind === 'output-worklet-stats') { workletStats.outputWorkletMaxGapMs = message.maxGapMs || 0; workletStats.outputWorkletFrames = message.processedFrames || 0; workletStats.outputWorkletPeak = message.peak || 0; workletStats.outputWorkletNonSilentFrames = message.nonSilentFrames || 0; return; }",
    "  if (message.kind === 'input-worklet-stats') { workletStats.inputFrames = message.processedFrames || 0; workletStats.inputSilenceFrames = message.silenceFrames || 0; workletStats.inputQueueSamples = message.queueSamples || 0; workletStats.inputQueueDepth = message.queueDepth || 0; workletStats.inputQueuePeakSamples = message.queuePeakSamples || 0; }",
    "};"
  ].join("\n");
  const state = {
    input: null,
    inputQueue: [],
    inputQueueSamples: 0,
    outputTracks: new Map(),
    transport: {
      socket: null,
      reconnectTimer: null,
      statsTimer: null,
      worker: null,
      workerUrl: null,
      mode: null
    },
    diagnostics: {
      outputCallbacks: 0,
      outputSamples: 0,
      outputLastCallbackAt: 0,
      outputMaxGapMs: 0,
      outputDroppedCallbacks: 0,
      outputAttachErrors: 0,
      outputLastError: null,
      outputCaptureMode: "pending",
      outputWorkletCaptures: 0,
      outputWorkletFallbacks: 0,
      outputWorkletMessages: 0,
      outputWorkletFrames: 0,
      outputWorkletPeak: 0,
      outputWorkletNonSilentFrames: 0,
      inputMessages: 0,
      inputBytes: 0,
      inputFrames: 0,
      inputSilenceFrames: 0,
      inputQueueSamples: 0,
      inputQueueDepth: 0,
      inputQueuePeakSamples: 0,
      inputDroppedMessages: 0,
      inputLastFrameAt: 0,
      transportReconnects: 0
    },
    peerConnections: new Set()
  };

  function rememberPeerConnection(connection) {
    if (!connection || state.peerConnections.has(connection)) return;
    state.peerConnections.add(connection);
    const cleanupConnectionTracks = () => {
      const stateName = String(connection.connectionState || '').toLowerCase();
      const iceState = String(connection.iceConnectionState || '').toLowerCase();
      if (!['closed', 'failed'].includes(stateName) && !['closed', 'failed'].includes(iceState)) return;
      for (const capture of state.outputTracks.values()) {
        if (capture.peerConnection === connection) capture.cleanup?.();
      }
    };
    connection.addEventListener?.('connectionstatechange', cleanupConnectionTracks);
    connection.addEventListener?.('iceconnectionstatechange', cleanupConnectionTracks);
  }

  function resumeOutputContexts() {
    for (const capture of state.outputTracks.values()) {
      if (capture.context?.state === "suspended") {
        void capture.context.resume().catch((error) => {
          state.diagnostics.outputAttachErrors += 1;
          state.diagnostics.outputLastError = error?.message || String(error);
        });
      }
    }
  }

  function notifyOutputTrackCount() {
    state.transport.worker?.postMessage({
      kind: "set-output-track-count",
      count: state.outputTracks.size
    });
    if (state.transport.mode === "main") {
      sendTransportStats();
    }
  }

  function pruneEndedOutputTracks() {
    for (const capture of state.outputTracks.values()) {
      const trackState = String(capture.track?.readyState || '').toLowerCase();
      const connectionState = String(capture.peerConnection?.connectionState || '').toLowerCase();
      const iceState = String(capture.peerConnection?.iceConnectionState || '').toLowerCase();
      if (trackState === "ended" || ["closed", "failed"].includes(connectionState) || ["closed", "failed"].includes(iceState)) {
        capture.cleanup?.();
      }
    }
  }

  function attachPeerConnectionOutputs(connection) {
    rememberPeerConnection(connection);
    connection?.getReceivers?.().forEach((receiver) => {
      if (receiver.track?.kind === "audio") {
        attachOutputStream(new MediaStream([receiver.track]), undefined, connection);
      }
    });
  }

  function recoverOutputStreams() {
    pruneEndedOutputTracks();
    try {
      document.querySelectorAll("audio,video").forEach((element) => {
        attachOutputStream(element.srcObject, element);
      });
    } catch (error) {
      state.diagnostics.outputAttachErrors += 1;
      state.diagnostics.outputLastError = error?.message || String(error);
    }
    for (const connection of state.peerConnections) {
      if (connection.connectionState === "closed") {
        state.peerConnections.delete(connection);
        continue;
      }
      try {
        attachPeerConnectionOutputs(connection);
      } catch (error) {
        state.diagnostics.outputAttachErrors += 1;
        state.diagnostics.outputLastError = error?.message || String(error);
      }
    }
    resumeOutputContexts();
  }

  window.__gptVoiceMediaBridge = {
    token: TRANSPORT_TOKEN,
    reconnect: () => {
      connectTransport();
      recoverOutputStreams();
    },
    recoverOutput: recoverOutputStreams,
    getDiagnostics: () => {
      pruneEndedOutputTracks();
      return {
        installed: true,
        mode: state.transport.mode,
      socketReadyState: state.transport.socket?.readyState ?? null,
      workerActive: Boolean(state.transport.worker),
      outputCallbacks: state.diagnostics.outputCallbacks,
      outputSamples: state.diagnostics.outputSamples,
      outputMaxGapMs: state.diagnostics.outputMaxGapMs,
      outputDroppedCallbacks: state.diagnostics.outputDroppedCallbacks,
      outputAttachErrors: state.diagnostics.outputAttachErrors,
      outputLastError: state.diagnostics.outputLastError,
      outputCaptureMode: state.diagnostics.outputCaptureMode,
      outputWorkletCaptures: state.diagnostics.outputWorkletCaptures,
      outputWorkletFallbacks: state.diagnostics.outputWorkletFallbacks,
      outputTrackCount: state.outputTracks.size,
      outputCaptureModes: [...state.outputTracks.values()].map((capture) => capture.mode),
      outputWorkletFrames: state.diagnostics.outputWorkletFrames,
      outputWorkletPeak: state.diagnostics.outputWorkletPeak,
      outputWorkletNonSilentFrames: state.diagnostics.outputWorkletNonSilentFrames,
      inputFrames: state.diagnostics.inputFrames,
      inputQueueSamples: state.diagnostics.inputQueueSamples,
      inputQueueDepth: state.diagnostics.inputQueueDepth,
      inputQueuePeakSamples: state.diagnostics.inputQueuePeakSamples,
      inputDroppedMessages: state.diagnostics.inputDroppedMessages,
      inputLastFrameAt: state.diagnostics.inputLastFrameAt,
        transportReconnects: state.diagnostics.transportReconnects
      };
    }
  };

  function createContext() {
    try {
      return new AudioContext({ sampleRate: 48000, latencyHint: "interactive" });
    } catch {
      return new AudioContext({ latencyHint: "interactive" });
    }
  }

  function decodePcm(encoded) {
    const binary = atob(encoded);
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) {
      bytes[index] = binary.charCodeAt(index);
    }
    return new Int16Array(bytes.buffer);
  }

  function encodePcmBytes(left, right) {
    const samples = new Int16Array(left.length * 2);
    for (let index = 0; index < left.length; index += 1) {
      const leftSample = Math.max(-1, Math.min(1, left[index]));
      const rightSample = Math.max(-1, Math.min(1, right[index] ?? left[index]));
      samples[index * 2] = leftSample < 0 ? leftSample * 32768 : leftSample * 32767;
      samples[index * 2 + 1] = rightSample < 0 ? rightSample * 32768 : rightSample * 32767;
    }
    return new Uint8Array(samples.buffer);
  }

  function enqueueInputSamples(samples) {
    if (!samples || samples.length < 2) {
      return;
    }
    state.inputQueue.push({ samples, offset: 0 });
    state.inputQueueSamples += samples.length;
    state.diagnostics.inputQueuePeakSamples = Math.max(
      state.diagnostics.inputQueuePeakSamples,
      state.inputQueueSamples
    );
    const maxSamples = 48000 * 2 * 4 / 50;
    while (state.inputQueueSamples > maxSamples && state.inputQueue.length > 1) {
      const dropped = state.inputQueue.shift();
      state.inputQueueSamples -= dropped.samples.length - dropped.offset;
      state.diagnostics.inputDroppedMessages += 1;
    }
    state.diagnostics.inputQueueSamples = state.inputQueueSamples;
    state.diagnostics.inputQueueDepth = state.inputQueue.length;
  }

  function scheduleTransportReconnect() {
    if (state.transport.reconnectTimer !== null) {
      return;
    }
    state.transport.reconnectTimer = window.setTimeout(() => {
      state.transport.reconnectTimer = null;
      connectTransport();
    }, 250);
  }

  function sendTransportStats() {
    const socket = state.transport.socket;
    if (!socket || socket.readyState !== window.WebSocket.OPEN) {
      return;
    }
    try {
      socket.send(JSON.stringify({
        kind: "gpt-voice-stats",
        diagnostics: {
          ...state.diagnostics,
          inputQueueSamples: state.inputQueueSamples,
          outputTrackCount: state.outputTracks.size,
          outputCaptureModes: [...state.outputTracks.values()].map((capture) => capture.mode),
          reportedAt: Date.now()
        }
      }));
    } catch {
      // The close handler will reconnect if the browser closes the socket.
    }
  }

  function switchToMainTransport() {
    if (state.transport.worker) {
      state.transport.worker.terminate();
      state.transport.worker = null;
    }
    if (state.transport.workerUrl) {
      URL.revokeObjectURL(state.transport.workerUrl);
      state.transport.workerUrl = null;
    }
    state.transport.mode = "main";
    connectTransport();
  }

  function createTransportWorker() {
    if (state.transport.worker) {
      return;
    }
    try {
      const source = new Blob([TRANSPORT_WORKER_SOURCE], {
        type: "application/javascript"
      });
      const workerUrl = URL.createObjectURL(source);
      const worker = new window.Worker(workerUrl);
      state.transport.worker = worker;
      state.transport.workerUrl = workerUrl;
      worker.addEventListener("error", () => {
        if (state.transport.mode === "worker") {
          switchToMainTransport();
        }
      });
      worker.postMessage({
        kind: "configure",
        url: TRANSPORT_URL,
        token: TRANSPORT_TOKEN
      });
    } catch {
      state.transport.mode = "main";
      connectTransport();
    }
  }

  function connectTransport() {
    if (state.transport.mode === null) {
      state.transport.mode = window.Worker && window.MessageChannel && window.AudioWorkletNode
        ? "worker"
        : "main";
    }
    if (state.transport.mode === "worker") {
      createTransportWorker();
      return;
    }
    const current = state.transport.socket;
    if (
      current &&
      (current.readyState === window.WebSocket.OPEN ||
        current.readyState === window.WebSocket.CONNECTING)
    ) {
      return;
    }

    const socket = new window.WebSocket(
      TRANSPORT_URL + "?token=" + encodeURIComponent(TRANSPORT_TOKEN)
    );
    socket.binaryType = "arraybuffer";
    socket.addEventListener("open", () => {
      if (state.transport.socket !== socket) {
        return;
      }
      if (state.transport.reconnectTimer !== null) {
        window.clearTimeout(state.transport.reconnectTimer);
        state.transport.reconnectTimer = null;
      }
      sendTransportStats();
    });
    socket.addEventListener("message", (event) => {
      if (state.transport.socket !== socket || !(event.data instanceof ArrayBuffer)) {
        return;
      }
      const byteLength = event.data.byteLength - (event.data.byteLength % 2);
      if (byteLength < 2) {
        return;
      }
      state.diagnostics.inputMessages += 1;
      state.diagnostics.inputBytes += byteLength;
      enqueueInputSamples(new Int16Array(event.data, 0, byteLength / 2));
    });
    socket.addEventListener("close", () => {
      if (state.transport.socket === socket) {
        state.transport.socket = null;
        state.diagnostics.transportReconnects += 1;
        scheduleTransportReconnect();
      }
    });
    socket.addEventListener("error", () => {
      // The close event schedules the reconnect. Keep browser console output
      // quiet because a local server may be restarting during navigation.
    });
    state.transport.socket = socket;
  }

  function sendOutputPcm(left, right) {
    sendOutputBytes(encodePcmBytes(left, right), left.length);
  }

  function sendOutputBytes(bytes, sampleCount) {
    const callbackAt = performance.now();
    const previousCallbackAt = state.diagnostics.outputLastCallbackAt;
    state.diagnostics.outputCallbacks += 1;
    state.diagnostics.outputSamples += sampleCount;
    state.diagnostics.outputMaxGapMs = Math.max(
      state.diagnostics.outputMaxGapMs,
      previousCallbackAt > 0 ? callbackAt - previousCallbackAt : 0
    );
    state.diagnostics.outputLastCallbackAt = callbackAt;
    const socket = state.transport.socket;
    if (!socket || socket.readyState !== window.WebSocket.OPEN) {
      if (state.transport.mode === "worker") {
        switchToMainTransport();
      } else {
        connectTransport();
      }
      state.diagnostics.outputDroppedCallbacks += 1;
      return;
    }
    if (socket.bufferedAmount > 256 * 1024) {
      state.diagnostics.outputDroppedCallbacks += 1;
      return;
    }
    try {
      socket.send(bytes);
    } catch {
      // The close handler will reconnect if the browser closes the socket
      // between the readyState check and send().
      state.diagnostics.outputDroppedCallbacks += 1;
    }
  }

  function readInputFrame(left, right) {
    let silenceFrames = 0;
    let item = state.inputQueue[0];
    if (!item) {
      left.fill(0);
      right.fill(0);
      silenceFrames = left.length;
    } else {
      for (let index = 0; index < left.length; index += 1) {
        item = state.inputQueue[0];
        if (!item) {
          left[index] = 0;
          right[index] = 0;
          silenceFrames += 1;
          continue;
        }
        const sampleIndex = item.offset;
        left[index] = item.samples[sampleIndex] / 32768;
        right[index] = item.samples[sampleIndex + 1] / 32768;
        item.offset += 2;
        state.inputQueueSamples -= 2;
        if (item.offset >= item.samples.length) {
          state.inputQueue.shift();
        }
      }
    }
    state.diagnostics.inputQueueSamples = state.inputQueueSamples;
    state.diagnostics.inputQueueDepth = state.inputQueue.length;
    state.diagnostics.inputFrames += left.length;
    state.diagnostics.inputSilenceFrames += silenceFrames;
    state.diagnostics.inputLastFrameAt = Date.now();
  }

  async function createInput() {
    if (state.input) {
      return state.input;
    }
    const context = createContext();
    const destination = context.createMediaStreamDestination();
    try {
      if (
        state.transport.mode !== "worker" ||
        !state.transport.worker ||
        !context.audioWorklet ||
        !window.AudioWorkletNode
      ) {
        throw new Error("Audio input worklet transport is unavailable");
      }
      const module = new Blob([OUTPUT_WORKLET_SOURCE], {
        type: "application/javascript"
      });
      const moduleUrl = URL.createObjectURL(module);
      try {
        await context.audioWorklet.addModule(moduleUrl);
      } finally {
        URL.revokeObjectURL(moduleUrl);
      }
      const node = new window.AudioWorkletNode(context, "gpt-voice-input", {
        numberOfInputs: 0,
        numberOfOutputs: 1,
        outputChannelCount: [2]
      });
      node.port.onmessage = (event) => {
        if (event.data?.kind !== "input-worklet-stats") {
          return;
        }
        state.diagnostics.inputFrames = event.data.processedFrames ?? 0;
        state.diagnostics.inputSilenceFrames = event.data.silenceFrames ?? 0;
        state.diagnostics.inputQueueSamples = event.data.queueSamples ?? 0;
        state.diagnostics.inputQueueDepth = event.data.queueDepth ?? 0;
        state.diagnostics.inputQueuePeakSamples = event.data.queuePeakSamples ?? 0;
        state.diagnostics.inputLastFrameAt = Date.now();
        state.transport.worker?.postMessage({
          kind: "input-worklet-stats",
          processedFrames: event.data.processedFrames,
          silenceFrames: event.data.silenceFrames,
          queuePeakSamples: event.data.queuePeakSamples
        });
      };
      const inputChannel = new MessageChannel();
      state.transport.worker.postMessage(
        { kind: "attach-input-port", port: inputChannel.port2 },
        [inputChannel.port2]
      );
      node.port.postMessage(
        { kind: "attach-input-port", port: inputChannel.port1 },
        [inputChannel.port1]
      );
      node.connect(destination);
      void context.resume();
      state.input = { context, destination, node, mode: "audio-worklet-worker" };
      return state.input;
    } catch {
      if (state.transport.mode === "worker") {
        switchToMainTransport();
      }
      const processor = context.createScriptProcessor(1024, 1, 2);
      const silentSource = context.createConstantSource();
      const silentGain = context.createGain();
      silentGain.gain.value = 0;
      silentSource.connect(silentGain).connect(processor);
      processor.connect(destination);
      processor.onaudioprocess = (event) => {
        readInputFrame(
          event.outputBuffer.getChannelData(0),
          event.outputBuffer.getChannelData(1)
        );
      };
      silentSource.start();
      void context.resume();
      state.input = { context, destination, processor, mode: "script-processor" };
      return state.input;
    }
  }

  window.__gptVoicePushPcm = (encoded) => {
    enqueueInputSamples(decodePcm(encoded));
  };

  connectTransport();
  state.transport.statsTimer = window.setInterval(sendTransportStats, 1_000);

  const originalGetUserMedia = navigator.mediaDevices?.getUserMedia?.bind(navigator.mediaDevices);
  if (originalGetUserMedia) {
    navigator.mediaDevices.getUserMedia = async (constraints) => {
      const wantsAudio = constraints === true || Boolean(constraints?.audio);
      if (!wantsAudio) {
        return originalGetUserMedia(constraints);
      }
      const input = await createInput();
      const tracks = [input.destination.stream.getAudioTracks()[0].clone()];
      if (constraints?.video) {
        const videoStream = await originalGetUserMedia({ audio: false, video: constraints.video });
        tracks.push(...videoStream.getVideoTracks());
      }
      return new MediaStream(tracks);
    };
  }

  function attachOutputStream(stream, element, peerConnection) {
    if (!stream?.getAudioTracks) {
      return;
    }
    const track = stream.getAudioTracks().find((candidate) => candidate.readyState !== "ended");
    if (!track) {
      return;
    }
    // ChatGPT can leave a live remote receiver disabled after replacing its
    // Voice stream. Re-enable it before building the capture graph; an ended
    // track is still filtered above and is never resurrected.
    try {
      track.enabled = true;
    } catch {
      // Some browser implementations expose enabled as read-only here.
    }
    if (element) {
      element.muted = true;
      element.volume = 0;
    }
    if (state.outputTracks.has(track.id)) {
      return;
    }
    try {
      const context = createContext();
      const source = context.createMediaStreamSource(new MediaStream([track]));
      const silentGain = context.createGain();
      // Keep the relay graph non-zero. A fully zero-gain branch can be
      // optimized as silent by Chromium, which stops the AudioWorklet from
      // receiving remote Voice frames while the page is backgrounded.
      silentGain.gain.value = 0.00001;
      // Chrome is more likely to deprioritize a fully silent background audio
      // graph. Keep the dedicated context audibly active with a -100 dB
      // constant signal; it is below normal playback audibility but prevents
      // the browser from treating this relay as an idle silent page.
      const keepAliveSource = context.createConstantSource();
      const keepAliveGain = context.createGain();
      keepAliveSource.offset.value = 1;
      keepAliveGain.gain.value = 0.00001;
      keepAliveSource.connect(keepAliveGain).connect(context.destination);
      keepAliveSource.start();
      const capture = {
        context,
        source,
        silentGain,
        keepAliveSource,
        keepAliveGain,
        playbackElement: null,
        track,
        peerConnection: peerConnection || null,
        node: null,
        processor: null,
        mode: "pending"
      };
      const cleanup = () => {
        if (state.outputTracks.get(track.id) !== capture) {
          return;
        }
        state.outputTracks.delete(track.id);
        if (capture.processor) {
          capture.processor.onaudioprocess = null;
        }
        if (capture.node) {
          capture.node.port.onmessage = null;
          capture.node.port.close?.();
        }
        try {
          source.disconnect();
          capture.processor?.disconnect();
          capture.node?.disconnect();
          silentGain.disconnect();
          keepAliveSource.stop();
          keepAliveSource.disconnect();
          keepAliveGain.disconnect();
          capture.playbackElement?.remove();
        } catch {
          // The browser may already have torn down part of this graph.
        }
        void context.close().catch(() => {});
      };
      capture.cleanup = cleanup;
      track.addEventListener("ended", cleanup, { once: true });
      state.outputTracks.set(track.id, capture);
      notifyOutputTrackCount();
      // Keep a muted media element attached as a decoder wake-up path. Some
      // Chromium/WebRTC builds do not advance a remote audio receiver while
      // it has no media-element consumer, even though an AudioContext source
      // has been created for it. The element is permanently muted and never
      // reaches the user's speakers.
      try {
        const playbackElement = document.createElement("audio");
        playbackElement.autoplay = true;
        playbackElement.playsInline = true;
        playbackElement.muted = true;
        playbackElement.volume = 0;
        playbackElement.srcObject = new MediaStream([track]);
        playbackElement.style.display = "none";
        (document.body || document.documentElement)?.appendChild(playbackElement);
        capture.playbackElement = playbackElement;
        void playbackElement.play().catch((error) => {
          state.diagnostics.outputAttachErrors += 1;
          state.diagnostics.outputLastError = error?.message || String(error);
        });
      } catch (error) {
        state.diagnostics.outputAttachErrors += 1;
        state.diagnostics.outputLastError = error?.message || String(error);
      }

      const installCapture = async () => {
        try {
          if (!context.audioWorklet || !window.AudioWorkletNode) {
            throw new Error("AudioWorklet is unavailable");
          }
          const module = new Blob([OUTPUT_WORKLET_SOURCE], {
            type: "application/javascript"
          });
          const moduleUrl = URL.createObjectURL(module);
          try {
            await context.audioWorklet.addModule(moduleUrl);
          } finally {
            URL.revokeObjectURL(moduleUrl);
          }
          if (state.outputTracks.get(track.id) !== capture) {
            return;
          }
          const node = new window.AudioWorkletNode(context, "gpt-voice-capture", {
            numberOfInputs: 1,
            numberOfOutputs: 1,
            outputChannelCount: [2]
          });
          if (state.transport.mode !== "worker" || !state.transport.worker) {
            throw new Error("Audio transport worker is unavailable");
          }
          capture.node = node;
          capture.mode = "audio-worklet-worker";
          state.diagnostics.outputCaptureMode = "audio-worklet-worker";
          state.diagnostics.outputWorkletCaptures += 1;
          node.port.onmessage = (event) => {
            if (state.outputTracks.get(track.id) !== capture) {
              return;
            }
            if (event.data?.kind !== "output-worklet-stats") {
              return;
            }
            state.diagnostics.outputMaxGapMs = Math.max(
              state.diagnostics.outputMaxGapMs,
              Number(event.data.maxGapMs) || 0
            );
            state.diagnostics.outputWorkletFrames = Number(event.data.processedFrames) || 0;
            state.diagnostics.outputWorkletPeak = Number(event.data.peak) || 0;
            state.diagnostics.outputWorkletNonSilentFrames = Number(event.data.nonSilentFrames) || 0;
            state.transport.worker?.postMessage({
              kind: "output-worklet-stats",
              processedFrames: event.data.processedFrames,
              maxGapMs: event.data.maxGapMs,
              peak: event.data.peak,
              nonSilentFrames: event.data.nonSilentFrames
            });
          };
          const outputChannel = new MessageChannel();
          state.transport.worker.postMessage(
            { kind: "attach-output-port", port: outputChannel.port2 },
            [outputChannel.port2]
          );
          node.port.postMessage(
            { kind: "attach-output-port", port: outputChannel.port1 },
            [outputChannel.port1]
          );
          state.transport.worker.postMessage({
            kind: "set-output-track-count",
            count: state.outputTracks.size
          });
          source.connect(node);
          node.connect(silentGain).connect(context.destination);
          void context.resume().catch((error) => {
            state.diagnostics.outputAttachErrors += 1;
            state.diagnostics.outputLastError = error?.message || String(error);
          });
        } catch (error) {
          if (state.outputTracks.get(track.id) !== capture) {
            return;
          }
          state.diagnostics.outputAttachErrors += 1;
          state.diagnostics.outputLastError = error?.message || String(error);
          if (capture.node) {
            capture.node.port.onmessage = null;
            capture.node.port.close?.();
            capture.node.disconnect();
            capture.node = null;
          }
          if (state.transport.mode === "worker") {
            switchToMainTransport();
          }
          const processor = context.createScriptProcessor(1024, 2, 2);
          capture.processor = processor;
          capture.mode = "script-processor";
          state.diagnostics.outputCaptureMode = "script-processor";
          state.diagnostics.outputWorkletFallbacks += 1;
          source.connect(processor);
          processor.connect(silentGain).connect(context.destination);
          processor.onaudioprocess = (event) => {
            const input = event.inputBuffer;
            const left = input.getChannelData(0);
            const right = input.numberOfChannels > 1 ? input.getChannelData(1) : left;
            sendOutputPcm(left, right);
          };
          void context.resume().catch((error) => {
            state.diagnostics.outputAttachErrors += 1;
            state.diagnostics.outputLastError = error?.message || String(error);
          });
        }
      };
      void installCapture();
    } catch (error) {
      state.diagnostics.outputAttachErrors += 1;
      state.diagnostics.outputLastError = error?.message || String(error);
      // A later media element or WebRTC receiver may expose the same track.
    }
  }

  const mediaPrototype = window.HTMLMediaElement?.prototype;
  const sourceDescriptor = mediaPrototype && Object.getOwnPropertyDescriptor(mediaPrototype, "srcObject");
  if (sourceDescriptor?.set && sourceDescriptor.get) {
    try {
      Object.defineProperty(mediaPrototype, "srcObject", {
        configurable: sourceDescriptor.configurable,
        enumerable: sourceDescriptor.enumerable,
        get: sourceDescriptor.get,
        set(value) {
          sourceDescriptor.set.call(this, value);
          attachOutputStream(value, this);
        }
      });
    } catch {
      // The browser may expose a non-configurable media property.
    }
  }

  if (mediaPrototype?.play) {
    const originalPlay = mediaPrototype.play;
    mediaPrototype.play = function (...args) {
      attachOutputStream(this.srcObject, this);
      return originalPlay.apply(this, args);
    };
  }

  const peerConnectionPrototype = window.RTCPeerConnection?.prototype;
  if (peerConnectionPrototype?.addEventListener) {
    const originalAddEventListener = peerConnectionPrototype.addEventListener;
    const trackObservers = new WeakSet();
    peerConnectionPrototype.addEventListener = function (type, listener, options) {
      rememberPeerConnection(this);
      if (type === "track" && !trackObservers.has(this)) {
        trackObservers.add(this);
        originalAddEventListener.call(this, "track", (event) => {
          if (event?.track?.kind === "audio") {
            attachOutputStream(
              event.streams?.[0] ?? new MediaStream([event.track])
            );
          }
        }, { capture: true });
      }
      return originalAddEventListener.call(this, type, listener, options);
    };
  }
  if (peerConnectionPrototype?.setRemoteDescription) {
    const originalSetRemoteDescription = peerConnectionPrototype.setRemoteDescription;
    peerConnectionPrototype.setRemoteDescription = async function (...args) {
      rememberPeerConnection(this);
      const result = await originalSetRemoteDescription.apply(this, args);
      const attachReceivers = () => {
        attachPeerConnectionOutputs(this);
      };
      attachReceivers();
      setTimeout(attachReceivers, 100);
      setTimeout(attachReceivers, 500);
      setTimeout(attachReceivers, 1_500);
      return result;
    };
  }

  if (peerConnectionPrototype?.dispatchEvent) {
    const originalDispatchEvent = peerConnectionPrototype.dispatchEvent;
    peerConnectionPrototype.dispatchEvent = function (event) {
      rememberPeerConnection(this);
      if (event?.type === "track" && event.track?.kind === "audio") {
        attachOutputStream(
          event.streams?.[0] ?? new MediaStream([event.track])
        );
      }
      return originalDispatchEvent.call(this, event);
    };
  }

  window.setInterval(recoverOutputStreams, 1_000);
  recoverOutputStreams();
})();`;
