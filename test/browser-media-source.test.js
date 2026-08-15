import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const bridgePath = path.join(repositoryRoot, "src", "browser", "media-bridge.js");

test("keeps the browser media bridge as a self-contained Tauri asset", () => {
  const source = fs.readFileSync(bridgePath, "utf8");
  const marker = "const BROWSER_INIT_SCRIPT = String.raw`";
  const start = source.indexOf(marker);
  const template = start >= 0 ? source.slice(start + marker.length).trim() : "";

  assert.ok(template.startsWith("(() => {"));
  assert.ok(template.endsWith("})();`;"), "the Rust host expects a closed raw template");
  assert.match(template, /getUserMedia/);
  assert.doesNotMatch(source, /^\s*(?:import|export)\b/m);
  assert.doesNotMatch(source, /BrowserMediaBridge/);
});
