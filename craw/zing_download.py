#!/usr/bin/env python3
"""
Download audio and lyric assets from a Zing MP3 song/album URL.

The script launches Chrome via the DevTools Protocol, reproduces the
play + karaoke actions needed by Zing, captures the real API responses
inside the browser session, then downloads the returned assets.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests


NODE_HELPER = r"""
const fs = require("fs");
const os = require("os");
const path = require("path");
const http = require("http");
const { spawn } = require("child_process");
const WebSocket = require("ws");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!arg.startsWith("--")) continue;
    const key = arg.slice(2);
    out[key] = argv[i + 1];
    i += 1;
  }
  return out;
}

function fetchJson(url, method = "GET") {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method }, (res) => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", (chunk) => body += chunk);
      res.on("end", () => {
        try {
          resolve(JSON.parse(body));
        } catch (err) {
          reject(new Error(`Invalid JSON from ${url}: ${err.message}`));
        }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

async function waitForBrowserWsEndpoint(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const info = await fetchJson(`http://127.0.0.1:${port}/json/version`);
      if (info.webSocketDebuggerUrl) return info.webSocketDebuggerUrl;
    } catch (err) {
      // retry until timeout
    }
    await sleep(250);
  }
  throw new Error("Timed out waiting for Chrome remote debugging endpoint");
}

async function createPageWsEndpoint(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      try {
        const info = await fetchJson(`http://127.0.0.1:${port}/json/new?about:blank`, "PUT");
        if (info.webSocketDebuggerUrl) return info.webSocketDebuggerUrl;
      } catch (err) {
        // Some Chrome builds reject PUT /json/new. Fall back below.
      }
      const pages = await fetchJson(`http://127.0.0.1:${port}/json/list`);
      const page = (pages || []).find((item) => item.type === "page" && item.webSocketDebuggerUrl);
      if (page) return page.webSocketDebuggerUrl;
    } catch (err) {
      // retry until timeout
    }
    await sleep(250);
  }
  throw new Error("Timed out creating a page target for Chrome DevTools");
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const targetUrl = args.url;
  const outputPath = args.output;
  const chromePath = args.chrome_path;
  const timeoutMs = Number(args.timeout_ms || 70000);
  const headless = args.headless !== "0";

  if (!targetUrl || !outputPath || !chromePath) {
    throw new Error("Missing required args: --url, --output, --chrome_path");
  }

  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "zing-dl-profile-"));
  const port = 9222 + Math.floor(Math.random() * 2000);
  const chromeArgs = [
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${userDataDir}`,
    "--autoplay-policy=no-user-gesture-required",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-background-networking",
    "--disable-features=Translate,MediaRouter",
    "--disable-popup-blocking",
    "--mute-audio",
    "--window-size=1440,960",
  ];
  if (headless) chromeArgs.push("--headless=new");

  const chrome = spawn(chromePath, chromeArgs, {
    stdio: "ignore",
    detached: false,
  });

  let ws;
  try {
    await waitForBrowserWsEndpoint(port, 15000);
    const wsUrl = await createPageWsEndpoint(port, 10000);
    ws = new WebSocket(wsUrl);
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
    });

    let nextId = 1;
    const pending = new Map();
    const captures = {
      song: null,
      streaming: null,
      lyric: null,
    };
    const requestMeta = new Map();

    function isRelevant(url) {
      return (
        url.includes("/api/v2/page/get/song?") ||
        url.includes("/api/v2/song/get/streaming?") ||
        url.includes("/api/v2/lyric/get/lyric?")
      );
    }

    function classify(url) {
      if (url.includes("/api/v2/page/get/song?")) return "song";
      if (url.includes("/api/v2/song/get/streaming?")) return "streaming";
      if (url.includes("/api/v2/lyric/get/lyric?")) return "lyric";
      return null;
    }

    function saveCapture(kind, meta, bodyObj) {
      captures[kind] = {
        url: meta.url,
        status: meta.status,
        mimeType: meta.mimeType,
        body: bodyObj,
      };
    }

    async function send(method, params = {}) {
      const id = nextId++;
      const payload = JSON.stringify({ id, method, params });
      ws.send(payload);
      return await new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
      });
    }

    async function evalJs(expression) {
      const result = await send("Runtime.evaluate", {
        expression,
        returnByValue: true,
        awaitPromise: true,
      });
      return result.result ? result.result.value : null;
    }

    function clickSelectorScript(selectorsJson) {
      return `
        (() => {
          const selectors = ${selectorsJson};
          const visible = (el) => {
            if (!el) return false;
            const rect = el.getBoundingClientRect();
            const style = window.getComputedStyle(el);
            return !!(rect.width && rect.height) &&
              style.visibility !== "hidden" &&
              style.display !== "none";
          };
          for (const sel of selectors) {
            const nodes = Array.from(document.querySelectorAll(sel));
            for (const el of nodes) {
              if (!visible(el)) continue;
              el.click();
              return {
                clicked: true,
                selector: sel,
                cls: el.className || "",
                text: (el.innerText || "").trim().slice(0, 100)
              };
            }
          }
          return { clicked: false };
        })();
      `;
    }

    async function waitForCapture(kind, waitMs) {
      const deadline = Date.now() + waitMs;
      while (Date.now() < deadline) {
        if (captures[kind]) return captures[kind];
        await sleep(250);
      }
      return null;
    }

    ws.on("message", async (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.id) {
        const entry = pending.get(msg.id);
        if (!entry) return;
        pending.delete(msg.id);
        if (msg.error) entry.reject(new Error(msg.error.message || "CDP error"));
        else entry.resolve(msg.result || {});
        return;
      }

      if (msg.method === "Network.responseReceived") {
        const resp = msg.params.response;
        const url = resp.url || "";
        if (!isRelevant(url)) return;
        requestMeta.set(msg.params.requestId, {
          url,
          status: resp.status,
          mimeType: resp.mimeType || "",
        });
      }

      if (msg.method === "Network.loadingFinished") {
        const meta = requestMeta.get(msg.params.requestId);
        if (!meta) return;
        const kind = classify(meta.url);
        if (!kind || captures[kind]) return;
        try {
          const bodyRes = await send("Network.getResponseBody", {
            requestId: msg.params.requestId,
          });
          const bodyText = bodyRes.base64Encoded
            ? Buffer.from(bodyRes.body, "base64").toString("utf8")
            : bodyRes.body;
          saveCapture(kind, meta, JSON.parse(bodyText));
        } catch (err) {
          saveCapture(kind, meta, {
            err: -999,
            msg: `Failed to read body: ${err.message}`,
          });
        }
      }
    });

    await send("Page.enable");
    await send("Runtime.enable");
    await send("DOM.enable");
    await send("Network.enable");
    await send("Page.navigate", { url: targetUrl });
    await sleep(5000);

    await evalJs(clickSelectorScript(JSON.stringify([
      "#onetrust-accept-btn-handler",
      "button[mode='primary']",
      "button",
    ])));
    await sleep(1500);

    if (targetUrl.includes("/album/")) {
      await evalJs(`
        (() => {
          const link = document.querySelector('a[href*="/bai-hat/"]');
          if (link) link.click();
          return !!link;
        })();
      `);
      await sleep(4000);
    }

    if (!captures.song) {
      await waitForCapture("song", 12000);
    }

    const playSelectors = JSON.stringify([
      "button.zm-btn.btn-action.play-btn",
      "button.btn-action.play-btn",
      ".zm-player button",
      ".player-controls button",
      ".zm-player [class*='play']",
      "button",
      ".zm-player",
    ]);

    const playDeadline = Date.now() + 20000;
    while (!captures.streaming && Date.now() < playDeadline) {
      await evalJs(clickSelectorScript(playSelectors));
      await sleep(2000);
    }

    if (captures.streaming) {
      await evalJs(clickSelectorScript(JSON.stringify([
        "button.btn-karaoke",
        "button[class*='karaoke']",
        ".zm-player button",
      ])));
      await waitForCapture("lyric", 12000);
    }

    fs.writeFileSync(outputPath, JSON.stringify({
      ok: true,
      url: targetUrl,
      captures,
    }, null, 2));
  } finally {
    try {
      if (ws && ws.readyState === WebSocket.OPEN) ws.close();
    } catch (err) {}
    try {
      chrome.kill("SIGKILL");
    } catch (err) {}
    try {
      fs.rmSync(userDataDir, { recursive: true, force: true });
    } catch (err) {}
  }
}

main().catch((err) => {
  const args = parseArgs(process.argv.slice(2));
  const outputPath = args.output;
  if (outputPath) {
    fs.writeFileSync(outputPath, JSON.stringify({
      ok: false,
      error: err.message,
      stack: err.stack,
    }, null, 2));
  }
  console.error(err.stack || String(err));
  process.exit(1);
});
"""


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download audio and lyric from a Zing MP3 song or album URL."
    )
    parser.add_argument("url", help="Zing MP3 song or album URL")
    parser.add_argument(
        "-o",
        "--out-dir",
        default="downloads/zing",
        help="Directory to store downloaded files",
    )
    parser.add_argument(
        "--chrome-path",
        default=shutil.which("google-chrome") or shutil.which("chromium") or "",
        help="Chrome/Chromium executable path",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Run Chrome with a visible window",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=70,
        help="Capture timeout in seconds",
    )
    return parser.parse_args()


def resolve_zing_url(url: str) -> str:
    if "/album/" not in url:
        return url

    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    match = re.search(r'(/bai-hat/[^"\']+\.html)', response.text)
    if not match:
        raise RuntimeError("Khong tim thay song URL trong trang album.")
    return f"https://zingmp3.vn{match.group(1)}"


def sanitize_name(text: str) -> str:
    text = re.sub(r"[\\/:*?\"<>|]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:180] or "zing_song"


def choose_audio_url(streaming_data: dict[str, Any]) -> tuple[str | None, str | None]:
    data = streaming_data.get("data")
    if not isinstance(data, dict):
        return None, None

    for quality in ("lossless", "320", "128"):
        value = data.get(quality)
        if isinstance(value, str) and value and value != "VIP":
            return normalize_url(value), quality

    for key, value in data.items():
        if (
            isinstance(value, str)
            and value != "VIP"
            and value.startswith(("http://", "https://", "//"))
        ):
            return normalize_url(value), str(key)

    return None, None


def normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    if url.startswith("//"):
        return f"https:{url}"
    return url


def detect_extension(url: str, default: str) -> str:
    path = urlparse(url).path
    suffix = Path(path).suffix.lower()
    return suffix or default


def ms_to_lrc_timestamp(ms: int) -> str:
    total_seconds = max(ms, 0) / 1000
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    hundredths = int(round((total_seconds - int(total_seconds)) * 100))
    if hundredths == 100:
        seconds += 1
        hundredths = 0
    if seconds == 60:
        minutes += 1
        seconds = 0
    return f"[{minutes:02d}:{seconds:02d}.{hundredths:02d}]"


def build_lrc_from_sentences(lyric_data: dict[str, Any]) -> str:
    sentences = lyric_data.get("sentences")
    if not isinstance(sentences, list):
        return ""

    lines: list[str] = []
    for sentence in sentences:
        words = sentence.get("words")
        if not isinstance(words, list) or not words:
            continue
        first_start = words[0].get("startTime")
        if not isinstance(first_start, int):
            continue
        text = " ".join(
            str(word.get("data", "")).strip()
            for word in words
            if str(word.get("data", "")).strip()
        ).strip()
        if not text:
            continue
        lines.append(f"{ms_to_lrc_timestamp(first_start)}{text}")
    return "\n".join(lines).strip() + ("\n" if lines else "")


def download_file(url: str, dest: Path) -> None:
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": "https://zingmp3.vn/",
        "Origin": "https://zingmp3.vn",
    }
    with requests.get(url, headers=headers, stream=True, timeout=120) as response:
        response.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    fh.write(chunk)


def run_capture(url: str, chrome_path: str, timeout: int, headful: bool) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="zing-dl-") as tmp_dir:
        helper_path = Path(tmp_dir) / "zing_capture.js"
        output_path = Path(tmp_dir) / "capture.json"
        helper_path.write_text(NODE_HELPER, encoding="utf-8")
        cmd = [
            "node",
            str(helper_path),
            "--url",
            url,
            "--output",
            str(output_path),
            "--chrome_path",
            chrome_path,
            "--timeout_ms",
            str(timeout * 1000),
            "--headless",
            "0" if headful else "1",
        ]
        completed = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=timeout + 20,
        )
        if not output_path.exists():
            raise RuntimeError(
                "Capture helper did not produce output.\n"
                f"stdout:\n{completed.stdout}\n"
                f"stderr:\n{completed.stderr}"
            )
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        if not payload.get("ok"):
            raise RuntimeError(payload.get("error") or "Unknown capture error")
        return payload


def save_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    args = parse_args()

    if not args.chrome_path or not Path(args.chrome_path).exists():
        print("Khong tim thay Chrome/Chromium. Dung --chrome-path de chi dinh.", file=sys.stderr)
        return 2

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    resolved_url = resolve_zing_url(args.url)

    capture = run_capture(
        url=resolved_url,
        chrome_path=args.chrome_path,
        timeout=args.timeout,
        headful=args.headful,
    )
    captures = capture.get("captures") or {}
    song_capture = captures.get("song") or {}
    streaming_capture = captures.get("streaming") or {}
    lyric_capture = captures.get("lyric") or {}

    song_body = song_capture.get("body") or {}
    streaming_body = streaming_capture.get("body") or {}
    lyric_body = lyric_capture.get("body") or {}

    title = (
        ((song_body.get("data") or {}).get("title"))
        or ((song_body.get("data") or {}).get("alias"))
        or "zing_song"
    )
    artists = ((song_body.get("data") or {}).get("artistsNames")) or ""
    base_name = sanitize_name(f"{title} - {artists}" if artists else title)

    metadata_path = out_dir / f"{base_name}.metadata.json"
    save_json(
        metadata_path,
        {
            "source_url": args.url,
            "resolved_url": resolved_url,
            "song_api_url": song_capture.get("url"),
            "streaming_api_url": streaming_capture.get("url"),
            "lyric_api_url": lyric_capture.get("url"),
            "song": song_body,
            "streaming": streaming_body,
            "lyric": lyric_body,
        },
    )

    audio_url, quality = choose_audio_url(streaming_body)
    if not audio_url:
        raise RuntimeError("Khong lay duoc direct audio URL tu response streaming.")

    audio_ext = detect_extension(audio_url, ".mp3")
    audio_path = out_dir / f"{base_name}.{quality or 'audio'}{audio_ext}"
    download_file(audio_url, audio_path)

    lyric_json_path = out_dir / f"{base_name}.lyric.json"
    save_json(lyric_json_path, lyric_body)

    lyric_file_url = normalize_url(((lyric_body.get("data") or {}).get("file")))
    lyric_file_path = None
    if lyric_file_url:
        lyric_ext = detect_extension(lyric_file_url, ".lrc")
        lyric_file_path = out_dir / f"{base_name}{lyric_ext}"
        download_file(lyric_file_url, lyric_file_path)
    else:
        lrc_text = build_lrc_from_sentences(lyric_body.get("data") or {})
        if lrc_text:
            lyric_file_path = out_dir / f"{base_name}.lrc"
            lyric_file_path.write_text(lrc_text, encoding="utf-8")

    print(f"Downloaded audio: {audio_path}")
    print(f"Saved lyric json: {lyric_json_path}")
    if lyric_file_path:
        print(f"Downloaded lyric file: {lyric_file_path}")
    else:
        print("Lyric file URL not found; kept lyric JSON only.")
    print(f"Saved metadata: {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
