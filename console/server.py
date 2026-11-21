#!/usr/bin/env python3
"""Gaia console server — serves the console and wires it to the live node.

Zero-dependency Python stdlib. Binds 127.0.0.1 only.

    python3 console/server.py            # port 11440
    GAIA_CONSOLE_PORT=8080 python3 console/server.py

Endpoints
    GET  /                  console/index.html (and static files beside it)
    GET  /api/journal       seam journal tail (last 8, full fields), read-only
    GET  /api/escalations   folded open escalations + gate reports, read-only
    GET  /api/spine         spine totals/head/last event + residency, live node
    POST /api/chat          {message} -> resident model's reply

Read-only guarantees: the seam journal is read through the SI package's
own read path (si_read.py: open + hash-chain verify + fold) in a
subprocess; the spine is read through the node's events endpoint. This
server never writes to a journal, never resolves an escalation, and
never posts to the seam egress (11437). Chat goes only to the resident
model's llama-server endpoint, discovered live (see below).

Configuration (environment):
    GAIA_CONSOLE_PORT    listen port (default 11440)
    GAIA_SEAM_BUNDLE     seam bundle dir (default ~/workspace/gaia-node/si-seam.si)
    GAIA_SI_PATH         SI package checkout (default ~/workspace/sentinel-main)
    GAIA_SI_PYTHON       interpreter for si_read.py (default the seam
                         service's venv python when present)
    GAIA_NODE_EVENTS     events endpoint (default http://127.0.0.1:11436/gaia/v1/events)
    GAIA_NODE_AGENCY     agency base (default http://127.0.0.1:11435)
    GAIA_MODEL_ENDPOINT  pin the model endpoint (e.g. http://127.0.0.1:44117);
                         otherwise discovered from the spine's last resident
                         event, falling back to gaia.log
    GAIA_NODE_HOME       node home for the gaia.log fallback (default
                         ~/workspace/gaia-node)
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
HOME = os.path.expanduser("~")

PORT = int(os.environ.get("GAIA_CONSOLE_PORT", "11440"))
SEAM_BUNDLE = os.environ.get(
    "GAIA_SEAM_BUNDLE", os.path.join(HOME, "workspace/gaia-node/si-seam.si"))
SI_PATH = os.environ.get(
    "GAIA_SI_PATH", os.path.join(HOME, "workspace/sentinel-main"))
NODE_HOME = os.environ.get(
    "GAIA_NODE_HOME", os.path.join(HOME, "workspace/gaia-node"))
EVENTS_URL = os.environ.get(
    "GAIA_NODE_EVENTS", "http://127.0.0.1:11436/gaia/v1/events")
AGENCY_BASE = os.environ.get("GAIA_NODE_AGENCY", "http://127.0.0.1:11435")


def _default_si_python():
    candidate = os.path.join(HOME, "workspace/si2-venv/bin/python")
    return candidate if os.path.exists(candidate) else sys.executable


SI_PYTHON = os.environ.get("GAIA_SI_PYTHON") or _default_si_python()
SI_READ = os.path.join(HERE, "si_read.py")

STATIC_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json",
    ".md": "text/plain; charset=utf-8",
    ".png": "image/png",
    ".webp": "image/webp",
    ".svg": "image/svg+xml",
    ".ico": "image/x-icon",
}


def _http_json(url, timeout=10, method="GET", payload=None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_si_read(mode):
    """Run the SI reader subprocess; returns its parsed JSON document."""
    env = dict(os.environ)
    env["PYTHONPATH"] = SI_PATH
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run(
        [SI_PYTHON, SI_READ, mode, SEAM_BUNDLE],
        capture_output=True, text=True, timeout=60, env=env)
    out = (proc.stdout or "").strip()
    if not out:
        raise RuntimeError("si_read produced no output: " +
                           (proc.stderr or "").strip()[:300])
    doc = json.loads(out)
    if proc.returncode != 0 or "error" in doc:
        raise RuntimeError("si_read failed: " + str(doc.get("error", proc.returncode)))
    return doc


# ---------------------------------------------------------------- spine

_spine_cache = {"at": 0.0, "doc": None}
_spine_lock = threading.Lock()


def fetch_spine():
    """Replay the node's events endpoint: count entries, find the head.

    The endpoint replays then follows live, so read until EOF or a
    short idle gap after data has arrived.
    """
    url = EVENTS_URL + "?source=hearth&after=0"
    req = urllib.request.Request(url, headers={"Accept": "text/event-stream"})
    total = 0
    last = None
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.fp.raw._sock.settimeout(2.0)
        buf = b""
        while True:
            try:
                chunk = resp.read1(65536) if hasattr(resp, "read1") else resp.read(65536)
            except (TimeoutError, OSError):
                break
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line.startswith(b"data:"):
                    continue
                try:
                    event = json.loads(line[5:].decode("utf-8"))
                except ValueError:
                    continue
                row = event.get("row") or {}
                total += 1
                if last is None or (row.get("seq") or 0) >= (last.get("seq") or 0):
                    last = row
    return total, last


def get_spine():
    with _spine_lock:
        now = time.time()
        if _spine_cache["doc"] is not None and now - _spine_cache["at"] < 10:
            return _spine_cache["doc"]
        total, last = fetch_spine()
        residency = None
        try:
            residency = _http_json(AGENCY_BASE + "/gaia/v1/residency", timeout=5)
        except Exception:
            residency = None
        doc = {
            "total_entries": total,
            "head_seq": (last or {}).get("seq"),
            "last_event": last,
            "residency": residency,
            "source": "live node events " + EVENTS_URL,
        }
        _spine_cache["doc"] = doc
        _spine_cache["at"] = now
        return doc


# ------------------------------------------------------------------ chat

def discover_model_endpoint():
    """(endpoint, model_name) for the resident model, discovered live."""
    pinned = os.environ.get("GAIA_MODEL_ENDPOINT")
    if pinned:
        return pinned.rstrip("/"), None
    # 1) spine: last 'resident' event for the resident model.
    try:
        spine = get_spine()
        residency = spine.get("residency") or {}
        resident = None
        for m in residency.get("models", []):
            if m.get("state") == "resident" and "ctx_pool" in m:
                resident = m.get("model")
                break
        last = spine.get("last_event") or {}
        if last.get("kind") == "resident" and (not resident or last.get("model") == resident):
            endpoint = (last.get("facts") or {}).get("endpoint")
            if endpoint:
                return "http://" + endpoint, last.get("model")
    except Exception:
        pass
    # 2) gaia.log: most recent 'loading on 127.0.0.1:PORT' line.
    try:
        with open(os.path.join(NODE_HOME, "gaia.log"), "r", errors="replace") as fh:
            text = fh.read()
        matches = re.findall(r"(\S+) loading on (127\.0\.0\.1:\d+)", text)
        if matches:
            model, endpoint = matches[-1]
            return "http://" + endpoint, model
    except OSError:
        pass
    raise RuntimeError("could not discover the resident model's endpoint "
                       "(spine replay and gaia.log both unavailable)")


def chat(message, history):
    endpoint, model = discover_model_endpoint()
    messages = []
    for item in (history or [])[-10:]:
        if (isinstance(item, dict) and item.get("role") in ("user", "assistant")
                and isinstance(item.get("content"), str)):
            messages.append({"role": item["role"],
                             "content": item["content"][:4000]})
    messages.append({"role": "user", "content": message})
    doc = _http_json(endpoint + "/v1/chat/completions", timeout=600,
                     method="POST",
                     payload={"messages": messages,
                              "max_tokens": 256,
                              "temperature": 0.7})
    choices = doc.get("choices") or []
    if not choices:
        raise RuntimeError("model endpoint returned no choices")
    reply = ((choices[0].get("message") or {}).get("content") or "").strip()
    return {
        "reply": reply,
        "model": model or doc.get("model"),
        "endpoint": endpoint,
        "usage": doc.get("usage"),
        "timings": doc.get("timings"),
    }


# ---------------------------------------------------------------- server

class Handler(BaseHTTPRequestHandler):
    server_version = "GaiaConsole/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[console] " + (fmt % args) + "\n")

    def _send_json(self, doc, status=200):
        body = json.dumps(doc).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, message, status=502):
        self._send_json({"error": message}, status=status)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        try:
            if path == "/api/journal":
                return self._send_json(run_si_read("journal"))
            if path == "/api/escalations":
                return self._send_json(run_si_read("escalations"))
            if path == "/api/spine":
                return self._send_json(get_spine())
        except Exception as exc:
            return self._send_error_json(str(exc))
        return self._serve_static(path)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path != "/api/chat":
            return self._send_error_json("not found", status=404)
        try:
            length = int(self.headers.get("Content-Length") or 0)
            doc = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        except Exception:
            return self._send_error_json("invalid JSON body", status=400)
        message = (doc.get("message") or "").strip()
        if not message:
            return self._send_error_json("message is required", status=400)
        try:
            return self._send_json(chat(message[:4000], doc.get("history")))
        except Exception as exc:
            return self._send_error_json("chat failed: " + str(exc))

    def _serve_static(self, path):
        if path in ("/", ""):
            path = "/index.html"
        rel = path.lstrip("/")
        full = os.path.normpath(os.path.join(HERE, rel))
        if not full.startswith(HERE) or not os.path.isfile(full):
            return self._send_error_json("not found", status=404)
        ext = os.path.splitext(full)[1].lower()
        with open(full, "rb") as fh:
            body = fh.read()
        self.send_response(200)
        self.send_header("Content-Type",
                         STATIC_TYPES.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print("gaia console: http://127.0.0.1:%d (seam bundle %s)" % (PORT, SEAM_BUNDLE),
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
