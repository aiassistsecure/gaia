#!/usr/bin/env python3
"""Gaia console server — serves the console and wires it to the live node.

Zero-dependency Python stdlib. Binds 127.0.0.1 only.

    python3 console/server.py            # port 11440
    GAIA_CONSOLE_PORT=8080 python3 console/server.py

Endpoints
    GET  /                  console/index.html (and static files beside it)
    GET  /api/journal       seam journal tail (last 8, full fields), read-only
    GET  /api/escalations   folded open escalations + gate reports, read-only
    GET  /api/commands      every tool intent in the journal, folded with
                            its outcome — the agent's command history
    GET  /api/spine         spine totals/head/last event + residency, live node
    GET  /api/config        console config state (autoapprove, kill state)
    POST /api/chat          {message} -> resident model's reply
    POST /api/resolve       {edit_id, resolution} -> journal an operator
                            resolution (SI-92 act, via si_act.py)
    POST /api/kill          {} -> killswitch: autoapprove off + stop the
                            node's systemd units (see "Killswitch" below)

Reads are read-only: the seam journal is read through the SI package's
own read path (si_read.py: open + hash-chain verify + fold) in a
subprocess; the spine is read through the node's events endpoint. The
one write path is resolutions: si_act.py performs the SI-92 verifying
act through si.gaia.escalation_cli.resolve — the same path as the
`si gaia-escalation resolve` CLI. A resolution never rewrites a gate
verdict; it is a separate journaled operator act. This server never
posts to the seam egress (11437). Chat goes only to the resident
model's llama-server endpoint, discovered live (see below).

Autoapprove (GAIA_AUTOAPPROVE=1, default OFF): a resolution POLICY,
never a gate bypass. The gate still escalates exactly what it
escalates; while enabled, the server polls the pending escalations
every few seconds and answers each with admit-after-review, credited
to operator "autoapprove (env GAIA_AUTOAPPROVE)" with the basis
recorded in si_act.py. The journal shows both halves: the ESCALATE
verdict, untouched, and the separate autoapprove resolution.

Killswitch (POST /api/kill): (1) autoapprove is disabled in this
process immediately and stays off; (2) the node's systemd units
(GAIA_NODE_UNITS, default "gaia-node.service si-seam-service.service")
are stopped with systemctl, and the response reports the verified
post-state (unit states, whether ports GAIA_NODE_PORTS still listen).
HONEST LIMIT: the gaia-node-watchdog hook polls every 120 s and
starts either unit when it finds it inactive — it has no
deliberate-stop marker (NODE.md: its deliberate-stop self-heal is a
designed behavior) — so a console kill halts the node immediately
but the watchdog will revive it within ~2 minutes unless the
watchdog hook is also disabled (Hatch runtime state this server
cannot toggle) or the units are masked (masking is not available
for these /etc-defined units on this systemd; verified 2026-11-21).
GAIA_KILL_DRY_RUN=1 makes /api/kill report what WOULD be stopped,
with current unit/port states, changing nothing.

Configuration (environment):
    GAIA_CONSOLE_PORT    listen port (default 11440)
    GAIA_SEAM_BUNDLE     seam bundle dir (default ~/workspace/gaia-node/si-seam.si)
    GAIA_SI_PATH         SI package checkout (default ~/workspace/sentinel-main)
    GAIA_SI_PYTHON       interpreter for si_read.py / si_act.py (default
                         the seam service's venv python when present)
    GAIA_NODE_EVENTS     events endpoint (default http://127.0.0.1:11436/gaia/v1/events)
    GAIA_NODE_AGENCY     agency base (default http://127.0.0.1:11435)
    GAIA_MODEL_ENDPOINT  pin the model endpoint (e.g. http://127.0.0.1:44117);
                         otherwise discovered from the spine's last resident
                         event, falling back to gaia.log
    GAIA_NODE_HOME       node home for the gaia.log fallback (default
                         ~/workspace/gaia-node)
    GAIA_AUTOAPPROVE     "1" enables the autoapprove policy (default off)
    GAIA_AUTOAPPROVE_INTERVAL  poll seconds (default 3)
    GAIA_KILL_DRY_RUN    "1" makes POST /api/kill a no-op report
    GAIA_NODE_UNITS      units the killswitch stops (space-separated)
    GAIA_NODE_PORTS      ports the killswitch verifies (comma-separated)
"""

import json
import os
import re
import socket
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

AUTOAPPROVE_ENV = os.environ.get("GAIA_AUTOAPPROVE") == "1"
AUTOAPPROVE_INTERVAL = float(os.environ.get("GAIA_AUTOAPPROVE_INTERVAL", "3"))
KILL_DRY_RUN = os.environ.get("GAIA_KILL_DRY_RUN") == "1"
NODE_UNITS = os.environ.get(
    "GAIA_NODE_UNITS", "gaia-node.service si-seam-service.service").split()
NODE_PORTS = [int(p) for p in os.environ.get(
    "GAIA_NODE_PORTS", "11435,11436,11437").split(",") if p.strip()]

# Runtime operator state: autoapprove starts as the env says; the
# killswitch turns it off for the life of this process, whatever
# the env says. Guarded by _state_lock.
_state_lock = threading.Lock()
_state = {
    "autoapprove_active": AUTOAPPROVE_ENV,
    "killed": False,
    "kill_report": None,
}


def autoapprove_active():
    with _state_lock:
        return bool(_state["autoapprove_active"])


def config_doc():
    with _state_lock:
        return {
            "autoapprove_env": AUTOAPPROVE_ENV,
            "autoapprove_active": bool(_state["autoapprove_active"]),
            "autoapprove_interval_s": AUTOAPPROVE_INTERVAL,
            "kill_dry_run": KILL_DRY_RUN,
            "killed": bool(_state["killed"]),
            "kill_report": _state["kill_report"],
            "node_units": NODE_UNITS,
            "node_ports": NODE_PORTS,
        }


def _default_si_python():
    candidate = os.path.join(HOME, "workspace/si2-venv/bin/python")
    return candidate if os.path.exists(candidate) else sys.executable


SI_PYTHON = os.environ.get("GAIA_SI_PYTHON") or _default_si_python()
SI_READ = os.path.join(HERE, "si_read.py")
SI_ACT = os.path.join(HERE, "si_act.py")

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


class SiRefusal(Exception):
    """The SI layer refused the act; the message is its own, verbatim."""


# All resolution acts (manual + autoapprove) serialize on this lock:
# one verified act at a time against the journal, never two writers
# racing the fold.
_act_lock = threading.Lock()


def run_si_act(args, timeout=180):
    """Run the SI actor subprocess (si_act.py); returns its JSON
    document. A refusal raises SiRefusal with the SI layer's own
    message; anything else raises RuntimeError."""
    env = dict(os.environ)
    env["PYTHONPATH"] = SI_PATH
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run(
        [SI_PYTHON, SI_ACT] + args + [SEAM_BUNDLE],
        capture_output=True, text=True, timeout=timeout, env=env)
    out = (proc.stdout or "").strip()
    if not out:
        raise RuntimeError("si_act produced no output: " +
                           (proc.stderr or "").strip()[:300])
    doc = json.loads(out)
    if proc.returncode != 0 or "error" in doc:
        raise SiRefusal(str(doc.get("error", proc.returncode)))
    return doc


def resolve_resolution(edit_id, resolution, operator, basis=""):
    """One operator resolution, serialized. Returns the act's
    document (resolution entry + folded intent after the act)."""
    args = ["resolve", edit_id, resolution, operator]
    if basis:
        args.append(basis)
    with _act_lock:
        return run_si_act(args)


def autoapprove_loop():
    """The GAIA_AUTOAPPROVE policy: while active, answer every
    pending escalation with admit-after-review (see header docs).
    The gate is untouched; only resolutions are journaled, each
    one a verified SI-92 act credited to the autoapprove operator."""
    while True:
        time.sleep(AUTOAPPROVE_INTERVAL)
        if not autoapprove_active():
            continue
        try:
            with _act_lock:
                doc = run_si_act(["autoapprove"])
            if doc.get("resolved"):
                sys.stderr.write(
                    "[console] autoapprove resolved: %s\n" % ", ".join(
                        "%s@%s" % (r["edit_id"], r["resolution_seq"])
                        for r in doc["resolved"]))
            for err in doc.get("errors") or []:
                sys.stderr.write("[console] autoapprove error on %s: %s\n"
                                 % (err.get("edit_id"), err.get("error")))
        except Exception as exc:
            sys.stderr.write("[console] autoapprove pass failed: %s\n" % exc)


# ------------------------------------------------------------------ kill

def _systemctl(*args, timeout=30):
    proc = subprocess.run(["systemctl"] + list(args),
                          capture_output=True, text=True, timeout=timeout)
    return proc.returncode, (proc.stdout or "").strip(), \
        (proc.stderr or "").strip()


def _unit_state(unit):
    rc, out, _ = _systemctl("is-active", unit)
    active = out or "unknown"
    rc2, out2, _ = _systemctl("is-enabled", unit)
    return {"unit": unit, "active": active, "enabled": out2 or "unknown"}


def _port_listening(port):
    sock = socket.socket()
    sock.settimeout(1.5)
    try:
        return sock.connect_ex(("127.0.0.1", port)) == 0
    finally:
        sock.close()


def kill_node():
    """The killswitch act. Returns the report document; the same
    document is stored for /api/config. In dry-run mode nothing is
    changed: the report lists what WOULD be stopped with the
    current unit/port states."""
    before_units = [_unit_state(u) for u in NODE_UNITS]
    before_ports = [{"port": p, "listening": _port_listening(p)}
                    for p in NODE_PORTS]
    if KILL_DRY_RUN:
        return {
            "dry_run": True,
            "would_disable_autoapprove": autoapprove_active(),
            "would_stop_units": before_units,
            "ports_now": before_ports,
            "watchdog_note": WATCHDOG_NOTE,
        }
    with _state_lock:
        _state["autoapprove_active"] = False
    stop_errors = []
    for unit in NODE_UNITS:
        try:
            rc, out, err = _systemctl("stop", unit, timeout=120)
            if rc != 0:
                stop_errors.append({"unit": unit, "error": err or out})
        except Exception as exc:
            stop_errors.append({"unit": unit, "error": str(exc)})
    # Verified post-state: poll briefly for the units to settle.
    after_units = before_units
    for _ in range(10):
        after_units = [_unit_state(u) for u in NODE_UNITS]
        if all(u["active"] != "active" for u in after_units):
            break
        time.sleep(1)
    after_ports = [{"port": p, "listening": _port_listening(p)}
                   for p in NODE_PORTS]
    report = {
        "dry_run": False,
        "autoapprove_disabled": True,
        "stopped_units": after_units,
        "stop_errors": stop_errors,
        "ports_after": after_ports,
        "node_down": all(u["active"] != "active" for u in after_units)
                     and not any(p["listening"] for p in after_ports),
        "watchdog_note": WATCHDOG_NOTE,
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with _state_lock:
        _state["killed"] = True
        _state["kill_report"] = report
    return report


WATCHDOG_NOTE = (
    "the gaia-node-watchdog hook polls every 120 s and starts either "
    "unit when it finds it inactive (no deliberate-stop marker; "
    "NODE.md), so this halt is immediate but the watchdog will "
    "revive the units within ~2 minutes unless the watchdog hook "
    "is also disabled — Hatch runtime state this server cannot "
    "toggle")


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
            if path == "/api/commands":
                return self._send_json(run_si_read("commands"))
            if path == "/api/config":
                return self._send_json(config_doc())
            if path == "/api/spine":
                return self._send_json(get_spine())
        except Exception as exc:
            return self._send_error_json(str(exc))
        return self._serve_static(path)

    def _read_body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length).decode("utf-8") or "{}")

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path not in ("/api/chat", "/api/resolve", "/api/kill"):
            return self._send_error_json("not found", status=404)
        try:
            doc = self._read_body()
        except Exception:
            return self._send_error_json("invalid JSON body", status=400)
        if path == "/api/kill":
            try:
                return self._send_json(kill_node())
            except Exception as exc:
                return self._send_error_json("kill failed: " + str(exc))
        if path == "/api/resolve":
            edit_id = (doc.get("edit_id") or "").strip()
            resolution = (doc.get("resolution") or "").strip()
            if not edit_id or not resolution:
                return self._send_error_json(
                    "edit_id and resolution are required", status=400)
            try:
                return self._send_json(resolve_resolution(
                    edit_id, resolution, "Mark (Gaia console)",
                    basis=(doc.get("basis") or "").strip()))
            except SiRefusal as exc:
                # The SI layer's refusal, verbatim.
                return self._send_error_json(str(exc), status=409)
            except Exception as exc:
                return self._send_error_json("resolve failed: " + str(exc))
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
    if AUTOAPPROVE_ENV:
        print("gaia console: AUTOAPPROVE on (GAIA_AUTOAPPROVE=1) — pending "
              "escalations will be resolved admit-after-review", flush=True)
        threading.Thread(target=autoapprove_loop, daemon=True).start()
    if KILL_DRY_RUN:
        print("gaia console: GAIA_KILL_DRY_RUN=1 — /api/kill reports only",
              flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
