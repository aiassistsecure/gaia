#!/usr/bin/env python3
"""Criterion (1) helper: SI's SI-49 reference follower, unmodified,
following the gaia fork's stream through a drop-and-reconnect.

This script is driven by the Rust test in ``sse_acceptance.rs``.
It does three things, and nothing else:

  * puts the SI checkout on ``sys.path`` and imports
    ``collect_followed`` / ``FollowConfig`` from
    ``si.gaia.events_client`` — the follower itself is SI's code,
    used exactly as SI ships it;
  * runs a small stdlib-socket TCP proxy in front of the real
    gaia server. The follower connects to the proxy; the proxy
    forwards SSE blocks to it one block at a time, and after the
    3rd event block has passed on the first connection it closes
    both sockets, once. Later connections are forwarded
    untouched, so the follower's reconnect has a clean path.
  * calls ``collect_followed`` for exactly 6 events and prints
    the collected ids as one JSON line on stdout. The Rust side
    asserts that line is ``[1, 2, 3, 4, 5, 6]``.

One adaptation lives here, stated plainly: SI's service has a
single event source, so SI's follower never sends a ``source``
parameter. The gaia fork serves two sources and defaults a
request without one to ``hearth``. The proxy therefore adds
``source=si`` to the forwarded request line when the follower's
request does not carry one. That changes which of the fork's
two journals is asked for — the SI journal, which is the one
this test pushes into — and nothing about the stream the
follower reads back.

Stdlib only, like the follower itself.

Usage: follow_si49.py <real-server host:port> <si-repo path>
"""

from __future__ import annotations

import json
import socket
import sys
import threading


def parse_host_port(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    if not host or not port:
        raise SystemExit(f"real server address is not host:port: {text!r}")
    return host, int(port)


class SeverState:
    """Shared proxy state: has the one planned sever happened?"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.event_blocks_seen = 0
        self.severed = False


def is_event_block(block: bytes) -> bool:
    """An SSE block is an event block when it carries an ``id:`` line.

    Comment blocks (the banner, heartbeats) carry no id and never
    count toward the sever point — the follower's cursor only
    moves on events, so the sever is counted in events too.
    """
    return any(line.startswith(b"id:") for line in block.split(b"\n"))


def add_source_param(request_head: bytes) -> bytes:
    """Add ``source=si`` to the request line's query, if absent.

    See the module docstring for why this adaptation exists and
    why it belongs to the proxy rather than to the follower.
    Only the first line (the request line) is touched.
    """
    head, _, rest = request_head.partition(b"\r\n")
    parts = head.split(b" ")
    if len(parts) < 2:
        return request_head
    target = parts[1]
    if b"source=" not in target:
        joiner = b"&" if b"?" in target else b"?"
        parts[1] = target + joiner + b"source=si"
    return b" ".join(parts) + b"\r\n" + rest


def handle_connection(client: socket.socket, real_addr, state: SeverState) -> None:
    """Forward one proxied connection, block by block.

    Blocks are forwarded whole, one at a time — never as raw
    chunks — so the planned sever lands exactly after the 3rd
    event block: nothing from block 4 onward can leak through
    inside a coalesced TCP segment before the close.
    """
    upstream: socket.socket | None = None
    try:
        # The follower's request head, up to the blank line.
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = client.recv(4096)
            if not chunk:
                return
            request += chunk
        request = add_source_param(request)

        upstream = socket.create_connection(real_addr, timeout=10)
        upstream.sendall(request)

        buffered = b""
        while True:
            chunk = upstream.recv(4096)
            if not chunk:
                return
            buffered += chunk
            # The HTTP head ends in \r\n\r\n, which holds no
            # \n\n, so the first block out of this split is the
            # head plus the banner — forwarded whole, counted as
            # the comment block it is.
            while b"\n\n" in buffered:
                block, buffered = buffered.split(b"\n\n", 1)
                client.sendall(block + b"\n\n")
                if not is_event_block(block):
                    continue
                with state.lock:
                    if state.severed:
                        continue
                    state.event_blocks_seen += 1
                    sever_now = state.event_blocks_seen >= 3
                    if sever_now:
                        state.severed = True
                if sever_now:
                    # The sever: both sockets close with block 3
                    # fully sent and nothing past it forwarded.
                    # The follower sees a clean end of stream and
                    # reconnects under its own SI-49 discipline.
                    return
    except OSError as exc:
        print(f"proxy: connection ended with {exc!r}", file=sys.stderr)
    finally:
        for sock in (client, upstream):
            if sock is not None:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass


def run_proxy(listener: socket.socket, real_addr, state: SeverState) -> None:
    """Accept proxied connections for as long as the script runs.

    The follower's connections are sequential — one live
    connection at a time, a reconnect only after a drop — but
    each is still handled on its own thread so a held-open
    follow stream can never block the accept loop.
    """
    while True:
        try:
            client, _ = listener.accept()
        except OSError:
            return
        threading.Thread(
            target=handle_connection,
            args=(client, real_addr, state),
            daemon=True,
        ).start()


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    real_addr = parse_host_port(sys.argv[1])
    si_repo = sys.argv[2]

    sys.path.insert(0, si_repo)
    # Imported after the path is set, on purpose: this is SI's
    # follower, from SI's checkout, not a copy of it.
    from si.gaia.events_client import FollowConfig, collect_followed

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    proxy_port = listener.getsockname()[1]
    state = SeverState()
    threading.Thread(
        target=run_proxy, args=(listener, real_addr, state), daemon=True
    ).start()

    proxy_base = f"http://127.0.0.1:{proxy_port}/gaia/v1"
    result = collect_followed(
        proxy_base,
        config=FollowConfig(
            max_events=6, backoff_base=0.05, backoff_cap=0.2, max_duration=60
        ),
        timeout=10,
    )
    ids = [event["id"] for event in result["events"]]
    if not state.severed:
        # The headline of criterion (1) is a real drop and a real
        # reconnect. If the stream never dropped, the ids alone
        # would look identical while proving less — say so loudly
        # instead of printing a passing-looking line.
        print(
            "proxy never severed the first connection; "
            "the follow was not a drop-and-reconnect",
            file=sys.stderr,
        )
        return 3
    print(json.dumps(ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
