//! SSE serving for the Gaia event stream (G-4, G-2 §4).
//!
//! This module is the server SI's reference follower (SI-48/49)
//! was written against, carried in the runtime process: one
//! listener, its own connection budget, and a fan-out hub whose
//! writers never wait on readers. The semantics are not new —
//! they are SI-47/48/49's, restated in G-2 §4 and fixed in
//! [`crate::events`]: an event's `id:` is its sequence in its own
//! source's space, the two sources' sequences are never merged,
//! resume is `Last-Event-ID` per source, filters are applied
//! before buffering, and a consumer that falls behind is told so
//! with a `resync` frame and closed, never silently dropped from
//! and never allowed to grow memory on the server's behalf.
//!
//! The shape is upstream's, deliberately: std-only networking
//! (`TcpListener`, one thread per connection, a bounded poll
//! loop), no async runtime and no new dependency. The events
//! listener is **not** the gateway's listener and does not draw
//! on the inference `max_inflight` pool — §4.4's point is that a
//! flood of subscribers and a flood of inference requests fail
//! independently, so this server counts its own connections
//! against its own budget and refuses past it with a 503.
//!
//! What this module does not do, stated where it lives: it does
//! not read the spine. `hearth`-source history arrives as an
//! [`EventLog`] the composition supplies (the mirror of §4.3),
//! and `si`-source history arrives through [`SiEventBuffer`],
//! whose ingest contract is "SI forwards in order" (§4.2). There
//! is no authentication, as there is none anywhere in this
//! workspace: the bind is loopback, and that is the whole story.
//!
//! ## Combined subscriptions (G-4b)
//!
//! `GET /gaia/v1/events` collects **all** `source` query params,
//! in order, deduped; absent means `[hearth]` (legacy). An
//! unknown source value is the existing 400
//! `gaia_event_source_unknown`. One source is **legacy mode**:
//! byte-identical wire behaviour to before — bare `id: <seq>`,
//! a bare numeric `Last-Event-ID`, the existing banner and
//! resync shapes. Two sources is **combined mode**, defined
//! completely here:
//!
//! * **Qualified ids.** Wire ids are source-qualified pairs —
//!   `id: hearth:<seq>` / `id: si:<seq>` — via
//!   `StreamEvent::sse_block_qualified`, used only here. Event
//!   names and `data:` are unchanged (data already carries
//!   `source` + `seq`). The banner names both sources. The two
//!   sequence spaces are **never merged** (G-2 §4.2); a
//!   qualified id is a pair of independent cursors (G-9).
//! * **Query cursors.** Each cursor pairs with the `source`
//!   named *before* it:
//!   `?source=hearth&since=1234&source=si&since=987`
//!   (`since` and `after` are both aliases; the first cursor
//!   named for a source wins; a source with no cursor defaults
//!   to 0). A cursor appearing before any `source` in a
//!   combined request is ambiguous and is refused loudly with
//!   400 `gaia_event_resume_invalid`.
//! * **Composite header.** `Last-Event-ID` beats the query, as
//!   in legacy mode. In combined mode its single value encodes
//!   **both** cursors in exactly this grammar:
//!   `hearth:<seq>,si:<seq>` — either order, both required;
//!   duplicates, unknown (or unrequested) sources, missing
//!   sources, and unparsable seqs are all 400
//!   `gaia_event_resume_invalid`. A bare numeric header on a
//!   combined request is 400 (no silent ambiguity), and a
//!   qualified/composite header on a single-source request is
//!   likewise 400.
//! * **Per-source resync.** The hub ring is one ring, but
//!   overflow is recorded per source. [`Drain`] keeps the
//!   legacy `resync_at` (the single-source case) and adds
//!   `resync_points: Vec<(Source, u64)>`. The combined serving
//!   loop emits one `resync` frame per overflowed source —
//!   each frame's data carrying its source and that source's
//!   newest seq, no `id:` line — then closes, exactly as the
//!   legacy frame never advances a cursor.
//! * **Replay order — stated honestly.** Replay is
//!   subscribe-before-replay, as before. Per source, replay is
//!   that log's `events_since` in seq order. Cross-source, the
//!   per-source lists are merged by event timestamp when both
//!   heads carry one (ties: request source order, then seq);
//!   when timestamps are not comparable, the merge falls back
//!   to a deterministic interleave in request source order.
//!   **Live-tail order is true hub arrival order** — publish
//!   order, preserved by the drain loop. **Cross-source replay
//!   interleaving is a presentation order that makes NO causal
//!   claim**: the two journals share no clock and no counter,
//!   so no cross-source ordering of replayed history can claim
//!   more than that. That is the honest limit, stated not
//!   hidden.

use std::collections::{HashMap, VecDeque};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::events::{EventFilter, EventLog, Source, StreamEvent};

/// Lock a mutex, recovering the guard if a previous holder
/// panicked. The state behind these mutexes is plain data — a
/// poisoned lock here means some thread died mid-update, and
/// refusing every later event would turn that one death into a
/// silent end of the whole stream. Recovering keeps the failure
/// where it happened.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

// ---------------------------------------------------------------------------
// EventHub: fan-out with bounded rings
// ---------------------------------------------------------------------------

struct Subscription {
    /// One source in legacy mode, both in combined mode. The
    /// subscription receives every listed source through the
    /// one filter; the ring is in hub arrival (publish) order.
    sources: Vec<Source>,
    filter: EventFilter,
    ring: VecDeque<StreamEvent>,
    /// Per-source overflow: the newest seq, per source, at
    /// which this subscription's ring was found full, while an
    /// overflow for that source is outstanding. An entry is
    /// both the resync marker and the "already counted" flag
    /// for that source's episode: it is refreshed on every
    /// overflowing publish for its source (so the resync point
    /// never goes stale) but counted only when first inserted,
    /// and cleared by `drain`. Kept in `sources` order.
    overflowed: Vec<(Source, u64)>,
}

struct HubInner {
    next_id: u64,
    subscriptions: HashMap<u64, Subscription>,
    published_total: u64,
    overflowed_total: u64,
}

/// The fan-out hub: publishers offer events, subscribers drain
/// bounded rings.
///
/// `publish` never blocks and never grows a ring past the
/// capacity the hub was built with. A subscriber whose ring is
/// full is marked overflowed instead of being dropped from
/// silently — the mark rides out on the next [`EventHub::drain`]
/// as `resync_at`, and the serving loop turns it into the
/// `resync` frame of §4.4 before closing that connection.
#[derive(Clone)]
pub struct EventHub {
    inner: Arc<Mutex<HubInner>>,
    ring_capacity: usize,
}

/// What one [`EventHub::drain`] hands back: everything queued
/// since the last drain, and the resync point if the ring
/// overflowed in the meantime.
#[derive(Debug, Clone, PartialEq)]
pub struct Drain {
    pub events: Vec<StreamEvent>,
    /// `Some(newest_seq)` when this subscription overflowed:
    /// the consumer must resume from the source's log after
    /// abandoning this stream's tail, because events past the
    /// ring were not kept for it. Carries no implication about
    /// the events that *are* in this drain — the serving loop
    /// delivers those first, then the resync. This is the
    /// legacy single-source field: for a single-source
    /// subscription it is exactly that source's resync point
    /// (the first — and only — entry of `resync_points`).
    pub resync_at: Option<u64>,
    /// Per-source resync points (G-4b): one `(source,
    /// newest_seq)` per overflowed source, in subscription
    /// source order. For a single-source subscription this is
    /// empty or the one point mirroring `resync_at`; combined
    /// serving reads this field, never `resync_at`.
    pub resync_points: Vec<(Source, u64)>,
}

/// A point-in-time count of the hub, for status and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubStats {
    pub active_subscribers: usize,
    pub published_total: u64,
    pub overflowed_total: u64,
}

impl EventHub {
    /// A hub whose per-subscription rings each hold at most
    /// `ring_capacity` events. §4.4's starting value is 256 —
    /// a policy constant, UNMEASURED, chosen by whoever builds
    /// the hub; the hub itself just enforces it.
    pub fn new(ring_capacity: usize) -> EventHub {
        EventHub {
            inner: Arc::new(Mutex::new(HubInner {
                next_id: 1,
                subscriptions: HashMap::new(),
                published_total: 0,
                overflowed_total: 0,
            })),
            ring_capacity,
        }
    }

    /// Register a subscription to one source through one filter,
    /// and return its id. Filters are applied in `publish`,
    /// before buffering (§4.4): a non-matching event never
    /// enters the ring at all.
    pub fn subscribe(&self, source: Source, filter: EventFilter) -> u64 {
        self.subscribe_multi(&[source], filter)
    }

    /// Register a combined subscription to several sources
    /// through one filter, and return its id (G-4b). Sources
    /// are deduped, request order kept — that order is the
    /// `resync_points` order. The one ring holds matching
    /// events from every listed source in publish order, which
    /// *is* hub arrival order; `publish` applies the filter
    /// before buffering, exactly as for `subscribe`.
    pub fn subscribe_multi(&self, sources: &[Source], filter: EventFilter) -> u64 {
        let mut deduped: Vec<Source> = Vec::new();
        for s in sources {
            if !deduped.contains(s) {
                deduped.push(*s);
            }
        }
        let mut inner = lock(&self.inner);
        let id = inner.next_id;
        inner.next_id += 1;
        inner.subscriptions.insert(
            id,
            Subscription {
                sources: deduped,
                filter,
                ring: VecDeque::new(),
                overflowed: Vec::new(),
            },
        );
        id
    }

    /// Remove a subscription. Unknown ids are ignored — a
    /// connection tearing down twice is a race, not an error.
    pub fn unsubscribe(&self, id: u64) {
        lock(&self.inner).subscriptions.remove(&id);
    }

    /// Offer one event to every subscription of its source whose
    /// filter matches. Non-blocking by construction: the only
    /// lock is the hub's own, held for the offers, and a full
    /// ring is marked, never waited on and never grown.
    pub fn publish(&self, ev: &StreamEvent) {
        let mut inner = lock(&self.inner);
        inner.published_total += 1;
        // Counted outside the ring loop: the loop borrows the
        // subscriptions, so new overflow episodes are tallied on
        // the side and folded into the stats after it.
        let mut new_overflow_episodes = 0u64;
        for sub in inner.subscriptions.values_mut() {
            if !sub.sources.contains(&ev.source) || !sub.filter.matches(ev) {
                continue;
            }
            if sub.ring.len() >= self.ring_capacity {
                // Per-source: counted once per source per
                // episode; the point is refreshed on every
                // overflow for that source — a consumer told to
                // resync at an older seq than the server has
                // already passed would be sent backwards.
                match sub.overflowed.iter_mut().find(|(s, _)| *s == ev.source) {
                    Some((_, point)) => *point = (*point).max(ev.seq),
                    None => {
                        new_overflow_episodes += 1;
                        // Insert in subscription source order.
                        let pos = sub
                            .sources
                            .iter()
                            .position(|s| *s == ev.source)
                            .unwrap_or(sub.overflowed.len());
                        let at = sub
                            .overflowed
                            .iter()
                            .position(|(s, _)| {
                                sub.sources
                                    .iter()
                                    .position(|x| x == s)
                                    .unwrap_or(usize::MAX)
                                    > pos
                            })
                            .unwrap_or(sub.overflowed.len());
                        sub.overflowed.insert(at, (ev.source, ev.seq));
                    }
                }
            } else {
                sub.ring.push_back(ev.clone());
            }
        }
        inner.overflowed_total += new_overflow_episodes;
    }

    /// Pop everything queued for `id`, clearing any overflow
    /// mark. An unknown id drains empty — the serving loop may
    /// race its own teardown, and an empty drain is the honest
    /// answer either way.
    pub fn drain(&self, id: u64) -> Drain {
        let mut inner = lock(&self.inner);
        match inner.subscriptions.get_mut(&id) {
            Some(sub) => {
                let resync_points: Vec<(Source, u64)> = std::mem::take(&mut sub.overflowed);
                Drain {
                    events: sub.ring.drain(..).collect(),
                    // Legacy field: the first point. For the
                    // single-source subscriptions it was built
                    // for, that is the only point there can be.
                    resync_at: resync_points.first().map(|(_, seq)| *seq),
                    resync_points,
                }
            }
            None => Drain {
                events: Vec::new(),
                resync_at: None,
                resync_points: Vec::new(),
            },
        }
    }

    pub fn stats(&self) -> HubStats {
        let inner = lock(&self.inner);
        HubStats {
            active_subscribers: inner.subscriptions.len(),
            published_total: inner.published_total,
            overflowed_total: inner.overflowed_total,
        }
    }
}

// ---------------------------------------------------------------------------
// SiEventBuffer: the §4.2 ingest for SI-source events
// ---------------------------------------------------------------------------

/// The fork-side buffer of forwarded SI journal rows.
///
/// SI's service adapter forwards its journal events in order;
/// this buffer is where they land. The order is a contract, not
/// a hope: a `seq` that does not strictly follow the last pushed
/// one is a loud `Err`, never silently reordered or deduplicated
/// — a renumbered or replayed-from-the-wrong-point SI stream is
/// exactly the history corruption SI-49's follower fails closed
/// on, and the server must not be more forgiving than its
/// reference consumer. On success the event is appended and
/// published to the attached hub (if any) in the same call, so
/// replay and live tail read one history.
#[derive(Clone, Default)]
pub struct SiEventBuffer {
    events: Arc<Mutex<Vec<StreamEvent>>>,
    hub: Arc<Mutex<Option<EventHub>>>,
}

impl SiEventBuffer {
    pub fn new() -> SiEventBuffer {
        SiEventBuffer::default()
    }

    /// Attach the hub pushed events are fanned out to. Pushing
    /// before any hub is attached is fine — the events are kept
    /// for replay and simply have no live audience yet.
    pub fn attach_hub(&self, hub: EventHub) {
        *lock(&self.hub) = Some(hub);
    }

    /// Ingest one forwarded SI event, per the contract above.
    pub fn push(&self, ev: StreamEvent) -> Result<(), String> {
        if ev.source != Source::Si {
            return Err(format!(
                "SiEventBuffer carries si-source events only, got source {:?} at seq {}",
                ev.source, ev.seq
            ));
        }
        let hub = {
            let mut events = lock(&self.events);
            if let Some(last) = events.last() {
                if ev.seq <= last.seq {
                    return Err(format!(
                        "si events arrive in order: seq {} does not strictly follow last pushed seq {} \
                         — refusing to reorder or deduplicate silently",
                        ev.seq, last.seq
                    ));
                }
            }
            events.push(ev.clone());
            lock(&self.hub).clone()
        };
        if let Some(hub) = hub {
            hub.publish(&ev);
        }
        Ok(())
    }
}

impl EventLog for SiEventBuffer {
    fn events_since(
        &self,
        source: Source,
        after_seq: u64,
        filter: &EventFilter,
    ) -> Vec<StreamEvent> {
        if source != Source::Si {
            return Vec::new();
        }
        lock(&self.events)
            .iter()
            .filter(|ev| ev.seq > after_seq && filter.matches(ev))
            .cloned()
            .collect()
    }

    fn latest_seq(&self, source: Source) -> u64 {
        if source != Source::Si {
            return 0;
        }
        lock(&self.events).last().map(|ev| ev.seq).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

/// Budgets and timings for the events listener.
///
/// Every value here is a **starting value from the design**
/// (G-2 §4.4), UNMEASURED: 32 connections because §4.4 names it,
/// a 15 s heartbeat because §4.4 names it, and a 5 s write
/// timeout so a consumer that stops reading costs its thread
/// seconds, not forever. G-8 exists to replace these with
/// numbers; until it lands, treat them as policy, not capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventsConfig {
    pub max_connections: usize,
    pub heartbeat: Duration,
    pub write_timeout: Duration,
}

impl Default for EventsConfig {
    fn default() -> Self {
        EventsConfig {
            max_connections: 32,
            heartbeat: Duration::from_secs(15),
            write_timeout: Duration::from_secs(5),
        }
    }
}

/// The events server: its own listener, its own budget.
///
/// Dropping the server stops its accept loop (a flag plus a
/// self-connection to wake the blocking `accept`). Connection
/// threads are detached: a held-open follow stream belongs to
/// its consumer, and the server going away does not yank it —
/// the next heartbeat or event write against the closing
/// process is the thread's exit, as with upstream's relay.
pub struct EventsServer {
    hub: EventHub,
    active: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    local_addr: String,
    accept_handle: Option<std::thread::JoinHandle<()>>,
}

impl EventsServer {
    /// Bind `addr` and start serving `GET /gaia/v1/events` for
    /// the two sources' logs. `hearth` and `si` are separate
    /// [`EventLog`]s because their sequence spaces are separate
    /// (§4.2); the hub carries the live tail for both.
    pub fn bind(
        addr: &str,
        hearth: Arc<dyn EventLog>,
        si: Arc<dyn EventLog>,
        hub: EventHub,
        config: EventsConfig,
    ) -> std::io::Result<EventsServer> {
        let listener = TcpListener::bind(addr)?;
        let local_addr = listener.local_addr()?.to_string();
        let active = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let accept_handle = {
            let active = Arc::clone(&active);
            let stop = Arc::clone(&stop);
            let hub = hub.clone();
            std::thread::spawn(move || {
                for incoming in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stream = match incoming {
                        Ok(s) => s,
                        Err(_) => {
                            if stop.load(Ordering::SeqCst) {
                                break;
                            }
                            continue;
                        }
                    };
                    // The budget is checked on the counter this
                    // server owns — never queued, never borrowed
                    // from the inference pool (§4.4). Claiming a
                    // slot and handing it back is how the check
                    // stays atomic under racing accepts.
                    let claimed = active.fetch_add(1, Ordering::SeqCst);
                    if claimed >= config.max_connections {
                        active.fetch_sub(1, Ordering::SeqCst);
                        std::thread::spawn(move || reject_over_budget(stream, config));
                        continue;
                    }
                    let hearth = Arc::clone(&hearth);
                    let si = Arc::clone(&si);
                    let hub = hub.clone();
                    let active = Arc::clone(&active);
                    std::thread::spawn(move || {
                        handle_connection(stream, hearth, si, hub, config);
                        active.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            })
        };

        Ok(EventsServer {
            hub,
            active,
            stop,
            local_addr,
            accept_handle: Some(accept_handle),
        })
    }

    /// The address the listener bound, in `host:port` form —
    /// the same shape as `hearth_api::Server::local_addr`.
    pub fn local_addr(&self) -> String {
        self.local_addr.clone()
    }

    /// Connections currently counted against this server's own
    /// budget. Over-budget connections are never counted: they
    /// are refused, not held.
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub fn hub(&self) -> EventHub {
        self.hub.clone()
    }
}

impl Drop for EventsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the blocking accept so it can see the flag. The
        // self-connection is dropped by the accept loop's stop
        // check, uncounted and unserved.
        let _ = TcpStream::connect(self.local_addr.as_str());
        if let Some(handle) = self.accept_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Answer an over-budget connection with the §4.4 refusal and
/// close it. The write happens on its own thread so a slow
/// refused client cannot stall the accept loop; the short drain
/// afterwards is so the close is a FIN the client reads the 503
/// through, not a RST that can discard it — the request head
/// this connection arrived with was never read.
fn reject_over_budget(mut stream: TcpStream, config: EventsConfig) {
    let _ = stream.set_write_timeout(Some(config.write_timeout));
    let resp = hearth_api::Response::openai_error(
        503,
        format!(
            "event connection budget exhausted ({} connections)",
            config.max_connections
        ),
        "gaia_event_budget_exhausted",
        json!({}),
    );
    let wire = hearth_api::http::render_response(resp.status, &resp.body, resp.retry_after);
    if stream.write_all(wire.as_bytes()).is_ok() {
        let _ = stream.flush();
    }
    let _ = stream.shutdown(Shutdown::Write);
    let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
    let mut buf = [0u8; 1024];
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(_) => continue,
        }
    }
}

/// Write a `gaia_*` error in the OpenAI envelope, the shape
/// `hearth_api::Response::openai_error` fixes and
/// `hearth_api::http::render_response` serialises.
fn write_error(stream: &mut TcpStream, status: u16, message: String, kind: &str) {
    let resp = hearth_api::Response::openai_error(status, message, kind, json!({}));
    let wire = hearth_api::http::render_response(resp.status, &resp.body, resp.retry_after);
    let _ = stream.write_all(wire.as_bytes());
    let _ = stream.flush();
}

fn write_text(stream: &mut TcpStream, text: &str) -> Result<(), std::io::Error> {
    stream.write_all(text.as_bytes())?;
    stream.flush()
}

/// Read one request head — everything up to the blank line —
/// capped at 16 KiB. An events request is a GET: there is no
/// body to honour, so bytes past the head are not this
/// connection's business. `None` means no complete head arrived
/// (the peer went away, or the cap did): the connection is
/// closed unanswered, because anything written back would be a
/// guess about a request that was never read.
fn read_head(stream: &mut TcpStream) -> Option<String> {
    const CAP: usize = 16 * 1024;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 2048];
    loop {
        if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= CAP {
            break;
        }
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => return None,
        }
    }
    if buf.is_empty() {
        return None;
    }
    // An over-cap head with no terminator is not a request this
    // server can parse, at any size it is willing to hold. (A
    // head that reaches the cap still terminated is served —
    // unlikely, but parseable is parseable.)
    if buf.len() >= CAP && !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        return None;
    }
    Some(String::from_utf8_lossy(&buf).into_owned())
}

struct ParsedHead {
    method: String,
    /// The path only — the query is in `params`.
    path: String,
    params: Vec<(String, String)>,
    /// Header names lowercased on the way in, as in `hearth-api`:
    /// clients disagree about casing and the names are
    /// case-insensitive by specification.
    headers: HashMap<String, String>,
}

fn parse_head(head: &str) -> Option<ParsedHead> {
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_ascii_uppercase();
    let target = parts.next()?;
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q),
        None => (target.to_string(), ""),
    };
    let params = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (pair.to_string(), String::new()),
        })
        .collect();
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }
    Some(ParsedHead {
        method,
        path,
        params,
        headers,
    })
}

fn param<'a>(params: &'a [(String, String)], name: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

/// All `source` params, in order, deduped; absent = `[hearth]`.
/// `Err(raw)` carries the first unknown raw value, for the
/// caller's `gaia_event_source_unknown` 400.
fn collect_sources(params: &[(String, String)]) -> Result<Vec<Source>, String> {
    let mut sources: Vec<Source> = Vec::new();
    for (k, v) in params {
        if k == "source" {
            let s = Source::parse(v).ok_or_else(|| v.clone())?;
            if !sources.contains(&s) {
                sources.push(s);
            }
        }
    }
    if sources.is_empty() {
        sources.push(Source::Hearth);
    }
    Ok(sources)
}

/// Parse the composite `Last-Event-ID` grammar for a combined
/// request: `hearth:<seq>,si:<seq>` — either order, every
/// expected source exactly once, nothing else. Bare numbers,
/// duplicates, unknown/unrequested sources, missing sources,
/// and unparsable seqs are all `Err` (the caller answers 400
/// `gaia_event_resume_invalid`).
fn parse_composite_last_event_id(
    raw: &str,
    expected: &[Source],
) -> Result<Vec<(Source, u64)>, String> {
    let mut out: Vec<(Source, u64)> = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        let (name, seq_raw) = part
            .split_once(':')
            .ok_or_else(|| format!("composite cursor part is not source-qualified: {part:?}"))?;
        let source = Source::parse(name.trim())
            .ok_or_else(|| format!("unknown source in composite cursor: {name:?}"))?;
        if !expected.contains(&source) {
            return Err(format!(
                "composite cursor names unrequested source {source:?}"
            ));
        }
        if out.iter().any(|(s, _)| *s == source) {
            return Err(format!("duplicate cursor for source {source:?}"));
        }
        let seq: u64 = seq_raw
            .trim()
            .parse()
            .map_err(|_| format!("composite cursor seq is not a number: {seq_raw:?}"))?;
        out.push((source, seq));
    }
    if out.len() != expected.len() {
        return Err(format!(
            "composite cursor must carry exactly one cursor per requested source ({expected:?})"
        ));
    }
    // Return in request source order, whatever the wire order.
    out.sort_by_key(|(s, _)| expected.iter().position(|e| e == s).unwrap_or(usize::MAX));
    Ok(out)
}

/// Cursors from a `Last-Event-ID` header value, for one or
/// several sources. Single-source: only a bare numeric value
/// is valid — a qualified/composite value is `Err`. Combined:
/// only the composite grammar is valid — a bare numeric value
/// is `Err` (no silent ambiguity).
fn cursors_from_header(raw: &str, sources: &[Source]) -> Result<Vec<(Source, u64)>, String> {
    if sources.len() == 1 {
        let raw = raw.trim();
        if raw.contains(':') || raw.contains(',') {
            return Err(format!(
                "qualified Last-Event-ID on a single-source request: {raw:?}"
            ));
        }
        let seq: u64 = raw
            .parse()
            .map_err(|_| format!("resume cursor is not a valid sequence number: {raw:?}"))?;
        return Ok(vec![(sources[0], seq)]);
    }
    parse_composite_last_event_id(raw, sources)
}

/// Cursors from a combined request's query string: each
/// `since`/`after` pairs with the `source` named before it.
/// A cursor before any `source` is `Err`. The first cursor
/// named for a source wins; sources with none default to 0.
/// Returned in request source order.
fn cursors_from_query_combined(
    params: &[(String, String)],
    sources: &[Source],
) -> Result<Vec<(Source, u64)>, String> {
    let mut assigned: Vec<(Source, u64)> = Vec::new();
    let mut current: Option<Source> = None;
    for (k, v) in params {
        match k.as_str() {
            "source" => {
                // Already validated by `collect_sources`.
                current = Source::parse(v);
            }
            "since" | "after" => {
                let source = current.ok_or_else(|| {
                    "resume cursor appears before any source in a combined request".to_string()
                })?;
                if assigned.iter().any(|(s, _)| *s == source) {
                    continue;
                }
                let seq: u64 = v
                    .trim()
                    .parse()
                    .map_err(|_| format!("resume cursor is not a valid sequence number: {v:?}"))?;
                assigned.push((source, seq));
            }
            _ => {}
        }
    }
    Ok(sources
        .iter()
        .map(|s| {
            (
                *s,
                assigned
                    .iter()
                    .find(|(a, _)| a == s)
                    .map(|(_, seq)| *seq)
                    .unwrap_or(0),
            )
        })
        .collect())
}

fn cursor_for(cursors: &[(Source, u64)], source: Source) -> u64 {
    cursors
        .iter()
        .find(|(s, _)| *s == source)
        .map(|(_, seq)| *seq)
        .unwrap_or(0)
}

/// Merge per-source replay lists (each in its own seq order,
/// given in request source order) into one presentation order.
///
/// When both heads carry a timestamp, the earlier timestamp
/// goes first (ties: request source order, then seq — the
/// earlier source's head wins a tie). When the heads'
/// timestamps are not comparable (either head has none, or a
/// comparison fails), the merge falls back to a deterministic
/// interleave in request source order. **This merged order is
/// a presentation order and makes NO causal claim** — the two
/// journals share no clock or counter (see module docs).
/// Live-tail order is a different thing entirely: true hub
/// arrival order, never this merge.
fn merge_replay(per_source: Vec<Vec<StreamEvent>>) -> Vec<StreamEvent> {
    let n = per_source.len();
    let mut idx = vec![0usize; n];
    let mut out: Vec<StreamEvent> = Vec::new();
    let mut interleave_turn = 0usize;
    loop {
        let alive: Vec<usize> = (0..n).filter(|&i| idx[i] < per_source[i].len()).collect();
        if alive.is_empty() {
            break;
        }
        if alive.len() == 1 {
            let i = alive[0];
            out.extend(per_source[i][idx[i]..].iter().cloned());
            break;
        }
        let (a, b) = (alive[0], alive[1]);
        let pick = match (per_source[a][idx[a]].ts, per_source[b][idx[b]].ts) {
            (Some(x), Some(y)) => match x.partial_cmp(&y) {
                Some(std::cmp::Ordering::Less) => a,
                Some(std::cmp::Ordering::Greater) => b,
                // Tie: request source order (`a` precedes `b`),
                // then seq — heads are each their source's
                // smallest remaining seq, so `a` is the answer.
                Some(std::cmp::Ordering::Equal) => a,
                None => {
                    let p = alive[interleave_turn % alive.len()];
                    interleave_turn += 1;
                    p
                }
            },
            _ => {
                let p = alive[interleave_turn % alive.len()];
                interleave_turn += 1;
                p
            }
        };
        out.push(per_source[pick][idx[pick]].clone());
        idx[pick] += 1;
    }
    out
}

/// Serve one connection to its end. Called on the connection's
/// own thread; the caller (the accept loop's spawn) owns the
/// budget counter around this call, and nothing in here touches
/// it — decrementing in exactly one place is what keeps the
/// count honest across every exit this function has.
fn handle_connection(
    mut stream: TcpStream,
    hearth: Arc<dyn EventLog>,
    si: Arc<dyn EventLog>,
    hub: EventHub,
    config: EventsConfig,
) {
    let _ = stream.set_write_timeout(Some(config.write_timeout));
    let head = match read_head(&mut stream) {
        Some(h) => h,
        None => return,
    };
    let parsed = match parse_head(&head) {
        Some(p) => p,
        None => return,
    };

    // Only the one route is served. Anything else — any other
    // path, any other method on this path — is the agency
    // surface's own 404, in its own vocabulary (§3.2).
    if parsed.method != "GET" || parsed.path != "/gaia/v1/events" {
        write_error(
            &mut stream,
            404,
            format!("no gaia route for {} {}", parsed.method, parsed.path),
            "gaia_route_not_found",
        );
        return;
    }

    // All `source` params, in order, deduped; absent = [hearth].
    let sources = match collect_sources(&parsed.params) {
        Ok(s) => s,
        Err(raw) => {
            write_error(
                &mut stream,
                400,
                format!("unknown event source: {raw:?} (expected \"hearth\" or \"si\")"),
                "gaia_event_source_unknown",
            );
            return;
        }
    };
    let combined = sources.len() > 1;

    // Resume is per source (§4.2), and the header takes
    // precedence over the query — SI's follower sends both with
    // the same value, so precedence is a rule for the requests
    // where they disagree, and there the standard header wins.
    // A cursor that does not parse is a refused resume, answered
    // with a clean 400: the SI follower fails closed on a 4xx
    // rather than retrying it (§4.1), which is only safe if the
    // refusal is unmistakably a refusal.
    //
    // Single-source is the legacy rule exactly (bare header, or
    // first `since`, else first `after`). Combined mode is the
    // G-4b scheme in the module docs: composite header, or
    // query cursors paired with the source named before them.
    let cursors: Vec<(Source, u64)> = match parsed.headers.get("last-event-id") {
        Some(v) => match cursors_from_header(v, &sources) {
            Ok(c) => c,
            Err(msg) => {
                write_error(&mut stream, 400, msg, "gaia_event_resume_invalid");
                return;
            }
        },
        None if combined => match cursors_from_query_combined(&parsed.params, &sources) {
            Ok(c) => c,
            Err(msg) => {
                write_error(&mut stream, 400, msg, "gaia_event_resume_invalid");
                return;
            }
        },
        None => {
            let raw = param(&parsed.params, "since").or_else(|| param(&parsed.params, "after"));
            match raw {
                None => vec![(sources[0], 0)],
                Some(raw) => match raw.trim().parse::<u64>() {
                    Ok(n) => vec![(sources[0], n)],
                    Err(_) => {
                        write_error(
                            &mut stream,
                            400,
                            format!("resume cursor is not a valid sequence number: {raw:?}"),
                            "gaia_event_resume_invalid",
                        );
                        return;
                    }
                },
            }
        }
    };

    let follow = matches!(param(&parsed.params, "follow"), Some("1") | Some("true"));
    let filter = EventFilter::from_params(&parsed.params);

    // Subscribe *before* replaying (§4.4's ordering): an event
    // published between the replay read and the tail would
    // otherwise fall through the gap, and an event the replay
    // also caught is recognised by its (source, seq) and skipped
    // in the tail — replay and tail are the same history read
    // twice, never two histories.
    let subscription = if follow {
        if combined {
            Some(hub.subscribe_multi(&sources, filter.clone()))
        } else {
            Some(hub.subscribe(sources[0], filter.clone()))
        }
    } else {
        None
    };

    if combined {
        serve_stream_combined(
            &mut stream,
            &hearth,
            &si,
            &sources,
            &cursors,
            &filter,
            subscription,
            &hub,
            &config,
        );
    } else {
        let source = sources[0];
        let log: &Arc<dyn EventLog> = match source {
            Source::Hearth => &hearth,
            Source::Si => &si,
        };
        serve_stream(
            &mut stream,
            log,
            source,
            cursor_for(&cursors, source),
            &filter,
            subscription,
            &hub,
            &config,
        );
    }

    if let Some(id) = subscription {
        hub.unsubscribe(id);
    }
}

/// The combined (two-source) SSE response: qualified ids, a
/// banner naming both sources, per-source replay merged by
/// [`merge_replay`], and one live drain loop whose order is
/// true hub arrival order. Per-source `last_sent` cursors
/// dedup by `(source, seq)` — the same seq in the other
/// source's space is a different event, never a duplicate.
#[allow(clippy::too_many_arguments)]
fn serve_stream_combined(
    stream: &mut TcpStream,
    hearth: &Arc<dyn EventLog>,
    si: &Arc<dyn EventLog>,
    sources: &[Source],
    cursors: &[(Source, u64)],
    filter: &EventFilter,
    subscription: Option<u64>,
    hub: &EventHub,
    config: &EventsConfig,
) {
    let head = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/event-stream\r\n",
        "Cache-Control: no-cache\r\n",
        "Connection: close\r\n",
        "\r\n"
    );
    if write_text(stream, head).is_err() {
        return;
    }
    // The banner names both sources, in request order.
    let names: Vec<&str> = sources.iter().map(|s| s.as_str()).collect();
    let banner = format!(": gaia events — sources {}\n\n", names.join(", "));
    if write_text(stream, &banner).is_err() {
        return;
    }

    let mut last_sent: Vec<(Source, u64)> = cursors.to_vec();
    let per_source: Vec<Vec<StreamEvent>> = sources
        .iter()
        .map(|s| {
            let log: &Arc<dyn EventLog> = match s {
                Source::Hearth => hearth,
                Source::Si => si,
            };
            log.events_since(*s, cursor_for(cursors, *s), filter)
        })
        .collect();
    for ev in merge_replay(per_source) {
        if write_text(stream, &ev.sse_block_qualified()).is_err() {
            return;
        }
        if let Some((_, seq)) = last_sent.iter_mut().find(|(s, _)| *s == ev.source) {
            *seq = ev.seq;
        }
    }

    let sub_id = match subscription {
        Some(id) => id,
        None => return,
    };

    let mut last_send = Instant::now();
    loop {
        let drained = hub.drain(sub_id);
        let mut sent_any = false;
        // Drained events are in hub publish order — arrival
        // order — and are served in exactly that order.
        for ev in &drained.events {
            if ev.seq <= cursor_for(&last_sent, ev.source) {
                continue;
            }
            if write_text(stream, &ev.sse_block_qualified()).is_err() {
                return;
            }
            if let Some((_, seq)) = last_sent.iter_mut().find(|(s, _)| *s == ev.source) {
                *seq = ev.seq;
            }
            sent_any = true;
        }
        if sent_any {
            last_send = Instant::now();
        }
        if !drained.resync_points.is_empty() {
            // One resync frame per overflowed source, each
            // carrying its own source and newest seq, no `id:`
            // line — a resync must never advance any cursor.
            for (src, newest) in &drained.resync_points {
                let frame = format!(
                    "event: resync\ndata: {{\"source\":\"{}\",\"seq\":{},\"reason\":\"slow_consumer\"}}\n\n",
                    src.as_str(),
                    newest
                );
                let _ = write_text(stream, &frame);
            }
            return;
        }
        if last_send.elapsed() >= config.heartbeat {
            if write_text(stream, ": hb\n\n").is_err() {
                return;
            }
            last_send = Instant::now();
            continue;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// The SSE response itself: headers, banner, replay, and — for a
/// following subscription — the tail loop. The argument list is
/// the connection's whole state, named rather than bundled: a
/// private serving routine, called from exactly one place.
#[allow(clippy::too_many_arguments)]
fn serve_stream(
    stream: &mut TcpStream,
    log: &Arc<dyn EventLog>,
    source: Source,
    cursor: u64,
    filter: &EventFilter,
    subscription: Option<u64>,
    hub: &EventHub,
    config: &EventsConfig,
) {
    let head = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Content-Type: text/event-stream\r\n",
        "Cache-Control: no-cache\r\n",
        "Connection: close\r\n",
        "\r\n"
    );
    if write_text(stream, head).is_err() {
        return;
    }
    // The banner is a comment block: SI's consumer reads it as
    // such — it carries no id and never advances a cursor — and
    // it tells a human looking at the raw stream which source's
    // sequence space the ids below belong to.
    let banner = format!(": gaia events — source {}\n\n", source.as_str());
    if write_text(stream, &banner).is_err() {
        return;
    }

    let mut last_sent = cursor;
    for ev in log.events_since(source, cursor, filter) {
        if write_text(stream, &ev.sse_block()).is_err() {
            return;
        }
        last_sent = ev.seq;
    }

    let sub_id = match subscription {
        Some(id) => id,
        // Replay-only: the history was the whole answer.
        None => return,
    };

    let mut last_send = Instant::now();
    loop {
        let drained = hub.drain(sub_id);
        let mut sent_any = false;
        for ev in &drained.events {
            if ev.seq <= last_sent {
                continue;
            }
            if write_text(stream, &ev.sse_block()).is_err() {
                return;
            }
            last_sent = ev.seq;
            sent_any = true;
        }
        if sent_any {
            last_send = Instant::now();
        }
        if let Some(newest) = drained.resync_at {
            // The final frame (§4.4): newest seq, no `id:` line —
            // a resync must never advance a consumer's cursor,
            // because the events it stands for were not
            // delivered. The consumer resumes from its own last
            // recorded id, through the log.
            let frame = format!(
                "event: resync\ndata: {{\"source\":\"{}\",\"seq\":{},\"reason\":\"slow_consumer\"}}\n\n",
                source.as_str(),
                newest
            );
            let _ = write_text(stream, &frame);
            return;
        }
        if last_send.elapsed() >= config.heartbeat {
            // A comment, never an event: it says "quiet", not
            // "something happened", and carries no id (§4.4).
            if write_text(stream, ": hb\n\n").is_err() {
                return;
            }
            last_send = Instant::now();
            continue;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn si_event(seq: u64) -> StreamEvent {
        StreamEvent {
            source: Source::Si,
            seq,
            kind: "task_open".to_string(),
            model: None,
            task: None,
            facts: json!({}),
            collection: "tasks".to_string(),
            row: json!({ "kind": "tool", "seq": seq }),
            row_kind: Some("tool".to_string()),
            hash: None,
            ts: None,
        }
    }

    fn hearth_event(seq: u64) -> StreamEvent {
        let mut ev = si_event(seq);
        ev.source = Source::Hearth;
        ev
    }

    struct StubLog {
        events: Vec<StreamEvent>,
    }

    impl EventLog for StubLog {
        fn events_since(
            &self,
            source: Source,
            after_seq: u64,
            filter: &EventFilter,
        ) -> Vec<StreamEvent> {
            self.events
                .iter()
                .filter(|ev| ev.source == source && ev.seq > after_seq && filter.matches(ev))
                .cloned()
                .collect()
        }

        fn latest_seq(&self, source: Source) -> u64 {
            self.events
                .iter()
                .filter(|ev| ev.source == source)
                .map(|ev| ev.seq)
                .max()
                .unwrap_or(0)
        }
    }

    fn server_with(
        hearth_events: Vec<StreamEvent>,
        si_events: Vec<StreamEvent>,
        hub: EventHub,
        config: EventsConfig,
    ) -> EventsServer {
        EventsServer::bind(
            "127.0.0.1:0",
            Arc::new(StubLog {
                events: hearth_events,
            }),
            Arc::new(StubLog { events: si_events }),
            hub,
            config,
        )
        .unwrap()
    }

    fn http_exchange(addr: &str, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).unwrap();
        out
    }

    // -- (a) hub: filter before buffering, overflow accounting --------

    #[test]
    fn hub_filter_applies_before_buffering() {
        let hub = EventHub::new(4);
        let filter = EventFilter {
            kinds: Some(["tool".to_string()].into_iter().collect()),
            ..EventFilter::default()
        };
        let sub = hub.subscribe(Source::Si, filter);

        let mut other = si_event(1);
        other.row_kind = Some("memory".to_string());
        hub.publish(&other);
        hub.publish(&si_event(2));
        // A different source entirely never reaches this ring,
        // matching filter or not.
        hub.publish(&hearth_event(3));

        let drained = hub.drain(sub);
        assert_eq!(
            drained.events.len(),
            1,
            "only the matching si event is buffered"
        );
        assert_eq!(drained.events[0].seq, 2);
        assert_eq!(drained.resync_at, None);
        assert_eq!(hub.stats().published_total, 3);
        assert_eq!(hub.stats().active_subscribers, 1);
    }

    #[test]
    fn hub_overflow_marks_resync_once_per_episode_and_drain_clears_it() {
        let hub = EventHub::new(2);
        let sub = hub.subscribe(Source::Si, EventFilter::default());
        for seq in 1..=4 {
            hub.publish(&si_event(seq));
        }
        let stats = hub.stats();
        assert_eq!(stats.published_total, 4);
        assert_eq!(
            stats.overflowed_total, 1,
            "two overflowing publishes in one episode count once"
        );

        let drained = hub.drain(sub);
        assert_eq!(
            drained.events.len(),
            2,
            "the ring never grew past its capacity"
        );
        assert_eq!(
            drained.resync_at,
            Some(4),
            "resync at the newest source seq"
        );

        // The mark is cleared: a second drain reports no resync,
        // and a fresh overflow is a new episode with its own count.
        let drained = hub.drain(sub);
        assert!(drained.events.is_empty());
        assert_eq!(drained.resync_at, None);
        for seq in 5..=7 {
            hub.publish(&si_event(seq));
        }
        assert_eq!(hub.stats().overflowed_total, 2);
        assert_eq!(hub.drain(sub).resync_at, Some(7));
    }

    // -- (b) SiEventBuffer ordering -----------------------------------

    #[test]
    fn si_buffer_rejects_out_of_order_and_duplicate_seqs_loudly() {
        let buf = SiEventBuffer::new();
        buf.push(si_event(2)).unwrap();
        assert!(buf.push(si_event(2)).is_err(), "a duplicate seq is refused");
        assert!(
            buf.push(si_event(1)).is_err(),
            "an out-of-order seq is refused"
        );
        assert!(
            buf.push(hearth_event(3)).is_err(),
            "a hearth event is not si ingest"
        );
        buf.push(si_event(3)).unwrap();

        let since = buf.events_since(Source::Si, 2, &EventFilter::default());
        assert_eq!(since.len(), 1);
        assert_eq!(since[0].seq, 3);
        assert_eq!(buf.latest_seq(Source::Si), 3);
        assert_eq!(
            buf.events_since(Source::Hearth, 0, &EventFilter::default()),
            Vec::new()
        );
        assert_eq!(buf.latest_seq(Source::Hearth), 0);
    }

    #[test]
    fn si_buffer_publishes_accepted_events_to_the_attached_hub() {
        let hub = EventHub::new(8);
        let sub = hub.subscribe(Source::Si, EventFilter::default());
        let buf = SiEventBuffer::new();
        buf.attach_hub(hub.clone());
        buf.push(si_event(1)).unwrap();
        let drained = hub.drain(sub);
        assert_eq!(drained.events.len(), 1);
        assert_eq!(drained.events[0].seq, 1);
    }

    // -- (c) real-socket replay ----------------------------------------

    #[test]
    fn replay_over_a_real_socket_sends_only_events_after_the_cursor_and_closes() {
        let server = server_with(
            Vec::new(),
            vec![si_event(1), si_event(2), si_event(3)],
            EventHub::new(16),
            EventsConfig::default(),
        );
        let out = http_exchange(
            &server.local_addr(),
            "GET /gaia/v1/events?source=si&after=1 HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
        );
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.contains("text/event-stream"), "{out}");
        assert!(out.contains(": gaia events — source si"), "{out}");
        assert!(
            !out.contains("id: 1\n"),
            "the cursor event itself is not replayed: {out}"
        );
        assert!(out.contains("id: 2\n"), "{out}");
        assert!(out.contains("id: 3\n"), "{out}");
        assert_eq!(
            out.matches("data: ").count(),
            2,
            "exactly the blocks after seq 1: {out}"
        );
        // The data carries the SI consumer's keys.
        assert!(out.contains("\"collection\""), "{out}");
        assert!(out.contains("\"row\""), "{out}");
        // `http_exchange` read to EOF: the replay-only connection
        // closing is the assertion that got us here.
    }

    // -- (d) malformed resume ------------------------------------------

    #[test]
    fn a_malformed_last_event_id_is_a_clean_400_refusal() {
        let server = server_with(
            Vec::new(),
            vec![si_event(1)],
            EventHub::new(16),
            EventsConfig::default(),
        );
        let out = http_exchange(
            &server.local_addr(),
            "GET /gaia/v1/events?source=si&follow=1 HTTP/1.1\r\nhost: 127.0.0.1\r\nLast-Event-ID: not-a-seq\r\n\r\n",
        );
        assert!(out.starts_with("HTTP/1.1 400"), "{out}");
        assert!(out.contains("gaia_event_resume_invalid"), "{out}");
    }

    // -- (e) the events budget is its own ------------------------------

    #[test]
    fn a_second_connection_past_the_budget_gets_503() {
        let config = EventsConfig {
            max_connections: 1,
            heartbeat: Duration::from_millis(50),
            write_timeout: Duration::from_secs(5),
        };
        let server = server_with(Vec::new(), Vec::new(), EventHub::new(16), config);
        let addr = server.local_addr();

        // Hold one following connection open: it takes the only slot.
        let mut held = TcpStream::connect(addr.as_str()).unwrap();
        held.set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        held.write_all(
            b"GET /gaia/v1/events?source=si&follow=1 HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
        )
        .unwrap();
        held.flush().unwrap();
        // Read until the banner arrives, however the segments
        // fall: headers and banner are two writes, and a single
        // `read` is only ever owed *some* of the bytes in flight.
        let mut banner = String::new();
        let mut buf = [0u8; 1024];
        let deadline = Instant::now() + Duration::from_secs(5);
        while !banner.contains(": gaia events") {
            assert!(Instant::now() < deadline, "banner never arrived: {banner}");
            let n = held.read(&mut buf).unwrap();
            assert!(n > 0, "held connection closed before its banner: {banner}");
            banner.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
        assert!(banner.contains("text/event-stream"), "{banner}");

        // The slot is visibly taken before the second attempt —
        // no racing the accept loop.
        let deadline = Instant::now() + Duration::from_secs(2);
        while server.active_connections() != 1 {
            assert!(
                Instant::now() < deadline,
                "held connection was never counted"
            );
            std::thread::sleep(Duration::from_millis(5));
        }

        let out = http_exchange(
            &addr,
            "GET /gaia/v1/events?source=si HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
        );
        assert!(out.starts_with("HTTP/1.1 503"), "{out}");
        assert!(out.contains("gaia_event_budget_exhausted"), "{out}");
        assert_eq!(
            server.active_connections(),
            1,
            "the refused connection was never counted against the budget"
        );
    }

    // -- (f) G-4b: composite header, query pairing, multi hub ---------

    fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    const BOTH: [Source; 2] = [Source::Hearth, Source::Si];

    #[test]
    fn composite_header_parses_in_either_order() {
        let a = parse_composite_last_event_id("hearth:1234,si:987", &BOTH).unwrap();
        assert_eq!(a, vec![(Source::Hearth, 1234), (Source::Si, 987)]);
        let b = parse_composite_last_event_id("si:987,hearth:1234", &BOTH).unwrap();
        assert_eq!(a, b, "wire order does not matter; request order out");
        assert_eq!(
            cursors_from_header("hearth:1,si:2", &BOTH).unwrap(),
            vec![(Source::Hearth, 1), (Source::Si, 2)]
        );
    }

    #[test]
    fn composite_header_rejects_duplicates_missing_unknown_and_bare() {
        assert!(
            parse_composite_last_event_id("hearth:1,hearth:2", &BOTH).is_err(),
            "duplicate"
        );
        assert!(
            parse_composite_last_event_id("hearth:1", &BOTH).is_err(),
            "missing si"
        );
        assert!(
            parse_composite_last_event_id("hearth:1,bogus:2", &BOTH).is_err(),
            "unknown"
        );
        assert!(
            parse_composite_last_event_id("hearth:1,si:x", &BOTH).is_err(),
            "bad seq"
        );
        // A bare numeric header on a combined request is refused.
        assert!(
            cursors_from_header("1234", &BOTH).is_err(),
            "bare on combined"
        );
        assert!(parse_composite_last_event_id("1234", &BOTH).is_err());
    }

    #[test]
    fn qualified_header_on_a_single_source_request_is_refused() {
        let one = [Source::Si];
        assert!(cursors_from_header("si:7", &one).is_err());
        assert!(cursors_from_header("hearth:1,si:2", &one).is_err());
        assert_eq!(
            cursors_from_header("7", &one).unwrap(),
            vec![(Source::Si, 7)]
        );
    }

    #[test]
    fn query_cursors_pair_with_the_source_named_before_them() {
        let got = cursors_from_query_combined(
            &params(&[
                ("source", "hearth"),
                ("since", "1234"),
                ("source", "si"),
                ("since", "987"),
            ]),
            &BOTH,
        )
        .unwrap();
        assert_eq!(got, vec![(Source::Hearth, 1234), (Source::Si, 987)]);
        // `after` alias, reversed source order, missing = 0.
        let got = cursors_from_query_combined(
            &params(&[("source", "si"), ("after", "5"), ("source", "hearth")]),
            &[Source::Si, Source::Hearth],
        )
        .unwrap();
        assert_eq!(got, vec![(Source::Si, 5), (Source::Hearth, 0)]);
        // A cursor before any source is ambiguous: refused.
        assert!(cursors_from_query_combined(
            &params(&[("since", "1"), ("source", "hearth"), ("source", "si")]),
            &BOTH,
        )
        .is_err());
        // Sources collect in order, deduped; absent = [hearth].
        assert_eq!(
            collect_sources(&params(&[
                ("source", "si"),
                ("source", "hearth"),
                ("source", "si")
            ]))
            .unwrap(),
            vec![Source::Si, Source::Hearth]
        );
        assert_eq!(collect_sources(&params(&[])).unwrap(), vec![Source::Hearth]);
        assert!(collect_sources(&params(&[("source", "bogus")])).is_err());
    }

    #[test]
    fn hub_multi_receives_both_sources_in_publish_order_through_one_filter() {
        let hub = EventHub::new(16);
        let sub = hub.subscribe_multi(&BOTH, EventFilter::default());
        hub.publish(&si_event(1));
        hub.publish(&hearth_event(10));
        hub.publish(&si_event(2));
        hub.publish(&hearth_event(11));
        let drained = hub.drain(sub);
        let got: Vec<(Source, u64)> = drained.events.iter().map(|e| (e.source, e.seq)).collect();
        assert_eq!(
            got,
            vec![
                (Source::Si, 1),
                (Source::Hearth, 10),
                (Source::Si, 2),
                (Source::Hearth, 11)
            ],
            "drain order is hub arrival (publish) order, sources unmerged"
        );
        assert!(drained.resync_points.is_empty());
    }

    #[test]
    fn hub_multi_overflow_is_per_source() {
        let hub = EventHub::new(2);
        let sub = hub.subscribe_multi(&BOTH, EventFilter::default());
        hub.publish(&hearth_event(1));
        hub.publish(&si_event(1));
        // Ring full from here: each source gets its own point.
        hub.publish(&hearth_event(2));
        hub.publish(&si_event(2));
        hub.publish(&hearth_event(3));
        let drained = hub.drain(sub);
        assert_eq!(drained.events.len(), 2);
        assert_eq!(
            drained.resync_points,
            vec![(Source::Hearth, 3), (Source::Si, 2)],
            "one point per overflowed source, request order, newest seq each"
        );
        assert_eq!(hub.stats().overflowed_total, 2, "one episode per source");
        let drained = hub.drain(sub);
        assert!(drained.resync_points.is_empty(), "drain clears the marks");
    }

    #[test]
    fn merge_replay_orders_by_ts_then_source_order_and_interleaves_without_ts() {
        let mut h1 = hearth_event(1);
        h1.ts = Some(2.0);
        let mut s1 = si_event(1);
        s1.ts = Some(1.0);
        let mut s2 = si_event(2);
        s2.ts = Some(2.0);
        let merged = merge_replay(vec![vec![h1], vec![s1, s2]]);
        let got: Vec<(Source, u64)> = merged.iter().map(|e| (e.source, e.seq)).collect();
        // si:1 (ts 1) first; ts-2 tie breaks to request order (hearth).
        assert_eq!(
            got,
            vec![(Source::Si, 1), (Source::Hearth, 1), (Source::Si, 2)]
        );
        // No timestamps: deterministic interleave, request order.
        let merged = merge_replay(vec![
            vec![hearth_event(1), hearth_event(2)],
            vec![si_event(1), si_event(2)],
        ]);
        let got: Vec<(Source, u64)> = merged.iter().map(|e| (e.source, e.seq)).collect();
        assert_eq!(
            got,
            vec![
                (Source::Hearth, 1),
                (Source::Si, 1),
                (Source::Hearth, 2),
                (Source::Si, 2)
            ]
        );
    }

    #[test]
    fn combined_replay_over_a_real_socket_uses_qualified_ids_and_names_both_sources() {
        let server = server_with(
            vec![hearth_event(1), hearth_event(2)],
            vec![si_event(1)],
            EventHub::new(16),
            EventsConfig::default(),
        );
        let out = http_exchange(
            &server.local_addr(),
            "GET /gaia/v1/events?source=hearth&source=si HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
        );
        assert!(out.starts_with("HTTP/1.1 200 OK"), "{out}");
        assert!(out.contains(": gaia events — sources hearth, si"), "{out}");
        assert!(out.contains("id: hearth:1\n"), "{out}");
        assert!(out.contains("id: si:1\n"), "{out}");
        assert!(
            !out.contains("id: 1\n"),
            "no bare ids in combined mode: {out}"
        );
        // A bare header on a combined request is a clean 400.
        let out = http_exchange(
            &server.local_addr(),
            "GET /gaia/v1/events?source=hearth&source=si HTTP/1.1\r\nhost: 127.0.0.1\r\nLast-Event-ID: 3\r\n\r\n",
        );
        assert!(out.starts_with("HTTP/1.1 400"), "{out}");
        assert!(out.contains("gaia_event_resume_invalid"), "{out}");
        // The composite header resumes per source.
        let out = http_exchange(
            &server.local_addr(),
            "GET /gaia/v1/events?source=hearth&source=si HTTP/1.1\r\nhost: 127.0.0.1\r\nLast-Event-ID: si:0,hearth:1\r\n\r\n",
        );
        assert!(out.contains("id: hearth:2\n"), "{out}");
        assert!(!out.contains("id: hearth:1\n"), "{out}");
        assert!(out.contains("id: si:1\n"), "{out}");
    }

    #[test]
    fn an_unknown_route_is_404_in_the_gaia_vocabulary() {
        let server = server_with(
            Vec::new(),
            Vec::new(),
            EventHub::new(16),
            EventsConfig::default(),
        );
        let out = http_exchange(
            &server.local_addr(),
            "GET /gaia/v1/nope HTTP/1.1\r\nhost: 127.0.0.1\r\n\r\n",
        );
        assert!(out.starts_with("HTTP/1.1 404"), "{out}");
        assert!(out.contains("gaia_route_not_found"), "{out}");
    }
}
