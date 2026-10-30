//! The spine mirror: a gaia-owned, append-only cache of spine
//! events (G-2 §4.3a).
//!
//! The spine (`hearth-store`) has sequence numbers and causal
//! history but no event feed — no `events_since`, no subscription,
//! no range read. Serving `hearth`-source events in seq order
//! from a cursor therefore needs exactly one new read, and this
//! module is it: a poller watches the spine, appends newly
//! observed events to a plain JSONL file keyed by spine seq, and
//! the SSE layer replays and tails from that file's contents.
//!
//! The mirror is a **cache of the spine, never a source of
//! truth**. Every entry records the spine `hash` of the event it
//! mirrors, so [`MirrorStore::verify_against`] can check the
//! mirror against spine truth at any time; if mirror and spine
//! ever disagree, the spine wins and the mirror is rebuilt
//! ([`MirrorStore::rebuild`]). Nothing here writes to the spine,
//! and nothing here invents an event the spine did not record.
//!
//! The honest costs, stated where they live: event latency
//! inherits the supervisor's tick, its probe latency, and the
//! poll cadence (G-2 §4.3a) — this is a polling cache, not a
//! push channel. And the collection walk behind a rebuild is
//! causal, not exhaustive (see [`collect_spine_events`]).

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};

use hearth_store::Spine;

use crate::events::{EventFilter, EventLog, Source, StreamEvent};

/// Serialise one mirrored event in the on-disk stored shape:
/// `{"seq","model","kind","facts","hash","ts"}` — exactly the
/// fields [`StreamEvent::from_spine`] builds a stream event
/// from, and nothing else. The mirror stores spine facts, not
/// wire shape; the wire shape is re-derived on load.
fn stored_line(ev: &StreamEvent) -> String {
    json!({
        "seq": ev.seq,
        "model": ev.model.clone().unwrap_or_default(),
        "kind": ev.kind,
        "facts": ev.facts,
        "hash": ev.hash.clone().unwrap_or_default(),
        "ts": ev.ts.unwrap_or(0.0),
    })
    .to_string()
}

/// Rebuild a stream event from one stored line, through the
/// same fields `StreamEvent::from_spine` reads — a loaded entry
/// is therefore the event `from_spine` would have built from
/// the spine node itself, not a second, drifting construction.
///
/// A line that does not parse, or that lacks a field the event
/// cannot be rebuilt without — the `hash` above all, since a
/// mirror entry without one can never be checked against spine
/// truth — is **not** silently dropped: `failures` gains a
/// string naming the line and the problem, and the entry is
/// skipped. The caller decides what a failure means; losing it
/// quietly is not an option this function offers.
fn stream_from_stored(
    line: &str,
    line_no: usize,
    failures: &mut Vec<String>,
) -> Option<StreamEvent> {
    let value: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            failures.push(format!("line {}: failed to parse: {e}", line_no + 1));
            return None;
        }
    };
    let Some(hash) = value.get("hash").and_then(Value::as_str) else {
        failures.push(format!(
            "line {}: missing hash — an entry without one cannot be verified against the spine",
            line_no + 1
        ));
        return None;
    };
    let (Some(seq), Some(model), Some(kind)) = (
        value.get("seq").and_then(Value::as_u64),
        value.get("model").and_then(Value::as_str),
        value.get("kind").and_then(Value::as_str),
    ) else {
        failures.push(format!(
            "line {}: missing or ill-typed seq/model/kind",
            line_no + 1
        ));
        return None;
    };
    let spine_event = hearth_store::Event {
        model: model.to_string(),
        kind: kind.to_string(),
        facts: value.get("facts").cloned().unwrap_or(Value::Null),
        seq,
        hash: hash.to_string(),
        ts: value.get("ts").and_then(Value::as_f64).unwrap_or(0.0),
    };
    Some(StreamEvent::from_spine(&spine_event))
}

/// An append-only mirror of spine events, backed by one JSONL
/// file and held in memory for replay.
///
/// Invariants, kept by construction rather than by discipline:
/// entries are appended in ascending seq order and only ever
/// past the current maximum (see [`MirrorStore::append`]), so
/// the in-memory list is the file, in file order, in seq order.
pub struct MirrorStore {
    path: PathBuf,
    entries: Vec<StreamEvent>,
    load_failures: Vec<String>,
}

impl MirrorStore {
    /// Open the mirror at `path`, creating the file if absent.
    ///
    /// Existing lines are loaded back through
    /// `stream_from_stored`; lines that cannot be rebuilt are
    /// recorded in [`MirrorStore::load_failures`] and skipped,
    /// never dropped silently and never fatal — a mirror with
    /// one bad line is still a cache, and `verify_against` /
    /// `rebuild` are how it gets made whole again. An I/O error
    /// creating or reading the file itself *is* fatal and is
    /// returned as `Err`.
    pub fn open(path: &Path) -> Result<MirrorStore, String> {
        // Create without truncating: opening an existing mirror
        // must never be the thing that empties it.
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("cannot create mirror file {}: {e}", path.display()))?;
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read mirror file {}: {e}", path.display()))?;

        let mut entries = Vec::new();
        let mut load_failures = Vec::new();
        for (line_no, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            if let Some(ev) = stream_from_stored(line, line_no, &mut load_failures) {
                entries.push(ev);
            }
        }
        Ok(MirrorStore {
            path: path.to_path_buf(),
            entries,
            load_failures,
        })
    }

    /// The lines that failed to load on `open`, each naming its
    /// line number and why. Empty means the file loaded clean.
    /// These are the record that something was skipped — the
    /// alternative, a silently shorter history, is exactly the
    /// failure mode a resume cursor cannot afford, because a
    /// consumer would take the gap for "no events".
    pub fn load_failures(&self) -> &[String] {
        &self.load_failures
    }

    /// The mirrored events, in ascending seq order.
    pub fn entries(&self) -> &[StreamEvent] {
        &self.entries
    }

    /// The highest mirrored spine seq, or 0 when the mirror is
    /// empty. This is the mirror's resume position: everything
    /// at or below it is already on disk.
    pub fn max_seq(&self) -> u64 {
        self.entries.iter().map(|e| e.seq).max().unwrap_or(0)
    }

    /// Append one event to the file and to memory.
    ///
    /// Only `hearth` events belong here — an `si` event in this
    /// file would put a foreign sequence space in the spine's
    /// cursor, so other sources are ignored and reported as
    /// `false`. Appending is **idempotent by seq**: an event at
    /// or below the current maximum is already mirrored (or
    /// older than what is), writes nothing, and returns `false`.
    /// A poller may therefore re-collect overlapping history
    /// freely; the seq check, not the caller's care, is what
    /// keeps the file duplicate-free.
    pub fn append(&mut self, ev: &StreamEvent) -> io::Result<bool> {
        if ev.source != Source::Hearth {
            return Ok(false);
        }
        if ev.seq <= self.max_seq() {
            return Ok(false);
        }
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        writeln!(file, "{}", stored_line(ev))?;
        self.entries.push(ev.clone());
        Ok(true)
    }

    /// Check every mirrored entry against spine truth.
    ///
    /// The truth map is built from [`collect_spine_events`] —
    /// the same causal walk a rebuild would write — so this
    /// answers "is the mirror faithful to what the spine can
    /// currently show", which is the strongest claim the walk
    /// supports (see that function's limit). An entry whose seq
    /// is absent from the spine, or whose model, kind or hash
    /// differs from the spine's event at that seq, is a failure
    /// string naming the seq. An empty result means the mirror
    /// is faithful; it does **not** claim the mirror is
    /// complete — an absent event is a polling gap, not a
    /// corruption, and the two are kept distinct on purpose.
    pub fn verify_against(&self, spine: &Spine) -> Vec<String> {
        let truth: BTreeMap<u64, (Option<String>, String, Option<String>)> =
            collect_spine_events(spine)
                .into_iter()
                .map(|ev| (ev.seq, (ev.model, ev.kind, ev.hash)))
                .collect();
        let mut failures = Vec::new();
        for entry in &self.entries {
            match truth.get(&entry.seq) {
                None => failures.push(format!(
                    "seq {}: mirrored but absent from the spine",
                    entry.seq
                )),
                Some((model, kind, hash)) => {
                    if &entry.model != model || &entry.kind != kind || &entry.hash != hash {
                        failures.push(format!(
                            "seq {}: mirror (model={:?}, kind={}, hash={:?}) differs from spine (model={:?}, kind={}, hash={:?})",
                            entry.seq, entry.model, entry.kind, entry.hash, model, kind, hash
                        ));
                    }
                }
            }
        }
        failures
    }

    /// Replace the mirror wholesale with the spine's currently
    /// collectable history and return the entry count.
    ///
    /// The new contents are written to a temp file in the same
    /// directory and renamed over the mirror, so a crash mid-
    /// rebuild leaves either the old mirror or the new one,
    /// never a torn half — and either way the next
    /// `verify_against` says which state things are in. Prior
    /// load failures are cleared: they described a file that no
    /// longer exists. On a healthy spine, `verify_against` is
    /// empty immediately after a rebuild.
    pub fn rebuild(&mut self, spine: &Spine) -> Result<usize, String> {
        let fresh = collect_spine_events(spine);
        let dir = self
            .path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let file_name = self
            .path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "mirror.jsonl".to_string());
        let tmp_path = dir.join(format!(".{file_name}.rebuild-{}.tmp", std::process::id()));
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .map_err(|e| format!("cannot write rebuild temp file: {e}"))?;
            for ev in &fresh {
                writeln!(tmp, "{}", stored_line(ev))
                    .map_err(|e| format!("cannot write rebuild temp file: {e}"))?;
            }
            tmp.flush()
                .map_err(|e| format!("cannot flush rebuild temp file: {e}"))?;
        }
        std::fs::rename(&tmp_path, &self.path)
            .map_err(|e| format!("cannot rename rebuilt mirror into place: {e}"))?;
        let count = fresh.len();
        self.entries = fresh;
        self.load_failures.clear();
        Ok(count)
    }
}

impl EventLog for MirrorStore {
    /// Replay from the mirror: `hearth` events after
    /// `after_seq`, in seq order, through the filter. For
    /// `si` the mirror has nothing to say — it is a spine cache
    /// only, and returning `hearth` rows for an `si` cursor
    /// would merge the two sequence spaces G-2 §4.2 keeps apart.
    fn events_since(
        &self,
        source: Source,
        after_seq: u64,
        filter: &EventFilter,
    ) -> Vec<StreamEvent> {
        if source != Source::Hearth {
            return Vec::new();
        }
        let mut out: Vec<StreamEvent> = self
            .entries
            .iter()
            .filter(|ev| ev.seq > after_seq && filter.matches(ev))
            .cloned()
            .collect();
        out.sort_by_key(|ev| ev.seq);
        out
    }

    fn latest_seq(&self, source: Source) -> u64 {
        match source {
            Source::Hearth => self.max_seq(),
            Source::Si => 0,
        }
    }
}

/// Collect the spine's currently reachable events as stream
/// events: for each model in `spine.all_latest()`, walk its
/// residency chain (`why`) and its pull chain (`provenance`),
/// convert with [`StreamEvent::from_spine`], dedupe by seq,
/// and sort ascending by seq.
///
/// The honest limit, stated plainly: chains are causal walks
/// (hearth-store traces are depth-capped), so a cold rebuild
/// sees chain-reachable history — events no live chain reaches
/// (an unlinked transition, a chain longer than the trace cap)
/// are not collected, and this function will not pretend
/// otherwise by inventing a scan the store does not offer.
/// Polling cadence keeps increments small: a poller that runs
/// at the supervisor's own cadence collects each event while
/// its chain is short and current, so the mirror accumulates
/// what a cold rebuild might miss. The mirror is therefore
/// most complete when it has been polling all along, and a
/// rebuild is a repair, not a time machine.
pub fn collect_spine_events(spine: &Spine) -> Vec<StreamEvent> {
    let mut by_seq: BTreeMap<u64, StreamEvent> = BTreeMap::new();
    for latest in spine.all_latest() {
        for ev in spine.why(&latest.model) {
            by_seq
                .entry(ev.seq)
                .or_insert_with(|| StreamEvent::from_spine(&ev));
        }
        for ev in spine.provenance(&latest.model) {
            by_seq
                .entry(ev.seq)
                .or_insert_with(|| StreamEvent::from_spine(&ev));
        }
    }
    by_seq.into_values().collect()
}

/// Run one poll: collect the spine's reachable events and
/// append every one newer than the mirror's maximum, in
/// ascending order. Returns the events actually appended —
/// exactly what a caller should fan out to subscribers, since
/// anything not in this list was already mirrored (or was not
/// the mirror's to take). Append I/O errors have no channel in
/// this signature and are not hidden by fabrication: a failed
/// append simply does not appear in the result, and the event
/// stays above `max_seq`, so the next poll tries it again.
pub fn poll_once(store: &mut MirrorStore, spine: &Spine) -> Vec<StreamEvent> {
    let mut appended = Vec::new();
    for ev in collect_spine_events(spine) {
        if ev.seq <= store.max_seq() {
            continue;
        }
        match store.append(&ev) {
            Ok(true) => appended.push(ev),
            Ok(false) => {}
            Err(_) => break,
        }
    }
    appended
}

/// Where newly mirrored events go: a shared callback the
/// poller invokes once per appended event, after the store
/// lock has been released, so a slow sink never holds the
/// mirror — or a subscriber replay — hostage.
pub type EventSink = Arc<dyn Fn(StreamEvent) + Send + Sync>;

/// Handle to a running poller thread. [`PollerHandle::stop`]
/// is the only way it ends: there is no detach-and-forget,
/// because a poller nobody can stop is a thread a composition
/// binary cannot shut down cleanly.
pub struct PollerHandle {
    stop_flag: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl PollerHandle {
    /// Signal the poller to stop and join its thread. The
    /// in-flight poll, if any, finishes; the wait between
    /// polls is interrupted promptly rather than slept out.
    pub fn stop(mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for PollerHandle {
    fn drop(&mut self) {
        // A dropped handle must not orphan a running thread:
        // signal stop and join, exactly as `stop` would. After
        // an explicit `stop` the thread slot is empty and this
        // is a no-op.
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Spawn the mirror poller: a thread that owns `spine` and
/// keeps `store` current at `interval` cadence.
///
/// The thread owns the spine outright — `Spine` is shared with
/// nobody while the poller runs (`Spine: Sync` is not required
/// or assumed) — and touches the store only under its mutex,
/// briefly: collect-and-append under the lock, then the lock is
/// released before `sink` sees any event. The first poll
/// happens immediately on spawn, so a fresh composition serves
/// current history without waiting a cadence for no reason.
pub fn spawn_poller(
    store: Arc<Mutex<MirrorStore>>,
    spine: Spine,
    interval: Duration,
    sink: EventSink,
) -> PollerHandle {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let thread_flag = Arc::clone(&stop_flag);
    let thread = std::thread::spawn(move || {
        let spine = spine;
        loop {
            if thread_flag.load(Ordering::SeqCst) {
                break;
            }
            let appended = {
                let mut guard = match store.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                poll_once(&mut guard, &spine)
            };
            for ev in appended {
                sink(ev);
            }
            // Sleep the interval in short slices so `stop`
            // joins promptly instead of waiting out a cadence.
            let mut waited = Duration::ZERO;
            while waited < interval {
                if thread_flag.load(Ordering::SeqCst) {
                    break;
                }
                let slice = std::cmp::min(interval - waited, Duration::from_millis(5));
                std::thread::sleep(slice);
                waited += slice;
            }
        }
    });
    PollerHandle {
        stop_flag,
        thread: Some(thread),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use hearth_store::Transition;

    /// A fresh mirror path under a per-test temp directory.
    /// (No `tempfile` dependency in this crate; a pid-and-name
    /// directory under the system temp dir is enough, and each
    /// test removes its own afterwards.)
    fn temp_mirror(test: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("gaia-mirror-{}-{test}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("mirror.jsonl");
        let _ = std::fs::remove_file(&path);
        (dir, path)
    }

    /// Record a declared -> loading -> resident chain for one
    /// model, each event caused by the previous one, so the
    /// spine's causal walks can actually reach all three.
    fn record_chain(spine: &Spine, model: &str) -> Vec<u64> {
        let declared = spine
            .record(
                model,
                &Transition::Declared {
                    vram_bytes: 4_000_000_000,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let loading = spine
            .record(
                model,
                &Transition::Loading {
                    pid: 4242,
                    endpoint: "127.0.0.1:8080".into(),
                },
                std::slice::from_ref(&declared),
            )
            .unwrap();
        let resident = spine
            .record(
                model,
                &Transition::Resident {
                    endpoint: "127.0.0.1:8080".into(),
                    warmup_ms: 43,
                },
                std::slice::from_ref(&loading),
            )
            .unwrap();
        vec![declared.seq, loading.seq, resident.seq]
    }

    #[test]
    fn append_open_roundtrip_preserves_entries() {
        let (dir, path) = temp_mirror("roundtrip");
        let spine = Spine::in_memory();
        record_chain(&spine, "muse");
        let collected = collect_spine_events(&spine);
        assert_eq!(collected.len(), 3);

        let before: Vec<StreamEvent> = {
            let mut store = MirrorStore::open(&path).unwrap();
            assert!(store.entries().is_empty());
            assert_eq!(store.max_seq(), 0);
            for ev in &collected {
                assert!(store.append(ev).unwrap());
            }
            store.entries().to_vec()
        };
        let reopened = MirrorStore::open(&path).unwrap();
        assert_eq!(reopened.entries(), before.as_slice());
        assert_eq!(reopened.max_seq(), collected.last().unwrap().seq);
        assert!(reopened.load_failures().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn poll_once_mirrors_recorded_transitions_in_seq_order_with_hashes() {
        let (dir, path) = temp_mirror("poll-once");
        let spine = Spine::in_memory();
        let seqs = record_chain(&spine, "muse");

        let mut store = MirrorStore::open(&path).unwrap();
        let appended = poll_once(&mut store, &spine);
        assert_eq!(appended.len(), 3);
        let appended_seqs: Vec<u64> = appended.iter().map(|e| e.seq).collect();
        assert_eq!(appended_seqs, seqs, "ascending spine seq order");
        // Hashes ride along and match the spine's own events.
        let truth = collect_spine_events(&spine);
        for (got, want) in appended.iter().zip(&truth) {
            assert_eq!(got.hash, want.hash);
            assert!(got.hash.as_deref().is_some_and(|h| !h.is_empty()));
            assert_eq!(got.kind, want.kind);
            assert_eq!(got.model, want.model);
        }
        assert_eq!(store.max_seq(), *seqs.last().unwrap());
        // A second poll over unchanged history appends nothing.
        assert!(poll_once(&mut store, &spine).is_empty());
        assert!(store.verify_against(&spine).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tampering_is_detected_by_verify_and_repaired_by_rebuild() {
        let (dir, path) = temp_mirror("tamper");
        let spine = Spine::in_memory();
        record_chain(&spine, "muse");
        let expected = collect_spine_events(&spine);
        {
            let mut store = MirrorStore::open(&path).unwrap();
            assert_eq!(poll_once(&mut store, &spine).len(), 3);
        }

        // Rewrite the file with one entry's hash altered.
        let text = std::fs::read_to_string(&path).unwrap();
        let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
        assert_eq!(lines.len(), 3);
        let mut tampered: Value = serde_json::from_str(&lines[1]).unwrap();
        let tampered_seq = tampered["seq"].as_u64().unwrap();
        tampered["hash"] = json!("tampered-hash");
        lines[1] = tampered.to_string();
        std::fs::write(&path, lines.join("\n") + "\n").unwrap();

        let mut store = MirrorStore::open(&path).unwrap();
        let failures = store.verify_against(&spine);
        assert_eq!(
            failures.len(),
            1,
            "exactly the tampered entry: {failures:?}"
        );
        assert!(
            failures[0].contains(&tampered_seq.to_string()),
            "the failure names the seq: {failures:?}"
        );

        let count = store.rebuild(&spine).unwrap();
        assert_eq!(count, expected.len());
        assert!(store.verify_against(&spine).is_empty());
        assert_eq!(store.entries(), expected.as_slice());
        // And the repaired file itself loads clean and faithful.
        let reopened = MirrorStore::open(&path).unwrap();
        assert_eq!(reopened.entries(), expected.as_slice());
        assert!(reopened.verify_against(&spine).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_is_idempotent_by_seq() {
        let (dir, path) = temp_mirror("idempotent");
        let spine = Spine::in_memory();
        record_chain(&spine, "muse");
        let collected = collect_spine_events(&spine);
        let ev = &collected[0];

        let mut store = MirrorStore::open(&path).unwrap();
        assert!(store.append(ev).unwrap(), "first append writes");
        assert!(
            !store.append(ev).unwrap(),
            "same event again writes nothing"
        );
        assert_eq!(store.entries().len(), 1);
        // The file holds exactly one line, too — idempotence is
        // on disk, not just in the in-memory view.
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 1);
        // Non-hearth events are not the mirror's to take.
        let si = StreamEvent::from_si(99, "task_open", "tasks", json!({"kind": "task"}));
        assert!(!store.append(&si).unwrap());
        assert_eq!(store.entries().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn spawn_poller_mirrors_immediately_and_stops_cleanly() {
        let (dir, path) = temp_mirror("poller");
        let spine = Spine::in_memory();
        let seqs = record_chain(&spine, "muse");

        let store = Arc::new(Mutex::new(MirrorStore::open(&path).unwrap()));
        let received: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let received_sink = Arc::clone(&received);
        let sink: EventSink = Arc::new(move |ev: StreamEvent| {
            received_sink.lock().unwrap().push(ev.seq);
        });
        let handle = spawn_poller(Arc::clone(&store), spine, Duration::from_millis(10), sink);
        // The first poll happens on spawn, so the whole chain
        // arrives without waiting for cadence luck; the deadline
        // is a failure bound, not an expectation.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while received.lock().unwrap().len() < seqs.len() {
            assert!(
                std::time::Instant::now() < deadline,
                "poller did not mirror the chain: {:?}",
                received.lock().unwrap()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        handle.stop();
        assert_eq!(*received.lock().unwrap(), seqs);
        assert_eq!(store.lock().unwrap().max_seq(), *seqs.last().unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_load_corrupted_line_is_reported_not_silently_dropped() {
        let (dir, path) = temp_mirror("corrupt");
        let good = json!({
            "seq": 7,
            "model": "muse",
            "kind": "resident",
            "facts": {"endpoint": "127.0.0.1:8080"},
            "hash": "abc123",
            "ts": 1.5,
        })
        .to_string();
        let no_hash = json!({
            "seq": 8,
            "model": "muse",
            "kind": "lost",
            "facts": {},
            "ts": 2.5,
        })
        .to_string();
        std::fs::write(&path, format!("{good}\nthis is not json\n{no_hash}\n")).unwrap();

        let store = MirrorStore::open(&path).unwrap();
        // The good line still loads; both bad lines are on the
        // record — one unparseable, one missing its hash.
        assert_eq!(store.entries().len(), 1);
        assert_eq!(store.entries()[0].seq, 7);
        assert_eq!(store.entries()[0].hash.as_deref(), Some("abc123"));
        assert_eq!(
            store.load_failures().len(),
            2,
            "both corrupted lines reported: {:?}",
            store.load_failures()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
