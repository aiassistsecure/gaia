//! The shared vocabulary of the Gaia event stream (G-4).
//!
//! Two journals feed one wire, and this module is where their
//! difference is kept honest. The `hearth` source is the runtime's
//! own spine (`hearth-store`): its sequence is the spine's global
//! NEDB seq, and every event carries the spine `hash` so a mirror
//! entry can be checked against spine truth. The `si` source is
//! SI's journal, forwarded by SI's service adapter: its sequence
//! is SI's journal seq, its `collection`/`row` are SI's native
//! envelope (SI-47), and the fork is a carrier for it, never an
//! editor. The two sequence spaces are **not** merged (G-2 §4.2):
//! a subscription follows one source, resumes in that source's
//! seq, and the wire `id:` is always that source's seq.
//!
//! The wire shape is G-2 §4.2's — `id: <seq>`,
//! `event: <source>:<kind>`, data carrying `source`, `seq`,
//! `facts`, and `model` or `task` when known — **plus** the
//! `collection` and `row` keys SI's reference consumer (SI-48/49)
//! reads, so the follower consumes the fork's stream unmodified.
//! For `si` events `collection`/`row` are SI's own; for `hearth`
//! events `collection` is `"spine"` and `row` is the full spine
//! event (`model`, `kind`, `facts`, `seq`, `hash`, `ts`).

use std::collections::HashSet;

use serde_json::{json, Value};

/// Which journal a stream event belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Source {
    /// The runtime spine (`hearth-store`); seq = spine seq.
    Hearth,
    /// SI's journal, forwarded in order; seq = SI journal seq.
    Si,
}

impl Source {
    pub fn as_str(&self) -> &'static str {
        match self {
            Source::Hearth => "hearth",
            Source::Si => "si",
        }
    }

    pub fn parse(s: &str) -> Option<Source> {
        match s {
            "hearth" => Some(Source::Hearth),
            "si" => Some(Source::Si),
            _ => None,
        }
    }
}

/// One event on the Gaia stream, in either sequence space.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEvent {
    pub source: Source,
    /// The event's sequence **in its source's space** — the SSE
    /// `id:` and the only cursor a consumer may resume from.
    pub seq: u64,
    /// The kind within the source: a spine transition kind
    /// (`resident`, `lost`, …) or an SI event name
    /// (`self_edit.tool`, `task_open`, …).
    pub kind: String,
    /// The model the event concerns, when it concerns one.
    pub model: Option<String>,
    /// The task the event concerns, when it concerns one (SI rows
    /// that carry a task reference; spine events never do).
    pub task: Option<String>,
    /// The event's facts: spine `facts` for `hearth`, the row's
    /// own payload summary for `si` (the full row rides in `row`).
    pub facts: Value,
    /// The collection dimension of the SI-48 filter: SI's journal
    /// collection for `si` events, `"spine"` for `hearth` events.
    pub collection: String,
    /// The full row, as SI's consumer expects it: SI's stored row
    /// for `si` events; `{model, kind, facts, seq, hash, ts}` for
    /// `hearth` events.
    pub row: Value,
    /// The row's own `kind` field, when the row carries one — the
    /// SI-48 `kind` filter dimension reads the row, not the
    /// derived event name. For `hearth` events this equals `kind`.
    pub row_kind: Option<String>,
    /// The spine node hash for `hearth` events — the mirror's
    /// check against spine truth. Always `None` for `si` events:
    /// the fork does not hash what it merely carries.
    pub hash: Option<String>,
    /// Event timestamp (seconds), when the source records one.
    pub ts: Option<f64>,
}

impl StreamEvent {
    /// The SSE event name: `<source>:<kind>` (G-2 §4.2).
    pub fn wire_name(&self) -> String {
        format!("{}:{}", self.source.as_str(), self.kind)
    }

    /// The `data:` payload: G-2 §4.2's fields plus the SI
    /// consumer's `collection`/`row` keys (see module docs).
    pub fn data_json(&self) -> Value {
        let mut data = json!({
            "source": self.source.as_str(),
            "seq": self.seq,
            "collection": self.collection,
            "row": self.row,
            "facts": self.facts,
        });
        if let Some(model) = &self.model {
            data["model"] = json!(model);
        }
        if let Some(task) = &self.task {
            data["task"] = json!(task);
        }
        data
    }

    /// One complete SSE block (`id:` / `event:` / `data:` / blank).
    pub fn sse_block(&self) -> String {
        format!(
            "id: {}\nevent: {}\ndata: {}\n\n",
            self.seq,
            self.wire_name(),
            self.data_json()
        )
    }

    /// Build the event for a spine event read out of `hearth-store`.
    pub fn from_spine(ev: &hearth_store::Event) -> StreamEvent {
        StreamEvent {
            source: Source::Hearth,
            seq: ev.seq,
            kind: ev.kind.clone(),
            model: Some(ev.model.clone()),
            task: None,
            facts: ev.facts.clone(),
            collection: "spine".to_string(),
            row: json!({
                "model": ev.model,
                "kind": ev.kind,
                "facts": ev.facts,
                "seq": ev.seq,
                "hash": ev.hash,
                "ts": ev.ts,
            }),
            row_kind: Some(ev.kind.clone()),
            hash: Some(ev.hash.clone()),
            ts: Some(ev.ts),
        }
    }

    /// Build the event for a forwarded SI journal row. `event_name`
    /// is SI's derived SSE name for the row (SI-47's
    /// `event_name_for_row`); the row rides unmodified.
    pub fn from_si(seq: u64, event_name: &str, collection: &str, row: Value) -> StreamEvent {
        let task = row.get("task_seq").map(|v| v.to_string()).or_else(|| {
            row.get("task_id")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
        StreamEvent {
            source: Source::Si,
            seq,
            kind: event_name.to_string(),
            model: None,
            task,
            facts: row.clone(),
            collection: collection.to_string(),
            row_kind: row.get("kind").and_then(Value::as_str).map(str::to_string),
            row,
            hash: None,
            ts: None,
        }
    }
}

/// The SI-48 filtered-subscription dimensions, applied server-side
/// before buffering (G-2 §4.4): `collection`, `event`, `kind`,
/// `task`, each a set of accepted values (repeated query params),
/// absent = accept all on that dimension.
///
/// Mapping onto fork events, stated once here: `event` matches
/// the wire name (`hearth:resident`) **or** the bare kind
/// (`resident`) — SI clients name events bare, fork clients may
/// name them sourced; accepting both is a filter convenience,
/// not a second semantics. `kind` matches the row's own `kind`
/// field, exactly as SI-48 reads it. `task` matches the event's
/// task reference (SI rows that carry one); the chain-derived
/// task membership SI-48 computes from the full journal is **not**
/// reproduced here — the fork sees forwarded rows, not SI's
/// journal, and will not pretend to derive what it cannot see.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventFilter {
    pub collections: Option<HashSet<String>>,
    pub event_names: Option<HashSet<String>>,
    pub kinds: Option<HashSet<String>>,
    pub task: Option<String>,
}

impl EventFilter {
    pub fn is_empty(&self) -> bool {
        self.collections.is_none()
            && self.event_names.is_none()
            && self.kinds.is_none()
            && self.task.is_none()
    }

    pub fn matches(&self, ev: &StreamEvent) -> bool {
        if let Some(cols) = &self.collections {
            if !cols.contains(&ev.collection) {
                return false;
            }
        }
        if let Some(names) = &self.event_names {
            if !names.contains(&ev.wire_name()) && !names.contains(&ev.kind) {
                return false;
            }
        }
        if let Some(kinds) = &self.kinds {
            match &ev.row_kind {
                Some(k) if kinds.contains(k) => {}
                _ => return false,
            }
        }
        if let Some(task) = &self.task {
            if ev.task.as_deref() != Some(task.as_str()) {
                return false;
            }
        }
        true
    }

    /// Parse the repeated filter params of SI-48's query shape
    /// (`collection=a&collection=b&event=…&kind=…&task=…`).
    /// `params` is the raw query string split on `&`/`=` by the
    /// caller (no percent-decoding: filter values are tokens from
    /// a closed vocabulary of names, as on the SI side).
    pub fn from_params(params: &[(String, String)]) -> EventFilter {
        let mut f = EventFilter::default();
        for (k, v) in params {
            if v.is_empty() {
                continue;
            }
            match k.as_str() {
                "collection" => f
                    .collections
                    .get_or_insert_with(HashSet::new)
                    .insert(v.clone()),
                "event" => f
                    .event_names
                    .get_or_insert_with(HashSet::new)
                    .insert(v.clone()),
                "kind" => f.kinds.get_or_insert_with(HashSet::new).insert(v.clone()),
                "task" => {
                    f.task = Some(v.clone());
                    true
                }
                _ => false,
            };
        }
        f
    }
}

/// A readable history of one source: everything after a cursor,
/// in seq order, projected through a filter. The mirror store
/// implements this for `hearth`, the ingest buffer for `si` —
/// replay and live tail read the same history, so a resume is
/// the same answer whether the event is a second or a day old.
pub trait EventLog: Send + Sync {
    fn events_since(
        &self,
        source: Source,
        after_seq: u64,
        filter: &EventFilter,
    ) -> Vec<StreamEvent>;
    fn latest_seq(&self, source: Source) -> u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn si_event() -> StreamEvent {
        StreamEvent::from_si(
            7,
            "self_edit.tool",
            "self_edits",
            json!({"kind": "tool", "task_seq": 3, "seq": 7}),
        )
    }

    #[test]
    fn the_wire_block_carries_both_shapes() {
        let ev = si_event();
        let block = ev.sse_block();
        assert!(
            block.starts_with("id: 7\nevent: si:self_edit.tool\n"),
            "{block}"
        );
        let data = ev.data_json();
        assert_eq!(data["source"], json!("si"));
        assert_eq!(data["seq"], json!(7));
        assert_eq!(data["collection"], json!("self_edits"));
        assert_eq!(data["row"]["kind"], json!("tool"));
        assert_eq!(data["task"], json!("3"));
    }

    #[test]
    fn filter_dimensions_read_what_si_reads() {
        let ev = si_event();
        let mut f = EventFilter::default();
        assert!(f.matches(&ev));
        f.kinds = Some(["tool".to_string()].into_iter().collect());
        assert!(f.matches(&ev));
        f.kinds = Some(["memory".to_string()].into_iter().collect());
        assert!(!f.matches(&ev));
        f.kinds = None;
        f.event_names = Some(["self_edit.tool".to_string()].into_iter().collect());
        assert!(f.matches(&ev));
        f.event_names = Some(["si:self_edit.tool".to_string()].into_iter().collect());
        assert!(f.matches(&ev));
        f.event_names = Some(["task_open".to_string()].into_iter().collect());
        assert!(!f.matches(&ev));
    }
}
