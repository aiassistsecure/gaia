//! The mirror binding: Python reads over the G-4 spine mirror.
//!
//! The mirror (`gaia_api::MirrorStore`) is a gaia-owned, append-only
//! JSONL cache of spine events, maintained by the Rust poller. This
//! binding is its reader from Python: `events_since` is the resume
//! read that was the mirror's reason to exist, `latest_seq` is the
//! resume position, and `load_failures` keeps the store's discipline
//! that a skipped line is a named failure, never a silent gap.
//!
//! Read-only by construction, like the spine binding: the store's
//! mutating methods (`append`, `rebuild`) are not exposed, and never
//! will be — the poller is the mirror's only writer, because a second
//! writer is how a cache stops being a cache of the spine and starts
//! being a second, disagreeing history.

use std::collections::HashSet;
use std::path::Path;

use gaia_api::{EventFilter, EventLog, MirrorStore, Source, StreamEvent};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde_json::{json, Value};

/// One mirrored event as a JSON object: the spine event's own fields
/// plus the stream context (`source`, `collection`) the SSE layer
/// derives from the same `StreamEvent` — one construction, so the
/// Python read and the wire can never drift into two shapes.
fn event_json(ev: &StreamEvent) -> Value {
    json!({
        "source": ev.source.as_str(),
        "seq": ev.seq,
        "kind": ev.kind,
        "model": ev.model,
        "facts": ev.facts,
        "collection": ev.collection,
        "hash": ev.hash,
        "ts": ev.ts,
    })
}

#[pyclass(name = "Mirror")]
pub struct PyMirror {
    inner: MirrorStore,
}

#[pymethods]
impl PyMirror {
    #[new]
    #[pyo3(signature = (path))]
    fn new(path: &str) -> PyResult<Self> {
        // The store's `open` creates the file when absent — right for
        // the Rust poller, which is the mirror's writer, wrong for a
        // reader: opening a mistyped path from Python would conjure
        // an empty mirror into existence and report "no events" for
        // a file that was never there. This binding is read-only, so
        // a missing mirror is a `ValueError`, never a creation.
        if !Path::new(path).exists() {
            return Err(PyValueError::new_err(format!(
                "mirror file {path} does not exist — the gaia-py reader never creates one"
            )));
        }
        let store = MirrorStore::open(Path::new(path)).map_err(PyValueError::new_err)?;
        Ok(PyMirror { inner: store })
    }

    /// Mirrored events after `after_seq`, ascending, as a JSON array
    /// string. `kinds` matches the bare kind or the sourced wire name
    /// (`resident` or `hearth:resident`) — the EventFilter's own
    /// convenience, not a second semantics. `models` is a post-filter
    /// on the event's model; the store's filter has no model
    /// dimension and this layer does not invent one inside it.
    #[pyo3(signature = (after_seq, kinds=None, models=None))]
    fn events_since(
        &self,
        py: Python<'_>,
        after_seq: u64,
        kinds: Option<Vec<String>>,
        models: Option<Vec<String>>,
    ) -> String {
        let store = &self.inner;
        let events = py.allow_threads(|| {
            let mut filter = EventFilter::default();
            if let Some(kinds) = kinds {
                filter.event_names = Some(kinds.into_iter().collect::<HashSet<_>>());
            }
            let mut out = store.events_since(Source::Hearth, after_seq, &filter);
            if let Some(models) = &models {
                let keep: HashSet<&str> = models.iter().map(String::as_str).collect();
                out.retain(|ev| ev.model.as_deref().is_some_and(|m| keep.contains(m)));
            }
            out
        });
        Value::Array(events.iter().map(event_json).collect()).to_string()
    }

    /// Every mirrored event, ascending — `events_since` from zero
    /// with no narrowing, so there is exactly one replay code path.
    fn entries(&self, py: Python<'_>) -> String {
        self.events_since(py, 0, None, None)
    }

    /// The mirror's resume position: highest mirrored seq, 0 when empty.
    fn latest_seq(&self, py: Python<'_>) -> u64 {
        let store = &self.inner;
        py.allow_threads(|| store.latest_seq(Source::Hearth))
    }

    /// The lines that failed to load on open, each naming its line
    /// number and why. Empty means the file loaded clean.
    fn load_failures(&self, py: Python<'_>) -> Vec<String> {
        let store = &self.inner;
        py.allow_threads(|| store.load_failures().to_vec())
    }
}
