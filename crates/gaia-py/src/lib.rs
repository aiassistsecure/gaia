//! Python bindings for the Gaia spine reader.
//!
// pyo3's #[pyclass] / #[pymethods] macros expand to a PyErr -> PyErr
// conversion that clippy flags as useless. It is useless, and it is also not
// ours — the alternative is contorting real signatures to satisfy a lint about
// generated code. Allowed here, narrowly, with the reason attached.
#![allow(clippy::useless_conversion)]
//!
//! Same discipline as the `hearth-py` side: every rule lives in
//! `hearth-store` and is tested there once. This layer moves data and
//! nothing else.
//!
//! Values cross as JSON strings and the thin `gaia` Python package turns
//! them into dicts. That keeps this file free of any conversion logic worth
//! getting wrong, which matters more than idiomatic signatures on a v0.1.
//!
//! READ-ONLY BY CONSTRUCTION: this module exposes no method that can
//! mutate the spine — no record, no flush, no writable open, and no path
//! that reaches `Spine::record` or any `Db` write. The absence of write
//! methods is the feature, and it is why Python holding a `Spine` cannot
//! rewrite the history it audits: the most a holder of this object can do
//! is read the history back and check it with `verify`.
//!
//! GIL DISCIPLINE: every method runs its spine call inside
//! `py.allow_threads(...)`, so the GIL is released around any call that
//! can block (a disk read, a hash walk, a full verify). `hearth-py` has
//! no such pattern because its calls are pure in-memory arithmetic; a
//! spine read is I/O against an embedded database and must not stall
//! every other Python thread behind it. The rule is uniform — even the
//! fast calls go through `allow_threads` — because a discipline with
//! exceptions for "fast enough" calls is a discipline that quietly
//! expires the first time a fast call grows a disk read. Each method
//! clones the `Arc` first and moves the clone into the closure; results
//! cross back as owned data and are only then turned into JSON strings.

use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde_json::{json, Value};

/// One spine event as a JSON object: `{"model","kind","facts","seq",
/// "hash","ts"}` — exactly the spine row shape `gaia-api`'s
/// `StreamEvent::from_spine` puts in `row`, so a Python consumer sees
/// the same event here that it sees on the SSE stream.
fn event_json(ev: &hearth_store::Event) -> Value {
    json!({
        "model": ev.model.clone(),
        "kind": ev.kind.clone(),
        "facts": ev.facts.clone(),
        "seq": ev.seq,
        "hash": ev.hash.clone(),
        "ts": ev.ts,
    })
}

/// A chain of spine events as a JSON array string, in the order the
/// spine returned them (newest first for `why` / `provenance`).
fn events_json(events: &[hearth_store::Event]) -> String {
    Value::Array(events.iter().map(event_json).collect()).to_string()
}

/// A read-only handle on the NEDB spine: the tamper-evident history of
/// every residency transition `hearth-store` recorded.
///
/// There is deliberately no way to write through this object — see the
/// module docs. Python holding a `Spine` can audit the history; it
/// cannot rewrite it.
#[pyclass(name = "Spine")]
struct PySpine {
    inner: std::sync::Arc<hearth_store::Spine>,
}

#[pymethods]
impl PySpine {
    /// Open the spine rooted at `root` and hold it read-only.
    ///
    /// Raises `ValueError` if the spine cannot be opened. Opening is a
    /// spine call that touches disk, so it runs under `allow_threads`
    /// like every other method here, even though a constructor has no
    /// other way to stall the interpreter yet — uniformity is the design.
    #[new]
    #[pyo3(signature = (root))]
    fn new(py: Python<'_>, root: &str) -> PyResult<Self> {
        let path = PathBuf::from(root);
        let inner = py
            .allow_threads(move || hearth_store::Spine::open(&path))
            .map_err(PyValueError::new_err)?;
        Ok(PySpine {
            inner: Arc::new(inner),
        })
    }

    /// Current global sequence — the clock `state_as_of` reads against.
    fn seq(&self, py: Python<'_>) -> u64 {
        let spine = Arc::clone(&self.inner);
        py.allow_threads(move || spine.seq())
    }

    /// The model's current state — the latest residency event — as a
    /// JSON string, or `None` if the spine has never seen the model.
    fn latest(&self, py: Python<'_>, model: &str) -> Option<String> {
        let spine = Arc::clone(&self.inner);
        let model = model.to_string();
        let ev = py.allow_threads(move || spine.latest(&model));
        ev.as_ref().map(|ev| event_json(ev).to_string())
    }

    /// The latest pull event for the model, as a JSON string, or `None`
    /// if it was never fetched.
    ///
    /// Separate from `latest` on purpose: a pull is not a residency
    /// state, and letting one answer "is this model warm" would conflate
    /// having the bytes with having them loaded.
    fn latest_pull(&self, py: Python<'_>, model: &str) -> Option<String> {
        let spine = Arc::clone(&self.inner);
        let model = model.to_string();
        let ev = py.allow_threads(move || spine.latest_pull(&model));
        ev.as_ref().map(|ev| event_json(ev).to_string())
    }

    /// The causal chain that produced the model's current state, as a
    /// JSON array string, newest first: the answer to "why is this
    /// model (not) resident?" An unknown model yields `[]`, not an error.
    fn why(&self, py: Python<'_>, model: &str) -> String {
        let spine = Arc::clone(&self.inner);
        let model = model.to_string();
        let events = py.allow_threads(move || spine.why(&model));
        events_json(&events)
    }

    /// Where the model's bytes came from, as a JSON array string,
    /// newest first: the pull chain, walked over the same causal links
    /// `why` follows. A model that was never pulled yields `[]`.
    fn provenance(&self, py: Python<'_>, model: &str) -> String {
        let spine = Arc::clone(&self.inner);
        let model = model.to_string();
        let events = py.allow_threads(move || spine.provenance(&model));
        events_json(&events)
    }

    /// What the model's state was at sequence `seq`, as a JSON string,
    /// or `None` if the model had no state at that point. Time travel.
    fn state_as_of(&self, py: Python<'_>, model: &str, seq: u64) -> Option<String> {
        let spine = Arc::clone(&self.inner);
        let model = model.to_string();
        let ev = py.allow_threads(move || spine.state_as_of(&model, seq));
        ev.as_ref().map(|ev| event_json(ev).to_string())
    }

    /// Latest residency event for every model the spine has ever seen,
    /// as a JSON array string.
    fn all_latest(&self, py: Python<'_>) -> String {
        let spine = Arc::clone(&self.inner);
        let events = py.allow_threads(move || spine.all_latest());
        events_json(&events)
    }

    /// Merkle head of the whole history.
    fn head(&self, py: Python<'_>) -> String {
        let spine = Arc::clone(&self.inner);
        py.allow_threads(move || spine.head())
    }

    /// Verify every node against its hash, as a JSON string:
    /// `{"checked": N, "failures": [...]}`. An empty `failures` list is
    /// the cryptographic proof the history wasn't rewritten — the one
    /// check a read-only holder most needs, and the slowest call here,
    /// which is exactly why it runs with the GIL released.
    fn verify(&self, py: Python<'_>) -> String {
        let spine = Arc::clone(&self.inner);
        let (checked, failures) = py.allow_threads(move || spine.verify());
        json!({
            "checked": checked,
            "failures": failures,
        })
        .to_string()
    }
}

#[pymodule]
fn _gaia(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySpine>()?;
    m.add_class::<mirror::PyMirror>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

mod mirror;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_json_carries_all_six_fields_for_a_recorded_lifecycle() {
        let s = hearth_store::Spine::in_memory();
        let declared = s
            .record(
                "muse",
                &hearth_store::Transition::Declared {
                    vram_bytes: 4_000_000_000,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let pull = s
            .record(
                "muse",
                &hearth_store::Transition::PullStarted {
                    source: "ollama:library/muse:latest".into(),
                },
                std::slice::from_ref(&declared),
            )
            .unwrap();
        let pulled = s
            .record(
                "muse",
                &hearth_store::Transition::PullCompleted {
                    path: "/blobs/sha256-abc".into(),
                    size_bytes: 4_000_000_000,
                },
                &[pull],
            )
            .unwrap();
        let loading = s
            .record(
                "muse",
                &hearth_store::Transition::Loading {
                    pid: 4242,
                    endpoint: "127.0.0.1:8080".into(),
                },
                &[pulled],
            )
            .unwrap();
        let resident = s
            .record(
                "muse",
                &hearth_store::Transition::Resident {
                    endpoint: "127.0.0.1:8080".into(),
                    warmup_ms: 43_000,
                },
                &[loading],
            )
            .unwrap();

        let ev = s.latest("muse").unwrap();
        let v = event_json(&ev);

        // Exactly the six spine-row fields, no more and no fewer.
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 6, "event json fields: {obj:?}");
        for key in ["model", "kind", "facts", "seq", "hash", "ts"] {
            assert!(obj.contains_key(key), "missing field {key}: {v}");
        }
        assert_eq!(v["model"], serde_json::json!("muse"));
        assert_eq!(v["kind"], serde_json::json!("resident"));
        assert_eq!(
            v["facts"],
            serde_json::json!({ "endpoint": "127.0.0.1:8080", "warmup_ms": 43_000 })
        );
        assert_eq!(v["seq"], serde_json::json!(resident.seq));
        assert_eq!(v["seq"], serde_json::json!(ev.seq));
        assert_eq!(v["hash"], serde_json::json!(resident.hash));
        assert_eq!(v["hash"], serde_json::json!(ev.hash));
        assert_eq!(v["ts"], serde_json::json!(ev.ts));
        assert!(v["ts"].is_number(), "ts crosses as a number: {v}");
    }

    #[test]
    fn why_chain_order_is_newest_first() {
        let s = hearth_store::Spine::in_memory();
        let declared = s
            .record(
                "muse",
                &hearth_store::Transition::Declared {
                    vram_bytes: 4_000_000_000,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let pull = s
            .record(
                "muse",
                &hearth_store::Transition::PullStarted {
                    source: "ollama:library/muse:latest".into(),
                },
                std::slice::from_ref(&declared),
            )
            .unwrap();
        let pulled = s
            .record(
                "muse",
                &hearth_store::Transition::PullCompleted {
                    path: "/blobs/sha256-abc".into(),
                    size_bytes: 4_000_000_000,
                },
                &[pull],
            )
            .unwrap();
        let loading = s
            .record(
                "muse",
                &hearth_store::Transition::Loading {
                    pid: 4242,
                    endpoint: "127.0.0.1:8080".into(),
                },
                &[pulled],
            )
            .unwrap();
        s.record(
            "muse",
            &hearth_store::Transition::Resident {
                endpoint: "127.0.0.1:8080".into(),
                warmup_ms: 43_000,
            },
            &[loading],
        )
        .unwrap();

        // why() walks the causal chain back through the whole story,
        // newest first — read back through the binding's JSON shape.
        let why = s.why("muse");
        let kinds: Vec<serde_json::Value> =
            why.iter().map(|e| event_json(e)["kind"].clone()).collect();
        assert_eq!(
            kinds,
            vec![
                serde_json::json!("resident"),
                serde_json::json!("loading"),
                serde_json::json!("pull_completed"),
                serde_json::json!("pull_started"),
                serde_json::json!("declared"),
            ]
        );
    }
}
