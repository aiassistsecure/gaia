//! Acceptance tests for S-1 — submission dedup and provenance
//! population (G-9 §9.3, §11.2, §15).
//!
//! S-1 closes two holes on the seam:
//!
//!   * a retried submission for the same SI journal sequence
//!     must record one work order, not two — `hearth.notify`
//!     and `hearth.repull` have no stale-state check to save
//!     them from a double actuation by accident (§9.3);
//!   * a completed record's status body must report the spine
//!     refs its actuation recorded, in
//!     `provenance.spine_event_refs`, instead of the empty list
//!     every record reported before S-1 (§11.2).
//!
//! Everything is exercised through the crate's pure decision
//! surface (`decide` / `run_actuation`) against an in-memory
//! work store: no sockets, no server. The provenance half is
//! already in the tree; the dedup half is implemented by a
//! parallel slice in `work.rs` — until that lands, the dedup
//! criteria below fail, which is expected and is not worked
//! around here.

use std::collections::HashMap;

use gaia_api::work::run_actuation;
use gaia_api::{decide, Actuator, GaiaError, MemoryWorkStore, WorkRecord};
use hearth_api::{Request, Response};
use serde_json::{json, Value};

// -- helpers ---------------------------------------------------------------

fn req(method: &str, path: &str, body: &str) -> Request {
    Request {
        method: method.to_string(),
        path: path.to_string(),
        headers: HashMap::new(),
        body: body.to_string(),
    }
}

fn body_json(resp: &Response) -> Value {
    serde_json::from_str(&resp.body)
        .unwrap_or_else(|e| panic!("response body is not JSON ({e}): {}", resp.body))
}

fn work_path(work_id: &str) -> String {
    format!("/gaia/v1/work/{work_id}")
}

/// A tool-intent submission body in the §3.2 shape, with the
/// SI journal sequence parameterised: dedup keys on that
/// sequence (§9.3), so each criterion names its own.
fn submission_body(journal_seq: u64, verdict: Option<&str>, class: Option<&str>) -> String {
    let mut body = json!({
        "type": "tool",
        "tool": "fs.read",
        "args": { "path": "/tmp/acceptance-fixture" },
        "reason": "S-1 acceptance test",
        "task_id": "task-s1",
        "journal_seq": journal_seq,
        "class": "read",
    });
    if let Some(class) = class {
        body["class"] = json!(class);
    }
    if let Some(verdict) = verdict {
        body["verdict"] = json!(verdict);
    }
    body.to_string()
}

/// POST one submission body and return its 202 acknowledgement.
fn post_work(store: &mut MemoryWorkStore, body: &str) -> Value {
    let resp = decide(&req("POST", "/gaia/v1/work", body), store);
    assert_eq!(resp.status, 202, "submission body: {}", resp.body);
    let ack = body_json(&resp);
    assert_eq!(ack["state"], json!("accepted"));
    ack
}

fn work_id_of(ack: &Value) -> String {
    let work_id = ack["work_id"]
        .as_str()
        .unwrap_or_else(|| panic!("202 body has no string work_id: {ack}"));
    assert!(!work_id.is_empty(), "work_id must not be empty");
    work_id.to_string()
}

fn status_of(store: &mut MemoryWorkStore, work_id: &str) -> Value {
    let resp = decide(&req("GET", &work_path(work_id), ""), store);
    assert_eq!(resp.status, 200, "status body: {}", resp.body);
    body_json(&resp)
}

/// An actuator that returns one fixed FleetActuator-shaped
/// result — the §11.2 result shape `finish` files in the
/// outcome store (`outcome`, `tool`, `model`, `detail`,
/// `caused_by`, `spine_event_refs`). It actuates nothing; it
/// only hands the lifecycle a result to complete with.
struct FixedActuator {
    result: Value,
}

impl Actuator for FixedActuator {
    fn actuate(&mut self, _work: &WorkRecord) -> Result<Value, GaiaError> {
        Ok(self.result.clone())
    }
}

// -- Criterion 1: an identical retry records one work order -----------------

#[test]
fn identical_resubmission_returns_the_same_work_id_and_records_once() {
    let mut store = MemoryWorkStore::new();
    // The identical body, byte for byte: a transport retry of
    // the submission SI already journaled at seq 4242.
    let body = submission_body(4242, Some("admit"), Some("read"));

    let first = post_work(&mut store, &body);
    let second = post_work(&mut store, &body);

    assert_eq!(first["journal_seq"], json!(4242));
    assert_eq!(second["journal_seq"], json!(4242));
    assert_eq!(
        work_id_of(&first),
        work_id_of(&second),
        "a retried submission returns the original work_id (§9.3)"
    );
    assert_eq!(
        store.records().len(),
        1,
        "the retry recorded nothing new: one journal_seq, one record"
    );
}

// -- Criterion 2: a different journal_seq is different work -----------------

#[test]
fn a_different_journal_seq_mints_a_different_work_id() {
    let mut store = MemoryWorkStore::new();

    let first = post_work(
        &mut store,
        &submission_body(4242, Some("admit"), Some("read")),
    );
    let second = post_work(
        &mut store,
        &submission_body(4243, Some("admit"), Some("read")),
    );

    assert_ne!(
        work_id_of(&first),
        work_id_of(&second),
        "a new journal_seq is new work, not a retry of the old"
    );
    assert_eq!(store.records().len(), 2, "the store holds both records");
}

// -- Criterion 3: provenance reports the completed result's refs ------------

#[test]
fn status_provenance_reports_the_completed_results_spine_event_refs() {
    let mut store = MemoryWorkStore::new();
    let ack = post_work(
        &mut store,
        &submission_body(4244, Some("admit"), Some("read")),
    );
    let work_id = work_id_of(&ack);

    // Before completion there is no outcome, and the honest
    // provenance list is the empty one.
    let before = status_of(&mut store, &work_id);
    assert_eq!(
        before["provenance"]["spine_event_refs"],
        json!([]),
        "provenance is empty before completion: {before}"
    );

    let mut actuator = FixedActuator {
        result: json!({
            "outcome": "executed",
            "tool": "fs.read",
            "model": "fake",
            "detail": "done",
            "caused_by": { "hash": "h0", "seq": 7 },
            "spine_event_refs": [
                { "hash": "h1", "seq": 8 },
                { "hash": "h2", "seq": 9 },
            ],
        }),
    };
    let done = run_actuation(&mut store, &mut actuator, &work_id)
        .expect("an admitted record actuates");
    assert_eq!(done.state.as_str(), "executed");

    let after = status_of(&mut store, &work_id);
    assert_eq!(
        after["provenance"]["spine_event_refs"],
        json!([
            { "hash": "h1", "seq": 8 },
            { "hash": "h2", "seq": 9 },
        ]),
        "provenance reports exactly the refs the result recorded (§11.2): {after}"
    );
    assert_eq!(
        after["result"]["outcome"],
        json!("executed"),
        "the status answer still carries the result itself: {after}"
    );
    assert_eq!(after["result"]["detail"], json!("done"));
}

#[test]
fn a_result_with_no_spine_event_refs_still_reports_empty_provenance() {
    // The `hearth.notify` shape: the actuation recorded an
    // outcome and wrote nothing to the spine, so its result
    // carries an empty ref list — and provenance reports
    // exactly that, not refs that were never written.
    let mut store = MemoryWorkStore::new();
    let ack = post_work(
        &mut store,
        &submission_body(4245, Some("admit"), Some("read")),
    );
    let work_id = work_id_of(&ack);

    let mut actuator = FixedActuator {
        result: json!({
            "outcome": "executed",
            "tool": "fs.read",
            "model": "fake",
            "detail": "done",
            "caused_by": { "hash": "h0", "seq": 7 },
            "spine_event_refs": [],
        }),
    };
    run_actuation(&mut store, &mut actuator, &work_id)
        .expect("an admitted record actuates");

    let after = status_of(&mut store, &work_id);
    assert_eq!(
        after["provenance"]["spine_event_refs"],
        json!([]),
        "an outcome that recorded no refs reports the empty list: {after}"
    );
    assert!(
        after.get("result").is_some(),
        "the status answer still carries the result itself: {after}"
    );
}
