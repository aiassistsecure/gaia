//! Acceptance tests for S-4a — the leash crossing, fork half
//! (G-9 §12.3, §10.3.2).
//!
//! SI's journal is the truth for the operator leash; these
//! routes carry journaled stop/resume records across to the
//! fork's replica and report heartbeats back the other way.
//! The criteria exercised here, through the crate's pure
//! handler surface (`gaia_api::leash::handle`) against a
//! recording sink — no sockets, no server:
//!
//!   * a stop record is delivered to the sink with its exact
//!     fields and acknowledged `200` with the sink's
//!     watermark;
//!   * a watermark no-op (`applied: false`, §10.3.2) is still
//!     a `200` acknowledgement, not an error — the caller
//!     learns its re-delivery changed nothing and what the
//!     watermark is;
//!   * a heartbeat delivers `model` / `leash_tip` /
//!     `journal_tip` verbatim and is acknowledged the same
//!     way;
//!   * a kind the journal never records (`"pause"`) is a
//!     `400` in the gaia vocabulary, and the sink is never
//!     called — a malformed record must not move the replica
//!     on its way to being refused.

use std::collections::HashMap;

use gaia_api::leash::{handle, LeashAck, LeashHeartbeatMsg, LeashRecordMsg, LeashSink};
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

/// A sink that records every delivery verbatim and answers
/// with one canned acknowledgement. Recording — not just
/// counting — is the point: the criteria below assert on the
/// exact fields the handler parsed out of the wire body, and
/// on the sink *not* being called for a malformed body.
#[derive(Default)]
struct RecordingSink {
    ack: Option<LeashAck>,
    records: Vec<LeashRecordMsg>,
    heartbeats: Vec<LeashHeartbeatMsg>,
}

impl RecordingSink {
    fn answering(applied: bool, last_leash_seq: u64) -> RecordingSink {
        RecordingSink {
            ack: Some(LeashAck {
                applied,
                last_leash_seq,
            }),
            ..Default::default()
        }
    }

    fn calls(&self) -> usize {
        self.records.len() + self.heartbeats.len()
    }
}

impl LeashSink for RecordingSink {
    fn apply_leash_record(&mut self, record: &LeashRecordMsg) -> Result<LeashAck, String> {
        self.records.push(record.clone());
        Ok(self
            .ack
            .clone()
            .expect("a recording sink is given an ack"))
    }

    fn leash_heartbeat(&mut self, heartbeat: &LeashHeartbeatMsg) -> Result<LeashAck, String> {
        self.heartbeats.push(heartbeat.clone());
        Ok(self
            .ack
            .clone()
            .expect("a recording sink is given an ack"))
    }
}

// -- (a) a stop record applies and is acknowledged ---------------------------

#[test]
fn a_stop_record_returns_200_and_the_sink_receives_the_exact_fields() {
    let mut sink = RecordingSink::answering(true, 23);
    let body = json!({
        "model": "muse",
        "kind": "stop",
        "journal_seq": 23,
        "reason": "operator stop for maintenance",
    })
    .to_string();

    let resp = handle(&req("POST", "/gaia/v1/leash", &body), &mut sink)
        .expect("the leash route is answered by handle");
    assert_eq!(resp.status, 200, "record body: {}", resp.body);
    let ack = body_json(&resp);
    assert_eq!(ack["applied"], json!(true));
    assert_eq!(
        ack["last_leash_seq"],
        json!(23),
        "the acknowledgement carries the sink's watermark: {ack}"
    );

    assert_eq!(sink.records.len(), 1, "exactly one record was delivered");
    let record = &sink.records[0];
    assert_eq!(record.model, "muse");
    assert_eq!(record.kind, "stop");
    assert_eq!(record.journal_seq, 23);
    assert_eq!(record.reason, "operator stop for maintenance");
    assert!(
        sink.heartbeats.is_empty(),
        "a record delivery is not a heartbeat"
    );
}

// -- (b) a watermark no-op is still a 200 -------------------------------------

#[test]
fn a_watermark_no_op_still_returns_200_with_applied_false() {
    // The sink already holds this sequence: re-delivery is
    // the normal consequence of an at-least-once crossing,
    // and §10.3.2's answer is an acknowledgement — `200`,
    // `applied: false`, the standing watermark — so the
    // caller can stop retrying instead of treating its own
    // retry as a failure.
    let mut sink = RecordingSink::answering(false, 23);
    let body = json!({
        "model": "muse",
        "kind": "resume",
        "journal_seq": 23,
        "reason": "",
    })
    .to_string();

    let resp = handle(&req("POST", "/gaia/v1/leash", &body), &mut sink)
        .expect("the leash route is answered by handle");
    assert_eq!(resp.status, 200, "no-op body: {}", resp.body);
    let ack = body_json(&resp);
    assert_eq!(ack["applied"], json!(false));
    assert_eq!(ack["last_leash_seq"], json!(23));
    assert!(
        ack.get("error").is_none(),
        "a watermark no-op carries no error envelope: {ack}"
    );
    assert_eq!(
        sink.records.len(),
        1,
        "the no-op answer comes from the sink, so the record was delivered"
    );
}

// -- (c) a heartbeat delivers its tips verbatim -------------------------------

#[test]
fn a_heartbeat_returns_200_and_the_sink_receives_the_tips_verbatim() {
    let mut sink = RecordingSink::answering(false, 23);
    let body = json!({
        "model": "muse",
        "leash_tip": 23,
        "journal_tip": 91,
    })
    .to_string();

    let resp = handle(&req("POST", "/gaia/v1/leash/heartbeat", &body), &mut sink)
        .expect("the heartbeat route is answered by handle");
    assert_eq!(resp.status, 200, "heartbeat body: {}", resp.body);
    let ack = body_json(&resp);
    assert_eq!(ack["applied"], json!(false));
    assert_eq!(ack["last_leash_seq"], json!(23));

    assert_eq!(sink.heartbeats.len(), 1, "exactly one heartbeat was delivered");
    let heartbeat = &sink.heartbeats[0];
    assert_eq!(heartbeat.model, "muse");
    assert_eq!(heartbeat.leash_tip, 23);
    assert_eq!(heartbeat.journal_tip, 91);
    assert!(
        sink.records.is_empty(),
        "a heartbeat applies no leash record"
    );
}

// -- (d) a malformed kind is refused before the sink --------------------------

#[test]
fn a_malformed_kind_is_400_and_the_sink_is_never_called() {
    // "pause" is not a leash transition the journal records;
    // there are exactly two. Accepting a third here would
    // record a state the operator never entered.
    let mut sink = RecordingSink::answering(true, 24);
    let body = json!({
        "model": "muse",
        "kind": "pause",
        "journal_seq": 24,
        "reason": "operator pause",
    })
    .to_string();

    let resp = handle(&req("POST", "/gaia/v1/leash", &body), &mut sink)
        .expect("the leash route answers its own malformed bodies");
    assert_eq!(resp.status, 400, "malformed body: {}", resp.body);
    let body = body_json(&resp);
    assert_eq!(
        body["error"]["type"],
        json!("gaia_leash_malformed"),
        "malformed leash bodies speak the gaia vocabulary: {body}"
    );
    assert_eq!(
        sink.calls(),
        0,
        "a malformed body must never reach the sink"
    );
}
