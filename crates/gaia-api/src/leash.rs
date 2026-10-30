//! The leash crossing — `POST /gaia/v1/leash` and
//! `POST /gaia/v1/leash/heartbeat` (G-9 §12.3, §10.3.2).
//!
//! SI's operator leash is the stop/resume signal that rides
//! above every other piece of agency work: while a stop is in
//! force the fork actuates nothing for that model, however
//! well-verdicted the queued work is. The leash itself lives
//! in SI's journal — that journal is the truth, and the
//! records that cross here are replicas of journal entries SI
//! has already written, carried so the fork's local copy of
//! the leash state cannot drift silently from the journaled
//! one.
//!
//! That is the whole job of this module, and it is worth
//! stating what it is not:
//!
//!   * **The sink is the replica boundary.** [`handle`] parses
//!     the wire shape and hands it to a [`LeashSink`]; the sink
//!     — implemented by the composition, not by this crate —
//!     is where the replica is stored and the watermark is
//!     kept. This crate only carries the wire shape: it does
//!     not decide what a stop means for work already claimed,
//!     and it never manufactures a leash record of its own.
//!   * **`applied: false` is not an error.** A record whose
//!     `journal_seq` is at or below the sink's watermark has
//!     already been applied — re-delivering it is the normal
//!     consequence of an at-least-once crossing, not a fault.
//!     The sink answers `applied: false` with the current
//!     `last_leash_seq` (§10.3.2), and this route returns that
//!     answer as a plain `200`: the caller learns its record
//!     was a no-op and what the watermark actually is, which
//!     is exactly the information a retry loop needs to stop
//!     retrying.
//!   * **The error vocabulary is gaia's alone.** Malformed
//!     envelopes are `gaia_leash_malformed` and sink failures
//!     are `gaia_leash_store_error`, both in the OpenAI
//!     envelope like every other agency error — never a
//!     `model_*` type, which belongs to the inference
//!     vocabulary (see [`crate::gateway`]).
//!   * **No auth.** Like the rest of the agency surface this
//!     is a loopback v1 (G-9 §14); the routes are answered
//!     only where the composition mounts them, and that mount
//!     is on the operator's own box.
//!
//! The heartbeat route is the liveness half of the same
//! crossing: SI periodically reports the tips it sees — its
//! leash tip and its journal tip — so the sink can tell a
//! quiet leash (no stops, nothing to say) from a dead
//! crossing (no word at all). A heartbeat carries no record
//! and changes no leash state; its acknowledgement is the
//! same shape as the record route's, so a caller reads one
//! watermark vocabulary for both.

use serde_json::{json, Map, Value};

/// One leash record as it crosses from SI: the wire shape of
/// a journaled stop or resume for one model.
///
/// `journal_seq` is the SI journal sequence the record was
/// journaled at. It is the ordering and the watermark key —
/// the sink applies records in journal order and treats a
/// sequence at or below its watermark as already applied —
/// so it cannot be defaulted or invented here: a record
/// without one is malformed, full stop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeashRecordMsg {
    pub model: String,
    /// Exactly `"stop"` or `"resume"` — the only two leash
    /// transitions the journal records. Anything else is a
    /// value the gate never wrote, and recording it as one it
    /// did would put words in the operator's mouth.
    pub kind: String,
    pub journal_seq: u64,
    /// Why the operator pulled or released the leash. May be
    /// empty — a stop needs no justification to be honoured —
    /// but the field itself must be present and a string, so
    /// an absent reason and an empty one are not confused.
    pub reason: String,
}

/// A leash heartbeat: the tips SI currently sees, reported
/// so the sink can distinguish a quiet leash from a dead
/// crossing. Carries no record and moves no leash state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeashHeartbeatMsg {
    pub model: String,
    /// The highest leash journal sequence SI has written for
    /// this model.
    pub leash_tip: u64,
    /// The highest journal sequence SI has written overall
    /// for this model, leash records included.
    pub journal_tip: u64,
}

/// The sink's answer to either route: whether this delivery
/// changed the replica, and the watermark as it now stands.
///
/// `applied: false` is the watermark no-op acknowledgement
/// (§10.3.2) — the delivery was a re-delivery of something
/// already applied — and `last_leash_seq` is still the
/// current watermark, so the caller can advance its own view
/// to match instead of guessing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeashAck {
    pub applied: bool,
    pub last_leash_seq: u64,
}

/// The replica boundary the composition implements.
///
/// Implementations own the leash replica and its watermark:
/// applying a record means storing it if — and only if — its
/// `journal_seq` is above the watermark, and answering with
/// the watermark as it stands afterwards either way. The
/// `String` error is a store failure the caller could not
/// have prevented (a disk, a lock, a replica that will not
/// open); it is reported as `gaia_leash_store_error`, never
/// folded into `applied: false`, because a caller that
/// cannot tell "already applied" from "could not apply"
/// will retry the first forever or drop the second silently.
pub trait LeashSink {
    fn apply_leash_record(&mut self, record: &LeashRecordMsg) -> Result<LeashAck, String>;
    fn leash_heartbeat(&mut self, heartbeat: &LeashHeartbeatMsg) -> Result<LeashAck, String>;
}

/// Answer a request if — and only if — it is one of the two
/// leash routes, with the same path normalisation
/// [`crate::residency::handle`] applies (query string
/// stripped, trailing slash ignored, method
/// case-insensitive). Any other request returns `None` so
/// the composition can fall through to the other surfaces.
pub fn handle(req: &hearth_api::Request, sink: &mut dyn LeashSink) -> Option<hearth_api::Response> {
    // The same normalisation `residency::handle` applies:
    // the method is case-insensitive, the query string is
    // stripped, and a trailing slash does not make a
    // different route.
    let method = req.method.to_ascii_uppercase();
    let p = req
        .path
        .split('?')
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    if method != "POST" {
        return None;
    }
    if p == "/gaia/v1/leash" {
        Some(match parse_record(&req.body) {
            Ok(record) => match sink.apply_leash_record(&record) {
                Ok(ack) => ack_response(&ack),
                Err(e) => store_error_response(e),
            },
            Err(e) => malformed_response(e),
        })
    } else if p == "/gaia/v1/leash/heartbeat" {
        Some(match parse_heartbeat(&req.body) {
            Ok(heartbeat) => match sink.leash_heartbeat(&heartbeat) {
                Ok(ack) => ack_response(&ack),
                Err(e) => store_error_response(e),
            },
            Err(e) => malformed_response(e),
        })
    } else {
        None
    }
}

/// The `200` both routes share: whether the delivery applied,
/// and the watermark as it stands. A watermark no-op is
/// answered here too, at `200` with `applied: false` — it is
/// an acknowledgement, not a failure (see the module docs).
fn ack_response(ack: &LeashAck) -> hearth_api::Response {
    hearth_api::Response::json(
        200,
        json!({
            "applied": ack.applied,
            "last_leash_seq": ack.last_leash_seq,
        }),
    )
}

/// A malformed envelope is the caller's to fix: `400`, in the
/// gaia vocabulary alone.
fn malformed_response(message: String) -> hearth_api::Response {
    hearth_api::Response::openai_error(400, message, "gaia_leash_malformed", json!({}))
}

/// A sink failure is ours, not the caller's: `500`, with the
/// store's own words as the message so the operator sees the
/// actual failure rather than a paraphrase of it.
fn store_error_response(message: String) -> hearth_api::Response {
    hearth_api::Response::openai_error(500, message, "gaia_leash_store_error", json!({}))
}

/// Parse the body as a JSON object, naming the failure. Every
/// failure in the parsers below is the client's envelope,
/// never a panic: this parser sits on the wire.
fn parse_object(body: &str) -> Result<Map<String, Value>, String> {
    let v: Value = serde_json::from_str(body)
        .map_err(|e| format!("request body is not valid JSON: {e}"))?;
    v.as_object()
        .cloned()
        .ok_or_else(|| "request body must be a JSON object".to_string())
}

/// Read a required string field out of an envelope, naming
/// the field in the failure.
fn required_string(obj: &Map<String, Value>, field: &str) -> Result<String, String> {
    match obj.get(field) {
        None => Err(format!(
            "request \"{field}\" is required and must be a string"
        )),
        Some(v) => v.as_str().map(|s| s.to_string()).ok_or_else(|| {
            format!("request \"{field}\" is required and must be a string")
        }),
    }
}

/// Read a required string field that must additionally name
/// something: an empty model names no leash, and a leash
/// record for nobody must not be applied as one for
/// everybody.
fn required_non_empty_string(obj: &Map<String, Value>, field: &str) -> Result<String, String> {
    let s = required_string(obj, field)?;
    if s.is_empty() {
        return Err(format!("request \"{field}\" must not be empty"));
    }
    Ok(s)
}

/// Read a required `u64` field out of an envelope. Sequences
/// and tips are journal positions: a negative, fractional, or
/// string value is not a position the journal ever held, and
/// coercing one would order this record against history it
/// was never part of.
fn required_u64(obj: &Map<String, Value>, field: &str) -> Result<u64, String> {
    obj.get(field).and_then(Value::as_u64).ok_or_else(|| {
        format!("request \"{field}\" is required and must be a non-negative integer")
    })
}

/// Parse a leash record body: a JSON object with a non-empty
/// string `model`, a `kind` of exactly `"stop"` or
/// `"resume"`, a `u64` `journal_seq`, and a string `reason`
/// (which may be empty). Strict where a guess would be a lie
/// — there is no default kind, no default sequence, and no
/// third kind to be tolerant of.
fn parse_record(body: &str) -> Result<LeashRecordMsg, String> {
    let obj = parse_object(body)?;
    let model = required_non_empty_string(&obj, "model")?;
    let kind = required_string(&obj, "kind")?;
    if kind != "stop" && kind != "resume" {
        return Err(format!(
            "request \"kind\" must be \"stop\" or \"resume\", got {kind:?}"
        ));
    }
    let journal_seq = required_u64(&obj, "journal_seq")?;
    let reason = required_string(&obj, "reason")?;
    Ok(LeashRecordMsg {
        model,
        kind,
        journal_seq,
        reason,
    })
}

/// Parse a heartbeat body: a JSON object with a non-empty
/// string `model` and `u64` `leash_tip` / `journal_tip`.
/// The tips are compared against the sink's watermark, so —
/// like the record's sequence — neither can be defaulted.
fn parse_heartbeat(body: &str) -> Result<LeashHeartbeatMsg, String> {
    let obj = parse_object(body)?;
    let model = required_non_empty_string(&obj, "model")?;
    let leash_tip = required_u64(&obj, "leash_tip")?;
    let journal_tip = required_u64(&obj, "journal_tip")?;
    Ok(LeashHeartbeatMsg {
        model,
        leash_tip,
        journal_tip,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink that records what it was handed and answers
    /// with a canned acknowledgement — or a canned failure,
    /// when `error` is set. Recording matters: several tests
    /// below are proofs that the sink was *not* called, which
    /// only a sink that counts its calls can attest to.
    #[derive(Default)]
    struct MockSink {
        ack: Option<LeashAck>,
        error: Option<String>,
        records: Vec<LeashRecordMsg>,
        heartbeats: Vec<LeashHeartbeatMsg>,
    }

    impl MockSink {
        fn answering(applied: bool, last_leash_seq: u64) -> MockSink {
            MockSink {
                ack: Some(LeashAck {
                    applied,
                    last_leash_seq,
                }),
                ..Default::default()
            }
        }

        fn failing(message: &str) -> MockSink {
            MockSink {
                error: Some(message.to_string()),
                ..Default::default()
            }
        }
    }

    impl LeashSink for MockSink {
        fn apply_leash_record(&mut self, record: &LeashRecordMsg) -> Result<LeashAck, String> {
            self.records.push(record.clone());
            match &self.error {
                Some(e) => Err(e.clone()),
                None => Ok(self
                    .ack
                    .clone()
                    .expect("a non-failing mock sink is given an ack")),
            }
        }

        fn leash_heartbeat(
            &mut self,
            heartbeat: &LeashHeartbeatMsg,
        ) -> Result<LeashAck, String> {
            self.heartbeats.push(heartbeat.clone());
            match &self.error {
                Some(e) => Err(e.clone()),
                None => Ok(self
                    .ack
                    .clone()
                    .expect("a non-failing mock sink is given an ack")),
            }
        }
    }

    fn req(method: &str, path: &str, body: &str) -> hearth_api::Request {
        hearth_api::Request {
            method: method.to_string(),
            path: path.to_string(),
            body: body.to_string(),
            ..Default::default()
        }
    }

    fn body_json(resp: &hearth_api::Response) -> Value {
        serde_json::from_str(&resp.body)
            .unwrap_or_else(|e| panic!("response body is not JSON ({e}): {}", resp.body))
    }

    fn record_body() -> String {
        json!({
            "model": "muse",
            "kind": "stop",
            "journal_seq": 17,
            "reason": "operator stop",
        })
        .to_string()
    }

    #[test]
    fn record_route_applies_and_answers_the_ack() {
        let mut sink = MockSink::answering(true, 17);
        let resp = handle(
            &req("POST", "/gaia/v1/leash", &record_body()),
            &mut sink,
        )
        .expect("the leash route is answered here");
        assert_eq!(resp.status, 200);
        let body = body_json(&resp);
        assert_eq!(body["applied"], json!(true));
        assert_eq!(body["last_leash_seq"], json!(17));
        // The sink received exactly the record that was sent.
        assert_eq!(sink.records.len(), 1);
        assert_eq!(sink.records[0].model, "muse");
        assert_eq!(sink.records[0].kind, "stop");
        assert_eq!(sink.records[0].journal_seq, 17);
        assert_eq!(sink.records[0].reason, "operator stop");
        assert!(sink.heartbeats.is_empty());
    }

    #[test]
    fn a_watermark_no_op_is_a_200_not_an_error() {
        // The record was already applied; §10.3.2's answer is
        // an acknowledgement carrying the watermark, so the
        // caller can stop re-delivering.
        let mut sink = MockSink::answering(false, 17);
        let resp = handle(
            &req("POST", "/gaia/v1/leash", &record_body()),
            &mut sink,
        )
        .expect("the leash route is answered here");
        assert_eq!(resp.status, 200);
        let body = body_json(&resp);
        assert_eq!(body["applied"], json!(false));
        assert_eq!(body["last_leash_seq"], json!(17));
        assert!(
            body.get("error").is_none(),
            "a no-op ack carries no error envelope: {body}"
        );
    }

    #[test]
    fn heartbeat_route_reports_the_tips_and_answers_the_ack() {
        let mut sink = MockSink::answering(false, 17);
        let body = json!({
            "model": "muse",
            "leash_tip": 17,
            "journal_tip": 42,
        })
        .to_string();
        let resp = handle(
            &req("POST", "/gaia/v1/leash/heartbeat", &body),
            &mut sink,
        )
        .expect("the heartbeat route is answered here");
        assert_eq!(resp.status, 200);
        let ack = body_json(&resp);
        assert_eq!(ack["applied"], json!(false));
        assert_eq!(ack["last_leash_seq"], json!(17));
        assert_eq!(sink.heartbeats.len(), 1);
        assert_eq!(sink.heartbeats[0].model, "muse");
        assert_eq!(sink.heartbeats[0].leash_tip, 17);
        assert_eq!(sink.heartbeats[0].journal_tip, 42);
        assert!(sink.records.is_empty(), "a heartbeat applies no record");
    }

    #[test]
    fn malformed_bodies_are_400_in_the_gaia_vocabulary() {
        let cases: Vec<(&str, String)> = vec![
            // A kind the journal never records.
            (
                "bad kind",
                json!({
                    "model": "muse",
                    "kind": "pause",
                    "journal_seq": 17,
                    "reason": "",
                })
                .to_string(),
            ),
            // The sequence is the watermark key; it cannot be
            // absent and it cannot be invented.
            (
                "missing journal_seq",
                json!({
                    "model": "muse",
                    "kind": "stop",
                    "reason": "",
                })
                .to_string(),
            ),
            (
                "missing model",
                json!({
                    "kind": "stop",
                    "journal_seq": 17,
                    "reason": "",
                })
                .to_string(),
            ),
            (
                "empty model",
                json!({
                    "model": "",
                    "kind": "stop",
                    "journal_seq": 17,
                    "reason": "",
                })
                .to_string(),
            ),
            // The envelope itself must be an object.
            ("non-object body", json!(["stop"]).to_string()),
            ("not json", "not json".to_string()),
        ];
        for (case, body) in &cases {
            let mut sink = MockSink::answering(true, 17);
            let resp = handle(&req("POST", "/gaia/v1/leash", body), &mut sink)
                .expect("the leash route answers its own malformed bodies");
            assert_eq!(resp.status, 400, "{case}: {}", resp.body);
            assert_eq!(
                body_json(&resp)["error"]["type"],
                json!("gaia_leash_malformed"),
                "{case}: {}",
                resp.body
            );
            assert!(
                sink.records.is_empty(),
                "{case}: a malformed body never reaches the sink"
            );
        }

        // The heartbeat parser is the same shape of strict.
        let mut sink = MockSink::answering(true, 17);
        let body = json!({ "model": "muse", "leash_tip": 17 }).to_string();
        let resp = handle(
            &req("POST", "/gaia/v1/leash/heartbeat", &body),
            &mut sink,
        )
        .expect("the heartbeat route answers its own malformed bodies");
        assert_eq!(resp.status, 400);
        assert_eq!(
            body_json(&resp)["error"]["type"],
            json!("gaia_leash_malformed")
        );
        assert!(sink.heartbeats.is_empty());
    }

    #[test]
    fn a_sink_error_is_a_500_in_the_gaia_vocabulary() {
        let mut sink = MockSink::failing("replica store is read-only");
        let resp = handle(
            &req("POST", "/gaia/v1/leash", &record_body()),
            &mut sink,
        )
        .expect("the leash route is answered here");
        assert_eq!(resp.status, 500);
        let body = body_json(&resp);
        assert_eq!(body["error"]["type"], json!("gaia_leash_store_error"));
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("replica store is read-only"),
            "the store's own words are the message: {body}"
        );
    }

    #[test]
    fn other_methods_and_paths_fall_through() {
        let mut sink = MockSink::answering(true, 17);
        // A GET on the leash path is not a leash delivery —
        // the composition's other surfaces may still claim it,
        // so this handler must decline it.
        assert!(handle(&req("GET", "/gaia/v1/leash", ""), &mut sink).is_none());
        assert!(
            handle(&req("GET", "/gaia/v1/leash/heartbeat", ""), &mut sink).is_none()
        );
        // Unrelated paths are not this module's business at
        // any method.
        assert!(
            handle(&req("POST", "/gaia/v1/work", "{}"), &mut sink).is_none()
        );
        assert!(handle(&req("POST", "/gaia/v1/nope", "{}"), &mut sink).is_none());
        assert!(
            sink.records.is_empty() && sink.heartbeats.is_empty(),
            "a declined request never reaches the sink"
        );
    }

    #[test]
    fn a_query_string_and_a_trailing_slash_are_the_same_route() {
        let mut sink = MockSink::answering(true, 17);
        assert!(
            handle(
                &req("POST", "/gaia/v1/leash?retry=1", &record_body()),
                &mut sink,
            )
            .is_some(),
            "a query string does not make a different route"
        );
        assert!(
            handle(&req("post", "/gaia/v1/leash/", &record_body()), &mut sink).is_some(),
            "a trailing slash — and a lowercase method — do not either"
        );
        assert_eq!(sink.records.len(), 2);
    }
}
