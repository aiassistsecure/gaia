//! The SI events ingest — `POST /gaia/v1/events/ingest`
//! (G-9 §15, S-6).
//!
//! Until now [`crate::sse::SiEventBuffer::push`] was an
//! in-process-only ingest: SI's journal events could land in
//! the fork's buffer only if the code forwarding them ran in
//! the same process as the fork. This module is the transport
//! G-9 §15 S-6 adds for that push — one SI journal event per
//! request, in SI's own event shape, carried over HTTP to the
//! buffer the SSE server already serves from.
//!
//! That is the whole job of this module, and it is worth
//! stating what it is not:
//!
//!   * **The forwarder is not here.** The SI-side forwarder
//!     that reads SI's journal and posts to this route is a
//!     separate, later SI slice — UNBUILT. This module is
//!     only the fork half of S-6: the receiving route. Nothing
//!     here reads SI's journal, and nothing here claims the
//!     crossing is live end to end until that forwarder lands.
//!   * **No auth.** Like the rest of the agency surface this
//!     is an unauthenticated loopback v1 per G-9 §14; the
//!     route is answered only where the composition mounts
//!     it, and that mount is on the operator's own box.
//!   * **The buffer's refusal is the protocol.** The buffer
//!     refuses a duplicate or out-of-order sequence loudly,
//!     by design (see [`crate::sse::SiEventBuffer::push`]):
//!     a renumbered or replayed-from-the-wrong-point stream
//!     is history corruption, not a transient fault. This
//!     route carries that refusal as a `409`
//!     `gaia_ingest_refused` with the buffer's own words as
//!     the message — never a `200`, never silent dedupe. A
//!     duplicate refusal is a protocol failure the forwarder
//!     must treat as such, not retry: re-posting the same
//!     event cannot succeed, and posting the next one past a
//!     gap the buffer refused would only compound it.
//!   * **Malformed is validated before any push.** A body
//!     that is not SI's event shape is a `400`
//!     `gaia_ingest_malformed`, naming the defect, and the
//!     buffer does not grow by one event for it: every field
//!     is validated before anything is built or pushed, so a
//!     bad envelope can never consume a sequence position.

use serde_json::{json, Map, Value};

use crate::events::{EventLog, Source, StreamEvent};
use crate::sse::SiEventBuffer;

/// Answer a request if — and only if — it is the events
/// ingest route, with the same path normalisation
/// [`crate::leash::handle`] applies (query string stripped,
/// trailing slash ignored, method case-insensitive). Any
/// other request returns `None` so the composition can fall
/// through to the other surfaces.
pub fn handle(req: &hearth_api::Request, buffer: &SiEventBuffer) -> Option<hearth_api::Response> {
    // The same normalisation `leash::handle` applies: the
    // method is case-insensitive, the query string is
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
    if p != "/gaia/v1/events/ingest" {
        return None;
    }
    Some(match parse_event(&req.body) {
        Ok((id, event, collection, row)) => {
            let ev = StreamEvent::from_si(id, &event, &collection, row);
            match buffer.push(ev) {
                Ok(()) => hearth_api::Response::json(
                    200,
                    json!({
                        "accepted": true,
                        "seq": id,
                        "latest_seq": buffer.latest_seq(Source::Si),
                    }),
                ),
                Err(e) => refused_response(e),
            }
        }
        Err(e) => malformed_response(e),
    })
}

/// A malformed envelope is the caller's to fix: `400`, in
/// the gaia vocabulary alone.
fn malformed_response(message: String) -> hearth_api::Response {
    hearth_api::Response::openai_error(400, message, "gaia_ingest_malformed", json!({}))
}

/// A buffer refusal is a protocol failure, not a retryable
/// fault: `409`, with the buffer's own words as the message
/// so the forwarder sees the actual refusal rather than a
/// paraphrase of it.
fn refused_response(message: String) -> hearth_api::Response {
    hearth_api::Response::openai_error(409, message, "gaia_ingest_refused", json!({}))
}

/// Parse the body as a JSON object, naming the failure.
/// Every failure in the parsers below is the client's
/// envelope, never a panic: this parser sits on the wire.
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
/// something: an empty event or collection names nothing,
/// and an event for nothing must not be carried as one.
fn required_non_empty_string(obj: &Map<String, Value>, field: &str) -> Result<String, String> {
    let s = required_string(obj, field)?;
    if s.is_empty() {
        return Err(format!("request \"{field}\" must not be empty"));
    }
    Ok(s)
}

/// Read a required `u64` field out of an envelope. The `id`
/// is the SI journal sequence: a negative, fractional, or
/// string value is not a position the journal ever held,
/// and coercing one would order this event against history
/// it was never part of.
fn required_u64(obj: &Map<String, Value>, field: &str) -> Result<u64, String> {
    obj.get(field).and_then(Value::as_u64).ok_or_else(|| {
        format!("request \"{field}\" is required and must be a non-negative integer")
    })
}

/// Read a required JSON object field out of an envelope.
/// The `row` is SI's stored row: it is an object by SI's
/// own shape, and anything else is not a row the journal
/// produced.
fn required_object(obj: &Map<String, Value>, field: &str) -> Result<Value, String> {
    match obj.get(field) {
        None => Err(format!(
            "request \"{field}\" is required and must be a JSON object"
        )),
        Some(v) => {
            if v.is_object() {
                Ok(v.clone())
            } else {
                Err(format!(
                    "request \"{field}\" is required and must be a JSON object"
                ))
            }
        }
    }
}

/// Parse one SI journal event body: a JSON object with a
/// `u64` `id`, a non-empty string `event`, a non-empty
/// string `collection`, and a JSON object `row` — exactly
/// the shape SI's `read_journal_events` produces and
/// [`StreamEvent::from_si`] consumes. Strict where a guess
/// would be a lie — there is no default id, no default
/// event, and no default collection to be tolerant of.
/// Everything is validated here, before the caller builds
/// or pushes anything.
fn parse_event(body: &str) -> Result<(u64, String, String, Value), String> {
    let obj = parse_object(body)?;
    let id = required_u64(&obj, "id")?;
    let event = required_non_empty_string(&obj, "event")?;
    let collection = required_non_empty_string(&obj, "collection")?;
    let row = required_object(&obj, "row")?;
    Ok((id, event, collection, row))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::events::EventFilter;

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

    fn event_body() -> String {
        json!({
            "id": 7,
            "event": "self_edit.tool",
            "collection": "self_edits",
            "row": {"kind": "tool", "task_seq": 3, "seq": 7},
        })
        .to_string()
    }

    fn buffer_len(buffer: &SiEventBuffer) -> usize {
        buffer
            .events_since(Source::Si, 0, &EventFilter::default())
            .len()
    }

    #[test]
    fn happy_path_200_and_buffer_holds_the_event() {
        let buffer = SiEventBuffer::new();
        let resp = handle(
            &req("POST", "/gaia/v1/events/ingest", &event_body()),
            &buffer,
        )
        .expect("the ingest route is answered here");
        assert_eq!(resp.status, 200);
        let body = body_json(&resp);
        assert_eq!(body["accepted"], json!(true));
        assert_eq!(body["seq"], json!(7));
        assert_eq!(body["latest_seq"], json!(7));

        assert_eq!(buffer_len(&buffer), 1);
        assert_eq!(buffer.latest_seq(Source::Si), 7);
        let events = buffer.events_since(Source::Si, 0, &EventFilter::default());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].seq, 7);
        assert_eq!(events[0].source, Source::Si);
        assert_eq!(events[0].kind, "self_edit.tool");
        assert_eq!(events[0].collection, "self_edits");
        assert_eq!(
            events[0].row,
            json!({"kind": "tool", "task_seq": 3, "seq": 7})
        );
    }

    #[test]
    fn duplicate_post_is_409_refused_and_buffer_unchanged() {
        let buffer = SiEventBuffer::new();
        let first = handle(
            &req("POST", "/gaia/v1/events/ingest", &event_body()),
            &buffer,
        )
        .expect("the ingest route is answered here");
        assert_eq!(first.status, 200);
        assert_eq!(buffer_len(&buffer), 1);

        let resp = handle(
            &req("POST", "/gaia/v1/events/ingest", &event_body()),
            &buffer,
        )
        .expect("the ingest route answers its own refusals");
        assert_eq!(resp.status, 409);
        let body = body_json(&resp);
        assert_eq!(body["error"]["type"], json!("gaia_ingest_refused"));
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("does not strictly follow"),
            "the buffer's own words are the message: {body}"
        );
        assert_eq!(buffer_len(&buffer), 1, "a refused push grows nothing");
        assert_eq!(buffer.latest_seq(Source::Si), 7);
    }

    #[test]
    fn out_of_order_post_is_409_refused() {
        let buffer = SiEventBuffer::new();
        let first = handle(
            &req("POST", "/gaia/v1/events/ingest", &event_body()),
            &buffer,
        )
        .expect("the ingest route is answered here");
        assert_eq!(first.status, 200);

        let older = json!({
            "id": 3,
            "event": "self_edit.tool",
            "collection": "self_edits",
            "row": {"kind": "tool", "seq": 3},
        })
        .to_string();
        let resp = handle(&req("POST", "/gaia/v1/events/ingest", &older), &buffer)
            .expect("the ingest route answers its own refusals");
        assert_eq!(resp.status, 409);
        assert_eq!(
            body_json(&resp)["error"]["type"],
            json!("gaia_ingest_refused")
        );
        assert_eq!(buffer_len(&buffer), 1);
        assert_eq!(buffer.latest_seq(Source::Si), 7);
    }

    #[test]
    fn malformed_bodies_are_400_and_grow_nothing() {
        let cases: Vec<(&str, String)> = vec![
            ("not json", "not json".to_string()),
            ("non-object body", json!(["self_edit.tool"]).to_string()),
            (
                "missing id",
                json!({
                    "event": "self_edit.tool",
                    "collection": "self_edits",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "id not a u64",
                json!({
                    "id": "7",
                    "event": "self_edit.tool",
                    "collection": "self_edits",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "id negative",
                json!({
                    "id": -1,
                    "event": "self_edit.tool",
                    "collection": "self_edits",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "missing event",
                json!({
                    "id": 7,
                    "collection": "self_edits",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "empty event",
                json!({
                    "id": 7,
                    "event": "",
                    "collection": "self_edits",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "event not a string",
                json!({
                    "id": 7,
                    "event": 7,
                    "collection": "self_edits",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "missing collection",
                json!({
                    "id": 7,
                    "event": "self_edit.tool",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "empty collection",
                json!({
                    "id": 7,
                    "event": "self_edit.tool",
                    "collection": "",
                    "row": {},
                })
                .to_string(),
            ),
            (
                "collection not a string",
                json!({
                    "id": 7,
                    "event": "self_edit.tool",
                    "collection": 7,
                    "row": {},
                })
                .to_string(),
            ),
            (
                "missing row",
                json!({
                    "id": 7,
                    "event": "self_edit.tool",
                    "collection": "self_edits",
                })
                .to_string(),
            ),
            (
                "row not an object",
                json!({
                    "id": 7,
                    "event": "self_edit.tool",
                    "collection": "self_edits",
                    "row": [1, 2],
                })
                .to_string(),
            ),
        ];
        for (case, body) in &cases {
            let buffer = SiEventBuffer::new();
            let resp = handle(&req("POST", "/gaia/v1/events/ingest", body), &buffer)
                .expect("the ingest route answers its own malformed bodies");
            assert_eq!(resp.status, 400, "{case}: {}", resp.body);
            assert_eq!(
                body_json(&resp)["error"]["type"],
                json!("gaia_ingest_malformed"),
                "{case}: {}",
                resp.body
            );
            assert_eq!(
                buffer_len(&buffer),
                0,
                "{case}: a malformed body grows the buffer by nothing"
            );
            assert_eq!(buffer.latest_seq(Source::Si), 0, "{case}");
        }
    }

    #[test]
    fn other_methods_and_paths_fall_through() {
        let buffer = SiEventBuffer::new();
        assert!(
            handle(&req("GET", "/gaia/v1/events/ingest", ""), &buffer).is_none()
        );
        assert!(
            handle(&req("PUT", "/gaia/v1/events/ingest", &event_body()), &buffer).is_none()
        );
        assert!(
            handle(&req("POST", "/gaia/v1/events", &event_body()), &buffer).is_none()
        );
        assert!(
            handle(&req("POST", "/gaia/v1/leash", &event_body()), &buffer).is_none()
        );
        assert!(
            handle(&req("POST", "/gaia/v1/nope", &event_body()), &buffer).is_none()
        );
        assert_eq!(
            buffer_len(&buffer),
            0,
            "a declined request never reaches the buffer"
        );
    }

    #[test]
    fn a_query_string_and_a_trailing_slash_are_the_same_route() {
        let buffer = SiEventBuffer::new();
        assert!(
            handle(
                &req("POST", "/gaia/v1/events/ingest?retry=1", &event_body()),
                &buffer,
            )
            .is_some(),
            "a query string does not make a different route"
        );
        let second = json!({
            "id": 8,
            "event": "self_edit.tool",
            "collection": "self_edits",
            "row": {"kind": "tool", "seq": 8},
        })
        .to_string();
        assert!(
            handle(&req("post", "/gaia/v1/events/ingest/", &second), &buffer).is_some(),
            "a trailing slash — and a lowercase method — do not either"
        );
        assert_eq!(buffer_len(&buffer), 2);
        assert_eq!(buffer.latest_seq(Source::Si), 8);
    }
}
