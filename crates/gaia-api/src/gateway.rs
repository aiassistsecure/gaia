//! What to do with an agency request. Pure, method-aware, and — like
//! `hearth-api`'s gateway — the whole product of this crate lives here.
//!
//! Three rules shape everything below, and each one exists because
//! breaking it would make a caller believe something untrue:
//!
//!   * **The two error vocabularies stay disjoint.** Inference errors
//!     diagnose residency (`model_lost`, `model_not_admitted`, …);
//!     agency errors diagnose work (`gaia_work_unknown`,
//!     `gaia_verdict_conflict`, …). Both travel in the OpenAI envelope
//!     so a client can parse either, but a `/gaia/*` route never
//!     produces a residency type and an inference route never produces
//!     a `gaia_*` one — a router that acted on the wrong vocabulary
//!     would score a node down for a work problem, or retry a work
//!     submission as if a model were warming.
//!   * **Submission is not execution.** `POST /gaia/v1/work` records
//!     an intent SI has already journaled and answers `202`. Nothing
//!     in this module actuates anything; the lifecycle in [`crate::work`]
//!     decides, separately, what may be acted on and when.
//!   * **The fork never manufactures a verdict.** An intent either
//!     arrives with the gate's verdict attached or it waits in
//!     `awaiting_verdict` until a verdict arrives on the verdict
//!     route, delivered by SI. Inventing an ADMIT here — however
//!     convenient for a demo — would turn the carrier into the
//!     decider, and the provenance trail the status route returns
//!     would then attest to a judgement nobody made.

use serde_json::{json, Map, Value};

use crate::work::{GaiaError, Intent, Verdict, WorkStore};

/// Which agency route a request is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Route {
    /// `POST /gaia/v1/work` — submit a journaled tool intent.
    SubmitWork,
    /// `GET /gaia/v1/work/{id}` — status (and result, when finished).
    WorkStatus(String),
    /// `POST /gaia/v1/work/{id}/verdict` — deliver the gate's verdict.
    WorkVerdict(String),
    /// `GET /gaia/v1/health` — the agency surface itself.
    Health,
    /// Anything else, including the wrong method on a known path.
    Unknown,
}

/// Classify a request by method and path. Query strings are stripped,
/// a trailing slash does not make a different route, and the method is
/// case-insensitive on the wire — clients vary on all three, and a 404
/// for a trailing slash is a support ticket, not a diagnosis.
///
/// Method-aware, unlike `hearth_api::gateway::classify`: the same work
/// path submitted, read, and verdicted is three different operations,
/// so the path alone cannot say which one this is.
pub fn classify(method: &str, path: &str) -> Route {
    let method = method.to_ascii_uppercase();
    let p = path.split('?').next().unwrap_or("").trim_end_matches('/');

    if method == "POST" && p == "/gaia/v1/work" {
        return Route::SubmitWork;
    }
    if method == "GET" && p == "/gaia/v1/health" {
        return Route::Health;
    }
    if method == "GET" {
        if let Some(id) = p.strip_prefix("/gaia/v1/work/") {
            if !id.is_empty() && !id.contains('/') {
                return Route::WorkStatus(id.to_string());
            }
        }
    }
    if method == "POST" {
        if let Some(rest) = p.strip_prefix("/gaia/v1/work/") {
            if let Some(id) = rest.strip_suffix("/verdict") {
                if !id.is_empty() && !id.contains('/') {
                    return Route::WorkVerdict(id.to_string());
                }
            }
        }
    }
    Route::Unknown
}

/// The decision. Pure except for the store it is handed: routing,
/// parsing, and the error mapping live here, and the store only ever
/// sees a parsed [`Intent`] or a parsed [`Verdict`].
pub fn decide(req: &hearth_api::Request, store: &mut dyn WorkStore) -> hearth_api::Response {
    match classify(&req.method, &req.path) {
        // Health is about the agency surface, not the work. A surface
        // that reported unhealthy because work was waiting for a
        // verdict would be pulled from rotation for doing its job.
        Route::Health => hearth_api::Response::json(
            200,
            json!({ "status": "ok", "service": "gaia" }),
        ),
        Route::Unknown => hearth_api::Response::openai_error(
            404,
            format!("no gaia route for {} {}", req.method, req.path),
            "gaia_route_not_found",
            json!({}),
        ),
        Route::SubmitWork => match parse_intent(&req.body) {
            Ok(intent) => match store.submit(intent) {
                // 202, not 200 and never an execution result: the
                // submission is recorded, and that is all this answer
                // claims. `journal_seq` echoes the SI journal sequence
                // the intent was journaled at, so the submitter can
                // match this answer to its own journal without a
                // second lookup.
                Ok(rec) => hearth_api::Response::json(
                    202,
                    json!({
                        "work_id": rec.id,
                        "state": "accepted",
                        "journal_seq": rec.intent.journal_seq,
                    }),
                ),
                Err(e) => error_response(&e),
            },
            Err(e) => error_response(&e),
        },
        Route::WorkStatus(id) => match store.get(&id) {
            Some(rec) => hearth_api::Response::json(200, rec.status_body()),
            None => error_response(&GaiaError::UnknownWork(id)),
        },
        Route::WorkVerdict(id) => match parse_verdict(&req.body) {
            Ok(verdict) => match store.record_verdict(&id, verdict) {
                Ok(rec) => hearth_api::Response::json(200, rec.status_body()),
                Err(e) => error_response(&e),
            },
            Err(e) => error_response(&e),
        },
    }
}

/// Map a work error onto the wire. The status and the `gaia_*` type
/// both come from the error itself, so the mapping the contract fixes
/// (404 unknown, 409 conflict / required / not-carried, 400 malformed)
/// is stated in exactly one place — [`crate::work`] — and cannot drift
/// between routes.
fn error_response(e: &GaiaError) -> hearth_api::Response {
    hearth_api::Response::openai_error(e.status(), e.message(), e.error_type(), json!({}))
}

/// Read a required string field out of an envelope, naming the field
/// in the failure. Every failure here is the client's envelope, never
/// a panic: this parser sits on the wire.
fn required_string(obj: &Map<String, Value>, field: &str) -> Result<String, GaiaError> {
    match obj.get(field) {
        None => Err(GaiaError::Malformed(format!(
            "request \"{field}\" is required and must be a string"
        ))),
        Some(v) => v.as_str().map(|s| s.to_string()).ok_or_else(|| {
            GaiaError::Malformed(format!(
                "request \"{field}\" is required and must be a string"
            ))
        }),
    }
}

/// Parse a submitted work order: the SI tool-intent envelope
/// (`{type:"tool", tool, args, reason}`) plus the task id and the SI
/// journal sequence at which the intent was journaled.
///
/// Tolerant where tolerance costs nothing — `args` defaults to `Null`
/// and an absent `verdict` simply means the intent is escalated, to be
/// verdicted later — and strict where a guess would be a lie: a
/// missing journal sequence cannot be invented, because the status
/// route returns it as provenance.
fn parse_intent(body: &str) -> Result<Intent, GaiaError> {
    let v: Value = serde_json::from_str(body).map_err(|e| {
        GaiaError::Malformed(format!("request body is not valid JSON: {e}"))
    })?;
    let obj = v.as_object().ok_or_else(|| {
        GaiaError::Malformed("request body must be a JSON object".to_string())
    })?;

    match obj.get("type") {
        None => {
            return Err(GaiaError::Malformed(
                "request \"type\" is required and must be \"tool\"".to_string(),
            ));
        }
        Some(v) => {
            if v.as_str() != Some("tool") {
                return Err(GaiaError::Malformed(format!(
                    "request \"type\" must be \"tool\", got {v}"
                )));
            }
        }
    }

    let tool = required_string(obj, "tool")?;
    if tool.is_empty() {
        return Err(GaiaError::Malformed(
            "request \"tool\" must not be empty".to_string(),
        ));
    }
    let reason = required_string(obj, "reason")?;
    let task_id = required_string(obj, "task_id")?;

    let journal_seq = obj
        .get("journal_seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            GaiaError::Malformed(
                "request \"journal_seq\" is required and must be a non-negative integer"
                    .to_string(),
            )
        })?;

    let args = obj.get("args").cloned().unwrap_or(Value::Null);

    // Pre-registry submissions name a tool whose class SI's registry
    // owns; the envelope may carry the class explicitly, and while it
    // is absent the fork treats the submission as the `execute` class,
    // which the default store carries. Naming the default here keeps
    // the store's class check — the 409 for a class the fork does not
    // carry — a check on a stated value, not on an absence.
    let class = match obj.get("class") {
        None => "execute".to_string(),
        Some(v) => v
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| {
                GaiaError::Malformed(
                    "request \"class\" must be a string".to_string(),
                )
            })?,
    };

    let verdict = match obj.get("verdict") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let s = v.as_str().ok_or_else(|| {
                GaiaError::Malformed(
                    "request \"verdict\" must be a string".to_string(),
                )
            })?;
            Some(Verdict::parse(s).ok_or_else(|| {
                GaiaError::Malformed(format!(
                    "request \"verdict\" is not a known verdict: {s:?} \
                     (expected \"admit\" or \"reject\")"
                ))
            })?)
        }
    };

    Ok(Intent {
        tool,
        args,
        reason,
        task_id,
        journal_seq,
        class,
        verdict,
    })
}

/// Parse a verdict body: `{"verdict": "admit"}` or `"reject"`. There
/// is no third value to be tolerant of — a verdict the gate never
/// issued must not be recorded as one it did.
fn parse_verdict(body: &str) -> Result<Verdict, GaiaError> {
    let v: Value = serde_json::from_str(body).map_err(|e| {
        GaiaError::Malformed(format!("request body is not valid JSON: {e}"))
    })?;
    let obj = v.as_object().ok_or_else(|| {
        GaiaError::Malformed("request body must be a JSON object".to_string())
    })?;
    let s = required_string(obj, "verdict")?;
    Verdict::parse(&s).ok_or_else(|| {
        GaiaError::Malformed(format!(
            "request \"verdict\" is not a known verdict: {s:?} \
             (expected \"admit\" or \"reject\")"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work::{Intent, WorkRecord};

    /// A store that carries nothing, for the routes that must not
    /// touch work at all (health, unknown) and for classify-level
    /// tests driven through `decide`.
    struct StubStore;

    impl WorkStore for StubStore {
        fn submit(&mut self, _intent: Intent) -> Result<WorkRecord, GaiaError> {
            Err(GaiaError::Malformed("stub store submits nothing".to_string()))
        }

        fn get(&self, _id: &str) -> Option<WorkRecord> {
            None
        }

        fn record_verdict(
            &mut self,
            _id: &str,
            _verdict: Verdict,
        ) -> Result<WorkRecord, GaiaError> {
            Err(GaiaError::Malformed("stub store records nothing".to_string()))
        }

        fn claim_for_actuation(&mut self, _id: &str) -> Result<WorkRecord, GaiaError> {
            Err(GaiaError::Malformed("stub store claims nothing".to_string()))
        }

        fn complete(
            &mut self,
            _id: &str,
            _result: Value,
        ) -> Result<WorkRecord, GaiaError> {
            Err(GaiaError::Malformed("stub store completes nothing".to_string()))
        }
    }

    fn req(method: &str, path: &str) -> hearth_api::Request {
        hearth_api::Request {
            method: method.to_string(),
            path: path.to_string(),
            ..Default::default()
        }
    }

    fn facts(r: &hearth_api::Response) -> Value {
        serde_json::from_str::<Value>(&r.body).unwrap()["error"].clone()
    }

    // -- classify ------------------------------------------------------------

    #[test]
    fn the_four_routes_are_recognised() {
        assert_eq!(classify("POST", "/gaia/v1/work"), Route::SubmitWork);
        assert_eq!(
            classify("GET", "/gaia/v1/work/work-1"),
            Route::WorkStatus("work-1".to_string())
        );
        assert_eq!(
            classify("POST", "/gaia/v1/work/work-1/verdict"),
            Route::WorkVerdict("work-1".to_string())
        );
        assert_eq!(classify("GET", "/gaia/v1/health"), Route::Health);
    }

    #[test]
    fn the_wrong_method_on_a_known_path_is_unknown() {
        // Method-aware means method-wrong is route-wrong: a GET on the
        // submission path must not quietly become a status read.
        assert_eq!(classify("GET", "/gaia/v1/work"), Route::Unknown);
        assert_eq!(classify("POST", "/gaia/v1/health"), Route::Unknown);
        assert_eq!(classify("POST", "/gaia/v1/work/work-1"), Route::Unknown);
        assert_eq!(
            classify("GET", "/gaia/v1/work/work-1/verdict"),
            Route::Unknown
        );
        assert_eq!(classify("DELETE", "/gaia/v1/work/work-1"), Route::Unknown);
    }

    #[test]
    fn a_trailing_slash_or_query_is_the_same_route() {
        assert_eq!(classify("POST", "/gaia/v1/work/"), Route::SubmitWork);
        assert_eq!(classify("POST", "/gaia/v1/work?x=1"), Route::SubmitWork);
        assert_eq!(classify("GET", "/gaia/v1/health/"), Route::Health);
        assert_eq!(
            classify("GET", "/gaia/v1/work/work-1/?verbose=true"),
            Route::WorkStatus("work-1".to_string())
        );
        assert_eq!(
            classify("POST", "/gaia/v1/work/work-1/verdict/"),
            Route::WorkVerdict("work-1".to_string())
        );
        // The method is case-insensitive on the wire.
        assert_eq!(classify("post", "/gaia/v1/work"), Route::SubmitWork);
        assert_eq!(classify("get", "/gaia/v1/health"), Route::Health);
    }

    #[test]
    fn an_id_with_a_slash_or_no_id_is_not_a_route() {
        assert_eq!(classify("GET", "/gaia/v1/work/a/b"), Route::Unknown);
        assert_eq!(classify("POST", "/gaia/v1/work/a/b/verdict"), Route::Unknown);
        assert_eq!(classify("GET", "/gaia/v1/work/"), Route::Unknown);
        assert_eq!(classify("GET", "/gaia/v1/nope"), Route::Unknown);
        assert_eq!(classify("GET", "/nope"), Route::Unknown);
    }

    // -- decide: the routes that carry no work -------------------------------

    #[test]
    fn health_is_about_the_agency_surface() {
        let mut store = StubStore;
        let r = decide(&req("GET", "/gaia/v1/health"), &mut store);
        assert_eq!(r.status, 200);
        let v: Value = serde_json::from_str(&r.body).unwrap();
        assert_eq!(v["status"], json!("ok"));
        assert_eq!(v["service"], json!("gaia"));
    }

    #[test]
    fn an_unknown_route_is_a_404_in_the_gaia_vocabulary() {
        let mut store = StubStore;
        let r = decide(&req("GET", "/gaia/v1/nope"), &mut store);
        assert_eq!(r.status, 404);
        // OpenAI-shaped, so a client surfaces the message — but the
        // type is gaia's, never a residency type.
        assert_eq!(facts(&r)["type"], json!("gaia_route_not_found"));
        assert!(
            facts(&r)["message"]
                .as_str()
                .unwrap()
                .contains("/gaia/v1/nope"),
            "{}",
            r.body
        );
    }
}
