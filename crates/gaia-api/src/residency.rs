//! Structured residency — `GET /gaia/v1/residency` (G-2 §3.2).
//!
//! Upstream's `/residency` renders the state as the human
//! `explain()` string; only the derived `operator_fault` /
//! `try_elsewhere` booleans are structured (§1.2). A policy
//! consumer — SI's gate, deciding what a loss means — should
//! not have to parse prose to learn which of four named things
//! happened. This module reads the same facts from the spine
//! and returns them as data:
//!
//! ```json
//! { "models": [
//!   { "model": "muse", "state": "lost",
//!     "lost_reason": "process_exited",
//!     "operator_fault": true, "try_elsewhere": true }
//! ]}
//! ```
//!
//! The mapping from a model's latest spine event to the view
//! follows `hearth_core::Route` semantics exactly, because the
//! booleans must mean the same thing in both vocabularies:
//!
//!   * `declared` → `unknown` (declared, never observed);
//!     `operator_fault: false`, `try_elsewhere: true`
//!     (`Route::Unknown` tries elsewhere, faults nobody).
//!   * `loading` → `loading`; `try_elsewhere: false`
//!     (`Route::Warming` — waiting here is the point).
//!   * `resident` → `resident`; `try_elsewhere: false`,
//!     `operator_fault: false`.
//!   * `lost` → `lost` + `lost_reason` echoed from the recorded
//!     fact; `operator_fault` is the reason's
//!     `is_operator_fault()` (false only for `gpu_detached`);
//!     `try_elsewhere: true`.
//!   * `unloaded` → `stopped`; `operator_fault: false`,
//!     `try_elsewhere: true` (upstream maps `Stopped` to
//!     `Route::Unknown`).
//!
//! One honest limit: `failed` never appears here. The spine
//! coarsens a fleet `Failed` to `lost{process_exited}` at
//! record time (§1.2), so a spine read cannot distinguish it —
//! claiming a `failed` state from this source would be
//! inventing resolution the record does not have.
//!
//! Serving split: this route needs the spine, which the
//! work-store-only [`crate::gateway::decide`] deliberately does
//! not hold. The composition routes the request here instead —
//! the same split as the events listener (G-4), which also
//! lives outside `decide`. [`handle`] returns `Some` only for
//! the residency route itself.

use hearth_store::Spine;
use serde_json::{json, Value};

/// The body of `GET /gaia/v1/residency`: one view per model
/// the spine has ever seen, sorted by model name (the spine
/// has no roster order; a policy consumer diffing two answers
/// deserves a stable one).
pub fn residency_body(spine: &Spine) -> Value {
    let mut views: Vec<Value> = spine.all_latest().iter().map(view_for).collect();
    // The spine has no roster order, so sort here: a policy
    // consumer diffing two answers deserves a stable one.
    views.sort_by(|a, b| {
        a.get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .cmp(b.get("model").and_then(Value::as_str).unwrap_or(""))
    });
    json!({ "models": views })
}

/// Map one model's latest spine event to its structured view,
/// following the table in the module docs. `lost_reason` is
/// built only in the `lost` arm, so it appears on lost views
/// and nowhere else — a consumer must never have to guess
/// whether an absent reason means "not lost" or "not recorded".
fn view_for(ev: &hearth_store::Event) -> Value {
    let model = ev.model.as_str();
    match ev.kind.as_str() {
        "declared" => json!({
            "model": model,
            "state": "unknown",
            "operator_fault": false,
            "try_elsewhere": true,
        }),
        "loading" => json!({
            "model": model,
            "state": "loading",
            "operator_fault": false,
            "try_elsewhere": false,
        }),
        "resident" => json!({
            "model": model,
            "state": "resident",
            "operator_fault": false,
            "try_elsewhere": false,
        }),
        "lost" => {
            // Parse the reason exactly as the store's own
            // residency reconstruction does: only the three
            // named strings below are themselves, and anything
            // else — "evicted", an unrecognised string, or a
            // missing/non-string fact — reads as evicted.
            // `operator_fault` is that reason's
            // `is_operator_fault()`: false only for a detached
            // GPU, because the host taking the card is not the
            // operator's doing.
            let reason_str = ev.facts.get("reason").and_then(Value::as_str);
            let (lost_reason, operator_fault) = match reason_str {
                Some("gpu_detached") => ("gpu_detached", false),
                Some("process_exited") => ("process_exited", true),
                Some("unhealthy") => ("unhealthy", true),
                _ => ("evicted", true),
            };
            json!({
                "model": model,
                "state": "lost",
                "lost_reason": lost_reason,
                "operator_fault": operator_fault,
                "try_elsewhere": true,
            })
        }
        "unloaded" => json!({
            "model": model,
            "state": "stopped",
            "operator_fault": false,
            "try_elsewhere": true,
        }),
        _ => json!({
            // A kind this mapping does not know should not occur
            // in the residency collection — pulls live in their
            // own collection — but if one ever did, "unknown"
            // is the honest answer: claiming a specific state
            // for a record we cannot read would be inventing a
            // diagnosis, and a wrong one misdirects the gate.
            "model": model,
            "state": "unknown",
            "operator_fault": false,
            "try_elsewhere": true,
        }),
    }
}

/// Answer a request if — and only if — it is
/// `GET /gaia/v1/residency`, with the same path normalization
/// `gateway::classify` applies (query string stripped,
/// trailing slash ignored, method case-insensitive). Any other
/// request returns `None` so the composition can fall through
/// to the work routes.
pub fn handle(req: &hearth_api::Request, spine: &Spine) -> Option<hearth_api::Response> {
    // The same normalisation `gateway::classify` applies:
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
    if method == "GET" && p == "/gaia/v1/residency" {
        Some(hearth_api::Response::json(200, residency_body(spine)))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hearth_core::LostReason;
    use hearth_store::Transition;

    fn req(method: &str, path: &str) -> hearth_api::Request {
        hearth_api::Request {
            method: method.to_string(),
            path: path.to_string(),
            ..Default::default()
        }
    }

    /// The single view for `model` in a body, cloned out so a
    /// test can assert on it without borrow gymnastics.
    fn view(body: &Value, model: &str) -> Value {
        body["models"]
            .as_array()
            .expect("models is an array")
            .iter()
            .find(|v| v["model"] == json!(model))
            .unwrap_or_else(|| panic!("no view for {model} in {body}"))
            .clone()
    }

    fn spine_with_resident(model: &str) -> Spine {
        let spine = Spine::in_memory();
        spine
            .record(
                model,
                &Transition::Resident {
                    endpoint: "127.0.0.1:8080".into(),
                    warmup_ms: 43,
                },
                &[],
            )
            .unwrap();
        spine
    }

    #[test]
    fn declared_only_is_unknown_and_tries_elsewhere_without_a_lost_reason() {
        let spine = Spine::in_memory();
        spine
            .record(
                "muse",
                &Transition::Declared {
                    vram_bytes: 4_000_000_000,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let body = residency_body(&spine);
        let v = view(&body, "muse");
        assert_eq!(v["state"], json!("unknown"));
        assert_eq!(v["try_elsewhere"], json!(true));
        assert_eq!(v["operator_fault"], json!(false));
        assert!(
            v.as_object().unwrap().get("lost_reason").is_none(),
            "lost_reason appears only on lost views: {v}"
        );
    }

    #[test]
    fn loading_does_not_try_elsewhere() {
        // Warming means waiting here is the point — sending a
        // policy consumer elsewhere would abandon a load that
        // is already underway.
        let spine = Spine::in_memory();
        spine
            .record(
                "muse",
                &Transition::Loading {
                    pid: 4242,
                    endpoint: "127.0.0.1:8080".into(),
                },
                &[],
            )
            .unwrap();
        let v = view(&residency_body(&spine), "muse");
        assert_eq!(v["state"], json!("loading"));
        assert_eq!(v["try_elsewhere"], json!(false));
        assert_eq!(v["operator_fault"], json!(false));
        assert!(v.as_object().unwrap().get("lost_reason").is_none());
    }

    #[test]
    fn resident_is_resident_and_faults_nobody() {
        let spine = spine_with_resident("muse");
        let v = view(&residency_body(&spine), "muse");
        assert_eq!(v["state"], json!("resident"));
        assert_eq!(v["operator_fault"], json!(false));
        assert_eq!(v["try_elsewhere"], json!(false));
        assert!(v.as_object().unwrap().get("lost_reason").is_none());
    }

    #[test]
    fn lost_reasons_echo_and_only_gpu_detached_is_not_the_operators_fault() {
        let cases = [
            (LostReason::Evicted, "evicted", true),
            (LostReason::GpuDetached, "gpu_detached", false),
            (LostReason::ProcessExited, "process_exited", true),
            (LostReason::Unhealthy, "unhealthy", true),
        ];
        for (reason, expected_str, expected_fault) in cases {
            let spine = Spine::in_memory();
            spine
                .record("muse", &Transition::Lost { reason }, &[])
                .unwrap();
            let v = view(&residency_body(&spine), "muse");
            assert_eq!(v["state"], json!("lost"), "{reason:?}");
            assert_eq!(
                v["lost_reason"],
                json!(expected_str),
                "the recorded reason echoes verbatim for {reason:?}"
            );
            assert_eq!(
                v["operator_fault"],
                json!(expected_fault),
                "operator_fault for {reason:?}"
            );
            assert_eq!(v["try_elsewhere"], json!(true), "{reason:?}");
        }
    }

    #[test]
    fn unloaded_is_stopped() {
        let spine = Spine::in_memory();
        spine.record("muse", &Transition::Unloaded, &[]).unwrap();
        let v = view(&residency_body(&spine), "muse");
        assert_eq!(v["state"], json!("stopped"));
        assert_eq!(v["operator_fault"], json!(false));
        assert_eq!(v["try_elsewhere"], json!(true));
        assert!(v.as_object().unwrap().get("lost_reason").is_none());
    }

    #[test]
    fn views_are_sorted_by_model() {
        let spine = Spine::in_memory();
        // Recorded out of alphabetical order on purpose: the
        // answer must not inherit the recording order.
        spine
            .record(
                "zebra",
                &Transition::Declared {
                    vram_bytes: 1,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        spine
            .record(
                "alpha",
                &Transition::Declared {
                    vram_bytes: 1,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let body = residency_body(&spine);
        let names: Vec<&str> = body["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["model"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["alpha", "zebra"]);
    }

    #[test]
    fn handle_answers_get_on_the_residency_route() {
        let spine = spine_with_resident("muse");
        let resp = handle(&req("GET", "/gaia/v1/residency"), &spine)
            .expect("the residency route is answered here");
        assert_eq!(resp.status, 200);
        assert!(
            resp.body.contains("muse"),
            "the body carries the model: {}",
            resp.body
        );
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(view(&body, "muse")["state"], json!("resident"));
    }

    #[test]
    fn handle_accepts_a_query_string_and_a_trailing_slash() {
        let spine = spine_with_resident("muse");
        assert!(
            handle(&req("GET", "/gaia/v1/residency?since=3"), &spine).is_some(),
            "a query string does not make a different route"
        );
        assert!(
            handle(&req("GET", "/gaia/v1/residency/"), &spine).is_some(),
            "a trailing slash does not make a different route"
        );
    }

    #[test]
    fn handle_declines_other_methods_and_other_paths() {
        let spine = spine_with_resident("muse");
        assert!(handle(&req("POST", "/gaia/v1/residency"), &spine).is_none());
        assert!(handle(&req("GET", "/gaia/v1/work"), &spine).is_none());
    }
}
