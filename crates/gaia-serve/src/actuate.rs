//! Gated actuation: the executor for gate-admitted intents
//! (G-2 §5.3, §3.3 step 4).
//!
//! This module is the one place in the fork where a decision
//! becomes an action on the runtime, and it is deliberately
//! narrow about how that can happen:
//!
//!   * **It runs only what it is handed.** The composition
//!     ([`crate::Composition::actuate_work`]) claims a work
//!     record from the work store first — only an ADMIT-ted
//!     record can be claimed — and hands the claimed record
//!     here. This executor never sees an un-verdicted intent,
//!     and it does not re-check the verdict: the claim *is*
//!     the gate check, and duplicating it here would invite
//!     the two checks to drift apart.
//!   * **Verbatim-args discipline (§5.3).** The actuator
//!     executes exactly the admitted intent's `args`, never
//!     re-derives them. The `model`, the `spine_event` the
//!     intent answers, and a repull's `reference` are read
//!     from the journaled args as they stand. Nothing is
//!     inferred from the current fleet, nothing is
//!     substituted, and no substitute action is improvised:
//!     proposals are answered, not reinterpreted.
//!   * **Leash first, stale-state second, primitive last
//!     (§5.2.1, §5.3).** For every tool that touches the
//!     runtime, the operator leash is checked before anything
//!     else, then the fleet's current state is checked
//!     against the event the intent answers, and only then
//!     does a §5.3 primitive run. A refusal at either check
//!     is an *answer* to the proposal — recorded, returned,
//!     and terminal for that work record. Re-asking means a
//!     new intent from SI, never a silent retry of a stale
//!     one.
//!   * **Every terminal path is recorded.** Executed,
//!     refused, and failed actuations each land in the
//!     [`OutcomeStore`] before the result is returned, so the
//!     result SI reads back (§3.3 step 4) and the record the
//!     fork keeps are the same fact stated once, not two
//!     facts that can disagree.
//!
//! What this module is *not*: it is not a policy. It holds no
//! opinion about whether an intent should have been admitted
//! — that judgement stays in SI, where G-2 put it. Its
//! refusals are mechanical (a leash stop is in force, the
//! state moved on, no stored spec exists), never judgement
//! calls dressed up as checks.

use gaia_api::{Actuator, GaiaError, WorkRecord};
use hearth_store::EventRef;
use serde_json::{json, Value};

use crate::leash::Leash;
use crate::outcomes::{Outcome, OutcomeRecord, OutcomeStore};
use crate::watcher::{TOOL_NOTIFY, TOOL_RESTART};

/// Candidate tool names not already named by the watcher.
/// `hearth.restart` and `hearth.notify` are proposed by the
/// watcher, so their spellings live in [`crate::watcher`];
/// stop and repull are only ever proposed by SI or an
/// operator (§5.2), so their spellings live here, with the
/// executor that carries them out.
pub const TOOL_STOP: &str = "hearth.stop";
pub const TOOL_REPULL: &str = "hearth.repull";

/// The fleet actuator: §5.3's primitives, wired to one
/// composition's supervisor, stored specs, leash, outcome
/// store, and pull configuration.
///
/// Every field is a borrow of the composition's own state —
/// the actuator owns nothing and outlives nothing. It exists
/// for exactly one `actuate` call per construction, which is
/// how [`crate::Composition::actuate_work`] uses it.
pub struct FleetActuator<'a> {
    pub supervisor: &'a mut hearth_serve::Supervisor,
    pub specs: &'a std::collections::HashMap<String, hearth_serve::server::ServerSpec>,
    pub leash: &'a Leash,
    pub outcomes: &'a mut OutcomeStore,
    pub pull_cfg: &'a hearth_pull::PullConfig,
}

/// A malformed admitted intent. This error means the journaled
/// args could not even be read, so — per the G-3 work-store
/// contract — it propagates and the record stays `Admitted`:
/// a defect in the journaled intent, to surface loudly, not
/// an outcome to file.
fn malformed(message: String) -> GaiaError {
    GaiaError::Malformed(message)
}

/// Read the required `model` arg: a non-empty string, for all
/// four tools. Anything else is unreadable, not a default —
/// actuating against a guessed model name would be the worst
/// possible reading of a missing one.
fn parse_model(args: &Value) -> Result<String, GaiaError> {
    args.get("model")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            malformed(format!(
                "admitted intent is missing a readable \"model\" string in its args: {args}"
            ))
        })
}

/// Read the required `spine_event` arg — the `{hash, seq}` ref
/// of the spine event the intent answers. Required for
/// restart, stop, and notify: those tools *answer an event*,
/// and an intent that cannot name the event it answers has
/// nothing to be checked for staleness against.
fn parse_spine_event(args: &Value) -> Result<EventRef, GaiaError> {
    parse_spine_event_opt(args)?.ok_or_else(|| {
        malformed(format!(
            "admitted intent is missing a readable \"spine_event\" ({{hash, seq}}) in its args: {args}"
        ))
    })
}

/// Read `spine_event` when present; `None` when absent. A
/// present-but-unreadable ref is still malformed — silently
/// dropping a ref the intent *did* carry would break the
/// provenance trail without anyone noticing. Repull uses this
/// form: it answers no residency event, so the ref is
/// optional provenance there, not a requirement.
fn parse_spine_event_opt(args: &Value) -> Result<Option<EventRef>, GaiaError> {
    let Some(raw) = args.get("spine_event") else {
        return Ok(None);
    };
    let hash = raw
        .get("hash")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let seq = raw.get("seq").and_then(Value::as_u64);
    match (hash, seq) {
        (Some(hash), Some(seq)) => Ok(Some(EventRef {
            hash: hash.to_string(),
            seq,
        })),
        _ => Err(malformed(format!(
            "admitted intent carries an unreadable \"spine_event\" (expected {{hash, seq}}): {raw}"
        ))),
    }
}

/// Read the required `reference` arg for a repull: the model
/// reference string `hearth_pull::pull` will resolve, exactly
/// as journaled.
fn parse_reference(args: &Value) -> Result<String, GaiaError> {
    args.get("reference")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            malformed(format!(
                "admitted repull intent is missing a readable \"reference\" string in its args: {args}"
            ))
        })
}

/// The JSON spelling of one spine ref, as the result and the
/// work-status provenance trail carry it.
fn ref_json(r: &EventRef) -> Value {
    json!({ "hash": r.hash, "seq": r.seq })
}

/// The ref of an event read back out of the spine.
fn event_ref(ev: &hearth_store::Event) -> EventRef {
    EventRef {
        hash: ev.hash.clone(),
        seq: ev.seq,
    }
}

impl FleetActuator<'_> {
    /// Record one terminal outcome and return its result.
    ///
    /// This is the single exit every executed / refused /
    /// failed path funnels through, so the result JSON and
    /// the [`OutcomeRecord`] are built from the same values,
    /// once: `outcome`, `tool`, `model`, `detail`, the
    /// `refusal` kind string (present only when refused),
    /// `caused_by`, and `spine_event_refs`.
    ///
    /// Ordering, owned honestly: the outcome is recorded
    /// *before* returning `Ok`, and if that record fails this
    /// returns `Err` — but by then the actuation itself may
    /// already have happened (a child may be running that no
    /// outcome record describes). There is no ordering that
    /// avoids that window without making the record precede
    /// an actuation that might then not happen, which is the
    /// worse lie. So the failure is made loud instead: an
    /// unrecorded outcome surfaces as an error to the caller,
    /// never as silence, and the work record stays
    /// `Admitted` where an operator can see the discrepancy.
    fn finish(
        &mut self,
        work: &WorkRecord,
        model: &str,
        outcome: Outcome,
        refusal: Option<&str>,
        detail: String,
        caused_by: Option<EventRef>,
        spine_event_refs: Vec<EventRef>,
    ) -> Result<Value, GaiaError> {
        let outcome_str = match outcome {
            Outcome::Executed => "executed",
            Outcome::Refused => "refused",
            Outcome::Failed => "failed",
        };
        let mut result = json!({
            "outcome": outcome_str,
            "tool": work.intent.tool,
            "model": model,
            "detail": detail,
            "caused_by": caused_by.as_ref().map(ref_json).unwrap_or(Value::Null),
            "spine_event_refs": spine_event_refs.iter().map(ref_json).collect::<Vec<_>>(),
        });
        if let Some(kind) = refusal {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("refusal".to_string(), json!(kind));
            }
        }
        let record = OutcomeRecord {
            work_id: work.id.clone(),
            tool: work.intent.tool.clone(),
            model: model.to_string(),
            outcome,
            refusal: refusal.map(str::to_string),
            detail,
            caused_by,
            spine_event_refs,
            result: result.clone(),
        };
        self.outcomes
            .record(record)
            .map_err(|e| GaiaError::Malformed(format!("cannot record actuation outcome: {e}")))?;
        Ok(result)
    }

    /// A refusal: a recorded, returned answer to the proposal,
    /// with no spine event refs — a refusal writes nothing to
    /// the spine and actuates nothing, and its ref list says
    /// exactly that.
    fn refuse(
        &mut self,
        work: &WorkRecord,
        model: &str,
        kind: &str,
        detail: String,
        caused_by: Option<EventRef>,
    ) -> Result<Value, GaiaError> {
        self.finish(
            work,
            model,
            Outcome::Refused,
            Some(kind),
            detail,
            caused_by,
            Vec::new(),
        )
    }

    /// The leash check (§5.2.1), first for every tool that
    /// touches the runtime: if SI's journal holds an un-resumed
    /// stop for `model`, the actuation refuses — a stopped
    /// mind's runtime does not restart its models behind its
    /// back. Returns the refusal result when the leash applies,
    /// `None` when actuation may proceed.
    fn leash_refusal(
        &mut self,
        work: &WorkRecord,
        model: &str,
        caused_by: Option<EventRef>,
    ) -> Option<Result<Value, GaiaError>> {
        // The stop's facts are copied out before `finish`
        // needs `&mut self`: the leash borrow cannot be held
        // across recording the refusal it caused.
        let stop = self
            .leash
            .stop_in_force(model)
            .map(|rec| (rec.journal_seq, rec.reason.clone()))?;
        let (journal_seq, reason) = stop;
        let detail = format!(
            "refused by the operator leash: a stop for {model} is in force from SI journal seq {journal_seq} ({reason}); no actuation while the leash holds"
        );
        Some(self.refuse(work, model, "leash", detail, caused_by))
    }

    /// The model's latest spine event, owned, for the
    /// stale-state check and its detail string.
    fn latest(&self, model: &str) -> Option<hearth_store::Event> {
        self.supervisor.spine().latest(model)
    }

    /// Describe the current latest event for a stale-state
    /// refusal detail: kind and seq when there is one, the
    /// honest "no spine event" when there is not.
    fn describe_latest(latest: &Option<hearth_store::Event>) -> String {
        match latest {
            Some(ev) => format!("{} at seq {}", ev.kind, ev.seq),
            None => "no spine event at all".to_string(),
        }
    }

    /// `hearth.notify`: no process action. The notification
    /// *is* the outcome record — it lands gaia-side where SI
    /// reads it back — and nothing is actuated on the runtime,
    /// which is why the leash does not apply here (the leash
    /// governs actuation, and notify actuates nothing) and why
    /// `spine_event_refs` is empty: no spine event was written.
    fn actuate_notify(&mut self, work: &WorkRecord, model: &str) -> Result<Value, GaiaError> {
        let spine_event = parse_spine_event(&work.intent.args)?;
        let detail = format!(
            "notification for {model} answering spine event seq {} was recorded and nothing was actuated: hearth.notify takes no process action",
            spine_event.seq
        );
        self.finish(
            work,
            model,
            Outcome::Executed,
            None,
            detail,
            Some(spine_event),
            Vec::new(),
        )
    }

    /// `hearth.restart`: re-run the stored spec, but only as
    /// an answer to the loss the intent names.
    fn actuate_restart(&mut self, work: &WorkRecord, model: &str) -> Result<Value, GaiaError> {
        let spine_event = parse_spine_event(&work.intent.args)?;

        // 1. Leash (§5.2.1).
        if let Some(refusal) = self.leash_refusal(work, model, Some(spine_event.clone())) {
            return refusal;
        }

        // 2. Stale state (§5.3): restart actuates only if the
        // model's latest spine event *is* the lost event the
        // intent answers — same kind, same hash, same seq. A
        // model that recovered, was restarted by an earlier
        // answer to this same loss, or moved on in any other
        // way has answered the question already; restarting it
        // again would be acting on a state that no longer
        // exists.
        let latest = self.latest(model);
        let answers_this_loss = matches!(
            &latest,
            Some(ev) if ev.kind == "lost" && ev.hash == spine_event.hash && ev.seq == spine_event.seq
        );
        if !answers_this_loss {
            let detail = format!(
                "stale state: {model}'s latest spine event is {}, not the lost event at seq {} the intent answers; refusing to restart against state that has moved on",
                Self::describe_latest(&latest),
                spine_event.seq
            );
            return self.refuse(work, model, "stale_state", detail, Some(spine_event));
        }

        // 3. Primitive: `Supervisor::start` over the stored
        // spec — the same call upstream's unsupervised path
        // makes internally, here behind the gate. A model with
        // no stored spec cannot be restarted: the composition
        // only re-runs specs it actually started the model
        // with, and improvising one (a guessed path, a guessed
        // port) would be inventing an action the gate never
        // saw.
        let Some(spec) = self.specs.get(model).cloned() else {
            let detail = format!(
                "no stored spec for {model}: the composition holds no ServerSpec it started this model with, and the actuator will not improvise one"
            );
            return self.refuse(work, model, "no_stored_spec", detail, Some(spine_event));
        };
        match self.supervisor.start(spec) {
            Ok(()) => {
                // `start` records `loading` before returning,
                // so the latest event after it is the restart's
                // own spine evidence, causally linked by the
                // supervisor to the loss it answers.
                let refs = self
                    .latest(model)
                    .map(|ev| vec![event_ref(&ev)])
                    .unwrap_or_default();
                let detail = format!(
                    "restarted {model} from its stored spec, answering the lost event at seq {}; the model is loading again",
                    spine_event.seq
                );
                self.finish(
                    work,
                    model,
                    Outcome::Executed,
                    None,
                    detail,
                    Some(spine_event),
                    refs,
                )
            }
            Err(e) => self.finish(
                work,
                model,
                Outcome::Failed,
                None,
                e,
                Some(spine_event),
                Vec::new(),
            ),
        }
    }

    /// `hearth.stop`: a deliberate stop of a model that is
    /// actually up (or coming up).
    fn actuate_stop(&mut self, work: &WorkRecord, model: &str) -> Result<Value, GaiaError> {
        let spine_event = parse_spine_event(&work.intent.args)?;

        // 1. Leash (§5.2.1): a stop already in force is also
        // the answer to a stop intent — the leash check is
        // first for every runtime-touching tool, with no
        // carve-out for "but this one agrees with the leash";
        // agreement is still the leash speaking, and the
        // refusal records that it did.
        if let Some(refusal) = self.leash_refusal(work, model, Some(spine_event.clone())) {
            return refusal;
        }

        // 2. Stale state (§5.3): stop actuates only while the
        // model is `resident` or `loading` — states with a
        // child to stop. Stopping a model that is already
        // lost, unloaded, or never recorded would report a
        // stop that stopped nothing.
        let latest = self.latest(model);
        let stoppable = matches!(
            &latest,
            Some(ev) if ev.kind == "resident" || ev.kind == "loading"
        );
        if !stoppable {
            let detail = format!(
                "stale state: {model}'s latest spine event is {}, not resident or loading; refusing to stop a model that is not up",
                Self::describe_latest(&latest)
            );
            return self.refuse(work, model, "stale_state", detail, Some(spine_event));
        }

        // 3. Primitive: `Supervisor::stop`.
        match self.supervisor.stop(model) {
            Ok(()) => {
                let refs = self
                    .latest(model)
                    .map(|ev| vec![event_ref(&ev)])
                    .unwrap_or_default();
                let detail = format!(
                    "stopped {model}; its latest spine event is the unloaded event recorded by the stop"
                );
                self.finish(
                    work,
                    model,
                    Outcome::Executed,
                    None,
                    detail,
                    Some(spine_event),
                    refs,
                )
            }
            Err(e) => self.finish(
                work,
                model,
                Outcome::Failed,
                None,
                e,
                Some(spine_event),
                Vec::new(),
            ),
        }
    }

    /// `hearth.repull`: fetch the bytes again, by the exact
    /// reference the intent journaled.
    fn actuate_repull(&mut self, work: &WorkRecord, model: &str) -> Result<Value, GaiaError> {
        let reference = parse_reference(&work.intent.args)?;
        let caused_by = parse_spine_event_opt(&work.intent.args)?;

        // 1. Leash (§5.2.1).
        if let Some(refusal) = self.leash_refusal(work, model, caused_by.clone()) {
            return refusal;
        }

        // 2. No stale-state check: repull has no residency
        // staleness. Bytes are bytes — having them re-fetched
        // answers a question about the *bytes* (stale-weights
        // suspicion, a failed pull), not about a residency
        // event that may have been superseded, so there is no
        // latest-event identity to compare against.
        //
        // 3. Primitive: `hearth_pull::pull`, as upstream's
        // `cmd_pull` calls it.
        match hearth_pull::pull(&reference, self.pull_cfg, self.supervisor.spine()) {
            Ok(pulled) => {
                // Note: a Local reference records nothing in
                // the spine, by `pull`'s own honest design —
                // claiming a pull for bytes that were already
                // here would be a lie — so `latest_pull` is
                // then absent and the refs are empty, which is
                // the truthful list for that case.
                let refs = self
                    .supervisor
                    .spine()
                    .latest_pull(&pulled.model)
                    .map(|ev| vec![event_ref(&ev)])
                    .unwrap_or_default();
                let detail = format!(
                    "re-pulled {reference}: {} at {} ({} bytes, already_had_it: {})",
                    pulled.model,
                    pulled.model_path.display(),
                    pulled.bytes,
                    pulled.already_had_it
                );
                self.finish(
                    work,
                    model,
                    Outcome::Executed,
                    None,
                    detail,
                    caused_by,
                    refs,
                )
            }
            Err(e) => self.finish(work, model, Outcome::Failed, None, e, caused_by, Vec::new()),
        }
    }
}

impl Actuator for FleetActuator<'_> {
    /// Execute one claimed, ADMIT-ted work record.
    ///
    /// The tool name decides which §5.3 primitive applies —
    /// and an unknown tool is malformed, not ignored: the gate
    /// admitted an intent this executor cannot name, and
    /// filing that as an outcome would pretend the work was
    /// answered when it was not even read.
    fn actuate(&mut self, work: &WorkRecord) -> Result<Value, GaiaError> {
        let tool = work.intent.tool.as_str();
        if tool != TOOL_RESTART && tool != TOOL_STOP && tool != TOOL_REPULL && tool != TOOL_NOTIFY {
            return Err(malformed(format!(
                "unknown tool {tool:?}: this actuator carries {TOOL_RESTART}, {TOOL_STOP}, {TOOL_REPULL}, and {TOOL_NOTIFY} only"
            )));
        }
        let model = parse_model(&work.intent.args)?;
        if tool == TOOL_RESTART {
            self.actuate_restart(work, &model)
        } else if tool == TOOL_STOP {
            self.actuate_stop(work, &model)
        } else if tool == TOOL_REPULL {
            self.actuate_repull(work, &model)
        } else {
            self.actuate_notify(work, &model)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use gaia_api::{Intent, Verdict, WorkState};
    use hearth_core::{Budget, Declared, GIB};
    use hearth_pull::PullConfig;
    use hearth_serve::server::{free_port, ServerSpec};
    use hearth_serve::Supervisor;
    use hearth_store::Spine;

    use crate::watcher::TOOL_RESTART as RESTART;

    fn budget() -> Budget {
        Budget {
            total_bytes: 24 * GIB,
            reserve_bytes: 2 * GIB,
        }
    }

    fn declared(model: &str) -> Declared {
        Declared {
            model: model.to_string(),
            weights_bytes: GIB,
            kv_bytes: 0,
        }
    }

    /// An executable stand-in runtime script, the G-5 pattern:
    /// it ignores the llama-server argv the supervisor passes
    /// and simply does what its body says — exit, or sleep.
    fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        path
    }

    fn spec_for(dir: &Path, model: &str, binary: &Path) -> ServerSpec {
        let gguf = dir.join("fake.gguf");
        if !gguf.exists() {
            std::fs::write(&gguf, b"not a real gguf").unwrap();
        }
        let mut spec = ServerSpec::new(model, &gguf, free_port().unwrap());
        spec.binary = binary.to_path_buf();
        spec.log_dir = dir.to_path_buf();
        spec
    }

    /// A supervisor whose only child died on startup, ticked
    /// until the spine records the loss. Returns the
    /// supervisor, the spec it was started with, and the lost
    /// event's ref — the ref a restart intent answers.
    fn lost_fleet(dir: &tempfile::TempDir) -> (Supervisor, ServerSpec, EventRef) {
        let dying = script(dir.path(), "dying.sh", "#!/bin/sh\nexit 1\n");
        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("fake")]);
        let spec = spec_for(dir.path(), "fake", &dying);
        sup.start(spec.clone()).unwrap();
        let mut lost = None;
        for _ in 0..100 {
            sup.tick();
            if let Some(ev) = sup.spine().latest("fake") {
                if ev.kind == "lost" {
                    lost = Some(EventRef {
                        hash: ev.hash.clone(),
                        seq: ev.seq,
                    });
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let lost = lost.expect("the dying child was never recorded lost");
        (sup, spec, lost)
    }

    fn work(tool: &str, args: Value) -> WorkRecord {
        WorkRecord {
            id: "work-000001".to_string(),
            intent: Intent {
                tool: tool.to_string(),
                args,
                reason: "test intent".to_string(),
                task_id: "task-1".to_string(),
                journal_seq: 42,
                class: "execute".to_string(),
                verdict: Some(Verdict::Admit),
            },
            state: WorkState::Admitted,
            result: None,
        }
    }

    fn pull_cfg(dir: &tempfile::TempDir) -> PullConfig {
        PullConfig {
            blobs_dir: dir.path().join("blobs"),
            progress: false,
            verify_existing: false,
        }
    }

    fn run(
        sup: &mut Supervisor,
        specs: &HashMap<String, ServerSpec>,
        leash: &Leash,
        outcomes: &mut OutcomeStore,
        cfg: &PullConfig,
        work: &WorkRecord,
    ) -> Result<Value, GaiaError> {
        let mut actuator = FleetActuator {
            supervisor: sup,
            specs,
            leash,
            outcomes,
            pull_cfg: cfg,
        };
        actuator.actuate(work)
    }

    fn specs_with(spec: &ServerSpec) -> HashMap<String, ServerSpec> {
        let mut specs = HashMap::new();
        specs.insert(spec.model.clone(), spec.clone());
        specs
    }

    #[test]
    fn restart_after_loss_executes_and_the_loading_event_chains_to_the_loss() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sup, spec, lost) = lost_fleet(&dir);
        let specs = specs_with(&spec);
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let w = work(
            RESTART,
            json!({ "model": "fake", "spine_event": { "hash": lost.hash, "seq": lost.seq } }),
        );

        let result = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(result["outcome"], json!("executed"));
        assert_eq!(result["tool"], json!(RESTART));
        assert_eq!(result["model"], json!("fake"));
        assert_eq!(
            result["caused_by"],
            json!({ "hash": lost.hash, "seq": lost.seq })
        );
        assert!(result.get("refusal").is_none());

        // The restart recorded a fresh loading event, and it
        // is the outcome's one spine ref.
        let latest = sup.spine().latest("fake").unwrap();
        assert_eq!(latest.kind, "loading");
        assert_eq!(
            result["spine_event_refs"],
            json!([{ "hash": latest.hash, "seq": latest.seq }])
        );

        // The causal chain reaches the loss: `why` walks
        // loading <- lost, so the restart provably answers
        // *this* loss and not some other history.
        let why = sup.spine().why("fake");
        assert!(
            why.iter()
                .any(|e| e.kind == "lost" && e.seq == lost.seq && e.hash == lost.hash),
            "causal chain does not reach the loss: {why:?}"
        );

        assert_eq!(outcomes.outcomes().len(), 1);
        assert_eq!(outcomes.outcomes()[0].outcome, Outcome::Executed);
        assert_eq!(outcomes.outcomes()[0].result, result);
    }

    #[test]
    fn restart_without_a_stored_spec_is_refused_no_stored_spec() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sup, _spec, lost) = lost_fleet(&dir);
        // Deliberately empty: the composition never noted a
        // spec for this model.
        let specs = HashMap::new();
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let w = work(
            RESTART,
            json!({ "model": "fake", "spine_event": { "hash": lost.hash, "seq": lost.seq } }),
        );

        let result = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(result["outcome"], json!("refused"));
        assert_eq!(result["refusal"], json!("no_stored_spec"));
        // Nothing was started: the latest event is still the loss.
        assert_eq!(sup.spine().latest("fake").unwrap().seq, lost.seq);
        assert_eq!(outcomes.outcomes()[0].outcome, Outcome::Refused);
        assert_eq!(
            outcomes.outcomes()[0].refusal.as_deref(),
            Some("no_stored_spec")
        );
    }

    #[test]
    fn a_second_restart_against_the_same_lost_ref_is_refused_stale_state() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sup, spec, lost) = lost_fleet(&dir);
        let specs = specs_with(&spec);
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let w = work(
            RESTART,
            json!({ "model": "fake", "spine_event": { "hash": lost.hash, "seq": lost.seq } }),
        );

        let first = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(first["outcome"], json!("executed"));

        // The same admitted args again: the loss has been
        // answered, the latest event is now `loading`, and
        // the stale check — not a second start — is the answer.
        let second = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(second["outcome"], json!("refused"));
        assert_eq!(second["refusal"], json!("stale_state"));
        assert!(
            second["detail"]
                .as_str()
                .unwrap()
                .contains(&lost.seq.to_string()),
            "detail names the answered seq: {second}"
        );
        assert_eq!(sup.spine().latest("fake").unwrap().kind, "loading");
        assert_eq!(outcomes.outcomes().len(), 2);
    }

    #[test]
    fn a_leash_stop_in_force_refuses_restart_and_leaves_the_spine_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let (mut sup, spec, lost) = lost_fleet(&dir);
        let specs = specs_with(&spec);
        let mut leash = Leash::ephemeral();
        leash.record_stop("fake", 99, "operator stop").unwrap();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let seq_before = sup.spine().seq();
        let w = work(
            RESTART,
            json!({ "model": "fake", "spine_event": { "hash": lost.hash, "seq": lost.seq } }),
        );

        let result = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(result["outcome"], json!("refused"));
        assert_eq!(result["refusal"], json!("leash"));
        assert!(
            result["detail"].as_str().unwrap().contains("99"),
            "detail names the stop's SI journal seq: {result}"
        );
        // Spine unchanged: no loading event, no new seq at all.
        assert_eq!(sup.spine().seq(), seq_before);
        assert_eq!(sup.spine().latest("fake").unwrap().seq, lost.seq);
        assert_eq!(outcomes.outcomes().len(), 1);
    }

    #[test]
    fn stop_on_the_sleeping_child_executes_and_records_unloaded() {
        let dir = tempfile::tempdir().unwrap();
        let sleeping = script(dir.path(), "sleeping.sh", "#!/bin/sh\nsleep 30\n");
        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("fake")]);
        let spec = spec_for(dir.path(), "fake", &sleeping);
        sup.start(spec).unwrap();
        let loading = sup.spine().latest("fake").unwrap();
        assert_eq!(loading.kind, "loading");

        let specs = HashMap::new();
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let w = work(
            TOOL_STOP,
            json!({
                "model": "fake",
                "spine_event": { "hash": loading.hash, "seq": loading.seq },
            }),
        );

        let result = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(result["outcome"], json!("executed"));
        let latest = sup.spine().latest("fake").unwrap();
        assert_eq!(latest.kind, "unloaded");
        assert_eq!(
            result["spine_event_refs"],
            json!([{ "hash": latest.hash, "seq": latest.seq }])
        );
        assert_eq!(outcomes.outcomes()[0].outcome, Outcome::Executed);
    }

    #[test]
    fn repull_from_a_localhost_server_records_pull_completed() {
        let dir = tempfile::tempdir().unwrap();
        let body = b"fake gguf bytes for the repull test";
        std::fs::write(dir.path().join("fake.gguf"), body).unwrap();

        // A one-shot static server over the tempdir, spawned
        // as a child process and killed before the assertions
        // so a failure cannot leak it past the test.
        let port = free_port().unwrap();
        let mut server = std::process::Command::new("python3")
            .args([
                "-m",
                "http.server",
                &port.to_string(),
                "--directory",
                &dir.path().display().to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawning python3 -m http.server");
        let mut reachable = false;
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                reachable = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("fake")]);
        let specs = HashMap::new();
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let w = work(
            TOOL_REPULL,
            json!({
                "model": "fake",
                "reference": format!("http://127.0.0.1:{port}/fake.gguf"),
            }),
        );
        let result = if reachable {
            run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w)
        } else {
            Err(malformed(
                "localhost server never became reachable".to_string(),
            ))
        };
        let _ = server.kill();
        let _ = server.wait();

        assert!(reachable, "python3 http.server never accepted a connection");
        let result = result.unwrap();
        assert_eq!(
            result["outcome"],
            json!("executed"),
            "repull result: {result}"
        );
        assert!(
            result["detail"]
                .as_str()
                .unwrap()
                .contains(&body.len().to_string()),
            "detail carries the pulled byte count: {result}"
        );
        let pull = sup
            .spine()
            .latest_pull("fake")
            .expect("the pull was recorded in the spine");
        assert_eq!(pull.kind, "pull_completed");
        assert_eq!(
            result["spine_event_refs"],
            json!([{ "hash": pull.hash, "seq": pull.seq }])
        );
        assert_eq!(outcomes.outcomes()[0].outcome, Outcome::Executed);
    }

    #[test]
    fn notify_records_an_outcome_and_touches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let sup_spine = Spine::in_memory();
        let mut sup = Supervisor::new(sup_spine, budget(), vec![declared("fake")]);
        let declared_ev = sup.spine().latest("fake").unwrap();
        let seq_before = sup.spine().seq();

        let specs = HashMap::new();
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);
        let w = work(
            TOOL_NOTIFY,
            json!({
                "model": "fake",
                "spine_event": { "hash": declared_ev.hash, "seq": declared_ev.seq },
            }),
        );

        let result = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &w).unwrap();
        assert_eq!(result["outcome"], json!("executed"));
        assert_eq!(result["spine_event_refs"], json!([]));
        assert!(
            result["detail"]
                .as_str()
                .unwrap()
                .contains("nothing was actuated"),
            "detail says nothing was actuated: {result}"
        );
        // Nothing touched: spine seq and latest are exactly as
        // they were, and the only new record is the outcome.
        assert_eq!(sup.spine().seq(), seq_before);
        assert_eq!(sup.spine().latest("fake").unwrap().kind, "declared");
        assert_eq!(outcomes.outcomes().len(), 1);
        assert_eq!(outcomes.outcomes()[0].outcome, Outcome::Executed);
    }

    #[test]
    fn unknown_tool_and_missing_model_args_are_malformed_not_outcomes() {
        let dir = tempfile::tempdir().unwrap();
        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("fake")]);
        let specs = HashMap::new();
        let leash = Leash::ephemeral();
        let mut outcomes = OutcomeStore::ephemeral();
        let cfg = pull_cfg(&dir);

        let unknown = work("hearth.explode", json!({ "model": "fake" }));
        let err = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &unknown).unwrap_err();
        assert!(
            matches!(err, GaiaError::Malformed(_)),
            "unknown tool is malformed, got {err:?}"
        );

        let no_model = work(RESTART, json!({ "spine_event": { "hash": "h", "seq": 1 } }));
        let err = run(&mut sup, &specs, &leash, &mut outcomes, &cfg, &no_model).unwrap_err();
        assert!(
            matches!(err, GaiaError::Malformed(_)),
            "missing model is malformed, got {err:?}"
        );

        // Neither defect was filed as an outcome: the record
        // stays Admitted for an operator to see, per contract.
        assert!(outcomes.outcomes().is_empty());
    }
}
