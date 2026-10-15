//! hearth-serve — the supervisor.
//!
//! Three layers, three jobs, wired here:
//!
//!   * `hearth_core::Fleet`  — the decision procedure. What state is each
//!     model in, what should we do next, is there budget for it.
//!   * `hearth_store::Spine` — the system of record. Every transition the
//!     fleet decides is written as a causal, bi-temporal, verifiable event
//!     BEFORE anything else happens. NEDB is not a log on the side: the
//!     supervisor's memory IS the database.
//!   * `server` / `probe`    — the mechanism. llama-server children and
//!     the health probes that produce facts about them.
//!
//! The loop is `tick()`: probe every child, hand each observation to the
//! fleet, and when the fleet's state changes, record the transition in the
//! spine with a causal link to the event that preceded it. Restart-safe by
//! construction — on boot the spine replays the last known state, and
//! `hearth why <model>` answers from the chain, not from memory.

pub mod probe;
pub mod server;
pub mod warmup;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hearth_core::{Budget, Declared, Fleet, LostReason, Millis, Observation, Residency};
use hearth_store::{EventRef, Spine, Transition};
use probe::ProbeResult;
use server::{ServerChild, ServerSpec};

pub fn now_ms() -> Millis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One model under supervision: its child, its last recorded event (the
/// causal parent of the next one), and what we last told the spine.
struct Managed {
    spec: ServerSpec,
    child: Option<ServerChild>,
    last_event: Option<EventRef>,
    /// The kind we last recorded, so transitions are recorded once, on
    /// change, not once per probe.
    recorded: &'static str,
    started_at: Millis,
}

/// The supervisor: fleet (decisions) + spine (record) + children (mechanism).
pub struct Supervisor {
    fleet: Fleet,
    spine: Spine,
    managed: HashMap<String, Managed>,
    probe_timeout: Duration,
}

impl Supervisor {
    /// Declare the fleet and open the spine. Every declaration — admitted
    /// or refused — is recorded before any process starts, because the
    /// budget's refusals are exactly the history you need when someone asks
    /// why a model never came up.
    pub fn new(spine: Spine, budget: Budget, declared: Vec<Declared>) -> Supervisor {
        let fleet = Fleet::declare(budget, declared);
        let sup = Supervisor {
            fleet,
            spine,
            managed: HashMap::new(),
            probe_timeout: Duration::from_millis(750),
        };
        for slot in sup.fleet.slots() {
            let _ = sup.spine.record(
                &slot.declared.model,
                &Transition::Declared {
                    vram_bytes: slot.declared.total_bytes(),
                    admitted: slot.admitted,
                },
                &[],
            );
        }
        sup
    }

    pub fn fleet(&self) -> &Fleet {
        &self.fleet
    }

    pub fn spine(&self) -> &Spine {
        &self.spine
    }

    /// Start serving a model. Spawns the child, tells the fleet, records
    /// `loading` in the spine — causally linked to the model's declaration.
    pub fn start(&mut self, spec: ServerSpec) -> Result<(), String> {
        let model = spec.model.clone();
        let slot = self
            .fleet
            .slot(&model)
            .ok_or_else(|| format!("{model} was never declared"))?;
        if !slot.admitted {
            return Err(format!(
                "{model} was refused by the VRAM budget at declaration; refusing to start it"
            ));
        }
        let endpoint = spec.endpoint();
        let child = ServerChild::spawn(spec.clone())?;
        let pid = child.pid();
        let now = now_ms();

        self.fleet.set_endpoint(&model, endpoint.clone());
        self.fleet.observe(&model, &Observation::LoadStarted, now);

        let declared_ref = self
            .spine
            .latest(&model)
            .map(|e| EventRef {
                hash: e.hash,
                seq: e.seq,
            })
            .into_iter()
            .collect::<Vec<_>>();
        let ev = self.spine.record(
            &model,
            &Transition::Loading { pid, endpoint },
            &declared_ref,
        )?;

        self.managed.insert(
            model,
            Managed {
                spec,
                child: Some(child),
                last_event: Some(ev),
                recorded: "loading",
                started_at: now,
            },
        );
        Ok(())
    }

    /// One supervision pass: probe every child, update the fleet, record
    /// every state CHANGE in the spine. Returns the number of transitions
    /// recorded, so a caller can log honestly ("2 transitions") instead of
    /// narrating every uneventful tick.
    pub fn tick(&mut self) -> usize {
        let now = now_ms();
        let mut transitions = 0;
        let models: Vec<String> = self.managed.keys().cloned().collect();

        for model in models {
            let (observation, endpoint) = {
                let m = self.managed.get_mut(&model).expect("just listed");
                let endpoint = m.spec.endpoint();
                let obs = match m.child.as_mut() {
                    None => continue, // stopped deliberately; nothing to watch
                    Some(child) => match child.exit_code() {
                        Some(code) => Observation::ProcessExited { code },
                        None => {
                            match probe::probe_http(&endpoint, "/health", self.probe_timeout) {
                                ProbeResult::Ok => Observation::ProbeOk {
                                    vram_bytes: self
                                        .fleet
                                        .slot(&model)
                                        .map(|s| s.declared.total_bytes())
                                        .unwrap_or(0),
                                },
                                // Warming is not a failure; keep waiting.
                                ProbeResult::Warming { .. } => continue,
                                ProbeResult::Unanswered { detail }
                                | ProbeResult::Unreachable { detail } => {
                                    // The most valuable bit: is the card still there?
                                    let gpu = probe::gpu_present().unwrap_or(true);
                                    Observation::ProbeFailed {
                                        gpu_present: gpu,
                                        detail,
                                    }
                                }
                            }
                        }
                    },
                };
                (obs, endpoint)
            };

            self.fleet.observe(&model, &observation, now);
            let state = self
                .fleet
                .slot(&model)
                .map(|s| s.state.clone())
                .unwrap_or_default();

            let m = self.managed.get_mut(&model).expect("just listed");
            let transition = match &state {
                Residency::Resident { .. } if m.recorded != "resident" => {
                    Some(Transition::Resident {
                        endpoint,
                        warmup_ms: now.saturating_sub(m.started_at),
                    })
                }
                Residency::Lost { reason, .. } if m.recorded != "lost" => {
                    Some(Transition::Lost { reason: *reason })
                }
                Residency::Failed { .. } if m.recorded != "lost" => Some(Transition::Lost {
                    reason: LostReason::ProcessExited,
                }),
                _ => None,
            };

            if let Some(t) = transition {
                let causes: Vec<EventRef> = m.last_event.clone().into_iter().collect();
                if let Ok(ev) = self.spine.record(&model, &t, &causes) {
                    m.last_event = Some(ev);
                    m.recorded = match t {
                        Transition::Resident { .. } => "resident",
                        _ => "lost",
                    };
                    transitions += 1;
                }
                // A lost child holds no port worth keeping.
                if matches!(t, Transition::Lost { .. }) {
                    if let Some(mut c) = m.child.take() {
                        c.stop();
                    }
                }
            }
        }
        transitions
    }

    /// Restart children that died unexpectedly. Only restarts `Lost` with
    /// `ProcessExited` or `Unhealthy` reasons — not `Evicted` (a deliberate
    /// VRAM decision) or `GpuDetached` (a host problem we cannot fix).
    ///
    /// A child that dies within 60s of starting is not restarted: that is a
    /// broken configuration (bad binary, bad args), not a transient failure,
    /// and hot-looping it would thrash the GPU. The 60s threshold is generous
    /// — even the 30B Q4_K_M loads in ~20s.
    ///
    /// Returns the number of children restarted.
    pub fn restart_lost(&mut self) -> usize {
        let now = now_ms();
        let to_restart: Vec<String> = self
            .managed
            .iter()
            .filter(|(model, m)| {
                let restartable = matches!(
                    self.fleet.slot(model).map(|s| &s.state),
                    Some(Residency::Lost {
                        reason: LostReason::ProcessExited,
                        ..
                    }) | Some(Residency::Lost {
                        reason: LostReason::Unhealthy,
                        ..
                    })
                );
                // Child must be gone (taken by tick) and old enough to have
                // been a real run, not a crash-loop on bad config.
                restartable && m.child.is_none() && now.saturating_sub(m.started_at) > 60_000
            })
            .map(|(model, _)| model.clone())
            .collect();

        let mut restarted = 0;
        for model in to_restart {
            // Confirm it still fits — next_to_load is the authority on what
            // should be running, and it refuses to evict to make room.
            let should_start = self
                .fleet
                .next_to_load()
                .map(|s| s.declared.model == model)
                .unwrap_or(false);
            if !should_start {
                continue;
            }
            let spec = match self.managed.get(&model) {
                Some(m) => m.spec.clone(),
                None => continue,
            };
            match self.start(spec) {
                Ok(()) => {
                    eprintln!("hearth: restarted {model} after unexpected death");
                    restarted += 1;
                }
                Err(e) => {
                    eprintln!("hearth: failed to restart {model}: {e}");
                }
            }
        }
        restarted
    }

    /// Block until `model` is ready or the deadline passes. Ticks while it
    /// waits, so every intermediate transition still lands in the spine.
    pub fn wait_ready(&mut self, model: &str, deadline: Duration) -> Result<String, String> {
        let start = std::time::Instant::now();
        loop {
            self.tick();
            match self.fleet.slot(model).map(|s| s.state.clone()) {
                Some(Residency::Resident { .. }) => {
                    let endpoint = self
                        .managed
                        .get(model)
                        .map(|m| m.spec.endpoint())
                        .unwrap_or_default();
                    return Ok(endpoint);
                }
                Some(Residency::Lost { reason, .. }) => {
                    return Err(format!("{model} lost while warming: {reason:?}"));
                }
                Some(Residency::Failed { reason, .. }) => {
                    return Err(format!("{model} failed to load: {reason}"));
                }
                _ => {}
            }
            if start.elapsed() > deadline {
                return Err(format!(
                    "{model} not ready after {:?} — state: {}",
                    deadline,
                    self.fleet
                        .slot(model)
                        .map(|s| s.state.explain(now_ms()))
                        .unwrap_or_else(|| "undeclared".into())
                ));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Deliberate stop: kill the child, tell the fleet, record `unloaded`.
    pub fn stop(&mut self, model: &str) -> Result<(), String> {
        let m = self
            .managed
            .get_mut(model)
            .ok_or_else(|| format!("{model} is not managed"))?;
        if let Some(mut child) = m.child.take() {
            child.stop();
        }
        self.fleet
            .observe(model, &Observation::StopRequested, now_ms());
        let causes: Vec<EventRef> = m.last_event.clone().into_iter().collect();
        let ev = self.spine.record(model, &Transition::Unloaded, &causes)?;
        m.last_event = Some(ev);
        m.recorded = "unloaded";
        Ok(())
    }

    /// The fleet's honest status report.
    pub fn report(&self) -> String {
        self.fleet.report(now_ms())
    }

    /// Stop everything. Called on shutdown; Drop on the children is the
    /// backstop, this is the version that records it.
    pub fn stop_all(&mut self) {
        let models: Vec<String> = self.managed.keys().cloned().collect();
        for m in models {
            let _ = self.stop(&m);
        }
        self.spine.flush();
    }
}

/// Where hearth keeps its state on this host: `$HEARTH_HOME` or
/// `~/.hearth`. The spine lives at `<home>/spine`, blobs at `<home>/blobs`.
pub fn hearth_home() -> PathBuf {
    if let Ok(h) = std::env::var("HEARTH_HOME") {
        return PathBuf::from(h);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".hearth")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> Budget {
        Budget {
            total_bytes: 24 * hearth_core::GIB,
            reserve_bytes: 2 * hearth_core::GIB,
        }
    }

    fn declared(model: &str) -> Declared {
        Declared {
            model: model.into(),
            weights_bytes: 4 * hearth_core::GIB,
            kv_bytes: hearth_core::GIB,
        }
    }

    #[test]
    fn declarations_are_recorded_before_anything_runs() {
        let sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("muse")]);
        let ev = sup.spine().latest("muse").unwrap();
        assert_eq!(ev.kind, "declared");
        assert_eq!(ev.facts["admitted"], serde_json::json!(true));
    }

    #[test]
    fn budget_refusal_is_recorded_and_start_is_refused() {
        let mut sup = Supervisor::new(
            Spine::in_memory(),
            budget(),
            vec![Declared {
                model: "whale".into(),
                weights_bytes: 400 * hearth_core::GIB,
                kv_bytes: 0,
            }],
        );
        assert_eq!(
            sup.spine().latest("whale").unwrap().facts["admitted"],
            serde_json::json!(false)
        );
        let err = sup
            .start(ServerSpec::new("whale", "/nope.gguf", 1))
            .unwrap_err();
        assert!(err.contains("refused by the VRAM budget"), "{err}");
    }

    #[test]
    fn starting_an_undeclared_model_is_an_error() {
        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![]);
        let err = sup
            .start(ServerSpec::new("ghost", "/nope.gguf", 1))
            .unwrap_err();
        assert!(err.contains("never declared"), "{err}");
    }

    #[test]
    fn a_dying_child_becomes_a_recorded_loss_with_a_causal_chain() {
        let dir = tempfile::tempdir().unwrap();
        let gguf = dir.path().join("fake.gguf");
        std::fs::write(&gguf, b"bytes").unwrap();

        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("fake")]);
        // /bin/sh exits immediately — a runtime that dies on startup.
        let mut spec = ServerSpec::new("fake", &gguf, server::free_port().unwrap());
        spec.binary = PathBuf::from("/bin/sh");
        spec.extra_args = vec!["-c".into(), "exit 1".into()];
        spec.log_dir = dir.path().to_path_buf();
        sup.start(spec).unwrap();

        // Let it die, then tick until the loss is recorded (bounded).
        let mut recorded = false;
        for _ in 0..40 {
            if sup.tick() > 0 {
                recorded = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(recorded, "loss never recorded");

        let latest = sup.spine().latest("fake").unwrap();
        assert_eq!(latest.kind, "lost");
        // why() tells the whole story: lost <- loading <- declared.
        let kinds: Vec<String> = sup
            .spine()
            .why("fake")
            .into_iter()
            .map(|e| e.kind)
            .collect();
        assert_eq!(kinds, vec!["lost", "loading", "declared"]);
    }

    #[test]
    fn deliberate_stop_is_unloaded_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        let gguf = dir.path().join("fake.gguf");
        std::fs::write(&gguf, b"bytes").unwrap();

        let mut sup = Supervisor::new(Spine::in_memory(), budget(), vec![declared("fake")]);
        let mut spec = ServerSpec::new("fake", &gguf, server::free_port().unwrap());
        spec.binary = PathBuf::from("/bin/sh");
        spec.extra_args = vec!["-c".into(), "sleep 30".into()];
        spec.log_dir = dir.path().to_path_buf();
        sup.start(spec).unwrap();
        sup.stop("fake").unwrap();

        assert_eq!(sup.spine().latest("fake").unwrap().kind, "unloaded");
    }

    #[test]
    fn spine_survives_what_the_supervisor_forgets() {
        // The restart story: a fresh supervisor over the same spine dir
        // still answers why(), because the history is on disk, not in RAM.
        let dir = tempfile::tempdir().unwrap();
        let spine_dir = dir.path().join("spine");
        {
            let mut sup = Supervisor::new(
                Spine::open(&spine_dir).unwrap(),
                budget(),
                vec![declared("muse")],
            );
            let gguf = dir.path().join("fake.gguf");
            std::fs::write(&gguf, b"bytes").unwrap();
            let mut spec = ServerSpec::new("muse", &gguf, server::free_port().unwrap());
            spec.binary = PathBuf::from("/bin/sh");
            spec.extra_args = vec!["-c".into(), "exit 1".into()];
            spec.log_dir = dir.path().to_path_buf();
            sup.start(spec).unwrap();
            for _ in 0..40 {
                if sup.tick() > 0 {
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            sup.stop_all();
        }
        let spine = Spine::open(&spine_dir).unwrap();
        let kinds: Vec<String> = spine.why("muse").into_iter().map(|e| e.kind).collect();
        assert!(
            kinds.starts_with(&["lost".to_string(), "loading".to_string()])
                || kinds.starts_with(&["unloaded".to_string()]),
            "unexpected chain: {kinds:?}"
        );
        let (checked, failures) = spine.verify();
        assert!(checked >= 3);
        assert!(failures.is_empty(), "{failures:?}");
    }
}
