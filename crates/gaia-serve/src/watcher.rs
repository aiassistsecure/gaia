//! The residency watcher: observe in Rust, propose to the gate
//! (G-2 §5.1).
//!
//! The watcher reads spine history through the G-4 mirror —
//! never the spine's write path, never a child process — and for
//! each `lost` event it applies the §5.2 reason → candidate
//! mapping and records **at most one** proposal in the
//! [`ProposalStore`]. It holds no policy beyond that mapping
//! table, and it never actuates anything: whether a candidate
//! runs is the SI gate's decision, journaled in SI before any
//! actuation (G-6). Three §5.5 invariants shape the pass:
//!
//!   * **Scope is opt-in.** A model whose declaration does not
//!     mark it in Gaia scope is never proposed for. The watcher
//!     is constructed with the in-scope set; everything else is
//!     invisible to it.
//!   * **A loss already answered stays answered.** Dedup is
//!     causal: the store is checked by the loss event's own
//!     [`EventRef`], so a second pass over the same history —
//!     or a restarted watcher over the same store — proposes
//!     nothing new.
//!   * **A superseded loss is not proposed for.** If the model's
//!     latest spine event is no longer the loss (it recovered,
//!     or moved on), proposing a restart for the old loss would
//!     answer a question nobody is asking anymore. For
//!     `unhealthy` this doubles as the confirming re-probe §5.2
//!     requires: the composition ticks the supervisor in the
//!     same step immediately before the pass, so "still latest
//!     after a fresh tick" means a fresh probe still finds it
//!     lost. There is no separate probe channel in the public
//!     supervisor API, and this module does not invent one.

use std::collections::HashSet;

use gaia_api::{Source, StreamEvent};
use hearth_core::{Fleet, LostReason};
use hearth_store::{EventRef, Spine};
use serde_json::json;

use crate::proposals::{Proposal, ProposalStore};

/// Candidate tool names, exactly as the SI registry spells them.
pub const TOOL_RESTART: &str = "hearth.restart";
pub const TOOL_NOTIFY: &str = "hearth.notify";

/// What the §5.2 table says to propose for one observed loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Candidate {
    Restart,
    Notify,
}

/// The §5.2 mapping, pure:
///
///   * `process_exited` → restart. The child is gone; restarting
///     it is the actuation upstream's unsupervised path performs
///     today, moved behind the gate (§5.2.1).
///   * `unhealthy` → restart (the pass's still-latest check is
///     the confirming re-probe; see the module docs).
///   * `evicted` → notify first; restart only when the fleet
///     has free VRAM at proposal time (`fleet_has_room`).
///     Evicted is ambiguous by construction — planner pressure
///     or a failed probe — so the proposal's reason carries the
///     causal chain either way.
///   * `gpu_detached` → notify, never restart. The card is
///     physically gone; no restart proposal is made against
///     absent hardware.
pub fn candidate(reason: LostReason, fleet_has_room: bool) -> Candidate {
    match reason {
        LostReason::ProcessExited | LostReason::Unhealthy => Candidate::Restart,
        LostReason::Evicted => {
            if fleet_has_room {
                Candidate::Restart
            } else {
                Candidate::Notify
            }
        }
        LostReason::GpuDetached => Candidate::Notify,
    }
}

/// Parse the spine's recorded reason string back into a
/// [`LostReason`]. Returns `None` for anything unrecognized —
/// deliberately *not* the store reader's default-to-evicted:
/// an unparseable reason is not evidence of an eviction, and
/// the watcher does not guess about losses it cannot name.
pub fn parse_lost_reason(s: &str) -> Option<LostReason> {
    match s {
        "evicted" => Some(LostReason::Evicted),
        "gpu_detached" => Some(LostReason::GpuDetached),
        "process_exited" => Some(LostReason::ProcessExited),
        "unhealthy" => Some(LostReason::Unhealthy),
        _ => None,
    }
}

/// The watcher. Stateless across passes except for what the
/// [`ProposalStore`] remembers — which is the point: dedup
/// survives restarts because it lives in the store, not here.
pub struct Watcher {
    in_scope: HashSet<String>,
}

impl Watcher {
    /// Build a watcher over an explicit in-scope model set
    /// (§5.5: scope is a declaration-time, opt-in property; the
    /// empty set watches nothing).
    pub fn new<I: IntoIterator<Item = String>>(in_scope: I) -> Watcher {
        Watcher {
            in_scope: in_scope.into_iter().collect(),
        }
    }

    /// Is this model in Gaia scope for the watcher?
    pub fn in_scope(&self, model: &str) -> bool {
        self.in_scope.contains(model)
    }

    /// One watcher pass over mirrored spine history.
    ///
    /// `entries` is the mirror's full entry list (the mirror is
    /// the watcher's only window onto the spine's history);
    /// `spine` is consulted live for the two questions the
    /// mirror cannot answer — is this loss still the model's
    /// latest state, and what is its causal chain (`why`).
    /// `fleet`, when the composition can provide it, answers
    /// the evicted branch's free-VRAM question; without a fleet
    /// the answer is "no room", and evictions propose notify.
    ///
    /// Returns the proposals recorded by this pass (possibly
    /// none). A store append failure aborts the pass with the
    /// error: proposals recorded before the failure stay
    /// recorded, and a later pass resumes where this one stopped
    /// because dedup is causal.
    pub fn pass(
        &self,
        entries: &[StreamEvent],
        spine: &Spine,
        store: &mut ProposalStore,
        fleet: Option<&Fleet>,
    ) -> Result<Vec<Proposal>, String> {
        // Process in ascending seq order whatever order the
        // caller handed the entries in: proposals should be
        // recorded in the order the losses happened.
        let mut ordered: Vec<&StreamEvent> = entries.iter().collect();
        ordered.sort_by_key(|entry| entry.seq);

        let mut recorded = Vec::new();
        for entry in ordered {
            // Only hearth losses are the watcher's business:
            // other sources and other kinds are history, not
            // losses, and a loss without a model or a hash has
            // no identity to dedup or propose against.
            if entry.source != Source::Hearth || entry.kind != "lost" {
                continue;
            }
            let (Some(model), Some(hash)) =
                (entry.model.as_deref(), entry.hash.as_deref())
            else {
                continue;
            };

            // Scope is opt-in (§5.5): an out-of-scope model is
            // invisible, however it was lost.
            if !self.in_scope(model) {
                continue;
            }

            let ev_ref = EventRef {
                hash: hash.to_string(),
                seq: entry.seq,
            };
            // A loss already answered stays answered (§5.5).
            if store.answered(&ev_ref) {
                continue;
            }

            // A superseded loss is not proposed for: if the
            // model's latest spine event is no longer this
            // loss, it recovered or moved on, and this entry
            // answers a question nobody is asking anymore.
            match spine.latest(model) {
                Some(latest) if latest.seq == entry.seq => {}
                _ => continue,
            }

            // An unparseable reason is not evidence for any
            // branch of the mapping — the watcher does not
            // guess about losses it cannot name.
            let Some(reason) = entry
                .facts
                .get("reason")
                .and_then(|v| v.as_str())
                .and_then(parse_lost_reason)
            else {
                continue;
            };

            // Without a fleet the evicted branch's answer is
            // "no room"; the other branches do not ask.
            let has_room = fleet.map(|f| fleet_has_room(f, model)).unwrap_or(false);
            let cand = candidate(reason, has_room);
            let chain = spine.why(model);
            let proposal = build_proposal(model, cand, reason, &ev_ref, &chain);
            // A failed append aborts the pass: proposals
            // recorded before it stay recorded, and a later
            // pass resumes here because dedup is causal.
            store.record(proposal.clone())?;
            recorded.push(proposal);
        }
        Ok(recorded)
    }
}

/// Does the fleet have room to bring `model` back — free VRAM
/// at least the model's declared total? Without the model's
/// slot there is nothing to size the question against, and the
/// answer is no.
pub fn fleet_has_room(fleet: &Fleet, model: &str) -> bool {
    match fleet.slot(model) {
        Some(slot) => fleet.live_free_bytes() >= slot.declared.total_bytes(),
        None => false,
    }
}

/// The proposal's reason text. It names the observed loss, and
/// it carries the spine causal chain — for `evicted` §5.2
/// requires the chain so the gate can see *which* eviction this
/// was; the chain rides on every proposal, because provenance
/// that only exists for one branch is provenance that gets
/// dropped by accident.
pub fn proposal_reason(reason: LostReason, loss: &EventRef, chain: &[hearth_store::Event]) -> String {
    let reason_name = match reason {
        LostReason::Evicted => "evicted",
        LostReason::GpuDetached => "gpu_detached",
        LostReason::ProcessExited => "process_exited",
        LostReason::Unhealthy => "unhealthy",
    };
    let chain_text = chain
        .iter()
        .map(|e| format!("{}#{}", e.kind, e.seq))
        .collect::<Vec<_>>()
        .join(" <- ");
    format!(
        "observed lost{{{reason_name}}} at seq {}; causal chain: {chain_text}",
        loss.seq
    )
}

/// Build the proposal for one mapped loss. `args` is exactly
/// the admitted-args shape of §5.1/§5.3: the model and the
/// spine event being answered, so actuation (G-6) executes the
/// journaled args verbatim or refuses — it never re-derives
/// them.
pub fn build_proposal(
    model: &str,
    cand: Candidate,
    reason: LostReason,
    loss: &EventRef,
    chain: &[hearth_store::Event],
) -> Proposal {
    Proposal {
        model: model.to_string(),
        tool: match cand {
            Candidate::Restart => TOOL_RESTART,
            Candidate::Notify => TOOL_NOTIFY,
        }
        .to_string(),
        args: json!({
            "model": model,
            "spine_event": { "hash": loss.hash, "seq": loss.seq },
        }),
        reason: proposal_reason(reason, loss, chain),
        caused_by: loss.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use gaia_api::mirror::collect_spine_events;
    use hearth_core::{Budget, Declared, Fleet, GIB};
    use hearth_store::Transition;

    /// Record a declared -> lost chain for one model, each
    /// event caused by the previous one, so the spine's causal
    /// walks can reach both. Returns the spine and the loss's
    /// own ref — the ref a proposal for this loss must cite.
    fn loss_spine(model: &str, reason: LostReason) -> (Spine, EventRef) {
        let spine = Spine::in_memory();
        let declared = spine
            .record(
                model,
                &Transition::Declared {
                    vram_bytes: GIB,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let lost = spine
            .record(
                model,
                &Transition::Lost { reason },
                std::slice::from_ref(&declared),
            )
            .unwrap();
        (spine, lost)
    }

    fn store_in(dir: &tempfile::TempDir) -> ProposalStore {
        ProposalStore::open(&dir.path().join("proposals.jsonl")).unwrap()
    }

    fn watcher_for(model: &str) -> Watcher {
        Watcher::new([model.to_string()])
    }

    #[test]
    fn candidate_table_maps_each_reason_as_section_5_2_says() {
        // process_exited and unhealthy restart regardless of room.
        for room in [false, true] {
            assert_eq!(candidate(LostReason::ProcessExited, room), Candidate::Restart);
            assert_eq!(candidate(LostReason::Unhealthy, room), Candidate::Restart);
            // gpu_detached notifies always: no restart proposal
            // is made against absent hardware.
            assert_eq!(candidate(LostReason::GpuDetached, room), Candidate::Notify);
        }
        // Evicted is the only branch the fleet answer moves.
        assert_eq!(candidate(LostReason::Evicted, true), Candidate::Restart);
        assert_eq!(candidate(LostReason::Evicted, false), Candidate::Notify);
    }

    #[test]
    fn parse_lost_reason_knows_four_strings_and_nothing_else() {
        assert_eq!(parse_lost_reason("evicted"), Some(LostReason::Evicted));
        assert_eq!(
            parse_lost_reason("gpu_detached"),
            Some(LostReason::GpuDetached)
        );
        assert_eq!(
            parse_lost_reason("process_exited"),
            Some(LostReason::ProcessExited)
        );
        assert_eq!(parse_lost_reason("unhealthy"), Some(LostReason::Unhealthy));
        // Unknown is None, never a guess at a known reason.
        assert_eq!(parse_lost_reason("mystery"), None);
        assert_eq!(parse_lost_reason(""), None);
    }

    #[test]
    fn gpu_detached_proposes_notify_once_and_a_second_pass_adds_nothing() {
        let (spine, _lost) = loss_spine("muse", LostReason::GpuDetached);
        let entries = collect_spine_events(&spine);
        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let watcher = watcher_for("muse");

        let first = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert_eq!(first.len(), 1, "exactly one proposal: {first:?}");
        assert_eq!(first[0].tool, TOOL_NOTIFY);

        // Acceptance criterion 4: a second pass over the same
        // history proposes nothing new, because the store
        // already answers this loss.
        let second = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert!(second.is_empty(), "second pass proposed: {second:?}");
        assert_eq!(store.proposals().len(), 1);
    }

    #[test]
    fn process_exited_proposes_restart_caused_by_the_loss() {
        let (spine, lost) = loss_spine("muse", LostReason::ProcessExited);
        let entries = collect_spine_events(&spine);
        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let watcher = watcher_for("muse");

        let proposals = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert_eq!(proposals.len(), 1);
        let proposal = &proposals[0];
        assert_eq!(proposal.tool, TOOL_RESTART);
        assert_eq!(proposal.caused_by, lost);
        assert_eq!(
            proposal.args,
            json!({
                "model": "muse",
                "spine_event": { "hash": lost.hash, "seq": lost.seq },
            })
        );
        assert!(
            proposal.reason.contains("lost{process_exited}"),
            "reason names the loss: {}",
            proposal.reason
        );
        assert!(
            proposal.reason.contains("lost#"),
            "reason carries the causal chain as kind#seq links: {}",
            proposal.reason
        );
        let intent = proposal.intent_json();
        assert_eq!(intent["type"], json!("tool"));
        assert_eq!(intent["tool"], json!(TOOL_RESTART));
    }

    #[test]
    fn a_model_out_of_scope_is_never_proposed_for() {
        let (spine, _lost) = loss_spine("muse", LostReason::ProcessExited);
        let entries = collect_spine_events(&spine);
        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let watcher = Watcher::new(["someone-else".to_string()]);

        let proposals = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert!(proposals.is_empty());
        assert!(store.proposals().is_empty());
    }

    #[test]
    fn a_superseded_loss_is_not_proposed_for() {
        let spine = Spine::in_memory();
        let declared = spine
            .record(
                "muse",
                &Transition::Declared {
                    vram_bytes: GIB,
                    admitted: true,
                },
                &[],
            )
            .unwrap();
        let lost = spine
            .record(
                "muse",
                &Transition::Lost {
                    reason: LostReason::ProcessExited,
                },
                std::slice::from_ref(&declared),
            )
            .unwrap();
        // The model recovered: the loss is no longer its latest
        // spine event, so it has been superseded.
        spine
            .record(
                "muse",
                &Transition::Resident {
                    endpoint: "127.0.0.1:8080".to_string(),
                    warmup_ms: 12,
                },
                std::slice::from_ref(&lost),
            )
            .unwrap();
        let entries = collect_spine_events(&spine);
        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let watcher = watcher_for("muse");

        let proposals = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert!(proposals.is_empty(), "superseded loss proposed: {proposals:?}");
        assert!(store.proposals().is_empty());
    }

    #[test]
    fn evicted_notifies_without_a_fleet_and_restarts_with_room() {
        let (spine, _lost) = loss_spine("muse", LostReason::Evicted);
        let entries = collect_spine_events(&spine);
        let watcher = watcher_for("muse");

        // No fleet: the answer to the free-VRAM question is
        // "no room", so an eviction proposes notify.
        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let proposals = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].tool, TOOL_NOTIFY);

        // A roomy fleet: the same loss, in a fresh store,
        // proposes restart.
        let fleet = Fleet::declare(
            Budget {
                total_bytes: 24 * GIB,
                reserve_bytes: 2 * GIB,
            },
            vec![Declared {
                model: "muse".to_string(),
                weights_bytes: GIB,
                kv_bytes: 0,
            }],
        );
        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let proposals = watcher
            .pass(&entries, &spine, &mut store, Some(&fleet))
            .unwrap();
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].tool, TOOL_RESTART);
    }

    #[test]
    fn an_unknown_reason_proposes_nothing() {
        let (spine, _lost) = loss_spine("muse", LostReason::ProcessExited);
        let mut entries = collect_spine_events(&spine);
        // Rewrite the mirrored loss with a reason the watcher
        // cannot name. Everything else — source, kind, model,
        // hash, seq, latest — still lines up, so only the
        // unparseable reason can explain silence.
        let lost_entry = entries
            .iter_mut()
            .find(|e| e.kind == "lost")
            .expect("the collected history holds the loss");
        lost_entry.facts = json!({ "reason": "mystery" });

        let dir = tempfile::tempdir().unwrap();
        let mut store = store_in(&dir);
        let watcher = watcher_for("muse");
        let proposals = watcher.pass(&entries, &spine, &mut store, None).unwrap();
        assert!(proposals.is_empty(), "unknown reason proposed: {proposals:?}");
        assert!(store.proposals().is_empty());
    }
}
