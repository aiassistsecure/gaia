//! gaia-serve — the composition.
//!
//! Upstream hearth is a runtime: it supervises children, records
//! what happens in the spine, and — in its `up` composition —
//! restarts dead children on its own authority (the
//! `restart_lost` policy wired into the upstream binary's ticker,
//! G-2 §5.2.1). Gaia is the same runtime with the authority
//! moved: this crate composes the *public* supervisor API into a
//! loop that observes exactly as upstream does and then does
//! something upstream does not — it **proposes**.
//!
//! The trust boundary is at composition, stated precisely
//! (§5.2.1): the upstream `hearth` binary is untouched and keeps
//! upstream behaviour, including unsupervised restarts. A
//! deployment running this crate's `gaia` binary gets the gated
//! behaviour: the loop calls `Supervisor::tick` and never calls
//! the restart method. Every restart a Gaia deployment performs
//! will have arrived as an admitted, SI-journaled intent —
//! that actuation is G-6; this slice (G-5) builds the observing
//! half: the watcher, the proposals, and the structured
//! residency the gate's policy reads.
//!
//! One composition step — [`Composition::tick_once`] — is:
//!
//!   1. `Supervisor::tick` — probe children, record transitions
//!      in the spine. Identical to upstream; the facts are the
//!      same facts.
//!   2. Mirror poll — append newly recorded spine events to the
//!      G-4 mirror and publish them to the event hub, so SSE
//!      subscribers and the watcher read the same history.
//!   3. Watcher pass — for each newly observed loss, at most
//!      one proposal, causally deduped, recorded gaia-side.
//!
//! Nothing in the step actuates anything. Actuation is a
//! separate, explicit act — [`Composition::actuate_work`] —
//! and G-6 is where it lands: a work order the SI gate has
//! ADMIT-ted (and only one) is executed through the §5.3
//! primitives by [`FleetActuator`], after the leash check and
//! the stale-state check, with the outcome — executed,
//! refused, or failed — recorded gaia-side in the
//! [`OutcomeStore`] and returned as the work's result. The
//! composition proposes; the gate decides; this crate acts
//! only on the decision, never in place of it.

pub mod actuate;
pub mod leash;
pub mod outcomes;
pub mod proposals;
pub mod watcher;

pub use actuate::FleetActuator;
pub use leash::{Leash, LeashKind, LeashRecord};
pub use outcomes::{Outcome, OutcomeRecord, OutcomeStore};
pub use proposals::{Proposal, ProposalStore};
pub use watcher::{candidate, Candidate, Watcher};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use gaia_api::mirror::poll_once;
use gaia_api::{
    Actuator, EventFilter, EventHub, EventLog, GaiaError, MirrorStore, Source, StreamEvent,
    WorkRecord, WorkStore,
};
use hearth_pull::PullConfig;
use hearth_serve::server::ServerSpec;
use hearth_serve::Supervisor;
use hearth_store::Spine;

/// The G-4 mirror behind a mutex, readable as an [`EventLog`]
/// for the SSE server.
///
/// `MirrorStore` implements `EventLog` directly, but a serving
/// composition needs the *same* mirror the poller appends to —
/// an `Arc<MirrorStore>` snapshot opened at bind time would go
/// stale with the first new event. This adapter locks briefly
/// per read and delegates, so replay reads see the mirror as it
/// is now, not as it was when the listener bound.
pub struct SharedMirror(pub Arc<Mutex<MirrorStore>>);

impl EventLog for SharedMirror {
    fn events_since(
        &self,
        source: Source,
        after_seq: u64,
        filter: &EventFilter,
    ) -> Vec<StreamEvent> {
        let guard = self.0.lock().unwrap_or_else(|p| p.into_inner());
        guard.events_since(source, after_seq, filter)
    }

    fn latest_seq(&self, source: Source) -> u64 {
        let guard = self.0.lock().unwrap_or_else(|p| p.into_inner());
        guard.latest_seq(source)
    }
}

/// What one composition step did, in counts and in the actual
/// records — a caller reporting "2 transitions, 1 proposal" is
/// reading this struct, not narrating.
#[derive(Debug)]
pub struct TickOutcome {
    /// Spine transitions the supervisor recorded this tick.
    pub transitions: usize,
    /// Spine events newly appended to the mirror (and published
    /// to the hub) this tick.
    pub mirrored: Vec<StreamEvent>,
    /// Proposals the watcher recorded this tick.
    pub proposals: Vec<Proposal>,
}

/// The composed runtime: supervisor + mirror + watcher +
/// proposal store + event hub, stepped by [`Composition::tick_once`].
pub struct Composition {
    supervisor: Supervisor,
    mirror: Arc<Mutex<MirrorStore>>,
    watcher: Watcher,
    proposals: ProposalStore,
    hub: EventHub,
    /// The `ServerSpec`s this composition started its models
    /// with, by model name — the *stored spec* of §5.3 that an
    /// admitted `hearth.restart` re-runs. The supervisor keeps
    /// its own copy internally but exposes no accessor, so the
    /// composition keeps the specs it handed over
    /// ([`Composition::note_spec`]). A model with no stored
    /// spec cannot be restart-actuated, and the actuator says
    /// so as a refusal rather than improvising one.
    specs: HashMap<String, ServerSpec>,
    /// G-6 outcome records: every actuation attempt that
    /// passed the gate check, and what it did. Ephemeral until
    /// the binary installs the file-backed store under its
    /// home ([`Composition::set_outcome_store`]).
    outcomes: OutcomeStore,
    /// The operator leash (§5.2.1): per-model stop/resume
    /// records surfaced from SI's journal through the verdict
    /// path. Ephemeral until the binary installs the
    /// file-backed one ([`Composition::set_leash`]).
    leash: Leash,
    /// Where re-pulls put their blobs (§5.3). The default is a
    /// relative `./blobs` with progress off — library-quiet;
    /// the binary points it at its home directory.
    pull_cfg: PullConfig,
}

impl Composition {
    pub fn new(
        supervisor: Supervisor,
        mirror: Arc<Mutex<MirrorStore>>,
        watcher: Watcher,
        proposals: ProposalStore,
        hub: EventHub,
    ) -> Composition {
        Composition {
            supervisor,
            mirror,
            watcher,
            proposals,
            hub,
            specs: HashMap::new(),
            outcomes: OutcomeStore::ephemeral(),
            leash: Leash::ephemeral(),
            pull_cfg: PullConfig {
                blobs_dir: PathBuf::from("./blobs"),
                progress: false,
                verify_existing: false,
            },
        }
    }

    /// One composition step: tick the supervisor, mirror what
    /// it recorded, then let the watcher propose over the full
    /// mirrored history (dedup makes re-scanning old history
    /// free of new proposals; it is what makes a restarted
    /// composition safe). Deliberately absent: any restart
    /// call. The supervisor's unsupervised-restart method is
    /// upstream's policy, and this composition does not invoke
    /// it — losses become proposals here.
    pub fn tick_once(&mut self) -> Result<TickOutcome, String> {
        let transitions = self.supervisor.tick();

        let mirrored = {
            let mut guard = self.mirror.lock().unwrap_or_else(|p| p.into_inner());
            poll_once(&mut guard, self.supervisor.spine())
        };
        for ev in &mirrored {
            self.hub.publish(ev);
        }

        let entries = {
            let guard = self.mirror.lock().unwrap_or_else(|p| p.into_inner());
            guard.entries().to_vec()
        };
        let proposals = {
            let spine = self.supervisor.spine();
            let fleet = self.supervisor.fleet();
            self.watcher
                .pass(&entries, spine, &mut self.proposals, Some(fleet))?
        };

        Ok(TickOutcome {
            transitions,
            mirrored,
            proposals,
        })
    }

    /// The supervisor, for the composition's owner (the binary
    /// starts declared models through it; tests kill children
    /// under it). The watcher never touches this handle.
    pub fn supervisor(&mut self) -> &mut Supervisor {
        &mut self.supervisor
    }

    /// The spine the supervisor records into.
    pub fn spine(&self) -> &Spine {
        self.supervisor.spine()
    }

    /// The shared mirror handle — the binary hands a
    /// [`SharedMirror`] over the same allocation to the SSE
    /// server.
    pub fn mirror(&self) -> Arc<Mutex<MirrorStore>> {
        Arc::clone(&self.mirror)
    }

    /// The event hub the composition publishes mirrored events
    /// to.
    pub fn hub(&self) -> EventHub {
        self.hub.clone()
    }

    /// Every proposal recorded so far, oldest first.
    pub fn proposals(&self) -> &[Proposal] {
        self.proposals.proposals()
    }

    /// The watcher (scope inspection for the binary's banner).
    pub fn watcher(&self) -> &Watcher {
        &self.watcher
    }

    /// Remember the spec a model was started with, so an
    /// admitted restart can re-run exactly it (§5.3: actuation
    /// executes the admitted args against the stored spec).
    /// The binary calls this for every model it starts.
    pub fn note_spec(&mut self, spec: ServerSpec) {
        self.specs.insert(spec.model.clone(), spec);
    }

    /// Install the file-backed outcome store (the binary opens
    /// it under its home). Until then the store is ephemeral.
    pub fn set_outcome_store(&mut self, outcomes: OutcomeStore) {
        self.outcomes = outcomes;
    }

    /// Install the file-backed leash (the binary opens it
    /// under its home). Until then the leash is ephemeral.
    pub fn set_leash(&mut self, leash: Leash) {
        self.leash = leash;
    }

    /// Point re-pull actuation at a blob directory (the binary
    /// uses `<home>/blobs`, as upstream's `cmd_pull` does).
    pub fn set_pull_config(&mut self, pull_cfg: PullConfig) {
        self.pull_cfg = pull_cfg;
    }

    /// The leash, mutably — the verdict path records stops and
    /// resumes here as SI's journal surfaces them (§5.2.1).
    pub fn leash(&mut self) -> &mut Leash {
        &mut self.leash
    }

    /// Every actuation outcome recorded so far, oldest first.
    pub fn outcomes(&self) -> &[OutcomeRecord] {
        self.outcomes.outcomes()
    }

    /// Act on one work order — the G-6 drain step.
    ///
    /// The order of operations is the safety story, and none
    /// of it is skippable:
    ///
    ///   1. **Claim.** The work store is asked to claim the
    ///      record. Only an ADMIT-ted record claims; a
    ///      rejected, un-verdicted, executed, or unknown id
    ///      returns the store's `GaiaError` here and the
    ///      actuator is never built, let alone invoked.
    ///   2. **Act.** The [`FleetActuator`] runs the admitted
    ///      args verbatim through §5.3 — leash check first,
    ///      stale-state check second, primitive last. Its
    ///      answer is an outcome: executed, refused, or
    ///      failed, each already recorded in the outcome
    ///      store by the actuator itself.
    ///   3. **Complete.** The outcome becomes the work's
    ///      result (§3.3 step 4): a refusal is an *answer* to
    ///      the proposal, not a queue position — the record
    ///      completes with it, and re-asking means a new
    ///      intent from SI, never a silent retry of a stale
    ///      one.
    ///
    /// An `Err` from the actuator itself means the admitted
    /// args could not even be read (no model, no reference):
    /// the error propagates and the record stays `Admitted`,
    /// per the work store's contract — a malformed journaled
    /// intent is a defect to surface, not an outcome to file.
    pub fn actuate_work(
        &mut self,
        store: &mut dyn WorkStore,
        id: &str,
    ) -> Result<WorkRecord, GaiaError> {
        let claimed = store.claim_for_actuation(id)?;
        let result = {
            let mut actuator = FleetActuator {
                supervisor: &mut self.supervisor,
                specs: &self.specs,
                leash: &self.leash,
                outcomes: &mut self.outcomes,
                pull_cfg: &self.pull_cfg,
            };
            actuator.actuate(&claimed)?
        };
        store.complete(id, result)
    }
}
