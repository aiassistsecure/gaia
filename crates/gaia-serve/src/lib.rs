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
//!
//! Proposals also have somewhere to go. The seam egress
//! (S-2b, G-9 §8) is a separate pass —
//! [`Composition::egress_pass`], driven by the same ticker
//! that steps this composition — which POSTs every proposal
//! SI has not yet ACKed to SI's proposal ingest and records
//! the disposition (acked / rejected / escalated / failed)
//! in the file-backed [`EgressStore`]. The pass is
//! deliberately *not* part of `tick_once`: observation must
//! not fail because SI is down, and an un-ACKed proposal is
//! simply re-sent on a later pass — SI's dedup by
//! `caused_by` makes the retry free (G-9 §8.3).

pub mod actuate;
pub mod egress;
pub mod egress_client;
pub mod leash;
pub mod leash_feed;
pub mod outcomes;
pub mod proposals;
pub mod watcher;

pub use actuate::FleetActuator;
pub use egress::{drive_egress, Disposition, EgressPass, EgressRecord, EgressStore};
pub use egress_client::{Ack, GateVerdict, HttpProposalSink, ProposalSink, SendOutcome};
pub use leash::{Leash, LeashKind, LeashRecord};
pub use leash_feed::{ApplyOutcome, LeashFeed, StaleCause};
pub use outcomes::{Outcome, OutcomeRecord, OutcomeStore};
pub use proposals::{Proposal, ProposalStore};
pub use watcher::{candidate, Candidate, Watcher};

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use gaia_api::mirror::poll_once;
use gaia_api::{
    Actuator, EventFilter, EventHub, EventLog, GaiaError, MirrorStore, SiEventBuffer, Source,
    StreamEvent, WorkRecord, WorkStore,
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

/// The leash routes' sink (S-4, G-9 §12.3): an adapter over
/// one composition's leash + leash feed, in the
/// [`SharedMirror`] pattern — the wire shape lives in
/// `gaia_api::leash`; the replica it feeds lives here. A
/// record the watermark refuses is an ACK with
/// `applied: false`, not an error; a store failure is an
/// error, because a stop SI journaled that this fork could
/// not record must be loud, never a cheerful ACK.
pub struct LeashSinkAdapter<'a> {
    feed: &'a mut LeashFeed,
    leash: &'a mut Leash,
}

impl gaia_api::LeashSink for LeashSinkAdapter<'_> {
    fn apply_leash_record(
        &mut self,
        record: &gaia_api::LeashRecordMsg,
    ) -> Result<gaia_api::LeashAck, String> {
        let rec = LeashRecord {
            model: record.model.clone(),
            kind: match record.kind.as_str() {
                "stop" => LeashKind::Stop,
                "resume" => LeashKind::Resume,
                // `gaia_api::leash::handle` validates the kind
                // before the sink is called; anything else
                // here is a direct caller bypassing the route,
                // and it is refused, not guessed at.
                other => return Err(format!("unknown leash kind {other:?}")),
            },
            journal_seq: record.journal_seq,
            reason: record.reason.clone(),
        };
        let outcome = self.feed.apply_record(self.leash, rec)?;
        Ok(gaia_api::LeashAck {
            applied: outcome == ApplyOutcome::Applied,
            last_leash_seq: self.feed.watermark(&record.model),
        })
    }

    fn leash_heartbeat(
        &mut self,
        heartbeat: &gaia_api::LeashHeartbeatMsg,
    ) -> Result<gaia_api::LeashAck, String> {
        self.feed
            .note_heartbeat(&heartbeat.model, heartbeat.leash_tip, heartbeat.journal_tip);
        Ok(gaia_api::LeashAck {
            applied: true,
            last_leash_seq: self.feed.watermark(&heartbeat.model),
        })
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
    /// The leash feed (S-4, G-9 §10.3): the watermark,
    /// heartbeat, and staleness state that turns `leash`
    /// from a locally-kept record into a derived replica of
    /// SI's journal — records arrive over the leash routes,
    /// apply in journal order under the watermark, and
    /// actuation refuses fail-closed when the replica is
    /// stale. Ephemeral until the binary installs the
    /// file-backed one ([`Composition::set_leash_feed`]).
    leash_feed: LeashFeed,
    /// Where re-pulls put their blobs (§5.3). The default is a
    /// relative `./blobs` with progress off — library-quiet;
    /// the binary points it at its home directory.
    pull_cfg: PullConfig,
    /// S-2b seam egress state (G-9 §8.4): the record of which
    /// proposals SI has answered, and how. Ephemeral until the
    /// binary installs the file-backed store under its home
    /// ([`Composition::set_egress_store`]) — restart-safe retry
    /// depends on the file, exactly as dedup depends on the
    /// proposal store's file.
    egress: EgressStore,
    /// The transport proposals are sent over. `None` until the
    /// binary installs one ([`Composition::set_egress_sink`]):
    /// a composition with no sink configured sends nothing and
    /// its proposals accumulate un-ACKed, which is the honest
    /// posture for "SI's ingest is not wired here" (G-9 §13.1).
    egress_sink: Option<Box<dyn ProposalSink + Send>>,
    /// The SI-source event buffer (S-6a, G-2 §4.2): the
    /// ingest history the events listener replays and tails
    /// for the `si` source. It is the composition's, not any
    /// one listener's — the agency listener's ingest route
    /// pushes into it and the events listener serves from it,
    /// so both read one history. Attached to the hub at
    /// construction ([`Composition::new`]), so pushed events
    /// fan out live as well as replay.
    si_events: SiEventBuffer,
}

impl Composition {
    pub fn new(
        supervisor: Supervisor,
        mirror: Arc<Mutex<MirrorStore>>,
        watcher: Watcher,
        proposals: ProposalStore,
        hub: EventHub,
    ) -> Composition {
        // The SI event buffer is created with the composition
        // and attached to its hub here, before any listener
        // exists, so a push that arrives ahead of the events
        // listener's first subscriber is still kept for replay
        // and fans out live from the first push onward.
        let si_events = SiEventBuffer::new();
        si_events.attach_hub(hub.clone());
        Composition {
            supervisor,
            mirror,
            watcher,
            proposals,
            hub,
            si_events,
            specs: HashMap::new(),
            outcomes: OutcomeStore::ephemeral(),
            leash: Leash::ephemeral(),
            leash_feed: LeashFeed::ephemeral(),
            pull_cfg: PullConfig {
                blobs_dir: PathBuf::from("./blobs"),
                progress: false,
                verify_existing: false,
            },
            egress: EgressStore::ephemeral(),
            egress_sink: None,
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

    /// The composition's SI event buffer. `SiEventBuffer` is
    /// `Clone` over `Arc`s — the returned clone shares the
    /// same history (a push through one clone is visible
    /// through the other), which is what lets the binary hand
    /// one clone to the events listener as its `si` log while
    /// the agency listener's ingest route pushes through
    /// another.
    pub fn si_events(&self) -> gaia_api::SiEventBuffer {
        self.si_events.clone()
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

    /// Install the file-backed leash feed (the binary opens
    /// it beside the leash, seeded from it). Until then the
    /// feed is ephemeral: watermarks and activation are
    /// forgotten on restart — which fails closed (an active
    /// model with no heartbeat in this process is stale),
    /// never open.
    pub fn set_leash_feed(&mut self, leash_feed: LeashFeed) {
        self.leash_feed = leash_feed;
    }

    /// The leash feed: watermark, heartbeat, and staleness
    /// state for the replica.
    pub fn leash_feed(&self) -> &LeashFeed {
        &self.leash_feed
    }

    /// The leash feed, mutably — the leash routes apply
    /// records and note heartbeats here.
    pub fn leash_feed_mut(&mut self) -> &mut LeashFeed {
        &mut self.leash_feed
    }

    /// Apply one leash record arriving over the seam (S-4):
    /// through the feed's watermark, into the leash. The
    /// composition owns both stores, so this — not two
    /// mutable accessors borrowed at once — is how a record
    /// is applied outside the route sink.
    pub fn apply_leash_record(&mut self, record: LeashRecord) -> Result<ApplyOutcome, String> {
        self.leash_feed.apply_record(&mut self.leash, record)
    }

    /// Apply a leash snapshot record (G-9 §10.5 re-sync):
    /// the same watermark path as an event record — the
    /// snapshot is the same shape, sent once per model.
    pub fn apply_leash_snapshot(&mut self, record: LeashRecord) -> Result<ApplyOutcome, String> {
        self.leash_feed.apply_snapshot(&mut self.leash, record)
    }

    /// Note a leash heartbeat from SI (G-9 §10.3.4): the
    /// feed stamps its local receipt time; the caller
    /// supplies SI's leash tip and journal tip as carried.
    pub fn note_leash_heartbeat(&mut self, model: &str, leash_tip: u64, journal_tip: u64) {
        self.leash_feed
            .note_heartbeat(model, leash_tip, journal_tip);
    }

    /// The sink the agency listener hands the leash routes:
    /// this composition's feed + leash behind the
    /// `gaia_api::LeashSink` boundary.
    pub fn leash_sink(&mut self) -> LeashSinkAdapter<'_> {
        LeashSinkAdapter {
            feed: &mut self.leash_feed,
            leash: &mut self.leash,
        }
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

    /// Install the file-backed egress store (the binary opens
    /// it under its home). Until then the store is ephemeral,
    /// and a restart would forget which proposals SI answered
    /// — re-sending them all, which SI's dedup absorbs but the
    /// operator should not have to rely on.
    pub fn set_egress_store(&mut self, egress: EgressStore) {
        self.egress = egress;
    }

    /// Install the seam egress transport (the binary builds an
    /// [`HttpProposalSink`] from its `--si-addr` flag). Until
    /// then [`Composition::egress_pass`] sends nothing.
    pub fn set_egress_sink(&mut self, sink: Box<dyn ProposalSink + Send>) {
        self.egress_sink = Some(sink);
    }

    /// The egress store: which proposals SI has answered, and
    /// how, in the order the answers were recorded.
    pub fn egress(&self) -> &EgressStore {
        &self.egress
    }

    /// One seam-egress pass (S-2b, G-9 §8.4): offer every
    /// proposal that has no terminal egress record to the
    /// sink, in proposal order, and record each answer.
    ///
    /// With no sink installed the pass is a no-op that reports
    /// zeros — "not configured" is a deployment fact, not an
    /// error, and the ticker should not log it as one. A store
    /// error (an ACK arrived but could not be recorded)
    /// propagates: the pass stops rather than claim answers
    /// the record does not hold, and the next pass re-sends —
    /// SI's `caused_by` dedup returns the identical ACK, so
    /// the re-send is free and the record heals.
    pub fn egress_pass(&mut self) -> Result<EgressPass, String> {
        let Some(sink) = &self.egress_sink else {
            return Ok(EgressPass::default());
        };
        // The proposal list is cloned so the drive can borrow
        // the egress store mutably while iterating it; the
        // list is small (one entry per observed loss) and the
        // clone is the honest price of the split.
        let proposals = self.proposals.proposals().to_vec();
        drive_egress(&proposals, &mut self.egress, sink.as_ref())
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
    ///      args verbatim through §5.3 — leash-feed staleness
    ///      first (S-4, for seam-active models), leash check
    ///      second, stale-state check third, primitive last.
    ///      Its
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
                leash_feed: Some(&self.leash_feed),
                outcomes: &mut self.outcomes,
                pull_cfg: &self.pull_cfg,
            };
            actuator.actuate(&claimed)?
        };
        store.complete(id, result)
    }
}
