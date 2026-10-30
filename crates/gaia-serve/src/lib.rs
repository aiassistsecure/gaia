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
//! Nothing in the step actuates anything.

pub mod proposals;
pub mod watcher;

pub use proposals::{Proposal, ProposalStore};
pub use watcher::{candidate, Candidate, Watcher};

use std::sync::{Arc, Mutex};

use gaia_api::mirror::poll_once;
use gaia_api::{EventFilter, EventHub, EventLog, MirrorStore, Source, StreamEvent};
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
}
