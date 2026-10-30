//! The agency surface of the gaia fork.
//!
//! `hearth-api` answers one question honestly: can this model serve
//! right now, and if not, why not. This crate answers a different one:
//! has this piece of work been recorded, what state is it in, and what
//! did the gate say about it. SI — the mind — journals a tool intent,
//! hands the envelope to the runtime, and reads the outcome back with
//! its provenance attached. See [`gateway`] for the routes and
//! [`work`] for the lifecycle they move through.
//!
//! G-3 built the transport — submission, status, and verdict — and
//! the verdict state machine it carries. G-4b gives the store a
//! durable backing ([`work::FileWorkStore`], JSONL snapshots folded
//! on open; the in-memory store remains the test/ephemeral
//! implementation behind the same trait), and lets one events
//! subscription follow both sources at once with source-qualified
//! ids and per-source resume. G-4 adds the
//! event stream: [`events`] is the shared two-source vocabulary,
//! [`mirror`] is the hash-checked cache of the spine the `hearth`
//! source is served from, and [`sse`] is the server —
//! `GET /gaia/v1/events` on its own listener with its own
//! connection budget, per-source `Last-Event-ID` resume, filtered
//! subscriptions, resync-on-overflow, and heartbeats. G-5 adds
//! [`residency`]: `GET /gaia/v1/residency`, the structured state
//! (enum + `lost_reason`) a policy consumer reads, where
//! upstream's `/residency` renders only the human string. No
//! tool is executed here — actuation is G-6. S-4 adds
//! [`leash`]: the wire shape of the leash crossing
//! (`POST /gaia/v1/leash`, `POST /gaia/v1/leash/heartbeat`),
//! carried to a [`LeashSink`] the composition implements —
//! SI's journal is the leash's home; what crosses here is a
//! replica feed, never a second source of truth. S-6 adds
//! [`ingest`]: `POST /gaia/v1/events/ingest`, the transport that
//! feeds the `si` source — SI journal events in SI's own event
//! shape land in the [`sse::SiEventBuffer`], whose loud refusal
//! of a duplicate or out-of-order seq is surfaced as its own
//! error, never smoothed into a dedupe.

pub mod events;
pub mod gateway;
pub mod ingest;
pub mod leash;
pub mod mirror;
pub mod residency;
pub mod sse;
pub mod work;

pub use events::{EventFilter, EventLog, Source, StreamEvent};
pub use gateway::{classify, decide, Route};
pub use ingest::handle as ingest_handle;
pub use leash::{LeashAck, LeashHeartbeatMsg, LeashRecordMsg, LeashSink};
pub use mirror::MirrorStore;
pub use sse::{EventHub, EventsConfig, EventsServer, SiEventBuffer};
pub use work::{
    run_actuation, Actuator, FileWorkStore, GaiaError, Intent, MemoryWorkStore, Verdict,
    WorkRecord, WorkState, WorkStore,
};
