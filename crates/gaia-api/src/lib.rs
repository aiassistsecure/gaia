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
//! This is the skeleton slice (G-3), and it says so plainly: the store
//! behind the routes is in-memory and dies with the process, there is
//! no SSE event stream and no `/gaia/v1/residency` yet, and no tool is
//! executed here. What exists is the transport — submission, status,
//! and verdict — and the verdict state machine it carries. Persistence
//! and actuation are later slices (G-4 and G-6).

pub mod gateway;
pub mod work;

pub use gateway::{classify, decide, Route};
pub use work::{
    run_actuation, Actuator, GaiaError, Intent, MemoryWorkStore, Verdict, WorkRecord, WorkState,
    WorkStore,
};
