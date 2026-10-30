//! The leash feed: SI's leash state, replicated to the fork.
//!
//! G-9 §10.1: **the leash lives in SI's journal.** The
//! operator's stop and resume are SI journal acts; the fold
//! over `queue_stop` / `queue_resume` rows is the state, and
//! a restart never clears a stop because the fold is all
//! there is. The fork's [`crate::leash::Leash`] store is a
//! **derived replica** of that state for the models the fork
//! serves — a cache with exactly one writer path (this
//! module), rebuildable from SI at any time, never
//! authoritative. If the two disagree, SI's journal wins and
//! the replica is re-synced (§10.5). The replica's only job
//! is to be *fresh enough at actuation time* (§10.3) on the
//! fork's side of the wire, and this module is what keeps it
//! fresh — and what says, honestly, when it is not.
//!
//! ## The watermark rule (§10.3.2)
//!
//! Every leash record carries the `journal_seq` of the SI
//! journal row that recorded it. This feed keeps, per model,
//! `last_leash_seq[model]` — the **watermark**: the highest
//! leash `journal_seq` applied for that model. A record with
//! `journal_seq <= watermark` is a duplicate or an
//! out-of-order arrival: it is ACKed by the caller (so the
//! sender stops retrying) but **not applied** here, and it
//! teaches this feed nothing — not its kind, not its tip.
//!
//! The watermark kills the one dangerous case. The bare
//! [`crate::leash::Leash`] store is latest-*arrival*-wins: a
//! delayed duplicate `resume`, retried by the transport and
//! landing after a newer `stop`, would under arrival order
//! silently lift the stop. Under the watermark,
//! latest-*journaled* wins and arrival order is irrelevant:
//! the delayed resume's sequence is at or below the
//! watermark the newer stop set, so it is a duplicate, and
//! the stop stands. Symmetrically, a delayed old `stop`
//! landing after a newer `resume` cannot manufacture a
//! phantom stop. Application is watermark-ordered, so the
//! replica's spoken state for a model is always the state
//! of the highest journal sequence applied for it.
//!
//! Records apply through the existing
//! [`crate::leash::Leash::record_stop`] /
//! [`crate::leash::Leash::record_resume`] — this module mints
//! no leash records of its own (§10.4: no fork-originated
//! leash records, ever) and is the only path by which seam
//! records reach the store.
//!
//! ## Staleness (§10.3.4)
//!
//! Freshness is a heartbeat discipline. While the seam is
//! active for a model, heartbeats arrive carrying SI's leash
//! tip for the model; this feed stamps each with its **local
//! monotonic** receipt time (the seam never compares wall
//! clocks across the wire). Leash state for a model is
//! **stale — and actuation for that model must refuse,
//! fail-closed — when any of:**
//!
//!   * no heartbeat has been received within the TTL, or
//!     none ever;
//!   * the watermark is behind a known tip (learned from a
//!     heartbeat, a piggybacked tip, or an applied record)
//!     by any amount — the feed knows records are missing
//!     even though it does not know their content; or
//!   * the feed has reported a transport fault it has not
//!     since recovered from. Recovery is a subsequent
//!     *successful apply* of a record or a snapshot: a
//!     heartbeat alone does **not** clear a fault, because
//!     a heartbeat carries a tip, not record content — it
//!     proves the sender is alive, not that the missed
//!     record arrived.
//!
//! [`LeashFeed::staleness`] checks the causes in exactly
//! that order and names the first that fires, as a
//! [`StaleCause`] whose [`StaleCause::describe`] text the
//! actuator composes into the §12.4 refusal detail
//! (`"leash state stale: <describe>; watermark <n>"`).
//! Staleness is per-model, matching the leash's own scope.
//!
//! Staleness only exists once the seam is **active** for a
//! model. Before the feed engages for a model, as-built
//! behaviour is unchanged (§10.4): a model with no seam
//! activity is not stale, it is simply not yet governed by
//! the feed — the fail-closed rule arrives with the feed
//! that makes it satisfiable, not before. Activation is by
//! seam traffic for the model: an applied record, a
//! heartbeat, or a piggybacked tip (see
//! [`LeashFeed::note_tip`] for that choice).
//!
//! ## No stop expiry, ever (§10.4)
//!
//! A stop does not lapse on a clock. The heartbeat TTL gates
//! *actuation freshness*, never the stop's validity: a stale
//! feed refuses everything for the model; it never resumes
//! anything. A resume is always an explicit journaled act
//! that crossed this feed as a record, or it did not happen.
//!
//! ## Re-sync (§10.5)
//!
//! On fork start, and whenever staleness fires persistently,
//! SI re-sends the current folded state per model as a
//! **snapshot**: a single record of the *same shape* as an
//! event record, applied under the same watermark rule via
//! [`LeashFeed::apply_snapshot`]. There is no separate
//! snapshot type and no second state machine — a snapshot
//! older than the applied state is a duplicate, a newer one
//! applies and replaces the model's spoken state. The
//! folded-state guarantee (that the snapshot record *is*
//! the model's current fold) is the sender's; this feed's
//! half of the contract is only the watermark discipline,
//! which it shares with event records exactly.
//!
//! ## Policy constants (UNMEASURED)
//!
//! The heartbeat interval (10 s, sender-side) and the TTL
//! ([`DEFAULT_HEARTBEAT_TTL`], 30 s) are starting values
//! from §10.3.4 — **UNMEASURED policy constants**, priced in
//! G-9 §16. No number in this module should be quoted as
//! measured; a measurement slice may move either.
//!
//! ## Persistence
//!
//! The durable state is one JSON object at the feed's path:
//! `{"models": {"<model>": {"last_leash_seq": n,
//! "known_tip": n, "active": bool}}}` — per-model watermark,
//! known tip, and activation. It is rewritten **atomically**
//! on every mutation (write a `.tmp` sibling, then rename),
//! because a torn watermark file is not a cosmetic problem:
//! [`LeashFeed::open`] treats a corrupt or unreadable state
//! file as fatal rather than guess, since a silently
//! dropped watermark could resurrect a lifted stop (the
//! mirror image of the dangerous case above). Two pieces of
//! state are deliberately **runtime-only, never persisted**:
//! heartbeat receipt stamps (a restarted fork has received
//! no heartbeat *in this process*, so an active model is
//! stale by never-received until the first heartbeat of the
//! new boot arrives — G-9 §15 S-4 acceptance 5) and the
//! fault flag (a restart is stale on the heartbeat cause
//! first, so no fail-open is bought by forgetting a fault).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use crate::leash::{Leash, LeashKind, LeashRecord};

/// The starting heartbeat TTL (§10.3.4): leash state for a
/// model is stale when no heartbeat has been received
/// within this window. An UNMEASURED policy constant —
/// a starting value, not a measured bound.
pub const DEFAULT_HEARTBEAT_TTL: Duration = Duration::from_secs(30);

/// Why a model's leash state is stale (§10.3.4), in the
/// order [`LeashFeed::staleness`] checks them. The actuator
/// composes the §12.4 refusal detail as
/// `"leash state stale: <describe>; watermark <n>"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StaleCause {
    /// The seam is active for the model but no heartbeat
    /// has been received in this process — none ever, or
    /// none since a restart (heartbeat stamps are
    /// runtime-only by design; see the module docs).
    NeverReceived,
    /// A heartbeat was received, but longer ago than the
    /// TTL. The feed has gone silent; freshness is gone
    /// even though the replica's content may be complete.
    HeartbeatExpired,
    /// The watermark is behind a tip the feed has been told
    /// about: records exist at SI that this replica has not
    /// applied. The feed knows it is missing records even
    /// though it does not know their content.
    BehindTip { watermark: u64, tip: u64 },
    /// Applying a record failed at the leash store. The
    /// record was not applied — loudly, as an `Err` — and
    /// the model stays stale on this cause until a
    /// successful apply or snapshot recovers the feed
    /// (a heartbeat does not; see the module docs).
    TransportFault,
}

impl StaleCause {
    /// The cause in words, for the §12.4 refusal detail.
    /// These spellings are wire-visible in outcome records;
    /// they are the §10.3.4 causes named plainly, with the
    /// known tip quoted for the behind-tip case so an
    /// operator can see how far behind the replica is
    /// believed to be.
    pub fn describe(&self) -> String {
        match self {
            StaleCause::NeverReceived => "no heartbeat received".to_string(),
            StaleCause::HeartbeatExpired => "no heartbeat within TTL".to_string(),
            StaleCause::BehindTip { tip, .. } => format!("behind known leash tip {tip}"),
            StaleCause::TransportFault => "leash feed transport fault unrecovered".to_string(),
        }
    }
}

/// What applying one record did (§10.3.2): `Applied` records
/// moved the replica; `Duplicate` records were ACK-worthy
/// no-ops under the watermark. Both are `Ok` — a duplicate
/// is the dedup discipline working, not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOutcome {
    Applied,
    Duplicate,
}

/// Per-model feed state. The first three fields are the
/// durable state (persisted; see the module docs); the rest
/// are runtime-only and start unset on every open.
#[derive(Debug, Clone, Default)]
struct ModelState {
    /// The watermark: highest leash `journal_seq` applied
    /// for this model (`last_leash_seq` on disk).
    watermark: u64,
    /// The highest leash tip learned for this model — from
    /// a heartbeat, a piggybacked tip, or an applied
    /// record's own sequence. `watermark < known_tip` is
    /// the behind-tip staleness cause.
    known_tip: u64,
    /// Whether the seam is active for this model (§10.4:
    /// staleness exists only once it is).
    active: bool,
    /// Local monotonic receipt time of the latest
    /// heartbeat. Runtime-only, NEVER persisted.
    last_heartbeat: Option<Instant>,
    /// An unrecovered transport fault (a failed apply).
    /// Runtime-only; cleared only by a successful apply.
    fault: bool,
    /// The `journal_tip` of the latest heartbeat, kept for
    /// provenance/future use. No staleness rule reads it in
    /// this slice — SI's journal tip is not the leash tip,
    /// and inventing a rule that compares them would merge
    /// two facts §10 keeps distinct. Runtime-only.
    last_journal_tip: Option<u64>,
}

/// The leash feed: the watermark, staleness, and re-sync
/// discipline over the fork's [`Leash`] replica.
///
/// The feed owns no records — records live in the `Leash`,
/// applied through it — and it is the *only* writer path
/// from the seam to that store. Its own state (watermarks,
/// tips, activation, heartbeat stamps, faults) is what
/// turns the store's latest-arrival-wins list into the
/// watermark-ordered replica §10.3 specifies, and what lets
/// the actuator ask the §10.3.4 question — "is this model's
/// leash state fresh enough to act on?" — before it acts.
pub struct LeashFeed {
    /// The durable state file, if any. An ephemeral feed has
    /// none: its state lives in memory only and dies with
    /// the process.
    path: Option<PathBuf>,
    models: HashMap<String, ModelState>,
    ttl: Duration,
}

impl LeashFeed {
    /// A feed with no file behind it and no seeding: every
    /// model starts unknown (watermark 0, inactive).
    pub fn ephemeral() -> LeashFeed {
        LeashFeed {
            path: None,
            models: HashMap::new(),
            ttl: DEFAULT_HEARTBEAT_TTL,
        }
    }

    /// Open the feed at `path`, over the replica `leash`
    /// the caller already opened.
    ///
    /// A missing state file is fresh state **seeded from
    /// the passed leash**: per model, the watermark is the
    /// highest `journal_seq` among the model's existing
    /// leash records, the known tip is the watermark (the
    /// replica knows of nothing beyond what it holds), and
    /// the model is *inactive* — pre-S-4 records exist, but
    /// the feed has not engaged for the model, so §10.4
    /// leaves as-built behaviour unchanged until it does.
    /// Seeding matters: without it a restart would reset
    /// the watermark to 0 and re-admit the delayed-duplicate
    /// records the watermark exists to refuse.
    ///
    /// An existing file is parsed strictly: a corrupt or
    /// unreadable file is `Err`, fatal like
    /// [`Leash::open`] — a silently dropped watermark could
    /// resurrect a lifted stop, which is worse than not
    /// starting. Heartbeat stamps and fault flags always
    /// start unset after an open, whatever the file says
    /// (they are never written to it): a restarted fork has
    /// received no heartbeat in this process.
    pub fn open(path: &Path, leash: &Leash) -> Result<LeashFeed, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut models: HashMap<String, ModelState> = HashMap::new();
                for record in leash.records() {
                    let state = models.entry(record.model.clone()).or_default();
                    state.watermark = state.watermark.max(record.journal_seq);
                }
                for state in models.values_mut() {
                    state.known_tip = state.watermark;
                    // `active` stays false, heartbeat and
                    // fault stay unset: seeded, not engaged.
                }
                return Ok(LeashFeed {
                    path: Some(path.to_path_buf()),
                    models,
                    ttl: DEFAULT_HEARTBEAT_TTL,
                });
            }
            Err(e) => {
                return Err(format!(
                    "cannot read leash feed state {}: {e}",
                    path.display()
                ));
            }
        };

        let value: Value = serde_json::from_str(&text)
            .map_err(|e| format!("cannot parse leash feed state {}: {e}", path.display()))?;
        let models_obj = value
            .as_object()
            .and_then(|obj| obj.get("models"))
            .and_then(|v| v.as_object())
            .ok_or_else(|| {
                format!(
                    "leash feed state {} is not a {{\"models\": {{…}}}} object",
                    path.display()
                )
            })?;
        let mut models = HashMap::new();
        for (model, entry) in models_obj {
            let entry_obj = entry.as_object().ok_or_else(|| {
                format!(
                    "leash feed state {}: entry for model {model:?} is not an object",
                    path.display()
                )
            })?;
            let field = |name: &str| -> Result<&Value, String> {
                entry_obj.get(name).ok_or_else(|| {
                    format!(
                        "leash feed state {}: entry for model {model:?} is missing {name:?}",
                        path.display()
                    )
                })
            };
            let watermark = field("last_leash_seq")?.as_u64().ok_or_else(|| {
                format!(
                    "leash feed state {}: last_leash_seq for model {model:?} is not a u64",
                    path.display()
                )
            })?;
            let known_tip = field("known_tip")?.as_u64().ok_or_else(|| {
                format!(
                    "leash feed state {}: known_tip for model {model:?} is not a u64",
                    path.display()
                )
            })?;
            let active = field("active")?.as_bool().ok_or_else(|| {
                format!(
                    "leash feed state {}: active for model {model:?} is not a bool",
                    path.display()
                )
            })?;
            models.insert(
                model.clone(),
                ModelState {
                    watermark,
                    known_tip,
                    active,
                    last_heartbeat: None,
                    fault: false,
                    last_journal_tip: None,
                },
            );
        }
        Ok(LeashFeed {
            path: Some(path.to_path_buf()),
            models,
            ttl: DEFAULT_HEARTBEAT_TTL,
        })
    }

    /// Set the heartbeat TTL (§10.3.4). A policy knob, not a
    /// measured bound — see [`DEFAULT_HEARTBEAT_TTL`].
    pub fn set_ttl(&mut self, ttl: Duration) {
        self.ttl = ttl;
    }

    /// The heartbeat TTL in force.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Apply one event record (§10.3.1) to the replica,
    /// under the watermark rule. See [`LeashFeed::apply`]
    /// for the shared path; event records and snapshots
    /// differ only in what the sender guarantees, never in
    /// how this feed applies them.
    pub fn apply_record(
        &mut self,
        leash: &mut Leash,
        record: LeashRecord,
    ) -> Result<ApplyOutcome, String> {
        self.apply(leash, record)
    }

    /// Apply one snapshot record (§10.5): SI's current
    /// folded state for the model, as a single record of the
    /// same shape as an event record.
    ///
    /// The path is identical to [`LeashFeed::apply_record`]
    /// by design — the snapshot is a convenience for
    /// recovery, not a second state machine. The folded-
    /// state guarantee is the sender's; this feed's half is
    /// the watermark: a snapshot older than the applied
    /// state is a [`ApplyOutcome::Duplicate`], and a newer
    /// one applies and replaces the model's spoken state —
    /// sound because `stop_in_force` follows the latest
    /// *applied* record, and watermark-ordered application
    /// makes the latest applied record the highest
    /// journal sequence, never a stale straggler. A
    /// successful snapshot is also a recovery path: it
    /// clears an unrecovered transport fault, because the
    /// record content the fault was about has now arrived
    /// (in folded form) and applied.
    pub fn apply_snapshot(
        &mut self,
        leash: &mut Leash,
        record: LeashRecord,
    ) -> Result<ApplyOutcome, String> {
        self.apply(leash, record)
    }

    /// The one writer path (§10.3.1/§10.3.2/§10.5), shared
    /// by event records and snapshots.
    ///
    /// A record at or below the watermark is a duplicate:
    /// `Ok(Duplicate)`, the leash untouched, and *no* state
    /// change at all — a duplicate teaches nothing, so it
    /// does not move the tip, does not activate the model,
    /// and does not clear (or set) a fault.
    ///
    /// A newer record applies through the leash store. On
    /// success the watermark becomes the record's sequence,
    /// the known tip rises to at least it, the model
    /// activates (seam traffic for the model is the feed
    /// engaging), any fault clears — a successful apply *is*
    /// the recovery §10.3.4 names — and the state persists.
    /// On a store error the record is **not** applied and
    /// nothing persists: the error returns loudly, and the
    /// fault flag is set so the model reads stale until a
    /// successful apply or snapshot recovers it. An
    /// unapplied record must never fail silently into a
    /// replica that looks fresh.
    ///
    /// If the leash apply succeeded but persisting this
    /// feed's own state fails, that error also returns (and
    /// also sets the fault): the replica moved but its
    /// watermark is not durable, so claiming `Applied`
    /// would overstate what a restart would preserve.
    fn apply(&mut self, leash: &mut Leash, record: LeashRecord) -> Result<ApplyOutcome, String> {
        if record.journal_seq <= self.watermark(&record.model) {
            return Ok(ApplyOutcome::Duplicate);
        }
        let applied = match record.kind {
            LeashKind::Stop => leash.record_stop(&record.model, record.journal_seq, &record.reason),
            LeashKind::Resume => {
                leash.record_resume(&record.model, record.journal_seq, &record.reason)
            }
        };
        match applied {
            Ok(_) => {
                {
                    let state = self.models.entry(record.model.clone()).or_default();
                    state.watermark = record.journal_seq;
                    state.known_tip = state.known_tip.max(record.journal_seq);
                    state.active = true;
                    state.fault = false;
                }
                if let Err(e) = self.persist() {
                    self.models.entry(record.model.clone()).or_default().fault = true;
                    return Err(e);
                }
                Ok(ApplyOutcome::Applied)
            }
            Err(e) => {
                self.models.entry(record.model.clone()).or_default().fault = true;
                Err(e)
            }
        }
    }

    /// Note a heartbeat for `model` (§10.3.4): stamp the
    /// local monotonic receipt time, learn SI's leash tip,
    /// and mark the seam active for the model.
    ///
    /// A heartbeat does **not** clear a transport fault: it
    /// carries a tip, not record content, so it proves the
    /// sender is alive without proving the record the fault
    /// was about ever arrived. Recovery is a successful
    /// apply or snapshot, and only that (§10.3.4).
    ///
    /// `journal_tip` is carried for provenance/future use
    /// and stored as the model's last-seen journal tip; no
    /// staleness rule reads it in this slice.
    ///
    /// The signature has no error channel, so persisting the
    /// activation/tip is best-effort here: the in-memory
    /// state is correct for this process, and the next
    /// mutation rewrites the whole state file regardless.
    pub fn note_heartbeat(&mut self, model: &str, leash_tip: u64, journal_tip: u64) {
        {
            let state = self.models.entry(model.to_string()).or_default();
            state.last_heartbeat = Some(Instant::now());
            state.known_tip = state.known_tip.max(leash_tip);
            state.active = true;
            state.last_journal_tip = Some(journal_tip);
        }
        let _ = self.persist();
    }

    /// Note a piggybacked leash tip for `model` (§10.3.3): a
    /// tip learned from a Flow-1 ACK or a Flow-2 message
    /// context, rather than from a heartbeat or a record.
    ///
    /// Choice, documented: a piggybacked tip **activates**
    /// the model. §10.3.3 is part of the active seam — a tip
    /// is only ever reported in the context of seam traffic
    /// for that model — so a model SI is quoting leash tips
    /// for is a model the feed has engaged for, and its
    /// state must satisfy the freshness rule rather than
    /// slip past it as "never active". Activation without a
    /// heartbeat reads stale by never-received, which is
    /// the honest posture for "SI mentioned this model's
    /// leash but the heartbeat stream has not spoken".
    ///
    /// Best-effort persistence, as in
    /// [`LeashFeed::note_heartbeat`].
    pub fn note_tip(&mut self, model: &str, tip: u64) {
        {
            let state = self.models.entry(model.to_string()).or_default();
            state.known_tip = state.known_tip.max(tip);
            state.active = true;
        }
        let _ = self.persist();
    }

    /// The watermark for `model`: the highest leash
    /// `journal_seq` applied for it, or 0 for a model the
    /// feed has never seen (no record can have applied
    /// below SI's first sequence and been missed — 0 is
    /// below every real journal sequence).
    pub fn watermark(&self, model: &str) -> u64 {
        self.models.get(model).map_or(0, |s| s.watermark)
    }

    /// Whether the seam is active for `model` (§10.4):
    /// staleness applies only to active models.
    pub fn is_active(&self, model: &str) -> bool {
        self.models.get(model).is_some_and(|s| s.active)
    }

    /// Is `model`'s leash state stale at `now`, and why
    /// (§10.3.4)? `None` means fresh — including for a model
    /// the seam is not active for, where the feed has no
    /// freshness claim to make at all (§10.4).
    ///
    /// For an active model the causes are checked in
    /// §10.3.4's order: heartbeat freshness first (none
    /// ever, then expired), then watermark behind a known
    /// tip, then an unrecovered transport fault. The first
    /// that fires is the answer, so a refusal names one
    /// cause — the earliest broken link in the chain — not
    /// every problem at once.
    ///
    /// `now` is a parameter, not read here, so callers and
    /// tests reason about fabricated instants against the
    /// stamped receipt times deterministically.
    pub fn staleness(&self, model: &str, now: Instant) -> Option<StaleCause> {
        let state = self.models.get(model)?;
        if !state.active {
            return None;
        }
        match state.last_heartbeat {
            None => return Some(StaleCause::NeverReceived),
            Some(last) if now.duration_since(last) > self.ttl => {
                return Some(StaleCause::HeartbeatExpired);
            }
            Some(_) => {}
        }
        if state.watermark < state.known_tip {
            return Some(StaleCause::BehindTip {
                watermark: state.watermark,
                tip: state.known_tip,
            });
        }
        if state.fault {
            return Some(StaleCause::TransportFault);
        }
        None
    }

    /// Rewrite the durable state atomically: serialise the
    /// whole `{"models": {…}}` object to a `.tmp` sibling of
    /// the state file, then rename it over the file, so a
    /// reader (or a crash) only ever sees the old state or
    /// the new one, never a torn half. An ephemeral feed has
    /// no file and persists nothing, successfully.
    ///
    /// Only watermark / known tip / active are written —
    /// heartbeat stamps, fault flags, and journal tips are
    /// runtime-only by design (see the module docs).
    fn persist(&self) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "cannot create leash feed state directory {}: {e}",
                        parent.display()
                    )
                })?;
            }
        }
        let mut models = Map::new();
        // Sorted by model name, so the file is stable to
        // read and to diff — the map's own order would be
        // hash order, which is neither.
        let mut names: Vec<&String> = self.models.keys().collect();
        names.sort();
        for name in names {
            let state = &self.models[name];
            models.insert(
                name.clone(),
                json!({
                    "last_leash_seq": state.watermark,
                    "known_tip": state.known_tip,
                    "active": state.active,
                }),
            );
        }
        let text = json!({ "models": models }).to_string();
        let tmp = tmp_path(path);
        std::fs::write(&tmp, text)
            .map_err(|e| format!("cannot write leash feed state {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path).map_err(|e| {
            format!(
                "cannot rename leash feed state {} over {}: {e}",
                tmp.display(),
                path.display()
            )
        })?;
        Ok(())
    }
}

/// The `.tmp` sibling `persist` writes before renaming: the
/// state file's full name plus `.tmp`, so `leash-feed.json`
/// stages as `leash-feed.json.tmp` in the same directory
/// (rename is only atomic within one filesystem).
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(model: &str, kind: LeashKind, journal_seq: u64, reason: &str) -> LeashRecord {
        LeashRecord {
            model: model.to_string(),
            kind,
            journal_seq,
            reason: reason.to_string(),
        }
    }

    fn stop(model: &str, journal_seq: u64) -> LeashRecord {
        record(model, LeashKind::Stop, journal_seq, "operator stop")
    }

    fn resume(model: &str, journal_seq: u64) -> LeashRecord {
        record(model, LeashKind::Resume, journal_seq, "operator resume")
    }

    #[test]
    fn stale_cause_descriptions_are_the_seam_spellings() {
        assert_eq!(
            StaleCause::NeverReceived.describe(),
            "no heartbeat received"
        );
        assert_eq!(
            StaleCause::HeartbeatExpired.describe(),
            "no heartbeat within TTL"
        );
        assert_eq!(
            StaleCause::BehindTip {
                watermark: 12,
                tip: 15,
            }
            .describe(),
            "behind known leash tip 15"
        );
        assert_eq!(
            StaleCause::TransportFault.describe(),
            "leash feed transport fault unrecovered"
        );
        assert_eq!(DEFAULT_HEARTBEAT_TTL, Duration::from_secs(30));
        assert_eq!(LeashFeed::ephemeral().ttl(), DEFAULT_HEARTBEAT_TTL);
    }

    #[test]
    fn watermark_duplicate_is_a_noop_and_does_not_touch_the_leash() {
        let mut feed = LeashFeed::ephemeral();
        let mut leash = Leash::ephemeral();

        assert_eq!(
            feed.apply_record(&mut leash, stop("muse", 10)).unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(leash.records().len(), 1);
        assert!(leash.is_stopped("muse"));

        // The same sequence again — even carrying the
        // *opposite* kind — is a duplicate: not applied, the
        // leash byte-for-byte untouched, the stop still the
        // spoken state, and nothing learned from it.
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 10)).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert_eq!(leash.records().len(), 1);
        assert!(leash.is_stopped("muse"));
        assert_eq!(feed.watermark("muse"), 10);

        // And a strictly older record is a duplicate too.
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 4)).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert_eq!(leash.records().len(), 1);
        assert!(leash.is_stopped("muse"));
    }

    #[test]
    fn out_of_order_arrivals_cannot_lift_a_stop_or_invent_one() {
        let mut feed = LeashFeed::ephemeral();
        let mut leash = Leash::ephemeral();

        // In order: stop@10, then resume@12 lifts it.
        assert_eq!(
            feed.apply_record(&mut leash, stop("muse", 10)).unwrap(),
            ApplyOutcome::Applied
        );
        assert!(leash.is_stopped("muse"));
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 12)).unwrap(),
            ApplyOutcome::Applied
        );
        assert!(!leash.is_stopped("muse"));

        // The transport retries resume@12: a duplicate, and
        // the (still lifted) state does not change.
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 12)).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert!(!leash.is_stopped("muse"));

        // A newer stop@15 lands. Now the *delayed* duplicate
        // resume@12 arrives again — the dangerous case of
        // §10.3.2. Under latest-arrival-wins it would lift
        // stop@15; under the watermark it is a duplicate and
        // the stop stands.
        assert_eq!(
            feed.apply_record(&mut leash, stop("muse", 15)).unwrap(),
            ApplyOutcome::Applied
        );
        assert!(leash.is_stopped("muse"));
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 12)).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert!(leash.is_stopped("muse"));
        assert_eq!(feed.watermark("muse"), 15);
        // Only the three applied records ever reached the
        // leash; both duplicates were refused at the feed.
        assert_eq!(leash.records().len(), 3);

        // The mirror case: a genuine resume@20 lifts the
        // stop, and then a delayed stop@9 straggles in. It
        // is a duplicate — no phantom stop appears.
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 20)).unwrap(),
            ApplyOutcome::Applied
        );
        assert!(!leash.is_stopped("muse"));
        assert_eq!(
            feed.apply_record(&mut leash, stop("muse", 9)).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert!(!leash.is_stopped("muse"));
        assert_eq!(leash.records().len(), 4);
        assert_eq!(feed.watermark("muse"), 20);
    }

    #[test]
    fn snapshot_replica_matches_the_record_by_record_replica() {
        // Replica A hears every event: stop@10, resume@12,
        // stop@15.
        let mut feed_a = LeashFeed::ephemeral();
        let mut leash_a = Leash::ephemeral();
        feed_a.apply_record(&mut leash_a, stop("muse", 10)).unwrap();
        feed_a
            .apply_record(&mut leash_a, resume("muse", 12))
            .unwrap();
        feed_a.apply_record(&mut leash_a, stop("muse", 15)).unwrap();

        // Replica B hears stop@10, misses resume@12, and is
        // re-synced by a snapshot carrying the folded state:
        // stop@15, the same record shape (§10.5).
        let mut feed_b = LeashFeed::ephemeral();
        let mut leash_b = Leash::ephemeral();
        feed_b.apply_record(&mut leash_b, stop("muse", 10)).unwrap();
        assert_eq!(
            feed_b
                .apply_snapshot(&mut leash_b, stop("muse", 15))
                .unwrap(),
            ApplyOutcome::Applied
        );

        // The two replicas agree on everything the actuator
        // or the staleness check can ask: the stop in force,
        // the watermark, and the known tip.
        assert_eq!(leash_a.stop_in_force("muse"), leash_b.stop_in_force("muse"));
        assert!(leash_b.is_stopped("muse"));
        assert_eq!(feed_a.watermark("muse"), feed_b.watermark("muse"));
        assert_eq!(feed_b.watermark("muse"), 15);

        // known_tip equality, observed through the public
        // surface: quote both feeds a tip beyond the
        // watermark and compare the behind-tip causes they
        // report once a heartbeat makes the earlier causes
        // pass.
        feed_a.note_heartbeat("muse", 15, 99);
        feed_b.note_heartbeat("muse", 15, 99);
        assert_eq!(
            feed_a.staleness("muse", Instant::now()),
            feed_b.staleness("muse", Instant::now())
        );
        feed_a.note_tip("muse", 21);
        feed_b.note_tip("muse", 21);
        let expected = Some(StaleCause::BehindTip {
            watermark: 15,
            tip: 21,
        });
        assert_eq!(feed_a.staleness("muse", Instant::now()), expected);
        assert_eq!(feed_b.staleness("muse", Instant::now()), expected);

        // A snapshot older than the applied state is a
        // duplicate under the same rule — it does not roll
        // the replica back to the resume it carries.
        assert_eq!(
            feed_b
                .apply_snapshot(&mut leash_b, resume("muse", 12))
                .unwrap(),
            ApplyOutcome::Duplicate
        );
        assert!(leash_b.is_stopped("muse"));
    }

    #[test]
    fn staleness_follows_the_section_10_3_4_causes_in_order() {
        let mut feed = LeashFeed::ephemeral();
        let now = Instant::now();

        // Inactive model: the feed makes no freshness claim
        // (§10.4) — unknown and known-but-inactive alike.
        assert_eq!(feed.staleness("muse", now), None);
        assert!(!feed.is_active("muse"));

        // Active (an applied record activates) but no
        // heartbeat ever in this process: NeverReceived.
        let mut leash = Leash::ephemeral();
        feed.apply_record(&mut leash, stop("muse", 10)).unwrap();
        assert!(feed.is_active("muse"));
        assert_eq!(
            feed.staleness("muse", Instant::now()),
            Some(StaleCause::NeverReceived)
        );

        // A heartbeat at tip 10 (== watermark) makes the
        // model fresh right now…
        feed.note_heartbeat("muse", 10, 10);
        assert_eq!(feed.staleness("muse", Instant::now()), None);

        // …and stale by expiry once `now` is fabricated past
        // the TTL from the stamped receipt.
        let after_ttl = Instant::now() + feed.ttl() + Duration::from_secs(1);
        assert_eq!(
            feed.staleness("muse", after_ttl),
            Some(StaleCause::HeartbeatExpired)
        );

        // Within the TTL, a tip beyond the watermark is the
        // behind-tip cause, quoting both numbers.
        feed.note_tip("muse", 14);
        assert_eq!(
            feed.staleness("muse", Instant::now()),
            Some(StaleCause::BehindTip {
                watermark: 10,
                tip: 14,
            })
        );

        // TTL is a policy knob: a shorter one expires the
        // same heartbeat sooner.
        feed.set_ttl(Duration::from_secs(5));
        assert_eq!(feed.ttl(), Duration::from_secs(5));
        assert_eq!(
            feed.staleness("muse", Instant::now() + Duration::from_secs(6)),
            Some(StaleCause::HeartbeatExpired)
        );
    }

    #[test]
    fn a_failed_apply_is_a_fault_until_a_successful_snapshot_recovers() {
        let dir = tempfile::tempdir().unwrap();
        // A leash whose appends must fail: open it while its
        // parent path does not exist (a missing file is an
        // empty store), then put a regular file where the
        // parent directory would have to be.
        let leash_path = dir.path().join("blocker").join("leash.jsonl");
        let mut bad_leash = Leash::open(&leash_path).unwrap();
        std::fs::write(dir.path().join("blocker"), "not a directory").unwrap();

        let mut feed = LeashFeed::ephemeral();
        // Fresh heartbeat, tip == watermark: neither
        // heartbeat cause nor the behind-tip cause can fire,
        // so the fault — once set — is the cause that reads.
        feed.note_heartbeat("muse", 0, 0);
        assert_eq!(feed.staleness("muse", Instant::now()), None);

        // The apply fails at the store: loud Err, nothing
        // applied, watermark unmoved — and the model is now
        // stale by transport fault.
        let result = feed.apply_record(&mut bad_leash, stop("muse", 10));
        assert!(result.is_err(), "an unapplied record is loud: {result:?}");
        assert_eq!(feed.watermark("muse"), 0);
        assert!(bad_leash.records().is_empty());
        assert_eq!(
            feed.staleness("muse", Instant::now()),
            Some(StaleCause::TransportFault)
        );

        // A heartbeat does NOT clear the fault (§10.3.4: it
        // carries no record content).
        feed.note_heartbeat("muse", 0, 0);
        assert_eq!(
            feed.staleness("muse", Instant::now()),
            Some(StaleCause::TransportFault)
        );

        // A successful snapshot against a working leash IS
        // the recovery: fault cleared, watermark advanced,
        // model fresh again.
        let mut good_leash = Leash::ephemeral();
        assert_eq!(
            feed.apply_snapshot(&mut good_leash, stop("muse", 15))
                .unwrap(),
            ApplyOutcome::Applied
        );
        assert_eq!(feed.watermark("muse"), 15);
        assert_eq!(feed.staleness("muse", Instant::now()), None);
    }

    #[test]
    fn persistence_round_trip_keeps_watermark_and_active_but_not_the_heartbeat() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leash-feed.json");

        {
            let leash_seed = Leash::ephemeral();
            let mut feed = LeashFeed::open(&path, &leash_seed).unwrap();
            let mut leash = Leash::ephemeral();
            feed.apply_record(&mut leash, stop("muse", 10)).unwrap();
            feed.note_heartbeat("muse", 10, 10);
            assert_eq!(feed.staleness("muse", Instant::now()), None);
        }

        // The file on disk is exactly the §-shaped object.
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            json!({
                "models": {
                    "muse": {"last_leash_seq": 10, "known_tip": 10, "active": true},
                },
            })
        );

        // Reopen: watermark and activation survive. The
        // heartbeat does not — it was never persisted — so
        // the active model is stale by never-received until
        // this boot's first heartbeat (S-4 acceptance 5).
        let leash_seed = Leash::ephemeral();
        let feed = LeashFeed::open(&path, &leash_seed).unwrap();
        assert_eq!(feed.watermark("muse"), 10);
        assert!(feed.is_active("muse"));
        assert_eq!(
            feed.staleness("muse", Instant::now()),
            Some(StaleCause::NeverReceived)
        );
    }

    #[test]
    fn open_seeds_from_existing_leash_records_without_activating() {
        let mut leash = Leash::ephemeral();
        leash.record_stop("muse", 7, "operator stop").unwrap();
        leash.record_resume("muse", 9, "operator resume").unwrap();
        leash.record_stop("other", 3, "operator stop").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leash-feed.json");
        let feed = LeashFeed::open(&path, &leash).unwrap();

        // Watermark = the model's highest existing sequence;
        // known tip = the watermark; nothing is active, so
        // nothing is stale — the feed has not engaged (§10.4).
        assert_eq!(feed.watermark("muse"), 9);
        assert_eq!(feed.watermark("other"), 3);
        assert!(!feed.is_active("muse"));
        assert_eq!(feed.staleness("muse", Instant::now()), None);

        // And the seeded watermark does its job from the
        // first record: a replay of resume@9 is a duplicate.
        let mut feed = feed;
        assert_eq!(
            feed.apply_record(&mut leash, resume("muse", 9)).unwrap(),
            ApplyOutcome::Duplicate
        );
        assert_eq!(leash.records().len(), 3);
    }

    #[test]
    fn a_corrupt_state_file_is_fatal_not_a_silent_reset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leash-feed.json");
        let leash = Leash::ephemeral();

        std::fs::write(&path, "this is not json").unwrap();
        assert!(LeashFeed::open(&path, &leash).is_err());

        // Well-formed JSON of the wrong shape is corrupt
        // too — a watermark must never be guessed at.
        std::fs::write(
            &path,
            json!({"models": {"muse": {"active": true}}}).to_string(),
        )
        .unwrap();
        assert!(LeashFeed::open(&path, &leash).is_err());
    }
}
