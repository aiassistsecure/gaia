//! The work lifecycle: submit, gate, act, record — and nothing more.
//!
//! A work order is an intent SI has *already journaled* on its side
//! (G-2 §3.3). This module is the fork-side state machine for that
//! order. It records what SI submitted, it records a verdict when
//! one arrives, and it hands a record to an actuator only when an
//! ADMIT verdict is on file. It never manufactures a verdict: an
//! intent that arrives verdict-less waits in `awaiting_verdict`
//! until the gate speaks, however long that takes.
//!
//! The G-3 skeleton store ([`MemoryWorkStore`]) is honest
//! about being one: its records live in memory and die with
//! the process. [`FileWorkStore`] is the same state machine
//! made durable — full-record JSONL snapshots, appended and
//! flushed before any transition is believed. What the status body reports
//! is exactly what the record holds: since S-1 (G-9 §11.2), once
//! work has completed, [`WorkRecord::status_body`] carries the
//! spine refs the actuation recorded in its outcome result —
//! and before completion, or when the outcome recorded none,
//! the honest empty list rather than refs that were never
//! written. The store is the verdict state machine and nothing
//! more: no policy, no gate checks, no judgement about whether
//! an intent *should* be admitted. That stays in SI, where
//! G-2 put it.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

/// The gate's decision on an intent, as delivered by SI.
///
/// The fork carries verdicts; it does not make them. A verdict
/// reaches a record exactly one of two ways: attached to the
/// submission (pre-admitted) or delivered later through
/// [`WorkStore::record_verdict`] for an escalated intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Admit,
    Reject,
}

impl Intent {
    /// The on-disk JSONL shape of one intent: exactly the
    /// submission's fields, with `verdict` as `null` for an
    /// escalated intent or its wire spelling otherwise.
    pub fn to_json(&self) -> Value {
        json!({
            "tool": self.tool,
            "args": self.args,
            "reason": self.reason,
            "task_id": self.task_id,
            "journal_seq": self.journal_seq,
            "class": self.class,
            "verdict": self.verdict.map(|v| v.as_str()),
        })
    }

    /// Parse one intent back from its JSONL shape. Returns
    /// `None` for anything not recognisably an intent — an
    /// unrecognised verdict spelling included — so the store
    /// counts the line as a load failure instead of guessing.
    pub fn from_json(v: &Value) -> Option<Intent> {
        let obj = v.as_object()?;
        let verdict = match obj.get("verdict") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(Verdict::parse(s)?),
            Some(_) => return None,
        };
        Some(Intent {
            tool: obj.get("tool")?.as_str()?.to_string(),
            args: obj.get("args").cloned().unwrap_or(Value::Null),
            reason: obj.get("reason")?.as_str()?.to_string(),
            task_id: obj.get("task_id")?.as_str()?.to_string(),
            journal_seq: obj.get("journal_seq")?.as_u64()?,
            class: obj.get("class")?.as_str()?.to_string(),
            verdict,
        })
    }
}

impl Verdict {
    /// Parse the wire spelling: `"admit"` or `"reject"`.
    ///
    /// Anything else is `None`, not a guess — a verdict string
    /// this does not recognise is malformed input for the caller
    /// to reject, never a verdict to round to the nearest one.
    pub fn parse(s: &str) -> Option<Verdict> {
        match s {
            "admit" => Some(Verdict::Admit),
            "reject" => Some(Verdict::Reject),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Verdict::Admit => "admit",
            Verdict::Reject => "reject",
        }
    }
}

/// An SI tool intent, as submitted, plus the gate bookkeeping the
/// lifecycle needs.
///
/// `journal_seq` is the sequence in *SI's* journal at which SI
/// recorded this intent before submitting it. It is provenance,
/// quoted back in [`WorkRecord::status_body`]; it is not a spine
/// sequence and the two are never mixed (G-2 §4.2). `class` is
/// the tool class declared in the submission — which executor, if
/// any, applies is a property of that declaration, never inferred
/// by the fork. `verdict` is `None` for an escalated intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub tool: String,
    pub args: Value,
    pub reason: String,
    pub task_id: String,
    pub journal_seq: u64,
    pub class: String,
    pub verdict: Option<Verdict>,
}

/// Where a work order stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkState {
    /// The 202 acknowledgement vocabulary of G-2 §3.2/§3.3: a
    /// submission was recorded. Stored records never rest in this
    /// state — `submit` resolves a record straight to `Admitted`,
    /// `Rejected`, or `AwaitingVerdict` from the verdict the
    /// intent carried in — but the spelling exists so the
    /// transport layer can answer 202 with it.
    Accepted,
    /// Submitted verdict-less. Held until the gate's verdict is
    /// delivered; cannot actuate.
    AwaitingVerdict,
    /// An ADMIT verdict is on record. The only state an actuator
    /// may claim, and the only state `complete` accepts.
    Admitted,
    /// A REJECT verdict is on record. Terminal: a rejected intent
    /// can never actuate.
    Rejected,
    /// Claimed, actuated, and completed with a result. Terminal.
    Executed,
}

impl WorkState {
    /// Parse the spelling [`WorkState::as_str`] writes.
    /// Anything else is `None`, not a guess.
    pub fn parse(s: &str) -> Option<WorkState> {
        match s {
            "accepted" => Some(WorkState::Accepted),
            "awaiting_verdict" => Some(WorkState::AwaitingVerdict),
            "admitted" => Some(WorkState::Admitted),
            "rejected" => Some(WorkState::Rejected),
            "executed" => Some(WorkState::Executed),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            WorkState::Accepted => "accepted",
            WorkState::AwaitingVerdict => "awaiting_verdict",
            WorkState::Admitted => "admitted",
            WorkState::Rejected => "rejected",
            WorkState::Executed => "executed",
        }
    }
}

/// One work order and everything this store knows about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkRecord {
    pub id: String,
    pub intent: Intent,
    pub state: WorkState,
    pub result: Option<Value>,
}

impl WorkRecord {
    /// The on-disk JSONL snapshot: one full record per line,
    /// appended by [`FileWorkStore`] on every state-changing
    /// transition. `result` is present only when the record
    /// holds one, so a completed-with-null result (`Some`
    /// holding JSON `null`) round-trips distinctly from a
    /// record that has not completed.
    pub fn to_json(&self) -> Value {
        let mut body = json!({
            "id": self.id,
            "intent": self.intent.to_json(),
            "state": self.state.as_str(),
        });
        if let Some(result) = &self.result {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("result".to_string(), result.clone());
            }
        }
        body
    }

    /// Parse one snapshot back. Returns `None` for anything
    /// not recognisably a work record — a torn final line
    /// after a crash must not make the store unreadable; the
    /// store counts it as a load failure instead. The id is
    /// read from `id`, with `work_id` accepted as its alias.
    pub fn from_json(v: &Value) -> Option<WorkRecord> {
        let obj = v.as_object()?;
        let id = obj
            .get("id")
            .and_then(Value::as_str)
            .or_else(|| obj.get("work_id").and_then(Value::as_str))?
            .to_string();
        let result = if obj.contains_key("result") {
            Some(obj.get("result")?.clone())
        } else {
            None
        };
        Some(WorkRecord {
            id,
            intent: Intent::from_json(obj.get("intent")?)?,
            state: WorkState::parse(obj.get("state")?.as_str()?)?,
            result,
        })
    }

    /// The `GET /gaia/v1/work/{id}` answer (G-2 §3.2): identity,
    /// state, the result once there is one, and the provenance
    /// trail — SI journal seqs plus spine event refs.
    ///
    /// `result` is present only when the work has completed; a
    /// caller can tell "not finished" from "finished with a null
    /// result" without guessing.
    pub fn status_body(&self) -> Value {
        // The provenance block's spine refs (S-1, G-9 §11.2):
        // a completed record's result is the exact JSON the
        // actuator filed in the outcome store, and it carries
        // the `spine_event_refs` the actuation recorded — so
        // the provenance block reports those, read out of the
        // outcome record's own result. Before completion there
        // is no outcome and the honest list is the empty one;
        // a completed outcome that recorded no refs (a refusal,
        // a `hearth.notify`) likewise reports the empty list.
        let spine_event_refs = self
            .result
            .as_ref()
            .and_then(|r| r.get("spine_event_refs"))
            .filter(|v| v.is_array())
            .cloned()
            .unwrap_or_else(|| json!([]));
        let mut body = json!({
            "work_id": self.id,
            "state": self.state.as_str(),
            "provenance": {
                "si_journal_seqs": [self.intent.journal_seq],
                "spine_event_refs": spine_event_refs,
            },
        });
        if let Some(result) = &self.result {
            if let Some(obj) = body.as_object_mut() {
                obj.insert("result".to_string(), result.clone());
            }
        }
        body
    }
}

/// Everything that can go wrong in the work lifecycle.
///
/// The `gaia_*` error types are deliberately disjoint from the
/// gateway's `model_*` vocabulary (G-2 §3.2): no client can
/// mistake a work problem for a residency diagnosis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GaiaError {
    /// No record exists under this id.
    UnknownWork(String),
    /// The record's state forbids the transition: a second
    /// verdict, a verdict after execution, actuating or
    /// completing work whose state does not allow it.
    VerdictConflict(String),
    /// Actuation was attempted with no ADMIT verdict on record.
    VerdictRequired(String),
    /// The intent's class is one this fork is configured never
    /// to carry (G-2 §3.2).
    ClassNotCarried(String),
    /// The submission or verdict itself could not be read.
    Malformed(String),
    /// The durable work store could not append a snapshot:
    /// the transition was not recorded, in memory or on disk.
    StoreIo(String),
}

impl GaiaError {
    pub fn error_type(&self) -> &'static str {
        match self {
            GaiaError::UnknownWork(_) => "gaia_work_unknown",
            GaiaError::VerdictConflict(_) => "gaia_verdict_conflict",
            GaiaError::VerdictRequired(_) => "gaia_verdict_required",
            GaiaError::ClassNotCarried(_) => "gaia_class_not_carried",
            GaiaError::Malformed(_) => "gaia_malformed_request",
            GaiaError::StoreIo(_) => "gaia_work_store_io",
        }
    }

    pub fn status(&self) -> u16 {
        match self {
            GaiaError::UnknownWork(_) => 404,
            GaiaError::VerdictConflict(_) => 409,
            GaiaError::VerdictRequired(_) => 409,
            GaiaError::ClassNotCarried(_) => 409,
            GaiaError::Malformed(_) => 400,
            GaiaError::StoreIo(_) => 500,
        }
    }

    /// The message carried by the error, formatted with its
    /// context (work id, state, class) by whoever constructed it.
    pub fn message(&self) -> String {
        match self {
            GaiaError::UnknownWork(m)
            | GaiaError::VerdictConflict(m)
            | GaiaError::VerdictRequired(m)
            | GaiaError::ClassNotCarried(m)
            | GaiaError::Malformed(m)
            | GaiaError::StoreIo(m) => m.clone(),
        }
    }
}

impl fmt::Display for GaiaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.error_type(), self.message())
    }
}

impl std::error::Error for GaiaError {}

/// The store contract: the verdict state machine, behind a trait
/// so G-4 can put the spine mirror under the same transitions
/// without the callers changing.
pub trait WorkStore {
    /// Record a submission and return its record. The initial
    /// state is derived from the verdict the intent carries:
    /// `Admit` ⇒ `Admitted`, `Reject` ⇒ `Rejected`, none ⇒
    /// `AwaitingVerdict`. A class this fork does not carry is
    /// refused before any id is spent.
    ///
    /// Submission dedup is part of this contract (G-9 §9.3): a
    /// submission whose `journal_seq` is already recorded
    /// returns the existing record, in whatever state it now
    /// stands, recording nothing new and spending no id. The
    /// carried-class refusal still comes first, so an
    /// uncarried class is refused even on a recorded sequence.
    /// The behaviour lives in the store, not the gateway, so
    /// every store consumer inherits it.
    fn submit(&mut self, intent: Intent) -> Result<WorkRecord, GaiaError>;

    /// A clone of the record under `id`, or `None`.
    fn get(&self, id: &str) -> Option<WorkRecord>;

    /// Record the gate's verdict for an escalated intent. Only a
    /// record in `AwaitingVerdict` accepts one; any other state
    /// already has its answer — or has acted on it — and a second
    /// verdict is a conflict, not an update.
    fn record_verdict(&mut self, id: &str, verdict: Verdict) -> Result<WorkRecord, GaiaError>;

    /// Claim a record for actuation. Only `Admitted` records
    /// claim successfully; claiming does not change the state —
    /// the record rests in `Admitted` until `complete`.
    fn claim_for_actuation(&mut self, id: &str) -> Result<WorkRecord, GaiaError>;

    /// Finish a claimed record with its actuation result,
    /// moving it to `Executed`. Only `Admitted` records complete.
    fn complete(&mut self, id: &str, result: Value) -> Result<WorkRecord, GaiaError>;
}

/// The in-memory [`WorkStore`] of the G-3 skeleton.
///
/// Records are kept in submission order in a `Vec`; the counts
/// here are work orders per process lifetime, and a linear scan
/// states that plainly. The store carries exactly one index:
/// the `journal_seq` → work id dedup index S-1 added (G-9
/// §9.3), because looking a prior submission up by its SI
/// journal sequence is the one lookup a retrying submitter
/// depends on. Ids are `work-000001`, `work-000002`, …, spent
/// only on submissions that are actually recorded — a refused
/// or duplicate submission burns no id.
#[derive(Debug, Clone)]
pub struct MemoryWorkStore {
    records: Vec<WorkRecord>,
    next: u64,
    carried: HashSet<String>,
    by_journal_seq: HashMap<u64, String>,
}

impl MemoryWorkStore {
    /// A store carrying the classes the fork actuates by
    /// default: `read`, `write`, `execute`. `external` is NOT
    /// carried by default — work that leaves the box is exactly
    /// the class G-2 §3.2 names as refused with 409 while
    /// unconfigured.
    pub fn new() -> Self {
        Self::with_carried_classes(
            ["read", "write", "execute"]
                .into_iter()
                .map(str::to_string),
        )
    }

    /// A store carrying exactly `classes`, for a deployment that
    /// has configured its carried set — including one that
    /// deliberately adds `external`.
    pub fn with_carried_classes<I: IntoIterator<Item = String>>(classes: I) -> Self {
        MemoryWorkStore {
            records: Vec::new(),
            next: 1,
            carried: classes.into_iter().collect(),
            by_journal_seq: HashMap::new(),
        }
    }

    fn find_mut(&mut self, id: &str) -> Option<&mut WorkRecord> {
        self.records.iter_mut().find(|r| r.id == id)
    }

    /// Every record in the store, in submission order — the drain's view:
    /// a caller (the composition's ticker) scans it for Admitted records
    /// to act on. Cloned, like `get`, so the scan never borrows the store.
    pub fn records(&self) -> Vec<WorkRecord> {
        self.records.clone()
    }
}

impl Default for MemoryWorkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkStore for MemoryWorkStore {
    fn submit(&mut self, intent: Intent) -> Result<WorkRecord, GaiaError> {
        if !self.carried.contains(&intent.class) {
            return Err(GaiaError::ClassNotCarried(format!(
                "class {:?} is not carried by this fork",
                intent.class
            )));
        }
        // Dedup (G-9 §9.3), after the class check on purpose: a
        // journal_seq already recorded is SI submitting the same
        // journaled intent again, not new work. Hand back the
        // existing record as it now stands — awaiting, admitted,
        // or executed — and record nothing, spend no id.
        if let Some(existing_id) = self.by_journal_seq.get(&intent.journal_seq).cloned() {
            if let Some(existing) = self.get(&existing_id) {
                return Ok(existing);
            }
        }
        // Accepted exists for the 202 acknowledgement vocabulary;
        // stored records start in one of the three states derived
        // here, never in Accepted.
        let state = match intent.verdict {
            Some(Verdict::Admit) => WorkState::Admitted,
            Some(Verdict::Reject) => WorkState::Rejected,
            None => WorkState::AwaitingVerdict,
        };
        let record = WorkRecord {
            id: format!("work-{:06}", self.next),
            intent,
            state,
            result: None,
        };
        self.next += 1;
        self.by_journal_seq
            .insert(record.intent.journal_seq, record.id.clone());
        self.records.push(record.clone());
        Ok(record)
    }

    fn get(&self, id: &str) -> Option<WorkRecord> {
        self.records.iter().find(|r| r.id == id).cloned()
    }

    fn record_verdict(&mut self, id: &str, verdict: Verdict) -> Result<WorkRecord, GaiaError> {
        let record = self
            .find_mut(id)
            .ok_or_else(|| GaiaError::UnknownWork(format!("unknown work id: {id}")))?;
        // This one rule yields both conflict cases: a second
        // verdict, and a verdict arriving after execution. A
        // verdict is recorded exactly once, while waiting for it.
        if record.state != WorkState::AwaitingVerdict {
            return Err(GaiaError::VerdictConflict(format!(
                "work {id} is in state {}; a verdict can only be recorded while awaiting_verdict",
                record.state.as_str()
            )));
        }
        record.intent.verdict = Some(verdict);
        record.state = match verdict {
            Verdict::Admit => WorkState::Admitted,
            Verdict::Reject => WorkState::Rejected,
        };
        Ok(record.clone())
    }

    fn claim_for_actuation(&mut self, id: &str) -> Result<WorkRecord, GaiaError> {
        let record = self
            .get(id)
            .ok_or_else(|| GaiaError::UnknownWork(format!("unknown work id: {id}")))?;
        match record.state {
            WorkState::Admitted => Ok(record),
            WorkState::AwaitingVerdict => Err(GaiaError::VerdictRequired(format!(
                "work {id} has no recorded verdict; an escalated intent cannot actuate until the gate's verdict is delivered"
            ))),
            WorkState::Rejected => Err(GaiaError::VerdictConflict(format!(
                "work {id} was rejected by the gate; a rejected intent can never actuate"
            ))),
            WorkState::Executed => Err(GaiaError::VerdictConflict(format!(
                "work {id} already executed"
            ))),
            // Unreachable for stored records (submit never rests
            // in Accepted); an un-verdicted record is the honest
            // reading if one ever appears.
            WorkState::Accepted => Err(GaiaError::VerdictRequired(format!(
                "work {id} has no recorded verdict"
            ))),
        }
    }

    fn complete(&mut self, id: &str, result: Value) -> Result<WorkRecord, GaiaError> {
        let record = self
            .find_mut(id)
            .ok_or_else(|| GaiaError::UnknownWork(format!("unknown work id: {id}")))?;
        if record.state != WorkState::Admitted {
            return Err(GaiaError::VerdictConflict(format!(
                "work {id} is in state {}; only admitted work can complete",
                record.state.as_str()
            )));
        }
        record.state = WorkState::Executed;
        record.result = Some(result);
        Ok(record.clone())
    }
}

/// The durable [`WorkStore`]: the same verdict state machine
/// as [`MemoryWorkStore`], backed by an append-only JSONL file
/// in the house discipline of the proposal and outcome stores.
///
/// On disk the file is one full [`WorkRecord`] snapshot per
/// line (see [`WorkRecord::to_json`]), appended on every
/// state-changing transition: a `submit` that records a new
/// record, a `record_verdict`, and a `complete`. Claiming
/// changes nothing and appends nothing; a duplicate submit
/// (the S-1 dedup hit) appends nothing. On open the snapshots
/// fold — first-seen id order is preserved, a later snapshot
/// for the same id replaces the earlier one — and the
/// `journal_seq` → work id dedup index and the next-id counter
/// are rebuilt from the folded records. A missing file is an
/// empty store; a malformed line, including a snapshot whose
/// `journal_seq` is already folded under a *different* work
/// id, is skipped and counted in
/// [`FileWorkStore::load_failures`], never fatal.
///
/// Durability rule: a snapshot is appended and flushed
/// *before* the in-memory change is kept. If the append
/// fails, the in-memory change is rolled back — it was never
/// made — and the caller gets a [`GaiaError::StoreIo`], never
/// a believed-but-unrecorded state.
#[derive(Debug, Clone)]
pub struct FileWorkStore {
    path: PathBuf,
    records: Vec<WorkRecord>,
    next: u64,
    carried: HashSet<String>,
    by_journal_seq: HashMap<u64, String>,
    load_failures: Vec<String>,
}

impl FileWorkStore {
    /// Open the durable store at `path`, carrying the default
    /// classes: `read`, `write`, `execute`.
    pub fn open(path: &Path) -> Result<FileWorkStore, String> {
        Self::open_with_carried_classes(
            path,
            ["read", "write", "execute"]
                .into_iter()
                .map(str::to_string),
        )
    }

    /// Open the durable store at `path`, carrying exactly
    /// `classes`. A missing file is an empty store, not an
    /// error. Malformed lines are skipped and counted in
    /// [`FileWorkStore::load_failures`]; any other read error
    /// is fatal, because loading a partial history silently
    /// would break the dedup the store exists for.
    pub fn open_with_carried_classes<I>(path: &Path, classes: I) -> Result<FileWorkStore, String>
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let carried: HashSet<String> = classes.into_iter().map(Into::into).collect();
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(FileWorkStore {
                    path: path.to_path_buf(),
                    records: Vec::new(),
                    next: 1,
                    carried,
                    by_journal_seq: HashMap::new(),
                    load_failures: Vec::new(),
                });
            }
            Err(e) => {
                return Err(format!("cannot read work store {}: {e}", path.display()));
            }
        };

        let mut records: Vec<WorkRecord> = Vec::new();
        let mut by_journal_seq: HashMap<u64, String> = HashMap::new();
        let mut load_failures: Vec<String> = Vec::new();
        for (idx, line) in text.lines().enumerate() {
            let line_no = idx + 1;
            if line.trim().is_empty() {
                continue;
            }
            let snapshot = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| WorkRecord::from_json(&v));
            let Some(snapshot) = snapshot else {
                load_failures.push(format!("line {line_no}: not a work record; skipped"));
                continue;
            };
            // A snapshot whose journal_seq is already folded
            // under a different work id is not this record's
            // history: it is a load failure, skipped and
            // counted, and it folds nothing.
            if let Some(existing_id) = by_journal_seq.get(&snapshot.intent.journal_seq) {
                if existing_id != &snapshot.id {
                    load_failures.push(format!(
                        "line {line_no}: journal_seq {} already recorded for {existing_id}; skipped",
                        snapshot.intent.journal_seq
                    ));
                    continue;
                }
            }
            match records.iter().position(|r| r.id == snapshot.id) {
                Some(pos) => {
                    let old_seq = records[pos].intent.journal_seq;
                    if old_seq != snapshot.intent.journal_seq
                        && by_journal_seq.get(&old_seq) == Some(&snapshot.id)
                    {
                        by_journal_seq.remove(&old_seq);
                    }
                    records[pos] = snapshot.clone();
                }
                None => records.push(snapshot.clone()),
            }
            by_journal_seq.insert(snapshot.intent.journal_seq, snapshot.id.clone());
        }
        // The next id is spent only past the highest folded
        // work-NNNNNN suffix: a reopened store never reissues
        // an id the file already holds.
        let next = records
            .iter()
            .filter_map(|r| {
                r.id.strip_prefix("work-")
                    .and_then(|suffix| suffix.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0)
            + 1;
        Ok(FileWorkStore {
            path: path.to_path_buf(),
            records,
            next,
            carried,
            by_journal_seq,
            load_failures,
        })
    }

    /// Every record in the store, in first-seen submission
    /// order — the drain's view, cloned like
    /// [`MemoryWorkStore::records`].
    pub fn records(&self) -> Vec<WorkRecord> {
        self.records.clone()
    }

    /// Lines that failed to load when the store was opened.
    pub fn load_failures(&self) -> &[String] {
        &self.load_failures
    }

    /// The file this store appends its snapshots to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn position_of(&self, id: &str) -> Option<usize> {
        self.records.iter().position(|r| r.id == id)
    }

    /// Append one snapshot line, flushed before returning —
    /// the file is written first because the in-memory list
    /// is only a view of the record.
    fn append(&self, record: &WorkRecord) -> Result<(), GaiaError> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    GaiaError::StoreIo(format!(
                        "cannot create work store directory {}: {e}",
                        parent.display()
                    ))
                })?;
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                GaiaError::StoreIo(format!(
                    "cannot open work store {}: {e}",
                    self.path.display()
                ))
            })?;
        writeln!(file, "{}", record.to_json()).map_err(|e| {
            GaiaError::StoreIo(format!(
                "cannot append to work store {}: {e}",
                self.path.display()
            ))
        })?;
        file.flush().map_err(|e| {
            GaiaError::StoreIo(format!(
                "cannot flush work store {}: {e}",
                self.path.display()
            ))
        })?;
        Ok(())
    }
}

impl WorkStore for FileWorkStore {
    fn submit(&mut self, intent: Intent) -> Result<WorkRecord, GaiaError> {
        if !self.carried.contains(&intent.class) {
            return Err(GaiaError::ClassNotCarried(format!(
                "class {:?} is not carried by this fork",
                intent.class
            )));
        }
        if let Some(existing_id) = self.by_journal_seq.get(&intent.journal_seq).cloned() {
            if let Some(existing) = self.get(&existing_id) {
                return Ok(existing);
            }
        }
        let state = match intent.verdict {
            Some(Verdict::Admit) => WorkState::Admitted,
            Some(Verdict::Reject) => WorkState::Rejected,
            None => WorkState::AwaitingVerdict,
        };
        let record = WorkRecord {
            id: format!("work-{:06}", self.next),
            intent,
            state,
            result: None,
        };
        // Append before believing: on failure nothing below
        // runs — no record, no dedup entry, no id spent.
        self.append(&record)?;
        self.next += 1;
        self.by_journal_seq
            .insert(record.intent.journal_seq, record.id.clone());
        self.records.push(record.clone());
        Ok(record)
    }

    fn get(&self, id: &str) -> Option<WorkRecord> {
        self.records.iter().find(|r| r.id == id).cloned()
    }

    fn record_verdict(&mut self, id: &str, verdict: Verdict) -> Result<WorkRecord, GaiaError> {
        let pos = self
            .position_of(id)
            .ok_or_else(|| GaiaError::UnknownWork(format!("unknown work id: {id}")))?;
        let mut updated = self.records[pos].clone();
        if updated.state != WorkState::AwaitingVerdict {
            return Err(GaiaError::VerdictConflict(format!(
                "work {id} is in state {}; a verdict can only be recorded while awaiting_verdict",
                updated.state.as_str()
            )));
        }
        updated.intent.verdict = Some(verdict);
        updated.state = match verdict {
            Verdict::Admit => WorkState::Admitted,
            Verdict::Reject => WorkState::Rejected,
        };
        self.append(&updated)?;
        self.records[pos] = updated.clone();
        Ok(updated)
    }

    fn claim_for_actuation(&mut self, id: &str) -> Result<WorkRecord, GaiaError> {
        let record = self
            .get(id)
            .ok_or_else(|| GaiaError::UnknownWork(format!("unknown work id: {id}")))?;
        match record.state {
            WorkState::Admitted => Ok(record),
            WorkState::AwaitingVerdict => Err(GaiaError::VerdictRequired(format!(
                "work {id} has no recorded verdict; an escalated intent cannot actuate until the gate's verdict is delivered"
            ))),
            WorkState::Rejected => Err(GaiaError::VerdictConflict(format!(
                "work {id} was rejected by the gate; a rejected intent can never actuate"
            ))),
            WorkState::Executed => Err(GaiaError::VerdictConflict(format!(
                "work {id} already executed"
            ))),
            WorkState::Accepted => Err(GaiaError::VerdictRequired(format!(
                "work {id} has no recorded verdict"
            ))),
        }
    }

    fn complete(&mut self, id: &str, result: Value) -> Result<WorkRecord, GaiaError> {
        let pos = self
            .position_of(id)
            .ok_or_else(|| GaiaError::UnknownWork(format!("unknown work id: {id}")))?;
        let mut updated = self.records[pos].clone();
        if updated.state != WorkState::Admitted {
            return Err(GaiaError::VerdictConflict(format!(
                "work {id} is in state {}; only admitted work can complete",
                updated.state.as_str()
            )));
        }
        updated.state = WorkState::Executed;
        updated.result = Some(result);
        self.append(&updated)?;
        self.records[pos] = updated.clone();
        Ok(updated)
    }
}

/// The actuation end of the lifecycle. Implementations execute
/// exactly the admitted intent's journaled args, verbatim — they
/// are handed the claimed record and nothing else, and they do
/// not reinterpret it.
pub trait Actuator {
    fn actuate(&mut self, work: &WorkRecord) -> Result<Value, GaiaError>;
}

/// Claim, actuate, complete — in that order, or not at all.
///
/// The claim is the gate check: it refuses without an ADMIT
/// verdict on record, and the actuator is invoked ONLY after a
/// successful claim. If actuation itself fails, the error
/// propagates and the record is left `Admitted` with no result —
/// not completed, not quietly retried here.
pub fn run_actuation(
    store: &mut dyn WorkStore,
    actuator: &mut dyn Actuator,
    id: &str,
) -> Result<WorkRecord, GaiaError> {
    let claimed = store.claim_for_actuation(id)?;
    let result = actuator.actuate(&claimed)?;
    store.complete(id, result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(verdict: Option<Verdict>) -> Intent {
        intent_in_class(verdict, "execute")
    }

    fn intent_seq(verdict: Option<Verdict>, seq: u64) -> Intent {
        let mut intent = intent(verdict);
        intent.journal_seq = seq;
        intent
    }

    fn intent_in_class(verdict: Option<Verdict>, class: &str) -> Intent {
        Intent {
            tool: "hearth.restart".to_string(),
            args: json!({ "model": "glimmer" }),
            reason: "observed lost{process_exited} at seq 7".to_string(),
            task_id: "task-1".to_string(),
            journal_seq: 42,
            class: class.to_string(),
            verdict,
        }
    }

    #[test]
    fn verdict_spellings_round_trip() {
        assert_eq!(Verdict::parse("admit"), Some(Verdict::Admit));
        assert_eq!(Verdict::parse("reject"), Some(Verdict::Reject));
        assert_eq!(Verdict::Admit.as_str(), "admit");
        assert_eq!(Verdict::Reject.as_str(), "reject");
        assert_eq!(Verdict::parse("Admit"), None);
        assert_eq!(Verdict::parse(""), None);
        assert_eq!(Verdict::parse("escalate"), None);
    }

    #[test]
    fn error_taxonomy_maps_type_and_status() {
        let cases = [
            (GaiaError::UnknownWork("x".into()), "gaia_work_unknown", 404),
            (
                GaiaError::VerdictConflict("x".into()),
                "gaia_verdict_conflict",
                409,
            ),
            (
                GaiaError::VerdictRequired("x".into()),
                "gaia_verdict_required",
                409,
            ),
            (
                GaiaError::ClassNotCarried("x".into()),
                "gaia_class_not_carried",
                409,
            ),
            (
                GaiaError::Malformed("x".into()),
                "gaia_malformed_request",
                400,
            ),
        ];
        for (err, kind, status) in cases {
            assert_eq!(err.error_type(), kind);
            assert_eq!(err.status(), status);
            assert_eq!(err.message(), "x");
        }
    }

    #[test]
    fn submit_derives_state_from_carried_verdict() {
        let mut store = MemoryWorkStore::new();
        let admitted = store.submit(intent_seq(Some(Verdict::Admit), 42)).unwrap();
        assert_eq!(admitted.state, WorkState::Admitted);
        assert_eq!(admitted.id, "work-000001");

        let rejected = store.submit(intent_seq(Some(Verdict::Reject), 43)).unwrap();
        assert_eq!(rejected.state, WorkState::Rejected);
        assert_eq!(rejected.id, "work-000002");

        let escalated = store.submit(intent_seq(None, 44)).unwrap();
        assert_eq!(escalated.state, WorkState::AwaitingVerdict);
        assert_eq!(escalated.id, "work-000003");

        // And get round-trips what submit returned.
        assert_eq!(store.get(&escalated.id), Some(escalated));
        assert_eq!(store.get("work-000099"), None);
    }

    #[test]
    fn records_returns_all_records_in_submission_order_with_their_states() {
        let mut store = MemoryWorkStore::new();
        let admitted = store.submit(intent_seq(Some(Verdict::Admit), 42)).unwrap();
        let escalated = store.submit(intent_seq(None, 43)).unwrap();
        let rejected = store.submit(intent_seq(Some(Verdict::Reject), 44)).unwrap();
        store
            .complete(&admitted.id, json!({ "restarted": true }))
            .unwrap();

        let records = store.records();
        assert_eq!(
            records.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            vec![
                admitted.id.as_str(),
                escalated.id.as_str(),
                rejected.id.as_str()
            ]
        );
        assert_eq!(
            records.iter().map(|r| r.state).collect::<Vec<_>>(),
            vec![
                WorkState::Executed,
                WorkState::AwaitingVerdict,
                WorkState::Rejected
            ]
        );
        // Cloned, like `get`: mutating the scan changes nothing.
        let mut scan = store.records();
        scan.clear();
        assert_eq!(store.records().len(), 3);
    }

    #[test]
    fn submit_refuses_a_class_the_fork_does_not_carry() {
        let mut store = MemoryWorkStore::new();
        let err = store
            .submit(intent_in_class(Some(Verdict::Admit), "external"))
            .unwrap_err();
        assert_eq!(err.error_type(), "gaia_class_not_carried");
        assert_eq!(err.status(), 409);
        assert!(err.message().contains("external"));

        // The refusal spent no id: the next recorded submission
        // is still the first.
        let ok = store.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(ok.id, "work-000001");

        // A store configured to carry external accepts it.
        let mut configured =
            MemoryWorkStore::with_carried_classes(["external".to_string()]);
        let ok = configured
            .submit(intent_in_class(Some(Verdict::Admit), "external"))
            .unwrap();
        assert_eq!(ok.state, WorkState::Admitted);
    }

    #[test]
    fn duplicate_submit_returns_the_existing_record_and_spends_no_id() {
        let mut store = MemoryWorkStore::new();
        let first = store.submit(intent(Some(Verdict::Admit))).unwrap();

        // The same journal_seq again is the same journaled
        // intent resubmitted, not new work.
        let duplicate = store.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(duplicate, first);
        assert_eq!(store.records().len(), 1);

        // The duplicate spent no id: the next distinct
        // submission takes the id it would have burned.
        let next = store.submit(intent_seq(Some(Verdict::Admit), 43)).unwrap();
        assert_eq!(next.id, "work-000002");
        assert_eq!(store.records().len(), 2);
    }

    #[test]
    fn duplicate_submit_after_a_verdict_returns_the_current_state() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(None)).unwrap();
        store.record_verdict(&rec.id, Verdict::Admit).unwrap();

        // Not a fresh awaiting record: the record as it stands,
        // verdict and all.
        let duplicate = store.submit(intent(None)).unwrap();
        assert_eq!(duplicate.id, rec.id);
        assert_eq!(duplicate.state, WorkState::Admitted);
        assert_eq!(duplicate.intent.verdict, Some(Verdict::Admit));
        assert_eq!(store.records().len(), 1);
    }

    #[test]
    fn duplicate_submit_after_execution_returns_the_executed_record() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        store
            .complete(&rec.id, json!({ "restarted": true }))
            .unwrap();

        let duplicate = store.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(duplicate.id, rec.id);
        assert_eq!(duplicate.state, WorkState::Executed);
        assert_eq!(duplicate.result, Some(json!({ "restarted": true })));
        assert_eq!(store.records().len(), 1);
    }

    #[test]
    fn class_refused_submission_records_no_dedup_entry() {
        let mut store = MemoryWorkStore::new();
        // journal_seq 42 is refused on class grounds, so nothing
        // is recorded under it — the refusal must not poison
        // the sequence for a later carried submission.
        let err = store
            .submit(intent_in_class(Some(Verdict::Admit), "external"))
            .unwrap_err();
        assert_eq!(err.error_type(), "gaia_class_not_carried");

        let ok = store.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(ok.id, "work-000001");
        assert_eq!(ok.state, WorkState::Admitted);
        assert_eq!(store.records().len(), 1);
    }

    #[test]
    fn verdict_transitions_an_awaiting_record_once() {
        let mut store = MemoryWorkStore::new();
        let a = store.submit(intent_seq(None, 42)).unwrap();
        let admitted = store.record_verdict(&a.id, Verdict::Admit).unwrap();
        assert_eq!(admitted.state, WorkState::Admitted);
        assert_eq!(admitted.intent.verdict, Some(Verdict::Admit));

        let r = store.submit(intent_seq(None, 43)).unwrap();
        let rejected = store.record_verdict(&r.id, Verdict::Reject).unwrap();
        assert_eq!(rejected.state, WorkState::Rejected);
        assert_eq!(rejected.intent.verdict, Some(Verdict::Reject));
    }

    #[test]
    fn double_verdict_is_a_conflict() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent_seq(None, 42)).unwrap();
        store.record_verdict(&rec.id, Verdict::Admit).unwrap();
        let err = store.record_verdict(&rec.id, Verdict::Reject).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert_eq!(err.status(), 409);
        assert!(err.message().contains("admitted"));

        // A pre-admitted submission already has its verdict, too.
        let pre = store.submit(intent_seq(Some(Verdict::Admit), 43)).unwrap();
        let err = store.record_verdict(&pre.id, Verdict::Admit).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
    }

    #[test]
    fn verdict_after_execution_is_a_conflict() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        store.complete(&rec.id, json!({ "ok": true })).unwrap();
        let err = store.record_verdict(&rec.id, Verdict::Admit).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert!(err.message().contains("executed"));

        let err = store
            .record_verdict("work-000099", Verdict::Admit)
            .unwrap_err();
        assert_eq!(err.error_type(), "gaia_work_unknown");
        assert_eq!(err.status(), 404);
    }

    #[test]
    fn claim_refuses_without_an_admit_verdict() {
        let mut store = MemoryWorkStore::new();

        let awaiting = store.submit(intent_seq(None, 42)).unwrap();
        let err = store.claim_for_actuation(&awaiting.id).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_required");
        assert_eq!(err.status(), 409);
        assert!(err.message().contains("no recorded verdict"));

        let rejected = store.submit(intent_seq(Some(Verdict::Reject), 43)).unwrap();
        let err = store.claim_for_actuation(&rejected.id).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert!(err.message().contains("can never actuate"));

        let done = store.submit(intent_seq(Some(Verdict::Admit), 44)).unwrap();
        store.complete(&done.id, json!(null)).unwrap();
        let err = store.claim_for_actuation(&done.id).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert!(err.message().contains("already executed"));

        let err = store.claim_for_actuation("work-000099").unwrap_err();
        assert_eq!(err.error_type(), "gaia_work_unknown");
    }

    #[test]
    fn claim_admitted_succeeds_and_does_not_change_state() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        let claimed = store.claim_for_actuation(&rec.id).unwrap();
        assert_eq!(claimed.state, WorkState::Admitted);
        assert_eq!(store.get(&rec.id).unwrap().state, WorkState::Admitted);
    }

    #[test]
    fn complete_moves_admitted_to_executed_with_result() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        let done = store.complete(&rec.id, json!({ "restarted": true })).unwrap();
        assert_eq!(done.state, WorkState::Executed);
        assert_eq!(done.result, Some(json!({ "restarted": true })));
        assert_eq!(store.get(&rec.id), Some(done));
    }

    #[test]
    fn complete_from_any_other_state_is_a_conflict() {
        let mut store = MemoryWorkStore::new();
        let awaiting = store.submit(intent_seq(None, 42)).unwrap();
        let err = store.complete(&awaiting.id, json!(null)).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert!(err.message().contains("awaiting_verdict"));

        let rejected = store.submit(intent_seq(Some(Verdict::Reject), 43)).unwrap();
        let err = store.complete(&rejected.id, json!(null)).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert!(err.message().contains("rejected"));

        let err = store.complete("work-000099", json!(null)).unwrap_err();
        assert_eq!(err.error_type(), "gaia_work_unknown");
        assert_eq!(err.status(), 404);
    }

    #[test]
    fn status_body_omits_result_until_one_exists() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        let body = rec.status_body();
        assert_eq!(body["work_id"], json!("work-000001"));
        assert_eq!(body["state"], json!("admitted"));
        assert_eq!(body["provenance"]["si_journal_seqs"], json!([42]));
        assert_eq!(body["provenance"]["spine_event_refs"], json!([]));
        assert!(body.get("result").is_none());

        let done = store.complete(&rec.id, json!({ "ok": true })).unwrap();
        let body = done.status_body();
        assert_eq!(body["state"], json!("executed"));
        assert_eq!(body["result"], json!({ "ok": true }));
    }

    struct CountingActuator {
        calls: usize,
        outcome: Result<Value, GaiaError>,
    }

    impl Actuator for CountingActuator {
        fn actuate(&mut self, work: &WorkRecord) -> Result<Value, GaiaError> {
            self.calls += 1;
            assert_eq!(work.state, WorkState::Admitted);
            self.outcome.clone()
        }
    }

    #[test]
    fn run_actuation_claims_actuates_completes() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        let mut actuator = CountingActuator {
            calls: 0,
            outcome: Ok(json!({ "restarted": true })),
        };
        let done = run_actuation(&mut store, &mut actuator, &rec.id).unwrap();
        assert_eq!(actuator.calls, 1);
        assert_eq!(done.state, WorkState::Executed);
        assert_eq!(done.result, Some(json!({ "restarted": true })));
    }

    #[test]
    fn run_actuation_never_touches_the_actuator_without_a_verdict() {
        let mut store = MemoryWorkStore::new();
        let mut actuator = CountingActuator {
            calls: 0,
            outcome: Ok(json!(null)),
        };

        // The acceptance case, proven: an escalated intent cannot
        // reach the actuator without a recorded verdict.
        let escalated = store.submit(intent_seq(None, 42)).unwrap();
        let err = run_actuation(&mut store, &mut actuator, &escalated.id).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_required");
        assert_eq!(actuator.calls, 0);

        let rejected = store.submit(intent_seq(Some(Verdict::Reject), 43)).unwrap();
        let err = run_actuation(&mut store, &mut actuator, &rejected.id).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert_eq!(actuator.calls, 0);

        // After the gate admits the escalated intent, the same
        // call actuates exactly once.
        store
            .record_verdict(&escalated.id, Verdict::Admit)
            .unwrap();
        let done = run_actuation(&mut store, &mut actuator, &escalated.id).unwrap();
        assert_eq!(actuator.calls, 1);
        assert_eq!(done.state, WorkState::Executed);
    }

    #[test]
    fn run_actuation_propagates_actuator_failure_without_completing() {
        let mut store = MemoryWorkStore::new();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        let mut actuator = CountingActuator {
            calls: 0,
            outcome: Err(GaiaError::Malformed("actuation blew up".to_string())),
        };
        let err = run_actuation(&mut store, &mut actuator, &rec.id).unwrap_err();
        assert_eq!(err.error_type(), "gaia_malformed_request");
        let after = store.get(&rec.id).unwrap();
        assert_eq!(after.state, WorkState::Admitted);
        assert_eq!(after.result, None);
    }

    /// A fresh work-store path under a per-test temp directory.
    /// (No `tempfile` dependency in this crate; a pid-and-name
    /// directory under the system temp dir is enough, and each
    /// test removes its own afterwards.)
    fn temp_work(test: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("gaia-work-{}-{test}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("work.jsonl");
        let _ = std::fs::remove_file(&path);
        (dir, path)
    }

    fn line_count(path: &Path) -> usize {
        std::fs::read_to_string(path)
            .map(|t| t.lines().count())
            .unwrap_or(0)
    }

    #[test]
    fn work_record_json_round_trips_result_presence_exactly() {
        let pending = WorkRecord {
            id: "work-000001".to_string(),
            intent: intent(None),
            state: WorkState::AwaitingVerdict,
            result: None,
        };
        assert_eq!(WorkRecord::from_json(&pending.to_json()), Some(pending));
        // A completed record whose result is JSON null is
        // `Some(null)`, not `None` — the distinction survives.
        let done_null = WorkRecord {
            id: "work-000002".to_string(),
            intent: intent_seq(Some(Verdict::Admit), 43),
            state: WorkState::Executed,
            result: Some(json!(null)),
        };
        assert_eq!(done_null.to_json()["state"], json!("executed"));
        assert_eq!(done_null.to_json()["intent"]["verdict"], json!("admit"));
        assert_eq!(
            WorkRecord::from_json(&done_null.to_json()),
            Some(done_null)
        );
        assert_eq!(WorkRecord::from_json(&json!({"nope": true})), None);
    }

    #[test]
    fn file_store_open_fold_round_trip_with_verdict_completion_and_provenance() {
        let (dir, path) = temp_work("fold-roundtrip");
        let result = json!({
            "restarted": true,
            "spine_event_refs": [{ "hash": "hash-two", "seq": 12 }],
        });
        let expected = {
            let mut store = FileWorkStore::open(&path).unwrap();
            assert!(store.records().is_empty());
            assert!(store.load_failures().is_empty());
            assert_eq!(store.path(), path.as_path());
            let escalated = store.submit(intent_seq(None, 42)).unwrap();
            store
                .record_verdict(&escalated.id, Verdict::Admit)
                .unwrap();
            let done = store.complete(&escalated.id, result.clone()).unwrap();
            let rejected = store.submit(intent_seq(Some(Verdict::Reject), 43)).unwrap();
            // submit + verdict + complete + submit: four lines.
            assert_eq!(line_count(&path), 4);
            vec![done, rejected]
        };
        let reopened = FileWorkStore::open(&path).unwrap();
        assert_eq!(reopened.records(), expected);
        assert!(reopened.load_failures().is_empty());
        let done = &expected[0];
        assert_eq!(done.state, WorkState::Executed);
        assert_eq!(done.intent.verdict, Some(Verdict::Admit));
        let body = done.status_body();
        assert_eq!(body["result"], result);
        assert_eq!(
            body["provenance"]["spine_event_refs"],
            json!([{ "hash": "hash-two", "seq": 12 }])
        );
        assert_eq!(body["provenance"]["si_journal_seqs"], json!([42]));
        // The next-id counter was rebuilt past the folded ids.
        let mut reopened = reopened;
        let next = reopened.submit(intent_seq(Some(Verdict::Admit), 44)).unwrap();
        assert_eq!(next.id, "work-000003");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_dedup_after_reopen_spends_no_id_and_appends_nothing() {
        let (dir, path) = temp_work("dedup-reopen");
        let first = {
            let mut store = FileWorkStore::open(&path).unwrap();
            store.submit(intent(Some(Verdict::Admit))).unwrap()
        };
        let mut reopened = FileWorkStore::open(&path).unwrap();
        let duplicate = reopened.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(duplicate, first);
        assert_eq!(reopened.records().len(), 1);
        assert_eq!(line_count(&path), 1, "a dedup hit appends nothing");
        let next = reopened.submit(intent_seq(Some(Verdict::Admit), 43)).unwrap();
        assert_eq!(next.id, "work-000002", "the duplicate burned no id");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_claim_appends_nothing() {
        let (dir, path) = temp_work("claim-no-append");
        let mut store = FileWorkStore::open(&path).unwrap();
        let rec = store.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(line_count(&path), 1);
        let claimed = store.claim_for_actuation(&rec.id).unwrap();
        assert_eq!(claimed.state, WorkState::Admitted);
        assert_eq!(line_count(&path), 1, "claim changes and appends nothing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_torn_final_line_is_skipped_counted_and_store_usable() {
        let (dir, path) = temp_work("torn-line");
        {
            let mut store = FileWorkStore::open(&path).unwrap();
            store.submit(intent(Some(Verdict::Admit))).unwrap();
        }
        // A crash mid-append leaves a truncated final line.
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            write!(file, "{{\"id\":\"work-0000").unwrap();
        }
        let mut reopened = FileWorkStore::open(&path).unwrap();
        assert_eq!(reopened.records().len(), 1);
        assert_eq!(reopened.load_failures().len(), 1);
        assert!(
            reopened.load_failures()[0].contains('2'),
            "the failure names its 1-based line: {:?}",
            reopened.load_failures()
        );
        let next = reopened.submit(intent_seq(Some(Verdict::Admit), 43)).unwrap();
        assert_eq!(next.id, "work-000002");
        assert_eq!(reopened.records().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_verdict_after_reopen_conflict_is_still_409() {
        let (dir, path) = temp_work("verdict-reopen-conflict");
        let rec = {
            let mut store = FileWorkStore::open(&path).unwrap();
            let rec = store.submit(intent(None)).unwrap();
            store.record_verdict(&rec.id, Verdict::Admit).unwrap();
            rec
        };
        let mut reopened = FileWorkStore::open(&path).unwrap();
        let err = reopened.record_verdict(&rec.id, Verdict::Reject).unwrap_err();
        assert_eq!(err.error_type(), "gaia_verdict_conflict");
        assert_eq!(err.status(), 409);
        assert_eq!(
            reopened.get(&rec.id).unwrap().state,
            WorkState::Admitted,
            "the refused verdict changed nothing"
        );
        assert_eq!(line_count(&path), 2, "the refused verdict appended nothing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_journal_seq_conflict_line_is_a_load_failure() {
        let (dir, path) = temp_work("seq-conflict");
        let first = WorkRecord {
            id: "work-000001".to_string(),
            intent: intent_seq(Some(Verdict::Admit), 42),
            state: WorkState::Admitted,
            result: None,
        };
        let conflicting = WorkRecord {
            id: "work-000002".to_string(),
            intent: intent_seq(Some(Verdict::Admit), 42),
            state: WorkState::Admitted,
            result: None,
        };
        std::fs::write(
            &path,
            format!("{}\n{}\n", first.to_json(), conflicting.to_json()),
        )
        .unwrap();
        let store = FileWorkStore::open(&path).unwrap();
        assert_eq!(store.records(), vec![first]);
        assert_eq!(store.load_failures().len(), 1);
        assert!(store.load_failures()[0].contains('2'));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_append_failure_rolls_back_submit_and_spends_no_id() {
        let (dir, path) = temp_work("append-failure");
        let nested = dir.join("blocker").join("work.jsonl");
        // Open while the parent does not exist (a missing file
        // is an empty store), then put a regular file where
        // the parent directory would have to be: the append
        // behind `submit` now has nowhere to go.
        let mut store = FileWorkStore::open(&nested).unwrap();
        std::fs::write(dir.join("blocker"), "not a directory").unwrap();
        let err = store.submit(intent(Some(Verdict::Admit))).unwrap_err();
        assert_eq!(err.error_type(), "gaia_work_store_io");
        assert_eq!(err.status(), 500);
        assert!(store.records().is_empty(), "the change was rolled back");
        assert_eq!(store.get("work-000001"), None);
        // Unblocked, the same submission takes the id the
        // failed attempt would have burned — it spent none.
        std::fs::remove_file(dir.join("blocker")).unwrap();
        let ok = store.submit(intent(Some(Verdict::Admit))).unwrap();
        assert_eq!(ok.id, "work-000001");
        assert_eq!(store.records().len(), 1);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_store_open_with_carried_classes_uses_the_given_set() {
        let (dir, path) = temp_work("carried-classes");
        let mut store =
            FileWorkStore::open_with_carried_classes(&path, ["external".to_string()]).unwrap();
        let ok = store
            .submit(intent_in_class(Some(Verdict::Admit), "external"))
            .unwrap();
        assert_eq!(ok.state, WorkState::Admitted);
        let err = store.submit(intent_seq(Some(Verdict::Admit), 43)).unwrap_err();
        assert_eq!(err.error_type(), "gaia_class_not_carried");
        assert_eq!(line_count(&path), 1, "a refused class appends nothing");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
