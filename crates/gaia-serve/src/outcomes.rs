//! Outcomes: every actuation outcome, recorded gaia-side.
//!
//! G-2 §3.3 step 4: outcomes are recorded and reported. An
//! actuation that passes the gate check ends in exactly one
//! of three ways — it executed, it was refused (the leash,
//! stale state, or no stored spec to act against), or it
//! failed — and whichever happened is a fact the gate is owed
//! back, not a log line the fork keeps to itself. This store
//! is that record: the outcome, why, and the exact JSON that
//! became the work's result, so what SI reads back is what
//! was filed here, byte for byte.
//!
//! Storage follows G-2 §6.2 and the [`crate::proposals`]
//! conventions exactly: gaia records live in gaia-owned
//! stores, never in upstream spine enums. The outcome store
//! is an append-only JSONL file — one JSON object per line,
//! in the format [`OutcomeRecord::to_json`] writes — or, in
//! an ephemeral store, in memory only. Provenance spans the
//! two sequence spaces §3.3's result shape assumes, kept
//! distinct: `caused_by` and `spine_event_refs` are spine
//! [`EventRef`]s (`hash` + `seq`); no SI journal sequence is
//! ever written into a spine-ref field (G-2 §4.2).

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use hearth_store::EventRef;
use serde_json::{json, Value};

/// How one actuation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Executed,
    Refused,
    Failed,
}

impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Outcome::Executed => "executed",
            Outcome::Refused => "refused",
            Outcome::Failed => "failed",
        }
    }

    /// Parse the wire spelling. Anything else is `None`,
    /// not a guess — an unrecognised outcome is not a record.
    fn parse(s: &str) -> Option<Outcome> {
        match s {
            "executed" => Some(Outcome::Executed),
            "refused" => Some(Outcome::Refused),
            "failed" => Some(Outcome::Failed),
            _ => None,
        }
    }
}

/// One actuation outcome, complete enough to report.
///
/// `refusal` names why a `Refused` outcome refused —
/// `"leash"`, `"stale_state"`, or `"no_stored_spec"` — and
/// is `None` for executed and failed outcomes. `detail` is
/// the human-readable account (a refusal quotes the stop or
/// the state it honoured). `result` is the exact JSON that
/// became the work's result under §3.3 step 4: it is stored,
/// not reconstructed, so the report cannot drift from what
/// the work store completed with.
#[derive(Debug, Clone, PartialEq)]
pub struct OutcomeRecord {
    pub work_id: String,
    pub tool: String,
    pub model: String,
    pub outcome: Outcome,
    pub refusal: Option<String>,
    pub detail: String,
    /// The spine event the intent answered, when there is one.
    pub caused_by: Option<EventRef>,
    pub spine_event_refs: Vec<EventRef>,
    pub result: Value,
}

fn event_ref_json(ev: &EventRef) -> Value {
    json!({ "hash": ev.hash, "seq": ev.seq })
}

fn event_ref_from_json(v: &Value) -> Option<EventRef> {
    let obj = v.as_object()?;
    Some(EventRef {
        hash: obj.get("hash")?.as_str()?.to_string(),
        seq: obj.get("seq")?.as_u64()?,
    })
}

impl OutcomeRecord {
    /// The on-disk JSONL record.
    pub fn to_json(&self) -> Value {
        json!({
            "work_id": self.work_id,
            "tool": self.tool,
            "model": self.model,
            "outcome": self.outcome.as_str(),
            "refusal": self.refusal,
            "detail": self.detail,
            "caused_by": self.caused_by.as_ref().map(event_ref_json),
            "spine_event_refs": self.spine_event_refs.iter().map(event_ref_json).collect::<Vec<_>>(),
            "result": self.result,
        })
    }

    /// Parse one JSONL record back. Returns `None` for
    /// anything that is not recognizably an outcome record —
    /// a torn final line after a crash must not make the
    /// store unreadable; the store counts it as a load
    /// failure instead (the same contract as
    /// [`crate::Proposal`]).
    pub fn from_json(v: &Value) -> Option<OutcomeRecord> {
        let obj = v.as_object()?;
        let refusal = match obj.get("refusal") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => return None,
        };
        let caused_by = match obj.get("caused_by") {
            None | Some(Value::Null) => None,
            Some(v) => Some(event_ref_from_json(v)?),
        };
        let spine_event_refs = match obj.get("spine_event_refs") {
            None => Vec::new(),
            Some(Value::Array(items)) => {
                let mut refs = Vec::with_capacity(items.len());
                for item in items {
                    refs.push(event_ref_from_json(item)?);
                }
                refs
            }
            Some(_) => return None,
        };
        Some(OutcomeRecord {
            work_id: obj.get("work_id")?.as_str()?.to_string(),
            tool: obj.get("tool")?.as_str()?.to_string(),
            model: obj.get("model")?.as_str()?.to_string(),
            outcome: Outcome::parse(obj.get("outcome")?.as_str()?)?,
            refusal,
            detail: obj.get("detail")?.as_str()?.to_string(),
            caused_by,
            spine_event_refs,
            result: obj.get("result").cloned().unwrap_or(Value::Null),
        })
    }
}

/// The append-only outcome log.
///
/// In memory it is the list of every outcome ever recorded
/// at `path`; on disk it is the same list as JSONL. Recording
/// is append + flush, so an outcome the actuator believes it
/// filed survives the process. An ephemeral store has no
/// file: its records live in memory only and die with the
/// process, which is honest for a composition the binary has
/// not yet given a home.
pub struct OutcomeStore {
    path: Option<PathBuf>,
    records: Vec<OutcomeRecord>,
    load_failures: Vec<String>,
}

impl OutcomeStore {
    /// Open the store at `path`, loading any outcomes
    /// already recorded there. A missing file is an empty
    /// store, not an error. Malformed lines are skipped and
    /// counted in [`OutcomeStore::load_failures`].
    pub fn open(path: &Path) -> Result<OutcomeStore, String> {
        // A missing file is an empty store, not an error:
        // no actuation has ever been recorded here, and that
        // absence is a fact, not a failure. Any other read
        // error is fatal, because loading a partial history
        // silently would misreport what actuation did.
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(OutcomeStore {
                    path: Some(path.to_path_buf()),
                    records: Vec::new(),
                    load_failures: Vec::new(),
                });
            }
            Err(e) => {
                return Err(format!(
                    "cannot read outcome store {}: {e}",
                    path.display()
                ));
            }
        };

        let mut records = Vec::new();
        let mut load_failures = Vec::new();
        for (idx, line) in text.lines().enumerate() {
            let line_no = idx + 1;
            if line.trim().is_empty() {
                continue;
            }
            // One JSONL record per line: parse the JSON
            // first, then the outcome shape. Either failure
            // is a load failure naming its 1-based line —
            // skipped, never dropped silently, and never
            // fatal to the rest.
            let record = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| OutcomeRecord::from_json(&v));
            match record {
                Some(record) => records.push(record),
                None => load_failures.push(format!(
                    "line {line_no}: not an outcome record; skipped"
                )),
            }
        }
        Ok(OutcomeStore {
            path: Some(path.to_path_buf()),
            records,
            load_failures,
        })
    }

    /// A store with no file behind it: records live in
    /// memory only.
    pub fn ephemeral() -> OutcomeStore {
        OutcomeStore {
            path: None,
            records: Vec::new(),
            load_failures: Vec::new(),
        }
    }

    /// Record an outcome: append its JSONL line to the file
    /// first (flushed before returning) and only then keep
    /// it in memory. If the append fails, the outcome is
    /// *not* recorded and the error says so — the in-memory
    /// list is only a view of the record.
    pub fn record(&mut self, record: OutcomeRecord) -> Result<(), String> {
        if let Some(path) = &self.path {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        format!(
                            "cannot create outcome store directory {}: {e}",
                            parent.display()
                        )
                    })?;
                }
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| {
                    format!("cannot open outcome store {}: {e}", path.display())
                })?;
            writeln!(file, "{}", record.to_json()).map_err(|e| {
                format!("cannot append to outcome store {}: {e}", path.display())
            })?;
            file.flush().map_err(|e| {
                format!("cannot flush outcome store {}: {e}", path.display())
            })?;
        }
        self.records.push(record);
        Ok(())
    }

    /// Every outcome recorded so far, oldest first.
    pub fn outcomes(&self) -> &[OutcomeRecord] {
        &self.records
    }

    /// Lines that failed to parse when the store was opened.
    pub fn load_failures(&self) -> &[String] {
        &self.load_failures
    }

    /// Every outcome recorded for one work order, oldest
    /// first. An order normally has exactly one; the list
    /// shape stays honest if a defect ever files two.
    pub fn for_work(&self, work_id: &str) -> Vec<&OutcomeRecord> {
        self.records.iter().filter(|r| r.work_id == work_id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(work_id: &str, kind: Outcome) -> OutcomeRecord {
        OutcomeRecord {
            work_id: work_id.to_string(),
            tool: "hearth.restart".to_string(),
            model: "muse".to_string(),
            outcome: kind,
            refusal: match kind {
                Outcome::Refused => Some("leash".to_string()),
                _ => None,
            },
            detail: format!("{} for {work_id}", kind.as_str()),
            caused_by: Some(EventRef {
                hash: "hash-one".to_string(),
                seq: 11,
            }),
            spine_event_refs: vec![EventRef {
                hash: "hash-two".to_string(),
                seq: 12,
            }],
            result: json!({ "outcome": kind.as_str(), "model": "muse" }),
        }
    }

    #[test]
    fn record_then_reopen_loads_both_outcomes_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outcomes.jsonl");
        let first = outcome("work-000001", Outcome::Executed);
        let second = outcome("work-000002", Outcome::Refused);

        {
            let mut store = OutcomeStore::open(&path).unwrap();
            assert!(store.outcomes().is_empty());
            store.record(first.clone()).unwrap();
            store.record(second.clone()).unwrap();
            assert_eq!(store.outcomes().len(), 2);
        }

        let reopened = OutcomeStore::open(&path).unwrap();
        assert_eq!(reopened.outcomes(), [first, second].as_slice());
        assert!(reopened.load_failures().is_empty());
    }

    #[test]
    fn ephemeral_records_nothing_to_disk_but_answers_queries() {
        let mut store = OutcomeStore::ephemeral();
        let recorded = outcome("work-000001", Outcome::Failed);
        store.record(recorded.clone()).unwrap();
        assert_eq!(recorded.outcome.as_str(), "failed");
        assert_eq!(store.outcomes(), [recorded].as_slice());
        assert_eq!(store.for_work("work-000001").len(), 1);
        assert!(store.load_failures().is_empty());
    }

    #[test]
    fn for_work_filters_by_work_id() {
        let mut store = OutcomeStore::ephemeral();
        let a = outcome("work-000001", Outcome::Executed);
        let b = outcome("work-000002", Outcome::Refused);
        let c = outcome("work-000001", Outcome::Failed);
        store.record(a.clone()).unwrap();
        store.record(b).unwrap();
        store.record(c.clone()).unwrap();

        let for_a = store.for_work("work-000001");
        assert_eq!(for_a, vec![&a, &c]);
        assert!(store.for_work("work-000099").is_empty());
    }

    #[test]
    fn a_garbage_line_is_a_load_failure_not_a_fatal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("outcomes.jsonl");
        let good = outcome("work-000001", Outcome::Executed);
        std::fs::write(&path, format!("{}\nthis is not json\n", good.to_json())).unwrap();

        let store = OutcomeStore::open(&path).unwrap();
        assert_eq!(store.outcomes(), [good].as_slice());
        assert_eq!(store.load_failures().len(), 1);
        assert!(
            store.load_failures()[0].contains('2'),
            "the failure names its 1-based line: {:?}",
            store.load_failures()
        );
    }

    #[test]
    fn outcome_json_round_trips_an_event_ref_bearing_record_exactly() {
        let record = outcome("work-000001", Outcome::Refused);
        assert_eq!(record.refusal.as_deref(), Some("leash"));
        let json = record.to_json();
        // The on-disk shape named in the contract.
        assert_eq!(json["caused_by"], json!({"hash": "hash-one", "seq": 11}));
        assert_eq!(
            json["spine_event_refs"],
            json!([{"hash": "hash-two", "seq": 12}])
        );
        assert_eq!(OutcomeRecord::from_json(&json), Some(record));

        // Null caused_by and no refusal round-trip too.
        let bare = OutcomeRecord {
            caused_by: None,
            spine_event_refs: Vec::new(),
            refusal: None,
            ..outcome("work-000003", Outcome::Executed)
        };
        assert_eq!(OutcomeRecord::from_json(&bare.to_json()), Some(bare));
        assert_eq!(OutcomeRecord::from_json(&json!({"nope": true})), None);
    }
}
