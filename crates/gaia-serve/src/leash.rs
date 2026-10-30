//! The operator leash: SI-journaled stops, recorded gaia-side.
//!
//! The leash is a record of SI-journaled stops surfaced through
//! the verdict path; this store is the fork's copy of that fact,
//! and actuation consults it before every process actuation.
//!
//! G-2 §5.2.1 states the rule: if SI's journal holds an
//! un-resumed stop for the target, the actuation refuses and
//! records the refusal. A stopped mind's runtime does not
//! restart its models behind its back. This module is only the
//! record of that fact — it makes no policy and issues no
//! stops of its own. Stops and resumes arrive here because SI
//! journaled them first and the verdict path surfaced them;
//! the fork carries the record so the actuator can read it at
//! the moment of actuation, without reaching back into SI.
//!
//! Storage follows G-2 §6.2 and the [`crate::proposals`]
//! conventions exactly: gaia records live in gaia-owned
//! stores, never in upstream spine enums. The leash store is
//! an append-only JSONL file — one JSON object per line, in
//! the format [`LeashRecord::to_json`] writes — or, in an
//! ephemeral store, in memory only. The latest record for a
//! model is the one that speaks: a stop followed by a resume
//! is not in force, and a resume followed by a stop is.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};

/// Whether a leash record stops its model or resumes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeashKind {
    Stop,
    Resume,
}

impl LeashKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            LeashKind::Stop => "stop",
            LeashKind::Resume => "resume",
        }
    }

    /// Parse the wire spelling. Anything else is `None`, not
    /// a guess — an unrecognised kind is not a record.
    fn parse(s: &str) -> Option<LeashKind> {
        match s {
            "stop" => Some(LeashKind::Stop),
            "resume" => Some(LeashKind::Resume),
            _ => None,
        }
    }
}

/// One stop or resume, as SI journaled it.
///
/// `journal_seq` is the sequence in *SI's* journal where SI
/// recorded the stop/resume — provenance, quoted in refusals;
/// it is NOT a spine seq (G-2 §4.2: the two sequence spaces
/// never mix). `reason` is SI's own words for why, carried
/// verbatim so a refusal can quote the stop it honours.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeashRecord {
    pub model: String,
    pub kind: LeashKind,
    pub journal_seq: u64,
    pub reason: String,
}

impl LeashRecord {
    /// The on-disk JSONL record.
    pub fn to_json(&self) -> Value {
        json!({
            "model": self.model,
            "kind": self.kind.as_str(),
            "journal_seq": self.journal_seq,
            "reason": self.reason,
        })
    }

    /// Parse one JSONL record back. Returns `None` for
    /// anything that is not recognizably a leash record — a
    /// torn final line after a crash must not make the store
    /// unreadable; the store counts it as a load failure
    /// instead (the same contract as [`crate::Proposal`]).
    pub fn from_json(v: &Value) -> Option<LeashRecord> {
        let obj = v.as_object()?;
        Some(LeashRecord {
            model: obj.get("model")?.as_str()?.to_string(),
            kind: LeashKind::parse(obj.get("kind")?.as_str()?)?,
            journal_seq: obj.get("journal_seq")?.as_u64()?,
            reason: obj.get("reason")?.as_str()?.to_string(),
        })
    }
}

/// The append-only leash log.
///
/// In memory it is the list of every stop/resume ever
/// recorded at `path`; on disk it is the same list as JSONL.
/// Recording is append + flush, so a stop the composition
/// believes it recorded survives the process — a leash that
/// forgot a stop across a restart would be worse than none.
/// An ephemeral leash has no file: its records live in
/// memory only and die with the process, which is honest for
/// a composition the binary has not yet given a home.
pub struct Leash {
    path: Option<PathBuf>,
    records: Vec<LeashRecord>,
    load_failures: Vec<String>,
}

impl Leash {
    /// Open the leash at `path`, loading any records already
    /// recorded there. A missing file is an empty leash, not
    /// an error. Malformed lines are skipped and counted in
    /// [`Leash::load_failures`].
    pub fn open(path: &Path) -> Result<Leash, String> {
        // A missing file is an empty store, not an error: no
        // stop has ever been recorded here, and that absence
        // is a fact, not a failure. Any other read error is
        // fatal, because loading a partial leash silently
        // could drop a stop that is still in force.
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Leash {
                    path: Some(path.to_path_buf()),
                    records: Vec::new(),
                    load_failures: Vec::new(),
                });
            }
            Err(e) => {
                return Err(format!(
                    "cannot read leash store {}: {e}",
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
            // first, then the leash shape. Either failure is
            // a load failure naming its 1-based line —
            // skipped, never dropped silently, and never
            // fatal to the rest.
            let record = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| LeashRecord::from_json(&v));
            match record {
                Some(record) => records.push(record),
                None => load_failures.push(format!(
                    "line {line_no}: not a leash record; skipped"
                )),
            }
        }
        Ok(Leash {
            path: Some(path.to_path_buf()),
            records,
            load_failures,
        })
    }

    /// A leash with no file behind it: records live in
    /// memory only.
    pub fn ephemeral() -> Leash {
        Leash {
            path: None,
            records: Vec::new(),
            load_failures: Vec::new(),
        }
    }

    /// Record a stop SI journaled for `model` at SI journal
    /// sequence `journal_seq`.
    pub fn record_stop(
        &mut self,
        model: &str,
        journal_seq: u64,
        reason: &str,
    ) -> Result<LeashRecord, String> {
        self.record(LeashRecord {
            model: model.to_string(),
            kind: LeashKind::Stop,
            journal_seq,
            reason: reason.to_string(),
        })
    }

    /// Record a resume SI journaled for `model` at SI
    /// journal sequence `journal_seq`.
    pub fn record_resume(
        &mut self,
        model: &str,
        journal_seq: u64,
        reason: &str,
    ) -> Result<LeashRecord, String> {
        self.record(LeashRecord {
            model: model.to_string(),
            kind: LeashKind::Resume,
            journal_seq,
            reason: reason.to_string(),
        })
    }

    /// Record one leash record: append its JSONL line to
    /// the file (flushed before returning) and only then
    /// keep it in memory. If the append fails, the record
    /// is *not* recorded and the error says so — the
    /// in-memory list is only a view of the record.
    fn record(&mut self, record: LeashRecord) -> Result<LeashRecord, String> {
        if let Some(path) = &self.path {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        format!(
                            "cannot create leash store directory {}: {e}",
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
                    format!("cannot open leash store {}: {e}", path.display())
                })?;
            writeln!(file, "{}", record.to_json()).map_err(|e| {
                format!("cannot append to leash store {}: {e}", path.display())
            })?;
            file.flush().map_err(|e| {
                format!("cannot flush leash store {}: {e}", path.display())
            })?;
        }
        self.records.push(record.clone());
        Ok(record)
    }

    /// The stop in force for `model`, if any: the model's
    /// *latest* record, returned only when it is a stop. A
    /// later resume lifts an earlier stop; a stop for one
    /// model says nothing about any other.
    pub fn stop_in_force(&self, model: &str) -> Option<&LeashRecord> {
        let latest = self.records.iter().rev().find(|r| r.model == model)?;
        match latest.kind {
            LeashKind::Stop => Some(latest),
            LeashKind::Resume => None,
        }
    }

    /// Is `model` currently stopped on the leash?
    pub fn is_stopped(&self, model: &str) -> bool {
        self.stop_in_force(model).is_some()
    }

    /// Every leash record recorded so far, oldest first.
    pub fn records(&self) -> &[LeashRecord] {
        &self.records
    }

    /// Lines that failed to parse when the store was opened.
    pub fn load_failures(&self) -> &[String] {
        &self.load_failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_then_reopen_loads_both_records_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leash.jsonl");

        let stop;
        let resume;
        {
            let mut leash = Leash::open(&path).unwrap();
            assert!(leash.records().is_empty());
            stop = leash.record_stop("muse", 7, "operator stop").unwrap();
            resume = leash.record_resume("muse", 9, "operator resume").unwrap();
            assert_eq!(leash.records().len(), 2);
        }

        let reopened = Leash::open(&path).unwrap();
        assert_eq!(reopened.records(), [stop, resume].as_slice());
        assert!(reopened.load_failures().is_empty());
    }

    #[test]
    fn ephemeral_records_nothing_to_disk_but_answers_queries() {
        let mut leash = Leash::ephemeral();
        let stop = leash.record_stop("muse", 7, "operator stop").unwrap();
        assert_eq!(stop.kind.as_str(), "stop");
        assert!(leash.is_stopped("muse"));
        assert_eq!(leash.stop_in_force("muse"), Some(&stop));
        assert_eq!(leash.records(), [stop].as_slice());
        assert!(leash.load_failures().is_empty());
    }

    #[test]
    fn stop_in_force_follows_the_latest_record() {
        let mut leash = Leash::ephemeral();
        // stop -> resume -> None: the resume lifts the stop.
        leash.record_stop("muse", 7, "stop").unwrap();
        assert!(leash.is_stopped("muse"));
        leash.record_resume("muse", 8, "resume").unwrap();
        assert_eq!(leash.stop_in_force("muse"), None);
        assert!(!leash.is_stopped("muse"));

        // resume -> stop -> Some: the stop is the latest word.
        let stop = leash.record_stop("muse", 9, "stop again").unwrap();
        assert_eq!(stop.kind, LeashKind::Stop);
        assert_eq!(LeashKind::Resume.as_str(), "resume");
        assert_eq!(leash.stop_in_force("muse"), Some(&stop));

        // A stop for model A says nothing about model B.
        assert_eq!(leash.stop_in_force("other"), None);
        assert!(!leash.is_stopped("other"));
        let mut other = Leash::ephemeral();
        other.record_stop("model-a", 1, "stop").unwrap();
        assert!(other.is_stopped("model-a"));
        assert!(!other.is_stopped("model-b"));
    }

    #[test]
    fn a_garbage_line_is_a_load_failure_not_a_fatal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("leash.jsonl");
        let good = LeashRecord {
            model: "muse".to_string(),
            kind: LeashKind::Stop,
            journal_seq: 7,
            reason: "operator stop".to_string(),
        };
        std::fs::write(&path, format!("{}\nthis is not json\n", good.to_json())).unwrap();

        let leash = Leash::open(&path).unwrap();
        assert_eq!(leash.records(), [good].as_slice());
        assert_eq!(leash.load_failures().len(), 1);
        assert!(
            leash.load_failures()[0].contains('2'),
            "the failure names its 1-based line: {:?}",
            leash.load_failures()
        );
    }

    #[test]
    fn leash_record_json_round_trips() {
        let record = LeashRecord {
            model: "muse".to_string(),
            kind: LeashKind::Resume,
            journal_seq: 42,
            reason: "back on".to_string(),
        };
        assert_eq!(LeashRecord::from_json(&record.to_json()), Some(record));
        assert_eq!(LeashRecord::from_json(&json!({"nope": true})), None);
    }
}
