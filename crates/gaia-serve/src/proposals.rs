//! Proposals: the watcher's output, recorded gaia-side.
//!
//! A proposal is an SI tool intent the fork *suggests* —
//! `{type:"tool", tool, args, reason}` in the SI-41 envelope —
//! plus the provenance that makes it auditable: the spine event
//! that caused it, carried as an [`EventRef`] (`hash` + `seq`).
//! The fork proposes; the SI gate decides; nothing in this crate
//! actuates anything (actuation is G-6).
//!
//! Storage follows G-2 §6.2: gaia records live in gaia-owned
//! stores, never in upstream spine enums. The proposal store is
//! an append-only JSONL file beside the mirror — one JSON object
//! per line, in the format [`Proposal::to_json`] writes. It is
//! the dedup memory of the watcher (§5.5): a loss event that
//! already has a proposal recorded against it is *answered*,
//! and the watcher stays silent about it forever.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use hearth_store::EventRef;
use serde_json::{json, Value};

/// One candidate action proposed to the SI gate.
///
/// `tool` is one of the §5.2 candidate names (`hearth.restart`,
/// `hearth.notify`). `args` is exactly what actuation would be
/// asked to execute if the gate admits the intent — for
/// residency proposals, `{model, spine_event: {hash, seq}}` —
/// so what the gate sees is what would run (§5.3: actuation
/// executes the admitted args verbatim).
#[derive(Debug, Clone, PartialEq)]
pub struct Proposal {
    pub model: String,
    pub tool: String,
    pub args: Value,
    pub reason: String,
    /// The spine event this proposal answers. Causal dedup keys
    /// on this ref: same hash + seq, same loss, already answered.
    pub caused_by: EventRef,
}

impl Proposal {
    /// The SI-41 intent envelope for this proposal — the object
    /// the gate will eventually see, byte for byte what was
    /// recorded here. The fork adds nothing and hides nothing
    /// between proposing and submission.
    pub fn intent_json(&self) -> Value {
        json!({
            "type": "tool",
            "tool": self.tool,
            "args": self.args,
            "reason": self.reason,
        })
    }

    /// The on-disk JSONL record: the intent envelope's fields
    /// plus `model` and the `caused_by` spine ref.
    pub fn to_json(&self) -> Value {
        json!({
            "model": self.model,
            "tool": self.tool,
            "args": self.args,
            "reason": self.reason,
            "caused_by": {
                "hash": self.caused_by.hash,
                "seq": self.caused_by.seq,
            },
        })
    }

    /// Parse one JSONL record back. Returns `None` for anything
    /// that is not recognizably a proposal — a torn final line
    /// after a crash must not make the store unreadable; the
    /// store counts it as a load failure instead (the same
    /// contract as `gaia_api::MirrorStore`).
    pub fn from_json(v: &Value) -> Option<Proposal> {
        let obj = v.as_object()?;
        let caused = obj.get("caused_by")?.as_object()?;
        Some(Proposal {
            model: obj.get("model")?.as_str()?.to_string(),
            tool: obj.get("tool")?.as_str()?.to_string(),
            args: obj.get("args").cloned().unwrap_or(Value::Null),
            reason: obj.get("reason")?.as_str()?.to_string(),
            caused_by: EventRef {
                hash: caused.get("hash")?.as_str()?.to_string(),
                seq: caused.get("seq")?.as_u64()?,
            },
        })
    }
}

/// The append-only proposal log.
///
/// In memory it is the list of every proposal ever recorded at
/// `path`; on disk it is the same list as JSONL. Recording is
/// append + flush, so a proposal the watcher believes it made
/// survives the process — dedup across restarts depends on it.
pub struct ProposalStore {
    path: PathBuf,
    proposals: Vec<Proposal>,
    load_failures: Vec<String>,
}

impl ProposalStore {
    /// Open the store at `path`, loading any proposals already
    /// recorded there. A missing file is an empty store, not an
    /// error. Malformed lines are skipped and counted in
    /// [`ProposalStore::load_failures`].
    pub fn open(path: &Path) -> Result<ProposalStore, String> {
        // A missing file is an empty store, not an error: the
        // first watcher run has recorded nothing yet, and that
        // absence is a fact, not a failure. Any other read
        // error is fatal, because loading a partial history
        // silently would break the dedup the store exists for.
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ProposalStore {
                    path: path.to_path_buf(),
                    proposals: Vec::new(),
                    load_failures: Vec::new(),
                });
            }
            Err(e) => {
                return Err(format!(
                    "cannot read proposal store {}: {e}",
                    path.display()
                ));
            }
        };

        let mut proposals = Vec::new();
        let mut load_failures = Vec::new();
        for (idx, line) in text.lines().enumerate() {
            let line_no = idx + 1;
            if line.trim().is_empty() {
                continue;
            }
            // One JSONL record per line: parse the JSON first,
            // then the proposal shape. Either failure is a load
            // failure naming its 1-based line — skipped, never
            // dropped silently, and never fatal to the rest.
            let proposal = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| Proposal::from_json(&v));
            match proposal {
                Some(proposal) => proposals.push(proposal),
                None => load_failures.push(format!(
                    "line {line_no}: not a proposal record; skipped"
                )),
            }
        }
        Ok(ProposalStore {
            path: path.to_path_buf(),
            proposals,
            load_failures,
        })
    }

    /// Every proposal recorded so far, oldest first.
    pub fn proposals(&self) -> &[Proposal] {
        &self.proposals
    }

    /// Lines that failed to parse when the store was opened.
    pub fn load_failures(&self) -> &[String] {
        &self.load_failures
    }

    /// Has this spine event already been answered by a proposal?
    /// The dedup question of §5.5, keyed on the event's identity
    /// (hash + seq), not on the model or the reason.
    pub fn answered(&self, ev: &EventRef) -> bool {
        self.proposals.iter().any(|p| &p.caused_by == ev)
    }

    /// Record a proposal: append its JSONL line to the file
    /// (flushed before returning) and keep it in memory. If the
    /// append fails, the proposal is *not* recorded and the
    /// error says so — a watcher must never believe it proposed
    /// something the record does not hold.
    pub fn record(&mut self, proposal: Proposal) -> Result<(), String> {
        // The file is written first, and flushed, because the
        // in-memory list is only a view of the record: if the
        // append fails, the proposal is not recorded anywhere,
        // and the caller gets the error rather than a belief
        // that a dedup entry exists when it does not.
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    format!(
                        "cannot create proposal store directory {}: {e}",
                        parent.display()
                    )
                })?;
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                format!(
                    "cannot open proposal store {}: {e}",
                    self.path.display()
                )
            })?;
        writeln!(file, "{}", proposal.to_json()).map_err(|e| {
            format!(
                "cannot append to proposal store {}: {e}",
                self.path.display()
            )
        })?;
        file.flush().map_err(|e| {
            format!(
                "cannot flush proposal store {}: {e}",
                self.path.display()
            )
        })?;
        self.proposals.push(proposal);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A proposal with a distinct causal ref per call site, so a
    /// test can say exactly which loss each record answers.
    fn proposal(model: &str, hash: &str, seq: u64) -> Proposal {
        Proposal {
            model: model.to_string(),
            tool: "hearth.restart".to_string(),
            args: json!({
                "model": model,
                "spine_event": { "hash": hash, "seq": seq },
            }),
            reason: format!("observed loss at seq {seq}"),
            caused_by: EventRef {
                hash: hash.to_string(),
                seq,
            },
        }
    }

    #[test]
    fn record_then_reopen_loads_both_proposals_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proposals.jsonl");
        let first = proposal("muse", "hash-one", 11);
        let second = proposal("muse", "hash-two", 12);

        {
            let mut store = ProposalStore::open(&path).unwrap();
            assert!(store.proposals().is_empty());
            store.record(first.clone()).unwrap();
            store.record(second.clone()).unwrap();
            assert_eq!(store.proposals().len(), 2);
        }

        let reopened = ProposalStore::open(&path).unwrap();
        assert_eq!(reopened.proposals(), [first.clone(), second.clone()].as_slice());
        assert!(reopened.load_failures().is_empty());
        assert!(reopened.answered(&first.caused_by));
        assert!(reopened.answered(&second.caused_by));
    }

    #[test]
    fn answered_keys_on_hash_and_seq_together() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proposals.jsonl");
        let recorded = proposal("muse", "hash-one", 11);

        let mut store = ProposalStore::open(&path).unwrap();
        store.record(recorded.clone()).unwrap();

        assert!(store.answered(&recorded.caused_by));
        // Same seq, different hash is a different event: the
        // seq alone is not the loss's identity.
        let same_seq_other_hash = EventRef {
            hash: "hash-other".to_string(),
            seq: recorded.caused_by.seq,
        };
        assert!(!store.answered(&same_seq_other_hash));
        // Same hash at a different seq is different too.
        let same_hash_other_seq = EventRef {
            hash: recorded.caused_by.hash.clone(),
            seq: recorded.caused_by.seq + 1,
        };
        assert!(!store.answered(&same_hash_other_seq));
    }

    #[test]
    fn a_garbage_line_is_a_load_failure_not_a_fatal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proposals.jsonl");
        let good = proposal("muse", "hash-one", 11);
        std::fs::write(&path, format!("{}\nthis is not json\n", good.to_json())).unwrap();

        let store = ProposalStore::open(&path).unwrap();
        assert_eq!(store.proposals(), [good].as_slice());
        assert_eq!(
            store.load_failures().len(),
            1,
            "the garbage line is on the record: {:?}",
            store.load_failures()
        );
        assert!(
            store.load_failures()[0].contains('2'),
            "the failure names its 1-based line: {:?}",
            store.load_failures()
        );
    }

    #[test]
    fn record_creates_a_nested_directory_that_does_not_exist_yet() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("deeper").join("proposals.jsonl");
        assert!(!path.parent().unwrap().exists());

        let recorded = proposal("muse", "hash-one", 11);
        let mut store = ProposalStore::open(&path).unwrap();
        store.record(recorded.clone()).unwrap();

        assert!(path.exists());
        let reopened = ProposalStore::open(&path).unwrap();
        assert_eq!(reopened.proposals(), [recorded].as_slice());
    }
}
