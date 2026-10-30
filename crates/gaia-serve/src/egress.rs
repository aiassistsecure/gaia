//! Egress: what SI answered, recorded sender-side.
//!
//! G-9 §8.4: on ACK from SI, the fork records the pair
//! `(caused_by → journal_seq, verdict)` in a sender-side
//! egress log. This store is that log. Proposals are born in
//! [`crate::proposals`]; the egress pass ([`drive_egress`])
//! offers each one to SI's proposal ingest, and this store
//! remembers what came back.
//!
//! It exists for two reasons, and both are about telling
//! apart situations a bare proposal list cannot:
//!
//!   * **Restart-safe retry state.** A proposal with no
//!     egress record has not been answered, so a later pass
//!     re-sends it; a proposal with a terminal record is
//!     never re-sent. That split has to survive the process,
//!     or a restart would either lose proposals or lean on
//!     SI's dedup to absorb re-sends of answered ones.
//!   * **The disposition record.** The watcher's
//!     `answered(caused_by)` already suppresses re-proposal
//!     of a loss, but it cannot say *how* the loss was
//!     answered. Operators must be able to distinguish
//!     "gated and refused" (rejected, escalated) from
//!     "never sent" (pending) — those are opposite facts
//!     about the gate, and only this log holds them.
//!
//! Only **terminal** dispositions are ever written here.
//! `Pending` is a derived state — a proposal with no egress
//! record — never a file line. That is what makes "kill
//! between POST and ACK, then retry" produce exactly one
//! egress-log line: nothing is written when the proposal is
//! sent, only when an answer is recorded, so a send that
//! never got its answer leaves no trace to double-count and
//! the retry writes the single line the answer earns.
//!
//! Storage follows G-2 §6.2 and the [`crate::proposals`] /
//! [`crate::outcomes`] conventions exactly: gaia records
//! live in gaia-owned stores, never in upstream spine enums.
//! The egress store is an append-only JSONL file — one JSON
//! object per line, in the format [`EgressRecord::to_json`]
//! writes — or, in an ephemeral store, in memory only.
//! Sequence spaces stay distinct (G-2 §4.2): `caused_by` is
//! a spine [`EventRef`] (`hash` + `seq`), while
//! `journal_seq` and `task_seq` are SI journal sequences,
//! carried as provenance and never written into a spine-ref
//! field.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use hearth_store::EventRef;
use serde_json::{json, Value};

use crate::egress_client::{Ack, GateVerdict, ProposalSink, SendOutcome};
use crate::proposals::Proposal;

/// Where one proposal stands with SI.
///
/// `Pending` is derived — the absence of a record — and is
/// never stored. The other four are terminal: once one is
/// recorded for a `caused_by`, the proposal is answered and
/// no egress pass offers it to SI again. `Acked`,
/// `Rejected`, and `Escalated` carry the ACK's pair of SI
/// sequences: `journal_seq`, the self-edit row the proposal
/// became — the intent's identity for every later flow
/// (G-9 §8.3) — and `task_seq`, the seam task it attached
/// to, when the ACK names one. `Failed` carries the HTTP
/// status of SI's terminal refusal instead: a 4xx is a
/// shape defect, an answer rather than a transport problem
/// (G-9 §8.3), so it is recorded with its status and never
/// retried blindly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    Pending,
    Acked {
        journal_seq: u64,
        task_seq: Option<u64>,
    },
    Rejected {
        journal_seq: u64,
        task_seq: Option<u64>,
    },
    Escalated {
        journal_seq: u64,
        task_seq: Option<u64>,
    },
    Failed {
        status: u16,
    },
}

impl Disposition {
    /// Terminal dispositions are answers: the proposal they
    /// belong to is never re-sent. `Pending` is the only
    /// non-terminal one, and it is the only one that is not
    /// a record.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Disposition::Pending)
    }

    /// The disposition an ACK records, by the gate verdict
    /// it carries: `Admit` was acked, `Reject` was rejected,
    /// `Escalate` was escalated. The ACK's sequences come
    /// along verbatim — they are SI's numbers, quoted, not
    /// derived from anything fork-side.
    pub fn from_ack(ack: &Ack) -> Disposition {
        match ack.verdict {
            GateVerdict::Admit => Disposition::Acked {
                journal_seq: ack.journal_seq,
                task_seq: ack.task_seq,
            },
            GateVerdict::Reject => Disposition::Rejected {
                journal_seq: ack.journal_seq,
                task_seq: ack.task_seq,
            },
            GateVerdict::Escalate => Disposition::Escalated {
                journal_seq: ack.journal_seq,
                task_seq: ack.task_seq,
            },
        }
    }

    /// The wire spelling, as written in the JSONL record.
    pub fn name(&self) -> &'static str {
        match self {
            Disposition::Pending => "pending",
            Disposition::Acked { .. } => "acked",
            Disposition::Rejected { .. } => "rejected",
            Disposition::Escalated { .. } => "escalated",
            Disposition::Failed { .. } => "failed",
        }
    }
}

/// One answered proposal: the spine event that caused it
/// and the terminal disposition SI's answer recorded.
///
/// The key is the whole `caused_by` pair — hash and seq
/// together, never either half alone (G-9 §8.2) — because
/// that pair is SI's dedup key as well as this log's: the
/// two sides agree on what "the same proposal" means by
/// construction, not by convention.
#[derive(Debug, Clone, PartialEq)]
pub struct EgressRecord {
    pub caused_by: EventRef,
    pub disposition: Disposition,
}

impl EgressRecord {
    /// The on-disk JSONL record. `task_seq` is omitted when
    /// the ACK named none, rather than written as a null
    /// the reader would have to interpret; `failed` records
    /// carry `status` and no journal sequences, because a
    /// refused-malformed proposal journaled nothing at SI
    /// and has no sequences to quote.
    pub fn to_json(&self) -> Value {
        let mut obj = json!({
            "caused_by": {
                "hash": self.caused_by.hash,
                "seq": self.caused_by.seq,
            },
            "disposition": self.disposition.name(),
        });
        match &self.disposition {
            Disposition::Acked {
                journal_seq,
                task_seq,
            }
            | Disposition::Rejected {
                journal_seq,
                task_seq,
            }
            | Disposition::Escalated {
                journal_seq,
                task_seq,
            } => {
                obj["journal_seq"] = json!(journal_seq);
                if let Some(task_seq) = task_seq {
                    obj["task_seq"] = json!(task_seq);
                }
            }
            Disposition::Failed { status } => {
                obj["status"] = json!(status);
            }
            // Pending is never a file line (see the module
            // docs): it serialises to its bare spelling here
            // only so this function is total, and `from_json`
            // refuses it below — a "pending" line found on
            // disk is a defect, not a record.
            Disposition::Pending => {}
        }
        obj
    }

    /// Parse one JSONL record back. Strict, in the
    /// [`crate::Proposal`] sense: returns `None` for
    /// anything that is not recognizably a terminal egress
    /// record — acked / rejected / escalated require a
    /// `journal_seq` (`task_seq` optional), failed requires
    /// a `status` that fits a `u16`, anything else (an
    /// unknown disposition spelling, a "pending" line, a
    /// missing sequence) is not a record. The store counts
    /// a `None` as a load failure instead of letting a torn
    /// or foreign line make the log unreadable.
    pub fn from_json(v: &Value) -> Option<EgressRecord> {
        let obj = v.as_object()?;
        let caused = obj.get("caused_by")?.as_object()?;
        let caused_by = EventRef {
            hash: caused.get("hash")?.as_str()?.to_string(),
            seq: caused.get("seq")?.as_u64()?,
        };
        let disposition = match obj.get("disposition")?.as_str()? {
            name @ ("acked" | "rejected" | "escalated") => {
                let journal_seq = obj.get("journal_seq")?.as_u64()?;
                let task_seq = match obj.get("task_seq") {
                    None | Some(Value::Null) => None,
                    Some(v) => Some(v.as_u64()?),
                };
                match name {
                    "acked" => Disposition::Acked {
                        journal_seq,
                        task_seq,
                    },
                    "rejected" => Disposition::Rejected {
                        journal_seq,
                        task_seq,
                    },
                    _ => Disposition::Escalated {
                        journal_seq,
                        task_seq,
                    },
                }
            }
            "failed" => Disposition::Failed {
                status: u16::try_from(obj.get("status")?.as_u64()?).ok()?,
            },
            _ => return None,
        };
        Some(EgressRecord {
            caused_by,
            disposition,
        })
    }
}

/// The append-only egress log.
///
/// In memory it is the list of every terminal answer ever
/// recorded at `path`; on disk it is the same list as
/// JSONL. Recording is append + flush, so an answer the
/// egress pass believes it filed survives the process —
/// restart-safe retry depends on it. An ephemeral store
/// has no file: its records live in memory only and die
/// with the process, which is honest for a composition the
/// binary has not yet given a home.
pub struct EgressStore {
    path: Option<PathBuf>,
    records: Vec<EgressRecord>,
    load_failures: Vec<String>,
}

impl EgressStore {
    /// Open the store at `path`, loading any answers
    /// already recorded there. A missing file is an empty
    /// store, not an error. Malformed lines are skipped and
    /// counted in [`EgressStore::load_failures`].
    pub fn open(path: &Path) -> Result<EgressStore, String> {
        // A missing file is an empty store, not an error:
        // no proposal has ever been answered here, and that
        // absence is a fact, not a failure. Any other read
        // error is fatal, because loading a partial history
        // silently would misreport which proposals SI has
        // answered — and answered proposals must not be
        // re-sent on a guess.
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(EgressStore {
                    path: Some(path.to_path_buf()),
                    records: Vec::new(),
                    load_failures: Vec::new(),
                });
            }
            Err(e) => {
                return Err(format!("cannot read egress store {}: {e}", path.display()));
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
            // first, then the egress shape. Either failure
            // is a load failure naming its 1-based line —
            // skipped, never dropped silently, and never
            // fatal to the rest.
            let record = serde_json::from_str::<Value>(line)
                .ok()
                .and_then(|v| EgressRecord::from_json(&v));
            match record {
                Some(record) => records.push(record),
                None => {
                    load_failures.push(format!("line {line_no}: not an egress record; skipped"))
                }
            }
        }
        Ok(EgressStore {
            path: Some(path.to_path_buf()),
            records,
            load_failures,
        })
    }

    /// A store with no file behind it: records live in
    /// memory only.
    pub fn ephemeral() -> EgressStore {
        EgressStore {
            path: None,
            records: Vec::new(),
            load_failures: Vec::new(),
        }
    }

    /// Every egress record recorded so far, oldest first.
    pub fn records(&self) -> &[EgressRecord] {
        &self.records
    }

    /// Lines that failed to parse when the store was opened.
    pub fn load_failures(&self) -> &[String] {
        &self.load_failures
    }

    /// The disposition of one proposal, keyed on the whole
    /// `caused_by` pair (hash + seq together): the *latest*
    /// record for the key speaks, and a key with no record
    /// is [`Disposition::Pending`] — derived here, at the
    /// read, which is the only place pending ever exists.
    pub fn disposition(&self, caused_by: &EventRef) -> Disposition {
        self.records
            .iter()
            .rev()
            .find(|r| &r.caused_by == caused_by)
            .map(|r| r.disposition.clone())
            .unwrap_or(Disposition::Pending)
    }

    /// Record an answer: append its JSONL line to the file
    /// first (flushed before returning) and only then keep
    /// it in memory. If the append fails, the answer is
    /// *not* recorded and the error says so — the in-memory
    /// list is only a view of the record, and a pass must
    /// never believe it filed a disposition the log does
    /// not hold.
    pub fn record(&mut self, record: EgressRecord) -> Result<(), String> {
        if let Some(path) = &self.path {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        format!(
                            "cannot create egress store directory {}: {e}",
                            parent.display()
                        )
                    })?;
                }
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|e| format!("cannot open egress store {}: {e}", path.display()))?;
            writeln!(file, "{}", record.to_json())
                .map_err(|e| format!("cannot append to egress store {}: {e}", path.display()))?;
            file.flush()
                .map_err(|e| format!("cannot flush egress store {}: {e}", path.display()))?;
        }
        self.records.push(record);
        Ok(())
    }
}

/// What one egress pass did, in counts. `attempted` counts
/// the proposals actually offered to the sink; `skipped`
/// counts the ones a terminal record already answered, so
/// they were not offered at all; `still_pending` counts the
/// ones the transport could not deliver an answer for, so
/// they will be offered again by a later pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EgressPass {
    pub attempted: usize,
    pub acked: usize,
    pub rejected: usize,
    pub escalated: usize,
    pub failed: usize,
    pub still_pending: usize,
    pub skipped: usize,
}

/// One egress pass: offer every proposal with no terminal
/// egress record to the sink, in proposal order, and record
/// each answer in the store.
///
/// The outcomes of one offer:
///
///   * **ACKed** — the disposition the ACK's verdict names
///     is recorded with the ACK's sequences, and counted
///     under that verdict.
///   * **Terminal** (a 4xx) — recorded as
///     [`Disposition::Failed`] with its status and counted
///     as failed: it is an answer, so the proposal is never
///     offered again.
///   * **Retryable** (transport failure, 5xx) — nothing is
///     written and the proposal stays pending; the reason
///     string is the transport's business, and the pass
///     summary counts the proposal without editorializing
///     about why it could not be delivered.
///
/// If recording an answer fails, the pass returns `Err`
/// immediately rather than claim answers the record does
/// not hold. The ACK is not lost knowledge when that
/// happens: SI dedups by `caused_by` (G-9 §8.3), so the
/// next pass re-sends the proposal and SI returns the
/// identical ACK, which the pass then re-records — the
/// record heals on the retry, and no answer is ever filed
/// twice for it.
pub fn drive_egress(
    proposals: &[Proposal],
    store: &mut EgressStore,
    sink: &dyn ProposalSink,
) -> Result<EgressPass, String> {
    let mut pass = EgressPass::default();
    for proposal in proposals {
        if store.disposition(&proposal.caused_by).is_terminal() {
            pass.skipped += 1;
            continue;
        }
        pass.attempted += 1;
        match sink.post_proposal(proposal) {
            SendOutcome::Acked(ack) => {
                let disposition = Disposition::from_ack(&ack);
                match &disposition {
                    Disposition::Acked { .. } => pass.acked += 1,
                    Disposition::Rejected { .. } => pass.rejected += 1,
                    Disposition::Escalated { .. } => pass.escalated += 1,
                    _ => {}
                }
                store.record(EgressRecord {
                    caused_by: proposal.caused_by.clone(),
                    disposition,
                })?;
            }
            SendOutcome::Terminal(status) => {
                store.record(EgressRecord {
                    caused_by: proposal.caused_by.clone(),
                    disposition: Disposition::Failed { status },
                })?;
                pass.failed += 1;
            }
            SendOutcome::Retryable(_) => {
                pass.still_pending += 1;
            }
        }
    }
    Ok(pass)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::collections::VecDeque;

    /// A proposal with a distinct causal ref per call site,
    /// so a test can say exactly which loss each send and
    /// each record answers.
    fn proposal(hash: &str, seq: u64) -> Proposal {
        Proposal {
            model: "muse".to_string(),
            tool: "hearth.restart".to_string(),
            args: json!({
                "model": "muse",
                "spine_event": { "hash": hash, "seq": seq },
            }),
            reason: format!("observed loss at seq {seq}"),
            caused_by: EventRef {
                hash: hash.to_string(),
                seq,
            },
        }
    }

    fn ack(verdict: GateVerdict, journal_seq: u64, task_seq: Option<u64>) -> SendOutcome {
        SendOutcome::Acked(Ack {
            task_seq,
            journal_seq,
            verdict,
        })
    }

    /// A sink that plays back a scripted queue of outcomes
    /// and remembers every proposal it was called with, so
    /// a test can assert both what was recorded and what
    /// was (and was not) sent.
    struct FakeSink {
        outcomes: RefCell<VecDeque<SendOutcome>>,
        calls: RefCell<Vec<EventRef>>,
    }

    impl FakeSink {
        fn new(outcomes: Vec<SendOutcome>) -> FakeSink {
            FakeSink {
                outcomes: RefCell::new(outcomes.into()),
                calls: RefCell::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<EventRef> {
            self.calls.borrow().clone()
        }
    }

    impl ProposalSink for FakeSink {
        fn post_proposal(&self, proposal: &Proposal) -> SendOutcome {
            self.calls.borrow_mut().push(proposal.caused_by.clone());
            self.outcomes
                .borrow_mut()
                .pop_front()
                .expect("FakeSink ran out of scripted outcomes")
        }
    }

    #[test]
    fn drive_records_each_verdict_with_the_acks_sequences() {
        let proposals = vec![
            proposal("hash-one", 11),
            proposal("hash-two", 12),
            proposal("hash-three", 13),
        ];
        let sink = FakeSink::new(vec![
            ack(GateVerdict::Admit, 101, Some(7)),
            ack(GateVerdict::Reject, 102, Some(7)),
            ack(GateVerdict::Escalate, 103, None),
        ]);
        let mut store = EgressStore::ephemeral();

        let pass = drive_egress(&proposals, &mut store, &sink).unwrap();

        assert_eq!(
            pass,
            EgressPass {
                attempted: 3,
                acked: 1,
                rejected: 1,
                escalated: 1,
                failed: 0,
                still_pending: 0,
                skipped: 0,
            }
        );
        assert_eq!(sink.calls().len(), 3);
        assert_eq!(store.records().len(), 3);
        assert_eq!(
            store.disposition(&proposals[0].caused_by),
            Disposition::Acked {
                journal_seq: 101,
                task_seq: Some(7),
            }
        );
        assert_eq!(
            store.disposition(&proposals[1].caused_by),
            Disposition::Rejected {
                journal_seq: 102,
                task_seq: Some(7),
            }
        );
        assert_eq!(
            store.disposition(&proposals[2].caused_by),
            Disposition::Escalated {
                journal_seq: 103,
                task_seq: None,
            }
        );
    }

    #[test]
    fn terminal_failure_is_recorded_and_never_resent() {
        let proposals = vec![proposal("hash-one", 11)];
        let sink = FakeSink::new(vec![SendOutcome::Terminal(400)]);
        let mut store = EgressStore::ephemeral();

        let first = drive_egress(&proposals, &mut store, &sink).unwrap();
        assert_eq!(first.attempted, 1);
        assert_eq!(first.failed, 1);
        assert_eq!(
            store.disposition(&proposals[0].caused_by),
            Disposition::Failed { status: 400 }
        );

        // The second pass sees the terminal record and does
        // not call the sink for this proposal again.
        let second = drive_egress(&proposals, &mut store, &sink).unwrap();
        assert_eq!(second.attempted, 0);
        assert_eq!(second.skipped, 1);
        assert_eq!(sink.calls().len(), 1);
    }

    #[test]
    fn retryable_leaves_pending_and_the_next_pass_resends() {
        let proposals = vec![proposal("hash-one", 11)];
        let sink = FakeSink::new(vec![
            SendOutcome::Retryable("connection refused".to_string()),
            ack(GateVerdict::Admit, 101, Some(7)),
        ]);
        let mut store = EgressStore::ephemeral();

        let first = drive_egress(&proposals, &mut store, &sink).unwrap();
        assert_eq!(first.attempted, 1);
        assert_eq!(first.still_pending, 1);
        // Nothing was written: pending is the absence of a
        // record, not a record of absence.
        assert!(store.records().is_empty());
        assert_eq!(
            store.disposition(&proposals[0].caused_by),
            Disposition::Pending
        );
        assert!(!Disposition::Pending.is_terminal());

        let second = drive_egress(&proposals, &mut store, &sink).unwrap();
        assert_eq!(second.attempted, 1);
        assert_eq!(second.acked, 1);
        assert_eq!(sink.calls().len(), 2);
        assert_eq!(
            store.disposition(&proposals[0].caused_by),
            Disposition::Acked {
                journal_seq: 101,
                task_seq: Some(7),
            }
        );
    }

    #[test]
    fn record_then_reopen_loads_the_same_dispositions_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("egress.jsonl");
        let records = vec![
            EgressRecord {
                caused_by: EventRef {
                    hash: "hash-one".to_string(),
                    seq: 11,
                },
                disposition: Disposition::Acked {
                    journal_seq: 101,
                    task_seq: Some(7),
                },
            },
            EgressRecord {
                caused_by: EventRef {
                    hash: "hash-two".to_string(),
                    seq: 12,
                },
                disposition: Disposition::Escalated {
                    journal_seq: 102,
                    task_seq: None,
                },
            },
            EgressRecord {
                caused_by: EventRef {
                    hash: "hash-three".to_string(),
                    seq: 13,
                },
                disposition: Disposition::Failed { status: 400 },
            },
        ];

        {
            let mut store = EgressStore::open(&path).unwrap();
            assert!(store.records().is_empty());
            for record in &records {
                store.record(record.clone()).unwrap();
            }
            assert_eq!(store.records().len(), records.len());
        }

        // One line per record: only terminal dispositions
        // are ever written, so the file holds exactly the
        // records, nothing derived and nothing pending.
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), records.len());

        let reopened = EgressStore::open(&path).unwrap();
        assert_eq!(reopened.records(), records.as_slice());
        assert!(reopened.load_failures().is_empty());
        for record in &records {
            assert_eq!(reopened.disposition(&record.caused_by), record.disposition);
            assert!(record.disposition.is_terminal());
        }
    }

    #[test]
    fn a_garbage_line_is_a_load_failure_not_a_fatal_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("egress.jsonl");
        let good = EgressRecord {
            caused_by: EventRef {
                hash: "hash-one".to_string(),
                seq: 11,
            },
            disposition: Disposition::Rejected {
                journal_seq: 101,
                task_seq: Some(7),
            },
        };
        std::fs::write(&path, format!("{}\nthis is not json\n", good.to_json())).unwrap();

        let store = EgressStore::open(&path).unwrap();
        assert_eq!(store.records(), [good].as_slice());
        assert_eq!(store.load_failures().len(), 1);
        assert!(
            store.load_failures()[0].contains('2'),
            "the failure names its 1-based line: {:?}",
            store.load_failures()
        );
    }

    #[test]
    fn a_record_failure_propagates_out_of_drive_egress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blocker").join("egress.jsonl");
        // Open while the parent does not exist (a missing
        // file is an empty store), then put a regular file
        // where the parent directory would have to be: the
        // append behind `record` now has nowhere to go.
        let mut store = EgressStore::open(&path).unwrap();
        std::fs::write(dir.path().join("blocker"), "not a directory").unwrap();

        let proposals = vec![proposal("hash-one", 11)];
        let sink = FakeSink::new(vec![ack(GateVerdict::Admit, 101, Some(7))]);

        let result = drive_egress(&proposals, &mut store, &sink);
        assert!(
            result.is_err(),
            "an ACK that cannot be recorded fails the pass: {result:?}"
        );
        // And the answer is not recorded anywhere: the
        // proposal is still pending, so the next pass
        // re-sends it and SI's dedup returns the same ACK.
        assert!(store.records().is_empty());
        assert_eq!(
            store.disposition(&proposals[0].caused_by),
            Disposition::Pending
        );
    }

    #[test]
    fn disposition_is_latest_wins_and_keys_on_the_whole_pair() {
        let mut store = EgressStore::ephemeral();
        let caused_by = EventRef {
            hash: "hash-one".to_string(),
            seq: 11,
        };
        store
            .record(EgressRecord {
                caused_by: caused_by.clone(),
                disposition: Disposition::Acked {
                    journal_seq: 101,
                    task_seq: Some(7),
                },
            })
            .unwrap();
        store
            .record(EgressRecord {
                caused_by: caused_by.clone(),
                disposition: Disposition::Failed { status: 500 },
            })
            .unwrap();

        // Two records for one key: the later speaks.
        assert_eq!(
            store.disposition(&caused_by),
            Disposition::Failed { status: 500 }
        );
        // Same seq under a different hash, and the same hash
        // at a different seq, are different keys: the pair
        // is the identity, and both are still pending.
        let same_seq_other_hash = EventRef {
            hash: "hash-other".to_string(),
            seq: 11,
        };
        assert_eq!(
            store.disposition(&same_seq_other_hash),
            Disposition::Pending
        );
        let same_hash_other_seq = EventRef {
            hash: "hash-one".to_string(),
            seq: 12,
        };
        assert_eq!(
            store.disposition(&same_hash_other_seq),
            Disposition::Pending
        );
    }

    #[test]
    fn egress_record_json_shape_and_strictness() {
        let acked = EgressRecord {
            caused_by: EventRef {
                hash: "hash-one".to_string(),
                seq: 11,
            },
            disposition: Disposition::Acked {
                journal_seq: 101,
                task_seq: Some(7),
            },
        };
        // The on-disk shape named in the contract.
        assert_eq!(
            acked.to_json(),
            json!({
                "caused_by": {"hash": "hash-one", "seq": 11},
                "disposition": "acked",
                "journal_seq": 101,
                "task_seq": 7,
            })
        );
        assert_eq!(EgressRecord::from_json(&acked.to_json()), Some(acked));

        // No task_seq in the ACK, no task_seq key on disk.
        let no_task = EgressRecord {
            caused_by: EventRef {
                hash: "hash-two".to_string(),
                seq: 12,
            },
            disposition: Disposition::Rejected {
                journal_seq: 102,
                task_seq: None,
            },
        };
        let no_task_json = no_task.to_json();
        assert!(no_task_json.get("task_seq").is_none());
        assert_eq!(no_task.disposition.name(), "rejected");
        assert_eq!(EgressRecord::from_json(&no_task_json), Some(no_task));

        let failed = EgressRecord {
            caused_by: EventRef {
                hash: "hash-three".to_string(),
                seq: 13,
            },
            disposition: Disposition::Failed { status: 422 },
        };
        assert_eq!(
            failed.to_json(),
            json!({
                "caused_by": {"hash": "hash-three", "seq": 13},
                "disposition": "failed",
                "status": 422,
            })
        );
        assert_eq!(EgressRecord::from_json(&failed.to_json()), Some(failed));

        // Strictness: an unknown spelling, a missing
        // journal_seq, an out-of-range status, and a
        // "pending" line are none of them records.
        assert_eq!(
            EgressRecord::from_json(&json!({
                "caused_by": {"hash": "h", "seq": 1},
                "disposition": "shrugged",
                "journal_seq": 1,
            })),
            None
        );
        assert_eq!(
            EgressRecord::from_json(&json!({
                "caused_by": {"hash": "h", "seq": 1},
                "disposition": "acked",
            })),
            None
        );
        assert_eq!(
            EgressRecord::from_json(&json!({
                "caused_by": {"hash": "h", "seq": 1},
                "disposition": "failed",
                "status": 70000,
            })),
            None
        );
        assert_eq!(
            EgressRecord::from_json(&json!({
                "caused_by": {"hash": "h", "seq": 1},
                "disposition": "pending",
            })),
            None
        );
    }
}
