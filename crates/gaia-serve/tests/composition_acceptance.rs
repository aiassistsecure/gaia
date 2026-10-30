//! Composition acceptance tests for G-5 (G-2 §7, G-5 criteria
//! (1) and (2)).
//!
//! These are the fleet-scenario tests the slice was specced
//! with, not unit tests of a function: a real supervisor starts
//! a real child process, the child is killed out from under it,
//! and the assertions read the spine, the fleet state, and the
//! proposal store the way an operator (or the SI gate) would.
//!
//! Criterion (1): killing a resident child's process in the
//! `gaia-serve` composition produces exactly one proposal,
//! causally linked to the spine `lost` event, and no restart
//! occurs.
//!
//! Criterion (2): the same scenario under the upstream
//! `hearth` binary keeps upstream behaviour — proven as a
//! composition-boundary test (the restart wiring exists in
//! upstream's `cmd_up` and in no `gaia-serve` source), with a
//! behavioural companion that pins what the upstream policy
//! itself does when it *is* called on a freshly dead child.
//!
//! This file lives in `tests/`, outside the `src/` scan the
//! second test performs — which is what lets it name the
//! upstream call shape it is scanning for.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gaia_api::{EventHub, MirrorStore};
use gaia_serve::{Composition, ProposalStore, Watcher};
use hearth_core::{Budget, Declared, LostReason, Residency, GIB};
use hearth_serve::server::{free_port, ServerSpec};
use hearth_serve::Supervisor;
use hearth_store::Spine;

/// The fake serving child: a tiny HTTP server that answers 200
/// with `{}` on any GET, including the supervisor's `/health`
/// probe. It parses `--port` out of its argv and ignores the
/// rest — the supervisor passes the llama-server argv shape
/// (`-m`, `--host`, `--n-gpu-layers`, …) and a stand-in runtime
/// is under no obligation to honour knobs it does not have.
const FAKE_SERVER: &str = r#"#!/usr/bin/env python3
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[sys.argv.index("--port") + 1])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"{}"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass

HTTPServer(("127.0.0.1", port), Handler).serve_forever()
"#;

#[test]
fn a_killed_resident_child_produces_one_proposal_and_no_restart() {
    let dir = tempfile::tempdir().unwrap();

    // The fake server script, executable, and a dummy weights
    // file — the supervisor only checks the path exists before
    // spawning, and the script never reads it.
    let script = dir.path().join("fake_server.py");
    std::fs::write(&script, FAKE_SERVER).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let gguf = dir.path().join("fake.gguf");
    std::fs::write(&gguf, b"not a real gguf").unwrap();

    // The composition, exactly as the binary builds it.
    let spine = Spine::open(&dir.path().join("spine")).unwrap();
    let mirror = Arc::new(Mutex::new(
        MirrorStore::open(&dir.path().join("mirror.jsonl")).unwrap(),
    ));
    let proposals = ProposalStore::open(&dir.path().join("proposals.jsonl")).unwrap();
    let watcher = Watcher::new(["fake".to_string()]);
    let hub = EventHub::new(256);
    let supervisor = Supervisor::new(
        spine,
        Budget {
            total_bytes: 24 * GIB,
            reserve_bytes: 2 * GIB,
        },
        vec![Declared {
            model: "fake".to_string(),
            weights_bytes: GIB,
            kv_bytes: 0,
        }],
    );
    let mut comp = Composition::new(supervisor, mirror, watcher, proposals, hub);

    // Start the model through the composition's supervisor.
    let mut spec = ServerSpec::new("fake", &gguf, free_port().unwrap());
    spec.binary = script.clone();
    spec.log_dir = dir.path().to_path_buf();
    comp.supervisor().start(spec).unwrap();

    // Step the composition until the spine calls the model
    // resident. Bounded: a model that never comes up is a
    // failure of this test, not a hang.
    let mut resident = false;
    for _ in 0..300 {
        comp.tick_once().unwrap();
        if comp
            .spine()
            .latest("fake")
            .is_some_and(|e| e.kind == "resident")
        {
            resident = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(resident, "fake never became resident");

    // The child's pid is a spine fact (the `loading` event's
    // facts), not a process-table guess.
    let pid = comp
        .spine()
        .why("fake")
        .into_iter()
        .find(|e| e.kind == "loading")
        .expect("a resident model has a loading event")
        .facts["pid"]
        .as_u64()
        .expect("the loading event records a pid");

    // Kill the child out from under the supervisor.
    let status = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success(), "kill -9 {pid} failed: {status}");

    // Wait until the child is actually dead (a zombie awaiting
    // the supervisor's reap, or gone entirely) before ticking
    // again. This is synchronisation, not patience: the
    // scenario under test is a *dead* child, and a probe that
    // lands in the window between the signal and the exit is a
    // different scenario with a different, upstream-correct
    // answer — §1.2 maps a resident's failed probe to
    // `Evicted`. Letting the race decide which loss this test
    // exercises would make the fleet-state assertion below a
    // coin flip (observed once under parallel test load).
    for _ in 0..200 {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => break, // Gone entirely: dead by any reading.
            Ok(stat) => {
                // The state is the field after the comm in
                // parentheses — comm itself may contain parens,
                // so split on the LAST closing paren.
                let state = stat
                    .rsplit_once(')')
                    .and_then(|(_, rest)| rest.split_whitespace().next());
                if state == Some("Z") {
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    // Step until the watcher has recorded exactly one proposal.
    for _ in 0..300 {
        comp.tick_once().unwrap();
        if comp.proposals().len() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        comp.proposals().len(),
        1,
        "exactly one proposal for the loss"
    );

    // Capture the lost event BEFORE asserting against it: the
    // spine's latest must be the loss the proposal answers.
    let lost = comp.spine().latest("fake").expect("a latest event");
    assert_eq!(lost.kind, "lost", "the latest spine event is the loss");
    let lost_seq = lost.seq;
    let lost_hash = lost.hash.clone();

    let proposal = &comp.proposals()[0];
    assert_eq!(proposal.tool, "hearth.restart");
    assert_eq!(proposal.caused_by.seq, lost_seq);
    assert_eq!(proposal.caused_by.hash, lost_hash);
    assert_eq!(proposal.args["model"].as_str(), Some("fake"));
    assert_eq!(proposal.args["spine_event"]["seq"].as_u64(), Some(lost_seq));

    // No restart: ten more composition steps change nothing.
    // The spine still ends at the loss, the causal chain holds
    // exactly one loading event (a restart would have recorded
    // a second), the fleet state is the loss itself, and the
    // proposal count is still one — criterion (4)'s behaviour
    // (a second pass proposes nothing new) at composition level.
    for _ in 0..10 {
        comp.tick_once().unwrap();
    }
    let latest = comp.spine().latest("fake").unwrap();
    assert_eq!(latest.kind, "lost", "still lost — nothing restarted it");
    assert_eq!(latest.seq, lost_seq);
    let loading_count = comp
        .spine()
        .why("fake")
        .into_iter()
        .filter(|e| e.kind == "loading")
        .count();
    assert_eq!(loading_count, 1, "exactly one loading event — no restart");
    let state = &comp
        .supervisor()
        .fleet()
        .slot("fake")
        .expect("fake is declared")
        .state;
    assert!(
        matches!(
            state,
            Residency::Lost {
                reason: LostReason::ProcessExited,
                ..
            }
        ),
        "fleet state is Lost{{ProcessExited}}, got {state:?}"
    );
    assert_eq!(comp.proposals().len(), 1, "still exactly one proposal");
}

#[test]
fn the_composition_boundary_is_where_the_design_says_it_is() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));

    // Upstream's ticker wires the restart policy: from
    // `fn cmd_up` onward, the upstream binary's source contains
    // the restart call. That behaviour stays upstream's — this
    // assertion documents §5.2.1 rather than discovering it in
    // production.
    let upstream_path = manifest.join("../hearth-serve/src/bin/hearth.rs");
    let upstream = std::fs::read_to_string(&upstream_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", upstream_path.display()));
    let cmd_up = upstream
        .find("fn cmd_up")
        .expect("upstream binary has a cmd_up");
    assert!(
        upstream[cmd_up..].contains(".restart_lost()"),
        "upstream cmd_up wires the unsupervised restart policy"
    );

    // And no gaia-serve source does. Every .rs file under this
    // crate's src/ (the library, the watcher, the proposals,
    // and the composition binary) is scanned: the call shape
    // must appear in none of them.
    let src_dir = manifest.join("src");
    let mut scanned = 0usize;
    for entry in std::fs::read_dir(&src_dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            !text.contains(".restart_lost()"),
            "{} must not wire the unsupervised restart policy",
            path.display()
        );
        scanned += 1;
    }
    assert!(
        scanned >= 4,
        "expected to scan the crate sources, scanned {scanned}"
    );

    // Behavioural companion: the upstream policy, called
    // directly on a freshly dead child, still restarts nothing
    // — honestly explained: upstream's policy has a 60-second
    // anti-crash-loop gate (§1.2), so a just-died child is not
    // restarted even when the method IS called. What differs
    // between the binaries is therefore the wiring proven
    // above, not the policy, which remains upstream's untouched
    // code in both cases.
    let dir = tempfile::tempdir().unwrap();
    let gguf = dir.path().join("fake.gguf");
    std::fs::write(&gguf, b"not a real gguf").unwrap();

    let mut sup = Supervisor::new(
        Spine::in_memory(),
        Budget {
            total_bytes: 24 * GIB,
            reserve_bytes: 2 * GIB,
        },
        vec![Declared {
            model: "fake".to_string(),
            weights_bytes: GIB,
            kv_bytes: 0,
        }],
    );
    // /bin/sh as a stand-in runtime, the pattern from
    // hearth-serve's own library tests: it exits immediately,
    // which is the whole scenario.
    let mut spec = ServerSpec::new("fake", &gguf, free_port().unwrap());
    spec.binary = Path::new("/bin/sh").to_path_buf();
    spec.extra_args = vec!["-c".into(), "exit 1".into()];
    spec.log_dir = dir.path().to_path_buf();
    sup.start(spec).unwrap();

    let mut lost = false;
    for _ in 0..300 {
        sup.tick();
        if sup
            .spine()
            .latest("fake")
            .is_some_and(|e| e.kind == "lost")
        {
            lost = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(lost, "the dying child was never recorded lost");

    assert_eq!(
        sup.restart_lost(),
        0,
        "a just-died child is inside the 60s anti-crash-loop gate — \
         the policy declines even when called"
    );
}
