//! spine_writer — test fixture for the gaia Python bindings (G-7).
//!
//! Records one full, causally-chained lifecycle for a single fixture
//! model into an on-disk hearth-store spine, flushes, then prints ONE
//! JSON line to stdout describing the resulting `why()` chain. The
//! Python tests parse that line as ground truth and assert the
//! read-only bindings reconstruct exactly the same chain.
//!
//! Usage: spine_writer <output-dir>
//!
//! Panics loudly on any error: a fixture that half-writes is worse
//! than none, because tests would then fail against a phantom history.

use std::env;
use std::path::Path;

use hearth_core::LostReason;
use hearth_store::{Spine, Transition};
use serde_json::json;

const MODEL: &str = "gaia-fixture:latest";

fn main() {
    let args: Vec<String> = env::args().collect();
    assert_eq!(
        args.len(),
        2,
        "usage: spine_writer <output-dir> (got {} args)",
        args.len() - 1
    );
    let root = &args[1];

    let spine = Spine::open(Path::new(root)).expect("spine_writer: Spine::open failed");

    let declared = spine
        .record(
            MODEL,
            &Transition::Declared {
                vram_bytes: 4_000_000_000,
                admitted: true,
            },
            &[],
        )
        .expect("spine_writer: record Declared failed");
    let pull_started = spine
        .record(
            MODEL,
            &Transition::PullStarted {
                source: "hf:fixture/gaia-fixture@Q4_K_M".to_string(),
            },
            std::slice::from_ref(&declared),
        )
        .expect("spine_writer: record PullStarted failed");
    let pull_completed = spine
        .record(
            MODEL,
            &Transition::PullCompleted {
                path: "/blobs/sha256-fixture".to_string(),
                size_bytes: 4_000_000_000,
            },
            std::slice::from_ref(&pull_started),
        )
        .expect("spine_writer: record PullCompleted failed");
    let loading = spine
        .record(
            MODEL,
            &Transition::Loading {
                pid: 4242,
                endpoint: "127.0.0.1:18080".to_string(),
            },
            std::slice::from_ref(&pull_completed),
        )
        .expect("spine_writer: record Loading failed");
    let resident = spine
        .record(
            MODEL,
            &Transition::Resident {
                endpoint: "127.0.0.1:18080".to_string(),
                warmup_ms: 1200,
            },
            std::slice::from_ref(&loading),
        )
        .expect("spine_writer: record Resident failed");
    let lost = spine
        .record(
            MODEL,
            &Transition::Lost {
                reason: LostReason::ProcessExited,
            },
            std::slice::from_ref(&resident),
        )
        .expect("spine_writer: record Lost failed");

    spine.flush();

    // Ground truth is derived from the spine itself, never hardcoded:
    // kinds/hashes/seqs are exactly what why() reconstructs, newest first.
    let chain = spine.why(MODEL);
    assert_eq!(
        chain.len(),
        6,
        "spine_writer: why() chain should have 6 events, got {}",
        chain.len()
    );
    assert_eq!(
        chain[0].hash, lost.hash,
        "spine_writer: why() head should be the Lost event"
    );

    let kinds: Vec<&str> = chain.iter().map(|e| e.kind.as_str()).collect();
    let hashes: Vec<&str> = chain.iter().map(|e| e.hash.as_str()).collect();
    let seqs: Vec<u64> = chain.iter().map(|e| e.seq).collect();

    let line = json!({
        "root": root,
        "kinds": kinds,
        "hashes": hashes,
        "seqs": seqs,
        "resident_seq": resident.seq,
        "latest_hash": lost.hash,
        // The global clock, read from the spine itself: NEDB's seq is
        // a global counter, not a per-chain one — it can sit above the
        // last event's seq (engine bookkeeping consumes seqs too), so
        // the tests must never assume seq() == last event seq.
        "spine_seq": spine.seq(),
    });
    println!("{line}");
}
