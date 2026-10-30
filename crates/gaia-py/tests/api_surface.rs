//! Acceptance (G-2 §7 G-7, criterion 2): the bindings expose no
//! mutating method.
//!
//! The bindings are a cdylib — there is no rlib surface for a Rust
//! test to enumerate — so, in the same spirit as the G-5 composition
//! boundary assertions, the surface is checked at the source: the two
//! files that define `#[pymethods]` are scanned for method
//! definitions, and no mutating name may appear among them. The
//! positive half matters as much: every read the slice promises must
//! be present, and the GIL discipline (`allow_threads` around spine
//! calls — §1.3's gap in hearth-py that this crate exists to close)
//! must be visible in the spine binding, not just claimed.

use std::path::PathBuf;

fn source(rel: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// `fn <name>(` definitions on lines that are not doc comments —
/// prose may *discuss* `Spine::record` (the docs explain why it is
/// absent); only a definition exposes it.
fn defined_methods(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in src.lines() {
        let t = line.trim_start();
        if t.starts_with("//") {
            continue;
        }
        if let Some(rest) = t.strip_prefix("fn ") {
            if let Some(name) = rest.split('(').next() {
                out.push(name.trim().to_string());
            }
        }
    }
    out
}

const MUTATING: &[&str] = &[
    "record",
    "flush",
    "append",
    "rebuild",
    "poll_once",
    "put",
    "delete",
    "remove",
    "insert",
    "update",
    "write",
    "set",
    "mutate",
];

#[test]
fn no_mutating_method_is_defined_in_the_bindings() {
    for rel in ["src/lib.rs", "src/mirror.rs"] {
        let methods = defined_methods(&source(rel));
        for name in &methods {
            assert!(
                !MUTATING.contains(&name.as_str()),
                "{rel} defines `{name}` — the gaia-py bindings are read-only by \
                 construction (G-2 §7 G-7); a mutating method here lets Python \
                 rewrite the history it audits"
            );
        }
    }
}

#[test]
fn the_promised_reads_are_all_present() {
    let lib = source("src/lib.rs");
    for name in [
        "fn new(",
        "fn seq(",
        "fn latest(",
        "fn latest_pull(",
        "fn why(",
        "fn provenance(",
        "fn state_as_of(",
        "fn all_latest(",
        "fn head(",
        "fn verify(",
    ] {
        assert!(lib.contains(name), "src/lib.rs is missing {name}");
    }
    let mirror = source("src/mirror.rs");
    for name in [
        "fn events_since(",
        "fn entries(",
        "fn latest_seq(",
        "fn load_failures(",
    ] {
        assert!(mirror.contains(name), "src/mirror.rs is missing {name}");
    }
}

#[test]
fn spine_calls_release_the_gil() {
    // One allow_threads per spine read: seq, latest, latest_pull,
    // why, provenance, state_as_of, all_latest, head, verify.
    let lib = source("src/lib.rs");
    let count = lib.matches("allow_threads").count();
    assert!(
        count >= 9,
        "src/lib.rs wraps {count} calls in allow_threads — every spine read \
         must release the GIL around a call that can block (§1.3)"
    );
}
