//! Acceptance tests for the KV fix: bounded child specs and
//! KV-aware admission in the gaia-serve composition.
//!
//! The scenario is the 2026-10-31 failure, at test scale: a
//! model with a 16,384-token native context (Imagine v12's
//! shape: 24 layers, 16 KV heads, 128-wide K and V — 196,608
//! bytes of KV per token) was spawned with the ServerSpec
//! defaults (ctx 0, parallel 8) and declared with kv_bytes 0,
//! so nothing bounded the ~24 GiB its slots would allocate
//! and the kernel OOM-killed it on a 7.7 GiB machine.
//!
//! The composition's model, exercised here end to end:
//! **ctx is the budget; parallel divides it.** Declarations
//! carry the pool's KV, admission refuses what does not fit
//! (and the refusal is a spine record), and the child is
//! spawned with per-slot ctx = pool / parallel — never 0.

use std::path::{Path, PathBuf};

use gaia_serve::fleet::{child_spec, declare_model, ChildBounds};
use hearth_core::{Budget, Declared, GIB};
use hearth_serve::Supervisor;
use hearth_store::Spine;

/// KV bytes per token for the v12-like shape below:
/// 24 layers × 16 KV heads × (128 + 128) elements × 2 bytes.
const KV_PER_TOKEN: u64 = 196_608;
/// The default pool's KV for that shape: 196,608 × 8,192.
const POOL_KV: u64 = 1_610_612_736;

/// Write a minimal GGUF header carrying exactly the metadata
/// `hearth_core::read_kv_shape` reads, with a v12-like shape:
/// 24 blocks, 16 KV heads, 128-wide K/V, native ctx 16,384.
fn write_gguf(dir: &Path) -> PathBuf {
    let entries: [(&str, u32); 5] = [
        ("testarch.block_count", 24),
        ("testarch.attention.head_count_kv", 16),
        ("testarch.attention.key_length", 128),
        ("testarch.attention.value_length", 128),
        ("testarch.context_length", 16_384),
    ];
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"GGUF");
    bytes.extend_from_slice(&3u32.to_le_bytes()); // version
    bytes.extend_from_slice(&0u64.to_le_bytes()); // tensor count
    bytes.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (key, value) in entries {
        bytes.extend_from_slice(&(key.len() as u64).to_le_bytes());
        bytes.extend_from_slice(key.as_bytes());
        bytes.extend_from_slice(&4u32.to_le_bytes()); // UINT32
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    let path = dir.join("imagine-like.gguf");
    std::fs::write(&path, bytes).unwrap();
    path
}

fn spec_for(path: &Path, bounds: ChildBounds, log_dir: &Path) -> hearth_serve::server::ServerSpec {
    child_spec(
        "imagine-like",
        path,
        39_999,
        bounds,
        Path::new("llama-server"),
        log_dir,
    )
}

/// (a) + (c): a model whose native context is 16,384 is
/// spawned bounded by the configured pool — with the default
/// bounds the spec carries ctx 8192 and parallel 1, and no
/// spec with ctx 0 reaches the spawn path.
#[test]
fn native_ctx_model_is_bounded_by_the_default_pool() {
    let dir = tempfile::tempdir().unwrap();
    let gguf = write_gguf(dir.path());

    let declared = declare_model("imagine-like", &gguf, ChildBounds::default()).unwrap();
    assert_eq!(
        declared.kv_bytes, POOL_KV,
        "the declaration accounts the pool's KV (8192 tokens), \
         not the native context and not zero"
    );
    assert_eq!(declared.weights_bytes, std::fs::metadata(&gguf).unwrap().len());

    let spec = spec_for(&gguf, ChildBounds::default(), dir.path());
    assert_eq!(spec.ctx, 8192, "default pool bounds the child's ctx");
    assert_ne!(spec.ctx, 0, "ctx 0 — llama.cpp's unbounded native fallback — never spawns");
    assert_eq!(spec.parallel, 1, "default parallelism is one slot");
    let argv = spec.argv();
    let c = argv.iter().position(|a| a == "-c").expect("argv carries -c");
    assert_eq!(argv[c + 1], "8192");
}

/// (b): a declaration whose weights + pool KV exceed the
/// budget is refused at admission — the refusal is recorded
/// in the spine as a `declared` event with `admitted: false`,
/// and starting the model is refused with it. The same model
/// inside the budget is admitted, with the pool's full cost
/// on its declared record.
#[test]
fn over_pool_declaration_is_refused_at_admission_and_recorded() {
    let dir = tempfile::tempdir().unwrap();
    let gguf = write_gguf(dir.path());
    // 2 GiB usable: the pool-8192 declaration (1.5 GiB KV)
    // fits; a pool-16384 declaration (3 GiB KV) cannot.
    let budget = Budget {
        total_bytes: 2 * GIB,
        reserve_bytes: 0,
    };

    let over = declare_model(
        "imagine-like",
        &gguf,
        ChildBounds {
            ctx_pool: 16_384,
            parallel: 1,
        },
    )
    .unwrap();
    assert_eq!(over.kv_bytes, KV_PER_TOKEN * 16_384);
    let mut sup = Supervisor::new(Spine::in_memory(), budget, vec![over.clone()]);
    let ev = sup.spine().latest("imagine-like").unwrap();
    assert_eq!(ev.kind, "declared");
    assert_eq!(
        ev.facts["admitted"],
        serde_json::json!(false),
        "the refusal is a spine record, not a log line"
    );
    assert_eq!(
        ev.facts["vram_bytes"],
        serde_json::json!(over.total_bytes()),
        "the declared record carries weights + pool KV"
    );
    let err = sup
        .start(spec_for(
            &gguf,
            ChildBounds {
                ctx_pool: 16_384,
                parallel: 1,
            },
            dir.path(),
        ))
        .expect_err("a refused declaration must refuse the start");
    assert!(err.contains("refused"), "start refusal names itself: {err}");

    let fits = declare_model("imagine-like", &gguf, ChildBounds::default()).unwrap();
    assert_eq!(fits.total_bytes(), fits.weights_bytes + POOL_KV);
    let sup = Supervisor::new(Spine::in_memory(), budget, vec![fits]);
    let ev = sup.spine().latest("imagine-like").unwrap();
    assert_eq!(ev.facts["admitted"], serde_json::json!(true));
}

/// The parallelism control: with the pool fixed, raising
/// parallel lowers the per-slot ctx the child is spawned
/// with and leaves both the declared KV and the admission
/// verdict unchanged — parallel carves the pool, it never
/// multiplies it, and it is never silently clamped (the
/// composition's CLI refuses parallel > pool outright).
#[test]
fn parallel_divides_the_pool_and_admission_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let gguf = write_gguf(dir.path());

    let mut declared_kv = None;
    for (parallel, per_slot) in [(1u32, 8192u32), (2, 4096), (8, 1024)] {
        let bounds = ChildBounds {
            ctx_pool: 8192,
            parallel,
        };
        let declared: Declared = declare_model("imagine-like", &gguf, bounds).unwrap();
        match declared_kv {
            None => declared_kv = Some(declared.kv_bytes),
            Some(kv) => assert_eq!(
                declared.kv_bytes, kv,
                "parallel {parallel} must not change the pool's declared KV"
            ),
        }
        let spec = spec_for(&gguf, bounds, dir.path());
        assert_eq!(spec.ctx, per_slot, "per-slot ctx at parallel {parallel}");
        assert_eq!(spec.parallel, parallel);
    }

    // Admission is a function of the pool alone: a budget that
    // fits the pool once admits the model at parallel 1 and at
    // parallel 8 alike.
    let one = declare_model(
        "imagine-like",
        &gguf,
        ChildBounds {
            ctx_pool: 8192,
            parallel: 1,
        },
    )
    .unwrap();
    let budget = Budget {
        total_bytes: one.total_bytes(),
        reserve_bytes: 0,
    };
    for parallel in [1u32, 8] {
        let declared = declare_model(
            "imagine-like",
            &gguf,
            ChildBounds {
                ctx_pool: 8192,
                parallel,
            },
        )
        .unwrap();
        let sup = Supervisor::new(Spine::in_memory(), budget, vec![declared]);
        let ev = sup.spine().latest("imagine-like").unwrap();
        assert_eq!(
            ev.facts["admitted"],
            serde_json::json!(true),
            "parallel {parallel} admits identically — the pool is the budget"
        );
    }
}
