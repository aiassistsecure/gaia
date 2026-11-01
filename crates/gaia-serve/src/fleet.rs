//! Fleet spawning bounds: the context pool a child is
//! admitted and spawned under.
//!
//! ## The pool model
//!
//! The operator's ctx is a **total pool**, and parallel
//! divides it: "ctx is the budget; parallel divides it." A
//! pool of 8192 with 2 slots is 4096 tokens of context per
//! slot and 8192 tokens of KV cache in total — never 16384.
//! Admission and spawning are two views of that one number:
//! admission budgets `kv_bytes_for(pool)` plus the weights,
//! once, and the child is spawned with per-slot ctx =
//! pool / parallel, compensating for llama-server's `-c`
//! being a per-slot figure rather than the total this
//! composition means by it.
//!
//! ## The failure this prevents
//!
//! Before this module, the composition built children from
//! `ServerSpec`'s own defaults — ctx 0, parallel 8 — and
//! declared every model at admission with a KV cost of 0.
//! Ctx 0 is llama-server's "use the model's native context,
//! per slot": a 1.6B model with a native context of 16384
//! was spawned holding 16384 tokens of KV cache in each of
//! 8 slots, a footprint nothing had budgeted for, and the
//! kernel OOM-killed it on a 7.7 GiB machine. The weights
//! fit; the unbudgeted, undivided KV cache is what did not.
//! A nonzero default pool, divided by the slot count before
//! it reaches the child and charged in full at admission,
//! is what keeps that arithmetic honest.

use std::io::BufReader;
use std::path::Path;

use hearth_serve::server::ServerSpec;

/// The default total context pool, in tokens, shared across
/// a child's slots.
pub const DEFAULT_CTX_POOL: u32 = 8192;

/// The default number of parallel slots. One, deliberately:
/// extra slots divide the pool, they do not extend it, and
/// an operator who wants concurrency should ask for it
/// knowing the per-slot context it costs.
pub const DEFAULT_PARALLEL: u32 = 1;

/// The bounds one child is spawned and admitted under: a
/// total context pool and the slot count that divides it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildBounds {
    /// Total context, in tokens, across every slot.
    pub ctx_pool: u32,
    /// Parallel slots the pool is divided across.
    pub parallel: u32,
}

impl Default for ChildBounds {
    fn default() -> Self {
        ChildBounds {
            ctx_pool: DEFAULT_CTX_POOL,
            parallel: DEFAULT_PARALLEL,
        }
    }
}

impl ChildBounds {
    /// The per-slot context to spawn the child with:
    /// pool / parallel, with a parallel of 0 treated as 1 (a
    /// child with no slots is not a smaller child, it is a
    /// configuration error this division must not turn into
    /// a divide-by-zero). A pool of 0 stays 0 — that is the
    /// operator explicitly asking for llama-server's own
    /// default, and it is the only way a spawned child can
    /// still carry a ctx of 0.
    pub fn per_slot_ctx(&self) -> u32 {
        self.ctx_pool / self.parallel.max(1)
    }

    /// Bytes of KV cache for the whole pool, at this model's
    /// shape.
    ///
    /// Pool semantics are this composition's own, so this
    /// deliberately does **not** go through
    /// `hearth_core::total_ctx_tokens`: that function encodes
    /// upstream's pass-through flag semantics (an explicit
    /// ctx is already the total; an unset ctx multiplies the
    /// native context by the slot count), which this
    /// composition deliberately supersedes. Here the pool is
    /// the total by construction, so the shape is charged
    /// for exactly `ctx_pool` tokens, whatever the slot
    /// count is.
    pub fn kv_bytes(&self, shape: &hearth_core::KvShape) -> u64 {
        shape.kv_bytes_for(self.ctx_pool as u64)
    }
}

/// The KV cache, in bytes, that `path`'s model will hold at
/// `bounds`' pool — read from the GGUF header's own shape,
/// never estimated from the file's size.
///
/// Failure to read the header is NOT fatal: the model is
/// budgeted at weights only, exactly as it was before KV
/// accounting existed, rather than refused over a header
/// this reader could not parse. It is loud on stderr,
/// though: a silent 0 here is the same silent gap this
/// function exists to close.
pub fn kv_bytes_for_model(path: &Path, bounds: ChildBounds) -> u64 {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "gaia: could not read KV shape from {} ({e}) — \
                 budgeting weights only for this model",
                path.display()
            );
            return 0;
        }
    };
    let mut reader = BufReader::new(file);
    match hearth_core::read_kv_shape(&mut reader) {
        Ok(shape) => bounds.kv_bytes(&shape),
        Err(e) => {
            eprintln!(
                "gaia: could not read KV shape from {} ({e}) — \
                 budgeting weights only for this model",
                path.display()
            );
            0
        }
    }
}

/// Declare one model for admission: its weights, measured
/// from the file itself, plus the KV cache its pool will
/// cost. A model whose file cannot even be measured is
/// refused by name — unlike an unreadable KV header (see
/// [`kv_bytes_for_model`]), a weights figure of 0 for a file
/// that is not there would admit a model that cannot load.
pub fn declare_model(
    name: &str,
    path: &Path,
    bounds: ChildBounds,
) -> Result<hearth_core::Declared, String> {
    let weights_bytes = std::fs::metadata(path)
        .map_err(|e| format!("cannot read model {name} at {}: {e}", path.display()))?
        .len();
    Ok(hearth_core::Declared {
        model: name.to_string(),
        weights_bytes,
        kv_bytes: kv_bytes_for_model(path, bounds),
    })
}

/// The spec a child is actually spawned with, under
/// `bounds`: per-slot ctx is the pool divided by the slot
/// count (see [`ChildBounds::per_slot_ctx`]), so the total
/// context the child holds is the pool admission charged
/// for — with default bounds that is a ctx of 8192, never
/// the 0 that once handed llama-server the model's native
/// context per slot.
pub fn child_spec(
    name: &str,
    path: &Path,
    port: u16,
    bounds: ChildBounds,
    binary: &Path,
    log_dir: &Path,
) -> ServerSpec {
    let mut spec = ServerSpec::new(name, path, port);
    spec.ctx = bounds.per_slot_ctx();
    spec.parallel = bounds.parallel;
    spec.binary = binary.to_path_buf();
    spec.log_dir = log_dir.to_path_buf();
    spec
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    // The v12-like shape the assertions below are built on:
    // 24 blocks x 16 KV heads x (128 + 128) wide x 2 bytes
    // (f16) = 196,608 bytes of KV cache per token.
    const BYTES_PER_TOKEN: u64 = 196_608;
    const POOL_8192_KV: u64 = 1_610_612_736; // 196,608 x 8192

    /// A minimal, hand-built GGUF metadata section — no
    /// tensors, because the KV reader never looks at them.
    /// Encodes exactly the keys a real file would carry for
    /// the shape above, all as U32 values (type tag 4).
    fn synthetic_gguf_bytes() -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(b"GGUF"); // magic, ASCII little-endian
        b.extend_from_slice(&3u32.to_le_bytes()); // version
        b.extend_from_slice(&0u64.to_le_bytes()); // tensor_count
        let entries: &[(&str, u64)] = &[
            ("testarch.block_count", 24),
            ("testarch.attention.head_count_kv", 16),
            ("testarch.attention.key_length", 128),
            ("testarch.attention.value_length", 128),
            ("testarch.context_length", 16384),
        ];
        b.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (key, value) in entries {
            b.extend_from_slice(&(key.len() as u64).to_le_bytes());
            b.extend_from_slice(key.as_bytes());
            b.extend_from_slice(&4u32.to_le_bytes()); // value type: U32
            b.extend_from_slice(&(*value as u32).to_le_bytes());
        }
        b
    }

    fn synthetic_file() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("model.gguf");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&synthetic_gguf_bytes()).unwrap();
        (dir, path)
    }

    fn synthetic_shape() -> hearth_core::KvShape {
        hearth_core::KvShape {
            block_count: 24,
            head_count_kv: 16,
            key_length: 128,
            value_length: 128,
            context_length: 16384,
        }
    }

    #[test]
    fn kv_bytes_at_pool_8192_is_the_full_pool_charged_once() {
        let bounds = ChildBounds {
            ctx_pool: 8192,
            parallel: 1,
        };
        assert_eq!(BYTES_PER_TOKEN * 8192, POOL_8192_KV);
        assert_eq!(bounds.kv_bytes(&synthetic_shape()), POOL_8192_KV);

        let (_dir, path) = synthetic_file();
        assert_eq!(kv_bytes_for_model(&path, bounds), POOL_8192_KV);
    }

    #[test]
    fn per_slot_ctx_divides_the_pool() {
        let bounds = |ctx_pool, parallel| ChildBounds { ctx_pool, parallel };
        assert_eq!(bounds(8192, 1).per_slot_ctx(), 8192);
        assert_eq!(bounds(8192, 2).per_slot_ctx(), 4096);
        assert_eq!(bounds(8192, 8).per_slot_ctx(), 1024);
    }

    #[test]
    fn parallel_divides_the_pool_and_never_multiplies_the_total() {
        let one = ChildBounds {
            ctx_pool: 8192,
            parallel: 1,
        };
        let eight = ChildBounds {
            ctx_pool: 8192,
            parallel: 8,
        };
        assert_eq!(one.kv_bytes(&synthetic_shape()), eight.kv_bytes(&synthetic_shape()));
        assert_eq!(eight.kv_bytes(&synthetic_shape()), POOL_8192_KV);
    }

    #[test]
    fn child_spec_spawns_the_divided_ctx() {
        let binary = Path::new("llama-server");
        let log_dir = Path::new("/tmp/logs");

        let spec = child_spec(
            "m",
            Path::new("model.gguf"),
            8080,
            ChildBounds {
                ctx_pool: 8192,
                parallel: 2,
            },
            binary,
            log_dir,
        );
        assert_eq!(spec.ctx, 4096);
        assert_eq!(spec.parallel, 2);
        let argv = spec.argv();
        let c = argv.iter().position(|a| a == "-c").expect("argv carries -c");
        assert_eq!(argv[c + 1], "4096");

        let default_spec = child_spec(
            "m",
            Path::new("model.gguf"),
            8080,
            ChildBounds::default(),
            binary,
            log_dir,
        );
        assert_eq!(default_spec.ctx, 8192, "default ctx is never 0");
        let argv = default_spec.argv();
        let c = argv.iter().position(|a| a == "-c").expect("argv carries -c");
        assert_eq!(argv[c + 1], "8192");
    }

    #[test]
    fn declare_model_measures_weights_and_budgets_the_pool() {
        let (_dir, path) = synthetic_file();
        let declared = declare_model("m", &path, ChildBounds::default()).unwrap();
        assert_eq!(declared.model, "m");
        assert_eq!(
            declared.weights_bytes,
            std::fs::metadata(&path).unwrap().len()
        );
        assert_eq!(declared.weights_bytes, synthetic_gguf_bytes().len() as u64);
        assert_eq!(declared.kv_bytes, POOL_8192_KV);
    }

    #[test]
    fn kv_bytes_for_a_missing_file_is_zero_not_an_error() {
        let path = Path::new("/nonexistent/gaia-fleet-test/model.gguf");
        assert_eq!(kv_bytes_for_model(path, ChildBounds::default()), 0);
    }
}
