# Gaia Console

The operator console for the Gaia runtime: resident mind status, chat with the
resident mind, the SI seam journal, and the escalation queue — one screen,
designed as a luxury-automobile instrument face (piano black, chrome,
ambient light) with Aceternity-style polish.

`index.html` is a self-contained static page. Its Journal and Escalations
data are a **live snapshot taken 2026-11-21** from the real journals
(`si-seam.si`, chain verified, tip seq 74; node spine head seq 965) — every
entry, hash, and gate report in the detail modals is verbatim from those
records. The five open escalations shown are se_0007–se_0011. The chat panel
is a concept preview and is not yet wired to the runtime; wiring it to the
node's SSE event spine and agency endpoints is the next step.
