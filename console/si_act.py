#!/usr/bin/env python3
"""The Gaia console's one write path: operator resolutions.

Invoked by console/server.py as a subprocess with the SI package on
PYTHONPATH (the seam service's interpreter, e.g. the loop venv):

    python si_act.py resolve     <edit_id> <resolution> <operator> \
                                 [basis] <bundle_dir>
    python si_act.py autoapprove <bundle_dir>

(The bundle dir is always the last argument, matching si_read.py's
convention and console/server.py's invocation.)

Prints one JSON document on stdout. A refusal prints
{"error": "<the SI layer's message, verbatim>"} and exits 1 —
nothing is journaled on a refusal, because the act itself
journals nothing on a refusal.

Every resolution goes through
si.gaia.escalation_cli.resolve — the SI-113 trigger's own path:
load_bundle (full bundle verification, fail closed), then
si.gaia.resolution.resolve_escalation (fold, verify the intent
is escalated and unresolved, journal the escalation_resolution),
then the manifest checkpoint re-anchor, exactly as seam ingest
does after journaling. There is no second write path here and
no verification of this module's own: the closed vocabulary
(admit-after-review / reject), the folded-state check, the
single-resolution rule, and the non-empty-operator rule all
stay inside resolve_escalation. The gate's verdict is never
rewritten — a resolution is a separate journaled operator act
standing beside the ESCALATE it answers (SI-41 §5.4, SI-92).

`autoapprove` is a resolution POLICY, not a gate bypass: the
gate still escalates exactly what it escalates; this mode then
answers every pending escalation with admit-after-review,
credited to the autoapprove operator, one verified act each.
"""

import json
import sys

AUTOAPPROVE_OPERATOR = "autoapprove (env GAIA_AUTOAPPROVE)"
AUTOAPPROVE_BASIS = (
    "autoapprove enabled in console server env "
    "(GAIA_AUTOAPPROVE=1); gate verdict preserved, resolution "
    "journaled as a separate operator act")


def _intent_seq_for_edit(bundle_dir, edit_id):
    """Map an edit_id to its folded intent seq (read-only open,
    chain verified). Raises ValueError when the journal folds
    no tool intent with that edit id."""
    from si.gaia.queue import fold_queue
    from si.journal import open_journal

    journal = open_journal(bundle_dir)
    try:
        if not journal.verify_chain():
            raise ValueError("journal hash chain does not verify")
        fold = fold_queue(journal)
        for seq in sorted(fold.by_seq):
            if fold.by_seq[seq].edit_id == edit_id:
                return seq
        raise ValueError(
            "no folded tool intent with edit_id %r in this "
            "journal (nothing journaled)" % (edit_id,))
    finally:
        journal.close()


def resolve_document(bundle_dir, edit_id, resolution, operator, basis):
    from si.gaia.escalation_cli import read_escalation_at, resolve

    intent_seq = _intent_seq_for_edit(bundle_dir, edit_id)
    entry = resolve(bundle_dir, intent_seq, resolution,
                    operator=operator, basis=basis)
    return {
        "resolved": True,
        "edit_id": edit_id,
        "intent_seq": intent_seq,
        "resolution": resolution,
        "operator": operator,
        "resolution_seq": entry.get("seq"),
        "folded": read_escalation_at(bundle_dir, intent_seq),
    }


def do_autoapprove(bundle_dir):
    from si.gaia.escalation_cli import list_pending_escalations, resolve
    from si.journal import open_journal

    journal = open_journal(bundle_dir)
    try:
        if not journal.verify_chain():
            raise ValueError("journal hash chain does not verify")
        pending = list_pending_escalations(journal)
    finally:
        journal.close()
    resolved, errors = [], []
    for view in pending:
        try:
            entry = resolve(bundle_dir, view["intent_seq"],
                            "admit-after-review",
                            operator=AUTOAPPROVE_OPERATOR,
                            basis=AUTOAPPROVE_BASIS)
            resolved.append({
                "edit_id": view["edit_id"],
                "intent_seq": view["intent_seq"],
                "resolution_seq": entry.get("seq"),
            })
        except Exception as exc:
            errors.append({"edit_id": view["edit_id"],
                           "error": str(exc)})
    return {"pending_seen": len(pending), "resolved": resolved,
            "errors": errors}


def main(argv):
    try:
        if len(argv) >= 2 and argv[1] == "resolve":
            if len(argv) not in (6, 7):
                print(json.dumps({"error": "usage: si_act.py resolve "
                                           "<edit_id> <resolution> "
                                           "<operator> [basis] <bundle_dir>"}))
                return 2
            # bundle dir is always the last argument
            result = resolve_document(argv[-1], argv[2], argv[3],
                                      argv[4],
                                      argv[5] if len(argv) == 7 else "")
        elif len(argv) == 3 and argv[1] == "autoapprove":
            result = do_autoapprove(argv[2])
        else:
            print(json.dumps({"error": "usage: si_act.py resolve|autoapprove ..."}))
            return 2
    except Exception as exc:  # the SI layer's refusal, verbatim
        print(json.dumps({"error": str(exc)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
