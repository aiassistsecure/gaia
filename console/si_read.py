#!/usr/bin/env python3
"""Read-only SI journal reader for the Gaia console server.

Invoked by console/server.py as a subprocess with the SI package on
PYTHONPATH (the seam service's interpreter, e.g. the loop venv):

    python si_read.py journal      <bundle_dir>
    python si_read.py escalations  <bundle_dir>

Prints one JSON document on stdout. The journal is opened exactly the
way si.gaia.escalation_cli opens it for reads: directly, with the hash
chain verified before anything is folded or listed (fail closed).
Nothing here writes: no resolutions, no checkpoints, no appends.
"""

import json
import sys

KIND_LABELS = {
    "checkpoint": "Checkpoint",
    "tool_result": "Tool Result",
    "escalation_resolution": "Escalation Resolution",
    "gaia_work": "Gaia Work",
}


def _s(v):
    return "" if v is None else str(v)


def _kind_label(kind):
    if kind in KIND_LABELS:
        return KIND_LABELS[kind]
    return str(kind).replace("_", " ").title()


def _ckpt_fields(row):
    """Checkpoint modal fields, mirroring the console's snapshot build:
    the note, then the manifest's identifying fields (never the raw
    ~5KB manifest_json blob)."""
    fields = [["Note", _s(row.get("note")), "l"]]
    try:
        mj = json.loads(row.get("manifest_json") or "{}")
    except ValueError:
        mj = {}
    base = mj.get("base") or {}
    constitution = mj.get("constitution") or {}
    journal = mj.get("journal") or {}

    def add(label, value, typ=""):
        if value is not None and value != "":
            fields.append([label, _s(value), typ])

    add("Head", row.get("head"))
    add("Manifest", row.get("manifest"), "h")
    add("Manifest name", mj.get("name"), "m")
    add("SI version", mj.get("si_version"), "m")
    add("Base mind", base.get("mind"))
    add("Base blob", base.get("blob"), "h")
    add("Base bytes", base.get("bytes"), "m")
    add("Tensor count", base.get("tensor_count"), "m")
    add("Total elements", base.get("total_elements"), "m")
    add("Constitution hash", constitution.get("hash"), "h")
    add("Policy hash", constitution.get("policy_hash"), "h")
    add("Journal backend (manifest)", journal.get("backend"))
    add("Journal last_seq (manifest)", journal.get("last_seq"), "m")
    add("Manifest created", mj.get("created_at"), "m")
    return fields


def _caused_by(fields, rec):
    cb = rec.get("caused_by")
    if isinstance(cb, dict) and cb.get("seq") is not None:
        fields.append(["Caused by", "spine seq " + _s(cb.get("seq")), ""])
        if cb.get("hash"):
            fields.append(["Caused-by hash", _s(cb["hash"]), "h"])


def _experience_fields(row):
    rec = row.get("record") or {}
    kind = row.get("kind")
    fields = []

    def add(label, value, typ=""):
        if value is not None and value != "":
            fields.append([label, _s(value), typ])

    if kind == "tool_result":
        add("Self-edit ID", row.get("self_edit_id") or rec.get("intent_id"), "m")
        add("Tool", rec.get("tool"), "m")
        add("Source", rec.get("source"), "m")
        add("Gate verdict", rec.get("gate_verdict"))
        add("Effective class", rec.get("effective_class"))
        add("Journal seq (intent)", rec.get("journal_seq"), "m")
        add("Work ID", rec.get("work_id"), "m")
        ex = rec.get("execution")
        if isinstance(ex, dict):
            if ex.get("execution"):
                add("Execution", ex.get("execution"), "l")
            add("Execution outcome", ex.get("outcome"))
            add("Refusal", ex.get("refusal"), "m")
            add("Execution detail", ex.get("detail"), "l")
            add("Model", ex.get("model"), "m")
        _caused_by(fields, rec)
        add("Returned via", rec.get("returned_via"))
        add("Via", rec.get("via"))
        add("Transport", rec.get("transport"), "m")
        add("Record label", rec.get("label"), "l")
    elif kind == "escalation_resolution":
        add("Edit ID", rec.get("edit_id"), "m")
        add("Resolution", rec.get("resolution"))
        add("Operator", rec.get("operator"))
        add("Basis", rec.get("basis"), "l")
        add("Detail", rec.get("detail"), "l")
        add("Intent seq", rec.get("intent_seq"), "m")
        add("Task seq", rec.get("task_seq"), "m")
        add("Source", rec.get("source"), "m")
    elif kind == "gaia_work":
        add("Self-edit ID", rec.get("edit_id") or row.get("self_edit_id"), "m")
        add("Operator", rec.get("operator"))
        add("Basis", rec.get("basis"), "l")
        add("Intent seq", rec.get("intent_seq"), "m")
        add("Task seq", rec.get("task_seq"), "m")
        add("Resolution", rec.get("resolution"))
        add("Resolution seq", rec.get("resolution_seq"), "m")
        add("Tool", rec.get("tool"), "m")
        add("Model", rec.get("model"), "m")
        add("Fork work ID", rec.get("fork_work_id"), "m")
        add("Status", rec.get("status"), "m")
        add("State", rec.get("state"))
        add("HTTP status", rec.get("http_status"), "m")
        add("Transport", rec.get("transport"), "m")
        add("Submitted via", rec.get("submitted_via"))
        add("Submission URL", rec.get("submission_url"), "m")
        if rec.get("is_replay") is not None:
            add("Is replay", str(rec.get("is_replay")).lower(), "m")
        _caused_by(fields, rec)
        payload = rec.get("payload") or {}
        add("Payload reason", payload.get("reason"), "l")
        add("Payload verdict", payload.get("verdict"))
        resp = rec.get("response") or {}
        if resp:
            add("Response", _s(resp.get("state")) + " · work_id " + _s(resp.get("work_id")))
        add("Record label", rec.get("label"), "l")
    else:
        # Unknown experience kind: show every scalar record field,
        # title-cased, rather than hiding journal data.
        for key in sorted(rec):
            v = rec[key]
            if isinstance(v, (str, int, float, bool)):
                add(key.replace("_", " ").title(), v,
                    "m" if len(_s(v)) <= 48 else "l")
    return fields


def _summary(row):
    """One-line row summary for the journal list (may contain <b>)."""
    import html

    def e(v):
        return html.escape(_s(v), quote=True)

    rec = row.get("record") or {}
    kind = row.get("kind")
    if row.get("_collection") == "checkpoints" or kind == "checkpoint":
        return e(row.get("note"))
    if kind == "tool_result":
        ex = rec.get("execution") or {}
        edit = row.get("self_edit_id") or rec.get("intent_id") or ""
        if ex.get("outcome"):
            return ("<b>" + e(rec.get("tool")) + "</b> — fork outcome " +
                    e(ex.get("outcome")) + " · " + e(ex.get("refusal")) +
                    " · edit " + e(edit))
        return ("<b>" + e(rec.get("tool")) + "</b> — gate verdict " +
                e(rec.get("gate_verdict")) + " · not executed · edit " + e(edit))
    if kind == "escalation_resolution":
        return ("<b>" + e(rec.get("edit_id")) + "</b> resolved " +
                e(rec.get("resolution")) + " — " + e(rec.get("operator")))
    if kind == "gaia_work":
        state = _s(rec.get("state")).replace("/", " / ")
        return ("Resolution spend — intent seq " + e(rec.get("intent_seq")) +
                " · <b>" + e(rec.get("fork_work_id")) + "</b> · " + e(state))
    return e(row.get("note") or kind)


def read_journal(bundle_dir, tail=8):
    from si.journal import COLLECTIONS, open_journal

    journal = open_journal(bundle_dir)
    try:
        if not journal.verify_chain():
            raise ValueError("journal hash chain does not verify")
        rows = []
        for coll in COLLECTIONS:
            for row in journal.backend.find(coll):
                row = dict(row)
                row["_collection"] = coll
                rows.append(row)
        rows.sort(key=lambda r: r.get("seq") or 0, reverse=True)
        tip_seq, tip_hash = journal.tip()
        entries = []
        for row in rows[:tail]:
            coll = row["_collection"]
            kind = row.get("kind") or coll.rstrip("s")
            if coll == "checkpoints":
                fields = _ckpt_fields(row)
            elif coll == "experiences":
                fields = _experience_fields(row)
            else:
                fields = _experience_fields(row)
            ts = _s(row.get("ts"))
            entries.append({
                "seq": row.get("seq"),
                "collection": coll,
                "kind": kind,
                "kindLabel": _kind_label(kind),
                "ts": ts,
                "tsShort": ts.replace("T", " ")[:19] + ("Z" if ts else ""),
                "entryHash": _s(row.get("entry_hash")),
                "prevHash": _s(row.get("prev_hash")),
                "summary": _summary(row),
                "fields": fields,
            })
        return {
            "tip": tip_seq,
            "tip_hash": tip_hash,
            "chain_verified": True,
            "total": len(rows),
            "entries": entries,
        }
    finally:
        journal.close()


def read_escalations(bundle_dir):
    from si.gaia.escalation_cli import list_pending_escalations
    from si.journal import open_journal

    journal = open_journal(bundle_dir)
    try:
        if not journal.verify_chain():
            raise ValueError("journal hash chain does not verify")
        pending = list_pending_escalations(journal)
        out = []
        for view in pending:
            edit_id = view.get("edit_id")
            source = None
            for row in journal.backend.find("self_edits",
                                            filter={"edit_id": edit_id}):
                source = row
                break
            gate = (source or {}).get("gate_report") or {}
            queue = gate.get("queue") or {}
            args = view.get("args") or {}
            spine_event = args.get("spine_event") or {}
            out.append({
                "id": edit_id,
                "intentSeq": view.get("intent_seq"),
                "taskSeq": view.get("task_seq"),
                "tool": view.get("tool"),
                "model": args.get("model"),
                "opened": view.get("intent_ts"),
                "reason": view.get("reason"),
                "verdict": view.get("verdict"),
                "effectiveClass": view.get("effective_class"),
                "state": view.get("state"),
                "resolution": view.get("resolution"),
                "spineSeq": spine_event.get("seq"),
                "spineHash": _s(spine_event.get("hash")),
                "entryHash": _s((source or {}).get("entry_hash")),
                "prevHash": _s((source or {}).get("prev_hash")),
                "orderingKey": queue.get("ordering_key"),
                "gateReport": gate,
                "folded": view,
            })
        return {"count": len(out), "escalations": out}
    finally:
        journal.close()


def main(argv):
    if len(argv) != 3 or argv[1] not in ("journal", "escalations"):
        print(json.dumps({"error": "usage: si_read.py journal|escalations <bundle_dir>"}))
        return 2
    try:
        if argv[1] == "journal":
            result = read_journal(argv[2])
        else:
            result = read_escalations(argv[2])
    except Exception as exc:  # fail closed: server maps this to 502
        print(json.dumps({"error": str(exc)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
