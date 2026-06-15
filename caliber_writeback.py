"""Validation write-back + telemetry for the harness (#4 Step-4).

TWO deliberately-split outputs (approved design, flag 5):

1. CONFIG write-back -- the sign-off SNAPSHOT OF RECORD. write_validation()
   stores the evaluator's measured record into the matching entry's
   `validation:{}` block, serialized by the step-1 canonical writer so the diff
   is clean. It writes ONLY on a CHANGE-OF-RECORD (first validation, or a
   verdict CHANGE like PASS->FAIL) -- never on an unchanged verdict, so a
   2-hourly re-run produces NO diff and the sign-off signal isn't buried in
   timestamp churn. Reads the RAW json (never the loader's runtime shape, which
   carries compiled regex / built urls that must never round-trip to disk).

2. TELEMETRY -- the CONTINUOUS stream. log_telemetry() appends one JSONL line
   per (retailer, caliber, url, verdict, numbers, timestamp) to a gitignored
   _validation_telemetry/ dir (uploaded as a GHA artifact). Every run logs;
   this is NOT a tracked record and NOT a Supabase table.

Caller pattern (the future wide run, step 5+):
    record = evaluate(fetch_page(config, url), caliber, kind, title_filter)
    log_telemetry(record, retailer=r, caliber=cal, path_query=url, timestamp=ts)  # ALWAYS
    write_validation(config_path, cal, url, record, validated_at=ts)              # change-of-record only

================================ HONESTY BOUNDARY ===========================
This module WRITES MEASUREMENTS ONLY. It NEVER flips a `status` field: not
candidate->active, not active->parked, never an auto-applied replacement. A
verdict in validation:{} is a recorded fact; the status flip is a SEPARATE
human-reviewed diff. That propose-never-auto-apply split is the whole reason
the config block is a snapshot-of-record and not an actuator -- do not add
code here that assigns entry['status'].
=============================================================================
"""
import json
import os

import caliber_paths_io

_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TELEMETRY_DIR = os.path.join(_ROOT, '_validation_telemetry')
DEFAULT_TELEMETRY_FILE = os.path.join(DEFAULT_TELEMETRY_DIR, 'validation.jsonl')

# The measured fields the evaluator produces, copied verbatim into both sinks.
_MEASURED = ('verdict', 'status', 'redirect', 'title_match',
             'gate_pass_pct', 'n_products', 'note')


def _entry_url(e):
    return e['path'] + ('?' + e['query'] if e.get('query') else '')


def _find_entry(cfg, caliber, path_query):
    """The raw entry dict whose url == path_query, in calibers[caliber] (per-
    caliber) or parent_paths (caliber is None). None if absent."""
    if caliber is None:
        entries = cfg.get('parent_paths') or []
    else:
        entries = (cfg.get('calibers') or {}).get(caliber) or []
    for e in entries:
        if _entry_url(e) == path_query:
            return e
    return None


def write_validation(config_path, caliber, path_query, record, *,
                     validated_at, method='harness'):
    """Write the measured record into the matching entry's validation:{} block,
    ONLY when it is a change-of-record (first validation, or a verdict change).
    Returns True if it wrote, False if the verdict was unchanged (no diff).

    NEVER touches entry['status'] -- measurements only, never an actuator."""
    cfg = caliber_paths_io.load_config(config_path)
    entry = _find_entry(cfg, caliber, path_query)
    if entry is None:
        raise KeyError(f"no entry for caliber={caliber!r} url={path_query!r} "
                       f"in {config_path}")

    new_block = {'method': method, 'validated_at': validated_at}
    new_block.update({k: record.get(k) for k in _MEASURED})

    old = entry.get('validation')
    old_verdict = old.get('verdict') if isinstance(old, dict) else None
    if old_verdict == new_block['verdict']:
        # No change-of-record -> no write -> byte-identical file (idempotent).
        return False

    # Change of record. Update ONLY the validation block; status is untouched.
    entry['validation'] = new_block
    with open(config_path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(caliber_paths_io.dump_config(cfg))
    return True


def log_telemetry(record, *, retailer, caliber, path_query, timestamp,
                  telemetry_path=None):
    """Append one JSONL measurement line to the gitignored telemetry sink.
    Runs EVERY validation (continuous), independent of the change-of-record
    config write. Returns the path written."""
    path = telemetry_path or DEFAULT_TELEMETRY_FILE
    os.makedirs(os.path.dirname(path), exist_ok=True)
    line = {'timestamp': timestamp, 'retailer': retailer, 'caliber': caliber,
            'url': path_query}
    line.update({k: record.get(k) for k in _MEASURED})
    with open(path, 'a', encoding='utf-8', newline='\n') as f:
        f.write(json.dumps(line, ensure_ascii=False) + '\n')
    return path
