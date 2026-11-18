#!/usr/bin/env python3
"""
Runs the phases one after the other, as listed in config.json -> pipeline.steps:

    listing  (phase1_listing.py)   capped by listing.limits.max_products
    enrich   (phase2_enrich.py)    stops starting new products at enrich.stop_at (server time, e.g. 23:00)
    convert  (phase3_convert.py)   builds the DBQ upload file from what was enriched
    upload   (phase4_upload.py)    uploads it to DBQ

The enrich phase leaves cleanly at its stop time (exit code 0, progress saved), so convert and upload then run on
whatever products.csv holds. A step that fails stops the pipeline (pipeline.stop_on_error).
The convert / upload script names in config.json are placeholders: point them at your real scripts.

    python run_pipeline.py                    # everything
    python run_pipeline.py --from convert     # skip listing + enrich
    python run_pipeline.py --only enrich
    python run_pipeline.py --skip listing
    python run_pipeline.py --list
"""
import argparse
import subprocess
import sys
import time

from nord_core import ROOT, load_config, log_err, log_head, log_info, log_ok, log_warn


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    ap.add_argument("--from", dest="from_step", help="start at this step name")
    ap.add_argument("--only", help="run just this step")
    ap.add_argument("--skip", action="append", default=[], help="skip this step (repeatable)")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()

    cfg = load_config(args.config)
    P = cfg.get("pipeline") or {}
    steps = P.get("steps") or []
    if not steps:
        raise SystemExit("config.json has no pipeline.steps")
    names = [s["name"] for s in steps]
    if args.list:
        for s in steps:
            print(f"{s['name']:<10} {s['script']} {' '.join(s.get('args') or [])}")
        return
    for n in [args.only, args.from_step, *args.skip]:
        if n and n not in names:
            raise SystemExit(f"unknown step {n!r}; steps: {', '.join(names)}")
    started = args.from_step is None
    t_all = time.time()
    for s in steps:
        name = s["name"]
        if args.from_step == name:
            started = True
        if not started or (args.only and name != args.only) or name in args.skip:
            continue
        script = ROOT / s["script"]
        log_head(f"pipeline: {name} ({s['script']})")
        if not script.exists():
            log_err(f"{script.name} not found - set the right file name in config.json -> pipeline.steps")
            if P.get("stop_on_error", True):
                sys.exit(2)
            continue
        t0 = time.time()
        try:
            rc = subprocess.run([sys.executable, str(script), *[str(a) for a in (s.get("args") or [])]], cwd=ROOT).returncode
        except KeyboardInterrupt:
            log_warn("Interrupted - stopping the pipeline (progress of the running step is saved)")
            sys.exit(130)
        (log_ok if rc == 0 else log_err)(f"{name} finished with exit code {rc} in {(time.time() - t0) / 60:.1f} min")
        if rc != 0 and P.get("stop_on_error", True):
            sys.exit(rc)
    log_ok(f"pipeline done in {(time.time() - t_all) / 60:.1f} min")


if __name__ == "__main__":
    main()
