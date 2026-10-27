#!/usr/bin/env python3
"""
Orchestrator - runs the Nordstrom pipeline phases in order, each as its own subprocess.

    1  phase1_listing.py   listing   -> output/listing.jsonl
    2  phase2_enrich.py    PDP data  -> output/products.csv
    3  phase3_convert.py   convert   -> data_file.txt
    4  phase4_upload.py    upload to DBQ S3

Usage:
    python main.py                      # run all 4 phases (each resumes on its own)
    python main.py --fresh              # fresh scrape (--fresh is passed to phases 1 and 2)
    python main.py --skip 1             # skip phase 1
    python main.py --skip 1 4           # skip phases 1 and 4   (also: --skip 1,4)
    python main.py --fresh --skip 4     # fresh scrape but do not upload
    python main.py --only 3 4           # run just phases 3 and 4
    python main.py --category women     # one category (phase1 --category, phase2 --job)
    python main.py --continue-on-error  # keep going even if a phase fails

A phase that exits non-zero stops the pipeline (unless --continue-on-error).
Ctrl-C stops the running phase and prints the summary so far.
"""
import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# number -> (script, label, accepts --fresh)
PHASES = {
    1: ("phase1_listing.py", "Listing", True),
    2: ("phase2_enrich.py", "PDP enrichment", True),
    3: ("phase3_convert.py", "Convert", False),
    4: ("phase4_upload.py", "Upload", False),
}


def fmt(secs: float) -> str:
    s = int(round(secs))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h} hr {m} min {s} sec"


def parse_phase_list(values, parser, flag):
    out = set()
    for v in values or []:
        for part in str(v).replace(",", " ").split():
            if not part.isdigit() or int(part) not in PHASES:
                parser.error(f"{flag}: '{part}' is not a valid phase (choose from 1-4)")
            out.add(int(part))
    return out


def build_cmd(n, args):
    script, _, takes_fresh = PHASES[n]
    cmd = [sys.executable, "-u", str(ROOT / script)]
    if args.fresh and takes_fresh:
        cmd.append("--fresh")
    if args.category:
        if n == 1:
            cmd += ["--category", args.category]
        elif n == 2:
            cmd += ["--job", args.category]
    return cmd


def banner(text):
    line = "=" * 70
    print(f"\n{line}\n  {text}\n{line}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="Run all scraper phases in order.")
    ap.add_argument("--fresh", action="store_true",
                    help="fresh scrape: passes --fresh to phase 1 and phase 2")
    ap.add_argument("--skip", nargs="+", metavar="N", help="phases to skip, e.g. --skip 1 or --skip 1,4")
    ap.add_argument("--only", nargs="+", metavar="N", help="run only these phases, e.g. --only 3 4")
    ap.add_argument("--category", help="one category only (women/men/kids)")
    ap.add_argument("--continue-on-error", action="store_true",
                    help="run the remaining phases even if one fails")
    args = ap.parse_args()

    skip = parse_phase_list(args.skip, ap, "--skip")
    only = parse_phase_list(args.only, ap, "--only")
    to_run = [n for n in PHASES if (not only or n in only) and n not in skip]
    if not to_run:
        ap.error("nothing to run: every phase was skipped")

    for n in to_run:
        if not (ROOT / PHASES[n][0]).exists():
            ap.error(f"{PHASES[n][0]} not found in {ROOT}")

    plan = ", ".join(f"{n} ({PHASES[n][1]})" for n in to_run)
    banner(f"Pipeline start {datetime.now():%Y-%m-%d %H:%M:%S} | phases: {plan}"
           + (" | FRESH" if args.fresh else ""))

    results = []          # (n, status, seconds)
    t_all = time.time()
    interrupted = False

    for n in to_run:
        script, label, _ = PHASES[n]
        cmd = build_cmd(n, args)
        banner(f"Phase {n} - {label}   ->   {' '.join(cmd[2:])}")
        t0 = time.time()
        try:
            rc = subprocess.run(cmd, cwd=ROOT).returncode
        except KeyboardInterrupt:
            results.append((n, "INTERRUPTED", time.time() - t0))
            interrupted = True
            break
        dt = time.time() - t0
        status = "OK" if rc == 0 else f"FAILED (exit {rc})"
        results.append((n, status, dt))
        print(f"\n>> Phase {n} {status} in {fmt(dt)}", flush=True)
        if rc != 0 and not args.continue_on_error:
            break

    # summary
    banner("Summary")
    ran = {n for n, _, _ in results}
    for n in PHASES:
        script, label, _ = PHASES[n]
        row = next((r for r in results if r[0] == n), None)
        if row:
            print(f"  Phase {n}  {label:<15} {row[1]:<16} {fmt(row[2])}")
        elif n in skip or (only and n not in only):
            print(f"  Phase {n}  {label:<15} {'skipped':<16} -")
        else:
            print(f"  Phase {n}  {label:<15} {'not run':<16} -")
    print(f"\n  Total: {fmt(time.time() - t_all)}", flush=True)

    failed = any(not s.startswith("OK") for _, s, _ in results)
    sys.exit(130 if interrupted else (1 if failed else 0))


if __name__ == "__main__":
    main()
