"""
dbq_upload.py
─────────────────────────────────────────────────────────────────────────────
Standalone DBQ (Data Boutique) uploader. No project utils required.

Uploads a local file to the DBQ S3 bucket, then polls for the validator
response (approved / refused).

Install deps:
    pip install boto3 python-dotenv

Usage:
    python dbq_upload.py                        # upload + wait for result
    python dbq_upload.py --status               # check latest result only
    python dbq_upload.py --list                 # list all objects under delivery path
    python dbq_upload.py --file path/to/file    # override local file
    python dbq_upload.py --config path/to/cfg   # override config file

Config (config.json) — minimum required under "upload" key:
    {
      "upload": {
        "aws_region":             "us-east-1",
        "s3_bucket":              "databoutique.com",
        "delivery_path":          "sellers/SELLER_ID/CONTRACT_ID",
        "local_file":             "output/data_file.txt",
        "check_interval_seconds": 30,
        "timeout_minutes":        30
      }
    }

    Final S3 path: sellers/SELLER_ID/CONTRACT_ID/YYYY-MM-DD/data_file.txt

.env (optional, can also use real environment variables):
    AWS_ACCESS_KEY_ID=...
    AWS_SECRET_ACCESS_KEY=...
"""

import argparse
import csv
import io
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


# ── Colour helpers (no deps) ──────────────────────────────────────────────────
_USE_COLOUR = sys.stdout.isatty() and os.name != "nt" or (
    os.name == "nt" and os.environ.get("WT_SESSION")
)

def _c(code, text):  return f"\033[{code}m{text}\033[0m" if _USE_COLOUR else text
def _green(t):       return _c("32", t)
def _yellow(t):      return _c("33", t)
def _red(t):         return _c("31", t)
def _cyan(t):        return _c("36", t)
def _grey(t):        return _c("90", t)
def _bold(t):        return _c("1",  t)


# ── Logging ───────────────────────────────────────────────────────────────────
def log_head(msg):  print(f"\n{_bold(_cyan('═══  ' + msg + '  ═══'))}\n")
def log_ok(msg):    print(f"  {_green('✔')}  {msg}")
def log_info(msg):  print(f"  {_cyan('·')}  {msg}")
def log_warn(msg):  print(f"  {_yellow('⚠')}  {msg}")
def log_err(msg):   print(f"  {_red('✘')}  {msg}", file=sys.stderr)


# ── Config + env loading ──────────────────────────────────────────────────────
def load_env(path=".env"):
    try:
        from dotenv import load_dotenv
        load_dotenv(path)
        return
    except ImportError:
        pass
    env = Path(path)
    if not env.exists():
        return
    for line in env.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def load_config(path="config.json"):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Upload progress bar ───────────────────────────────────────────────────────
class UploadProgress:
    BAR = 35

    def __init__(self, path):
        self._total     = os.path.getsize(path)
        self._seen      = 0
        self._start     = time.time()
        self._last_seen = 0
        self._last_ts   = self._start
        self._lock      = threading.Lock()

    @staticmethod
    def _fmt(n):
        for u in ("B", "KB", "MB", "GB"):
            if n < 1024: return f"{n:.1f} {u}"
            n /= 1024
        return f"{n:.1f} TB"

    @staticmethod
    def _fmt_t(s):
        s = int(s)
        if s < 60:  return f"{s}s"
        m, s = divmod(s, 60)
        if m < 60:  return f"{m}m {s:02d}s"
        h, m = divmod(m, 60)
        return f"{h}h {m:02d}m"

    def __call__(self, chunk):
        with self._lock:
            self._seen += chunk
            now     = time.time()
            elapsed = now - self._start
            dt      = now - self._last_ts
            if dt >= 0.4:
                speed = (self._seen - self._last_seen) / dt
                self._last_seen, self._last_ts = self._seen, now
            else:
                speed = self._seen / elapsed if elapsed else 0

            pct  = self._seen / self._total
            fill = int(pct * self.BAR)
            bar  = _green("█" * fill) + _grey("░" * (self.BAR - fill))
            eta  = self._fmt_t((self._total - self._seen) / speed) if speed else "?"
            sys.stdout.write(
                f"\r  [{bar}] {_bold(f'{pct:5.1%}')}  "
                f"{self._fmt(self._seen)}/{self._fmt(self._total)}  "
                f"@ {self._fmt(speed)}/s  ETA {eta}   "
            )
            sys.stdout.flush()
            if self._seen >= self._total:
                print(f"\n  {_green('Uploaded in')} {self._fmt_t(elapsed)} "
                      f"{_grey(f'(avg {self._fmt(self._total / elapsed)}/s)')}")


# ── S3 helpers ────────────────────────────────────────────────────────────────
def make_s3_client(region):
    import boto3
    return boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = region,
    )


def list_results(s3, bucket, prefix, keyword, after=None):
    """List objects under prefix whose key contains keyword.
    If after (a timezone-aware datetime) is given, only returns objects
    modified after that time — so stale responses are never picked up.
    """
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    out  = []
    for obj in resp.get("Contents", []):
        if keyword in obj["Key"].lower():
            lm = obj["LastModified"]
            if after and lm <= after:
                continue
            out.append((obj["Key"], lm))
    return sorted(out, key=lambda x: x[1], reverse=True)


def print_refused(s3, bucket, key, lm):
    log_err(f"REFUSED → {key}")
    log_err(f"Validator responded at {lm.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    obj  = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    rows = list(csv.reader(io.StringIO(text), delimiter=";", quotechar='"'))
    if not rows:
        log_warn("(empty response file)")
        return

    headers = rows[0]
    data    = rows[1:]
    col_w   = [
        max(len(str(headers[i])),
            max((len(str(r[i])) for r in data if i < len(r)), default=0))
        for i in range(len(headers))
    ]

    def fmt_row(r):
        return "  " + " │ ".join(
            str(r[i]).ljust(col_w[i]) if i < len(r) else " " * col_w[i]
            for i in range(len(headers))
        )

    sep = "  " + "─┼─".join("─" * w for w in col_w)
    print(_bold(fmt_row(headers)))
    print(_grey(sep))
    for r in data:
        print(fmt_row(r))
    print(f"\n  {_red(str(len(data)))} error(s) found")

    debug_path = "refused_debug.csv"
    with open(debug_path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)
    log_info(f"Saved → {debug_path}")


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Upload a file to DBQ S3 and poll for validator response."
    )
    parser.add_argument("--status", action="store_true",
                        help="Check validator result only, skip upload")
    parser.add_argument("--list",   action="store_true",
                        help="List all objects under delivery path")
    parser.add_argument("--file",   default=None,
                        help="Override local_file from config")
    parser.add_argument("--config", default="config.json",
                        help="Path to config.json (default: config.json)")
    parser.add_argument("--env",    default=".env",
                        help="Path to .env file (default: .env)")
    args = parser.parse_args()

    load_env(args.env)

    try:
        cfg = load_config(args.config)
    except FileNotFoundError:
        log_err(f"Config not found: {args.config}")
        sys.exit(1)

    ucfg = cfg.get("upload", {})
    if not ucfg:
        log_err("No 'upload' key found in config.json")
        sys.exit(1)

    bucket       = ucfg["s3_bucket"]
    region       = ucfg.get("aws_region", "us-east-1")
    base_path    = ucfg["delivery_path"].rstrip("/")
    content_date = datetime.now().strftime("%Y-%m-%d")
    dated_path   = f"{base_path}/{content_date}/"   # sellers/.../CONTRACT/YYYY-MM-DD/
    local_file   = args.file or ucfg["local_file"]
    interval     = ucfg.get("check_interval_seconds", 30)
    timeout_secs = ucfg.get("timeout_minutes", 30) * 60

    try:
        import boto3
    except ImportError:
        log_err("boto3 not installed — run: pip install boto3")
        sys.exit(1)

    s3 = make_s3_client(region)

    log_head("DBQ Upload")
    log_info(f"Bucket   : s3://{bucket}")
    log_info(f"Path     : {dated_path}")

    # ── List mode — searches from base path to show all dates ─────────
    if args.list:
        resp    = s3.list_objects_v2(Bucket=bucket, Prefix=base_path + "/")
        objects = resp.get("Contents", [])
        if not objects:
            log_warn("No objects found.")
        else:
            for obj in objects:
                print(f"  {obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')}  {obj['Key']}")
        sys.exit(0)

    # ── Status-only mode — searches dated path ────────────────────────
    if args.status:
        log_info(f"Checking latest validator response for {content_date} …")
        approved = list_results(s3, bucket, dated_path, "approved")
        refused  = list_results(s3, bucket, dated_path, "refused")
        if not approved and not refused:
            log_warn(f"No validator response found under {dated_path}")
            log_warn("Run --list to see all objects in the bucket.")
            sys.exit(0)
        la = approved[0] if approved else None
        lr = refused[0]  if refused  else None
        # Pick whichever response is most recent
        if la and (not lr or la[1] >= lr[1]):
            log_ok(f"APPROVED → {la[0]}")
            log_ok(f"Responded at {la[1].strftime('%Y-%m-%d %H:%M:%S UTC')}")
            if lr:
                log_warn(f"Note: an older refused response also exists from {lr[1].strftime('%H:%M:%S UTC')}")
        else:
            if la:
                log_warn(f"Note: an older approved response also exists from {la[1].strftime('%H:%M:%S UTC')}")
            print_refused(s3, bucket, *lr)
        sys.exit(0)

    # ── Upload ────────────────────────────────────────────────────────
    if not os.path.exists(local_file):
        log_err(f"File not found: {local_file}")
        sys.exit(1)

    s3_key  = dated_path + os.path.basename(local_file)
    size_mb = os.path.getsize(local_file) / 1024 / 1024
    log_info(f"File     : {local_file}  ({size_mb:.2f} MB)")
    log_info(f"Target   : s3://{bucket}/{s3_key}")
    print()

    progress = UploadProgress(local_file)
    s3.upload_file(local_file, bucket, s3_key, Callback=progress)
    upload_time = datetime.now(timezone.utc)
    log_ok(f"Uploaded at {upload_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # ── Poll ──────────────────────────────────────────────────────────
    log_info(f"Polling every {interval}s (timeout {ucfg.get('timeout_minutes', 30)}m) …\n")
    log_info(f"Ignoring any validator responses before {upload_time.strftime('%H:%M:%S UTC')}")
    print()
    deadline = time.time() + timeout_secs

    while time.time() < deadline:
        approved = list_results(s3, bucket, dated_path, "approved", after=upload_time)
        refused  = list_results(s3, bucket, dated_path, "refused",  after=upload_time)

        if approved:
            key, lm = approved[0]
            print()
            log_ok(f"APPROVED → {key}")
            log_ok(f"Responded at {lm.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            log_ok("Contract is active — data is purchasable.")
            sys.exit(0)
        if refused:
            print()
            print_refused(s3, bucket, *refused[0])
            sys.exit(1)

        elapsed = int((time.time() - (deadline - timeout_secs)) / 60)
        print(f"    {_grey(f'No response yet … ({elapsed}m elapsed)')}", end="\r")
        time.sleep(interval)

    print()
    log_warn("Timed out — no validator response received.")
    sys.exit(2)


if __name__ == "__main__":
    main()