#!/usr/bin/env python3
"""
Shared helpers for the Nordstrom scraper: config loading, logging, file paths,
proxy handling and the DBQ E0005 column set.

Self-contained on purpose: it does NOT import the old common.py, so the old phase files
(phase1_brands.py, phase2_listing.py, ...) can stay in the folder without clashing.
"""
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


# ── logging ───────────────────────────────────────────────────────────────
def _ts():
    return datetime.now().strftime("%H:%M:%S")

def log_head(m): print(f"\n\033[1;35m=== {m} ===\033[0m", flush=True)
def log_info(m): print(f"\033[36m[{_ts()}] i  {m}\033[0m", flush=True)
def log_ok(m):   print(f"\033[32m[{_ts()}] ok {m}\033[0m", flush=True)
def log_warn(m): print(f"\033[33m[{_ts()}] !  {m}\033[0m", flush=True)
def log_err(m):  print(f"\033[31m[{_ts()}] x  {m}\033[0m", file=sys.stderr, flush=True)


# ── config ────────────────────────────────────────────────────────────────
_ENV_RE = re.compile(r"\$\{([A-Za-z0-9_]+)\}")

# every key the scripts rely on; load_config() fails early with a readable message
REQUIRED_KEYS = [
    "site.url", "output_dir", "categories", "browser.workers", "browser.tabs_per_browser",
    "listing.source", "listing.config_keys", "enrich.max_attempts",
    "retry.base_backoff_seconds", "files.listing_jsonl", "files.products_csv",
    "fixed_values",
]

def _clean(o):
    """Drop documentation keys (those starting with '_') and expand ${ENV_VAR} in strings."""
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items() if not k.startswith("_")}
    if isinstance(o, list):
        return [_clean(v) for v in o]
    if isinstance(o, str):
        return _ENV_RE.sub(lambda m: os.getenv(m.group(1), ""), o)
    return o

def _dig(cfg, dotted):
    cur = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur

def load_config(path=None) -> dict:
    path = Path(path) if path else ROOT / "config.json"
    if not path.exists():
        raise SystemExit(f"Config not found: {path}")
    try:
        cfg = _clean(json.loads(path.read_text(encoding="utf-8")))
    except ValueError as e:
        raise SystemExit(f"{path.name} is not valid JSON: {e}")
    missing = [k for k in REQUIRED_KEYS if _dig(cfg, k) is None]
    if missing:
        raise SystemExit(f"{path.name} is missing: {', '.join(missing)}")
    if not cfg["categories"]:
        raise SystemExit(f"{path.name}: 'categories' is empty")
    for name, cat in cfg["categories"].items():
        if "path" not in cat or "breadcrumb" not in cat:
            raise SystemExit(f"{path.name}: category {name!r} needs 'path' and 'breadcrumb'")
    if cfg["listing"]["source"] not in ("page", "api"):
        raise SystemExit("listing.source must be 'page' or 'api'")
    # environment overrides, handy for quick tests without editing the file
    env = os.environ
    if env.get("HEADLESS", "").strip():
        cfg["browser"]["headless"] = env["HEADLESS"].strip().lower() in ("1", "true", "yes")
    if env.get("WORKERS", "").strip():
        cfg["browser"]["workers"] = int(env["WORKERS"])
    if env.get("TABS", "").strip():
        cfg["browser"]["tabs_per_browser"] = int(env["TABS"])
    return cfg

def rpath(cfg, name) -> Path:
    """Absolute path of cfg['files'][name]; the parent folder is created."""
    p = ROOT / cfg["files"][name]
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def save_json_atomic(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)

def load_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


# ── proxies ───────────────────────────────────────────────────────────────
def parse_proxy(raw: str):
    """Accepts 'http://user:pass@host:port', 'user:pass@host:port', 'host:port' or
    'host:port:user:pass'. Returns {'server', 'username', 'password', 'label', 'url'} or None."""
    raw = (raw or "").strip()
    if not raw:
        return None
    scheme = "http"
    if "://" in raw:
        scheme, raw = raw.split("://", 1)
    user = pw = None
    if "@" in raw:
        creds, hostport = raw.rsplit("@", 1)
        user, _, pw = creds.partition(":")
    else:
        parts = raw.split(":")
        if len(parts) == 4:
            hostport, user, pw = f"{parts[0]}:{parts[1]}", parts[2], parts[3]
        else:
            hostport = raw
    out = {"server": f"{scheme}://{hostport}", "label": hostport}
    if user:
        out["username"], out["password"] = user, pw or ""
        out["url"] = f"{scheme}://{user}:{pw or ''}@{hostport}"
    else:
        out["url"] = out["server"]
    return out

def proxy_list() -> list:
    """PROXY_LIST in .env: comma or newline separated. Empty -> direct connection."""
    raw = os.getenv("PROXY_LIST", "")
    return [p for p in (x.strip() for x in re.split(r"[,\n]", raw)) if p]

class ProxyPool:
    """Hands out proxies least-recently-used first and skips ones that just failed.
    With no proxies configured, acquire() returns None (direct connection)."""

    def __init__(self, raw_list, cooldown=120):
        self.items = [parse_proxy(p) for p in raw_list if parse_proxy(p)]
        self.cooldown = cooldown
        self.last_used = {p["label"]: 0.0 for p in self.items}
        self.cool_until = {p["label"]: 0.0 for p in self.items}
        self.in_use = set()

    def __len__(self):
        return len(self.items)

    def acquire(self, tag=""):
        if not self.items:
            return None
        now = time.time()
        free = [p for p in self.items if p["label"] not in self.in_use and self.cool_until[p["label"]] <= now]
        if not free:
            free = [p for p in self.items if p["label"] not in self.in_use] or list(self.items)
        pick = min(free, key=lambda p: self.last_used[p["label"]])
        self.last_used[pick["label"]] = now
        self.in_use.add(pick["label"])
        return pick

    def release(self, proxy):
        if proxy:
            self.in_use.discard(proxy["label"])

    def fail(self, proxy):
        if proxy:
            self.cool_until[proxy["label"]] = time.time() + self.cooldown

    def ok(self, proxy):
        if proxy:
            self.cool_until[proxy["label"]] = 0.0


# ── DBQ E0005 columns ─────────────────────────────────────────────────────
COLUMNS = [
    "dbq_prd_type", "website_name", "competence_date", "country_code", "currency_code",
    "store_id", "store_name", "store_address", "store_zip", "store_url",
    "merchant_id", "merchant_name", "merchant_url", "merchant_image", "merchant_descr",
    "product_code", "additional_code_1", "additional_code_1_type",
    "additional_code_2", "additional_code_2_type",
    "additional_code_3", "additional_code_3_type",
    "size_info", "brand", "color_info", "material_info", "variant_info",
    "description", "product_title", "specifications", "delivery", "in_stock", "quantity",
    "category1", "category2", "category3", "category4", "category5",
    "category6", "category7", "category8", "category9", "category10",
    "full_price", "price", "ppu", "unit_type", "promotion_type", "promotion_end_date",
    "package_desc", "additional_tags", "additional_content",
    "itemurl", "main_image_url", "all_images", "rank",
    "contract_id", "seller_id", "delivery_id",
]

def build_base_row(cfg) -> dict:
    """Empty row with every column, fixed_values stamped on, competence_date = today."""
    row = {c: "" for c in COLUMNS}
    row.update(cfg.get("fixed_values") or {})
    row["competence_date"] = today()
    return row

def json_list_capped(items, limit=32000) -> str:
    """JSON array string that stays under `limit` characters (drops items from the end)."""
    items = list(items)
    s = json.dumps(items, ensure_ascii=False)
    while len(s) > limit and items:
        items.pop()
        s = json.dumps(items, ensure_ascii=False)
    return s
