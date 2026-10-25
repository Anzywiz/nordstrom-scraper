#!/usr/bin/env python3
"""
Phase 1 - Nordstrom listing scraper.

Pages through the category browse API (women / men / kids) and writes one
record per product to output/products_listing.jsonl .

Why a browser? The endpoint rejects plain navigation / non-browser clients
(Akamai). So we open nordstrom.com with SeleniumBase CDP (which passes), then
call the API with fetch() *from inside that page*: same origin, real cookies,
real TLS fingerprint, plus the nord-* headers the site's own JS sends.

Usage:
    python phase1_listing.py --discover              # page 1 of each job, dumps facets
    python phase1_listing.py --job women --max-pages 2   # smoke test
    python phase1_listing.py                         # all jobs, resumes automatically
    python phase1_listing.py --fresh                 # ignore progress + output
    python phase1_listing.py --export-csv            # just rebuild the CSV
"""
import argparse
import csv
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

from common import (COLUMNS, build_base_row, check_proxies, ensure_relay,  # noqa: E402
                    json_list_capped, next_proxy, proxy_label, proxy_list)


# ── logging ───────────────────────────────────────────────────────────────
def _ts():
    return datetime.now().strftime("%H:%M:%S")

def log_info(m): print(f"\033[36m[{_ts()}] i  {m}\033[0m", flush=True)
def log_ok(m):   print(f"\033[32m[{_ts()}] ok {m}\033[0m", flush=True)
def log_warn(m): print(f"\033[33m[{_ts()}] !  {m}\033[0m", flush=True)
def log_err(m):  print(f"\033[31m[{_ts()}] x  {m}\033[0m", file=sys.stderr, flush=True)


# ── config / io ───────────────────────────────────────────────────────────
def load_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    if os.getenv("NORD_API_KEY"):
        cfg["headers"]["nord-api-key"] = os.environ["NORD_API_KEY"].strip()
    if os.getenv("HEADLESS", "").strip():
        cfg["headless"] = os.environ["HEADLESS"].strip().lower() in ("1", "true", "yes")
    return cfg

def rpath(cfg, key) -> Path:
    p = ROOT / cfg[key]
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def save_json_atomic(path: Path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


# ── JSON helpers (response nesting wasn't fully visible, so search for keys)
def find_first(obj, key):
    stack = [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if key in o:
                return o[key]
            stack.extend(o.values())
        elif isinstance(o, list):
            stack.extend(o)
    return None

def find_products(obj):
    """Return the {product_id: product} dict wherever it lives in the response."""
    stack = [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if any(isinstance(v, dict) and "webPathAlias" in v for v in o.values()):
                return o
            stack.extend(o.values())
        elif isinstance(o, list):
            stack.extend(o)
    return {}

def strip_products(obj, products):
    if obj is products:
        return f"<{len(products)} products omitted>"
    if isinstance(obj, dict):
        return {k: strip_products(v, products) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_products(v, products) for v in obj]
    return obj


# ── browser session ───────────────────────────────────────────────────────
FETCH_JS = """
(function () {
  window.__nr = null;
  fetch(__URL__, {method: 'GET', headers: __HEADERS__, credentials: 'include'})
    .then(async r => { const t = await r.text(); window.__nr = {status: r.status, text: t}; })
    .catch(e => { window.__nr = {status: 0, text: String(e)}; });
  return true;
})()
"""

class Session:
    def __init__(self, cfg):
        self.cfg = cfg
        self.sb = None
        self.headers = {}

    def start(self):
        from seleniumbase import sb_cdp
        kw = dict(locale="en", guest=True)
        if self.cfg.get("headless"):
            kw["headless"] = True
        proxy = next_proxy()
        if proxy:
            kw["proxy"] = ensure_relay(proxy)
        log_info(f"Opening browser and warming up session (proxy: {proxy_label(proxy)})...")
        self.sb = sb_cdp.Chrome(**kw)
        self.sb.goto(self.cfg["site_url"] + self.cfg.get("warmup_path", "/"))
        time.sleep(self.cfg.get("warmup_seconds", 10))
        self.headers = self._build_headers()
        log_ok("Session ready")

    def stop(self):
        try:
            self.sb.driver.stop()
        except Exception:
            pass
        self.sb = None

    def restart(self):
        log_warn("Restarting browser session...")
        self.stop()
        time.sleep(random.uniform(3, 6))
        self.start()

    def _build_headers(self) -> dict:
        h = dict(self.cfg["headers"])
        h["nord-context-id"] = str(uuid.uuid4())
        h["ads-nord-context-id"] = str(uuid.uuid4())
        h["eventcustomer"] = json.dumps({"idType": "SHOPPER_ID", "id": uuid.uuid4().hex})
        # reuse the experiment id the site put in its own cookie, if present
        exp = None
        try:
            ck = self.sb.evaluate("document.cookie") or ""
            for part in ck.split(";"):
                if "ExperimentId=" in part:
                    exp = part.split("ExperimentId=")[1].strip()
        except Exception:
            pass
        h["experimentid"] = exp or str(uuid.uuid4())
        return h

    def fetch(self, url: str, timeout: int = 60):
        js = (FETCH_JS.replace("__URL__", json.dumps(url))
                      .replace("__HEADERS__", json.dumps(self.headers)))
        try:
            self.sb.evaluate(js)
            for _ in range(timeout * 4):
                raw = self.sb.evaluate("window.__nr ? JSON.stringify(window.__nr) : ''")
                if raw:
                    r = json.loads(raw)
                    return r["status"], r["text"]
                time.sleep(0.25)
            return 0, "timeout waiting for fetch"
        except Exception as e:
            return 0, f"browser error: {e}"


# ── request layer ─────────────────────────────────────────────────────────
def build_url(cfg, job, page: int) -> str:
    params = [
        ("top", cfg["page_size"]),
        ("include", "ads"),
        ("include", "enticements"),
        ("breadcrumb", job["breadcrumb"]),
        ("origin", "topnav"),
        ("page", page),
        ("isDynamicFacetsEnabled", "true"),
    ]
    for k, v in (job.get("extra_params") or {}).items():
        for item in (v if isinstance(v, list) else [v]):
            params.append((k, item))
    return f"{cfg['site_url']}/sbn/v2/category/query/browse/{job['path']}?{urlencode(params)}"

def get_page(sess: Session, cfg, url: str):
    fails = 0
    for attempt in range(1, cfg["max_retries"] + 1):
        status, text = sess.fetch(url)
        if status == 200:
            try:
                return json.loads(text)
            except ValueError:
                log_warn(f"HTTP 200 but not JSON (block page?): {text[:100]!r}")
        elif status == 404:
            log_err(f"404 for {url} - check the job's path/breadcrumb in config.json")
            return None
        else:
            log_warn(f"HTTP {status}: {text[:120]!r}")
        fails += 1
        if fails >= cfg["restart_after_failures"]:
            sess.restart()
            fails = 0
        wait = cfg["retry_delay"] * attempt + random.uniform(0, 3)
        if status == 429:
            wait *= 3
        log_warn(f"Attempt {attempt}/{cfg['max_retries']} failed, retrying in {wait:.0f}s")
        time.sleep(wait)
    return None


# ── product parsing ───────────────────────────────────────────────────────
def _rng(d, k):
    d = d or {}
    return d.get(k)

def parse_product(pid, p, site, job_name, page, rank, is_organic, now):
    prop = (p.get("propositions") or [{}])[0]
    avail = prop.get("availability") or {}
    sell = prop.get("sellingRetailPriceRange")
    base = prop.get("baseRetailPriceRange")
    pct = prop.get("sellingRetailPercentageOffRange")
    clear = prop.get("clearancePriceRange")

    skus_by_choice = {c.get("coreChoiceId"): c.get("sellableSkus", [])
                      for c in prop.get("coreChoiceSelections", [])}
    core = (p.get("coreProducts") or [{}])[0]
    colors = []
    for core_p in p.get("coreProducts", []):
        for ch in core_p.get("coreChoices", []):
            shots = ch.get("orderedShots", [])
            cid = ch.get("coreChoiceId")
            colors.append({
                "core_choice_id": cid,
                "color": ch.get("displayColorDescription"),
                "color_family": (ch.get("colorFamily") or {}).get("label"),
                "sellable_skus": skus_by_choice.get(cid, []),
                "images": [s["imageUrl"] for s in shots
                           if s.get("shotName") != "swatch" and s.get("imageUrl")],
            })
    # default colour first -> main image
    dflt = p.get("defaultCoreChoice")
    colors.sort(key=lambda c: c["core_choice_id"] != dflt)
    main_image = next((c["images"][0] for c in colors if c["images"]), None)
    rev = p.get("reviews") or {}

    return {
        "product_id": pid,
        "style_group_id": p.get("legacyStyleGroupId"),
        "title": p.get("copyProductTitle"),
        "brand": p.get("labelDisplayName"),
        "brand_label_id": p.get("labelId"),
        "url": f"{site}/s/{p.get('webPathAlias')}/{pid}",
        "job": job_name,
        "gender": (core.get("gender") or {}).get("label"),
        "npt_hierarchy": core.get("nptHierarchy"),
        "price": _rng(sell, "min"),
        "price_max": _rng(sell, "max"),
        "full_price": _rng(base, "min"),
        "full_price_max": _rng(base, "max"),
        "clearance_price": _rng(clear, "min"),
        "discount_pct_max": _rng(pct, "max"),
        "enticements": [t.get("type") for t in prop.get("enticementTags", [])],
        "salability": (prop.get("salability") or {}).get("status"),
        "ship_quantity": avail.get("shipQuantity"),
        "pick_quantity": avail.get("pickQuantity"),
        "n_colors": len(colors),
        "n_skus": sum(len(c["sellable_skus"]) for c in colors),
        "colors": colors,
        "main_image": main_image,
        "review_count": rev.get("numberOfReviews"),
        "avg_rating": rev.get("averageRating"),
        "as_of": prop.get("asOf"),
        "page": page,
        "rank": rank,
        "is_organic": is_organic,
        "scraped_at": now,
    }

def _is_in_stock(r):
    return 1 if (r.get("salability") == "SELLABLE" and (r.get("ship_quantity") or 0) > 0) else 0

def listing_row(r, base):
    """Phase-1 record -> E0005 row. One row per product: the listing API has no
    per-size data, so size_info is blank and quantity is the product's total
    shippable quantity. Per-size rows come from phase2_enrich.py."""
    row = dict(base)
    cats = [r["job"].split("_")[0].capitalize()] + [
        x.replace("-", " ").title() for x in (r.get("npt_hierarchy") or "").split(".")
        if x and x != "npt"]
    for n, c in enumerate(cats[:10], 1):
        row[f"category{n}"] = c
    colors = r.get("colors") or []
    # first 2 images per colour (round-robin), capped < Excel's 32,767-char cell limit
    per_color = [c.get("images", [])[:2] for c in colors]
    images = list(dict.fromkeys(i for grp in zip(*[g + [None] * (2 - len(g)) for g in per_color]) for i in grp if i)) if per_color else []
    row.update({
        "product_code": r.get("style_group_id") or "",
        "brand": r.get("brand") or "",
        "product_title": r.get("title") or "",
        "color_info": " | ".join(c["color"] for c in colors if c.get("color")),
        "in_stock": _is_in_stock(r),
        "quantity": "" if r.get("ship_quantity") is None else r["ship_quantity"],
        "price": r.get("price") or "",
        "full_price": r.get("full_price") or "",
        "promotion_type": ", ".join(r.get("enticements") or []),
        "additional_tags": ", ".join(r.get("enticements") or []),
        "additional_content": json.dumps({
            "web_style_id": r.get("product_id"), "price_max": r.get("price_max"),
            "full_price_max": r.get("full_price_max"), "discount_pct_max": r.get("discount_pct_max"),
            "n_colors": r.get("n_colors"), "n_skus": r.get("n_skus"),
            "review_count": r.get("review_count"), "avg_rating": r.get("avg_rating"),
            "as_of": r.get("as_of")}, ensure_ascii=False),
        "itemurl": r.get("url") or "",
        "main_image_url": r.get("main_image") or "",
        "all_images": json_list_capped(images),
    })
    return row

def export_csv(cfg):
    src, dst = rpath(cfg, "listing_jsonl"), rpath(cfg, "listing_csv")
    if not src.exists():
        log_warn("No JSONL to export yet")
        return
    base, n = build_base_row(cfg), 0
    with open(src, encoding="utf-8") as f, open(dst, "w", newline="", encoding="utf-8") as out:
        w = csv.DictWriter(out, fieldnames=COLUMNS)
        w.writeheader()
        for line in f:
            w.writerow(listing_row(json.loads(line), base))
            n += 1
    log_ok(f"Exported {n:,} rows -> {dst.name}")


# ── main ──────────────────────────────────────────────────────────────────
def load_seen_ids(jsonl: Path) -> set:
    seen = set()
    if jsonl.exists():
        with open(jsonl, encoding="utf-8") as f:
            for line in f:
                try:
                    seen.add(json.loads(line)["product_id"])
                except Exception:
                    pass
    return seen

def run_discover(sess, cfg, jobs):
    out_dir = ROOT / cfg["output_dir"]
    out_dir.mkdir(exist_ok=True)
    for name, job in jobs.items():
        data = get_page(sess, cfg, build_url(cfg, job, 1))
        if data is None:
            log_err(f"[{name}] discover failed")
            continue
        prods = find_products(data)
        dst = out_dir / f"discover_{name}.json"
        dst.write_text(json.dumps(strip_products(data, prods), indent=2), encoding="utf-8")
        log_ok(f"[{name}] pageCount={find_first(data, 'pageCount')} "
               f"resultCount={find_first(data, 'resultCount')} products_on_page={len(prods)} -> {dst.name}")
        # also save one raw product so we can see every available field
        if prods:
            sample = next(iter(prods.values()))
            (out_dir / f"discover_{name}_sample_product.json").write_text(
                json.dumps(sample, indent=2), encoding="utf-8")
        time.sleep(cfg["delay"])

def crawl_job(sess, cfg, name, job, st, prog_file, progress, jsonl, seen, args):
    """Crawl one job. Sets st['done'] ONLY when we walk past the last page (page > pageCount).
    An empty page before that is a soft block, not the end: retry with a fresh session
    (next proxy); if it keeps coming back empty, stop WITHOUT marking done."""
    head, page_size = f"[{name}]", cfg["page_size"]
    page, pages_this_run, empties = st["next_page"], 0, 0
    max_empty = cfg.get("empty_page_retries", 4)
    while True:
        if st["page_count"] and page > st["page_count"]:
            st["done"] = True
            break
        if args.max_pages and pages_this_run >= args.max_pages:
            log_info(f"{head} --max-pages reached")
            break

        data = get_page(sess, cfg, build_url(cfg, job, page))
        if data is None:
            log_err(f"{head} giving up on page {page}; progress saved, re-run to resume")
            break

        prods = find_products(data)
        if not prods:
            empties += 1
            if empties > max_empty:
                log_err(f"{head} page {page} still empty after {max_empty} fresh-session retries "
                        f"(pageCount={st['page_count']}) -> stalled, NOT marked done; resumes here next run")
                break
            wait = 15 * empties + random.uniform(0, 5)
            log_warn(f"{head} page {page} empty (pageCount={st['page_count']}) - looks like a soft block, "
                     f"retry {empties}/{max_empty} with a fresh session in {wait:.0f}s")
            sess.restart()
            time.sleep(wait)
            continue
        empties = 0

        ids_block = find_first(data, "productDisplayIds") or {}
        organic = [str(i) for i in (ids_block.get("organic") or [])]
        rank_of = {pid: i + 1 for i, pid in enumerate(organic)}

        if st["page_count"] is None:
            st["page_count"] = find_first(data, "pageCount")
            st["result_count"] = find_first(data, "resultCount")
            reachable = (st["page_count"] or 0) * page_size
            log_info(f"{head} pageCount={st['page_count']} resultCount={st['result_count']}")
            if st["result_count"] and reachable < st["result_count"]:
                log_warn(f"{head} pagination only reaches ~{reachable:,} of "
                         f"{st['result_count']:,} products. Add partition jobs "
                         f"(filters/sorts via extra_params) - see --discover output.")

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        new = 0
        with open(jsonl, "a", encoding="utf-8") as f:
            for pid, p in prods.items():
                pid = str(pid)
                if pid in seen:
                    continue
                rank = ((page - 1) * page_size + rank_of[pid]) if pid in rank_of else None
                rec = parse_product(pid, p, cfg["site_url"], name, page, rank, pid in rank_of, now)
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                seen.add(pid)
                new += 1

        st["next_page"] = page + 1
        save_json_atomic(prog_file, progress)
        pages_this_run += 1
        print(f"  {head} page {page}/{st['page_count']}  +{new} new "
              f"({len(prods)} on page, {len(seen):,} total)", flush=True)
        page += 1
        time.sleep(random.uniform(cfg["delay"] * 0.8, cfg["delay"] * 1.3))

    save_json_atomic(prog_file, progress)
    if st["done"]:
        log_ok(f"{head} complete")

def run_jobs(sess, cfg, jobs, args):
    jsonl, prog_file = rpath(cfg, "listing_jsonl"), rpath(cfg, "progress_file")
    today_s = datetime.now().strftime("%Y-%m-%d")
    progress = json.loads(prog_file.read_text()) if prog_file.exists() else {"jobs": {}}
    stale = cfg.get("auto_reset_daily", True) and progress.get("run_date") not in (None, today_s)
    if args.fresh or stale:
        if stale:
            log_info("New day: starting a fresh listing run")
        for p in (jsonl, prog_file):
            if p.exists():
                p.unlink()
        progress = {"jobs": {}}
    progress["run_date"] = today_s

    if args.reopen:   # undo a wrongly-set done flag
        for n in (list(jobs) if args.reopen == "all" else [args.reopen]):
            if n in progress["jobs"]:
                progress["jobs"][n]["done"] = False
                log_info(f"[{n}] reopened, resumes at page {progress['jobs'][n]['next_page']}")
    save_json_atomic(prog_file, progress)

    seen = load_seen_ids(jsonl)
    log_info(f"Resuming with {len(seen):,} products already saved")
    for name in jobs:
        progress["jobs"].setdefault(name, {"next_page": 1, "page_count": None,
                                           "result_count": None, "done": False})

    rounds = cfg.get("job_rounds", 3)
    for rnd in range(1, rounds + 1):
        todo = [n for n in jobs if not progress["jobs"][n]["done"]]
        if not todo:
            break
        if rnd > 1:
            log_warn(f"Round {rnd}/{rounds}: incomplete jobs {todo}; new session in 60s")
            time.sleep(60)
            sess.restart()
        for name in todo:
            crawl_job(sess, cfg, name, jobs[name], progress["jobs"][name], prog_file,
                      progress, jsonl, seen, args)
        if args.max_pages:
            break

    for n in jobs:
        st = progress["jobs"][n]
        state = "DONE" if st["done"] else f"INCOMPLETE (next page {st['next_page']}/{st['page_count']})"
        (log_ok if st["done"] else log_warn)(f"[{n}] {state}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    ap.add_argument("--job", help="run a single job from config.json (e.g. women)")
    ap.add_argument("--fresh", action="store_true", help="ignore progress and delete output")
    ap.add_argument("--max-pages", type=int, default=0, help="limit pages per job (testing)")
    ap.add_argument("--discover", action="store_true", help="fetch page 1 and dump facets/structure")
    ap.add_argument("--reopen", help="job name or 'all': set done=false so it resumes")
    ap.add_argument("--export-csv", action="store_true", help="only rebuild the CSV from the JSONL")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    if args.export_csv:
        export_csv(cfg)
        return

    jobs = cfg["jobs"]
    if args.job:
        if args.job not in jobs:
            log_err(f"Unknown job {args.job!r}. Available: {', '.join(jobs)}")
            sys.exit(1)
        jobs = {args.job: jobs[args.job]}

    if proxy_list():
        check_proxies()
    sess = Session(cfg)
    sess.start()
    try:
        if args.discover:
            run_discover(sess, cfg, jobs)
        else:
            run_jobs(sess, cfg, jobs, args)
            export_csv(cfg)
    except KeyboardInterrupt:
        log_warn("Interrupted - progress saved, re-run to resume")
        export_csv(cfg)
    finally:
        sess.stop()

if __name__ == "__main__":
    main()
