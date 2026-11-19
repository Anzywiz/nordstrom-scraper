#!/usr/bin/env python3
"""
Phase 2 - PDP enrichment -> output/products.csv (DBQ E0005, one row per colour x size).

Reads output/listing.jsonl (phase 1), opens every product page in CloakBrowser and reads
window.__INITIAL_CONFIG__ straight from the page. Parsing (sizes, quantities, categories, ...) is
your previous parse_payload(), unchanged.

Two sources (enrich.source in config.json, or --source on the command line):
  api  - GET /pdp/product-display/<id> with XMLHttpRequest from inside a nordstrom.com page (the page's own
         scripts add the bot-manager headers; we add the nord-* headers). ~0.6 s per product instead of a
         full page load. Measured limits: a browser session (IP + cookies) is cut off after ~28 requests, and a
         reload or a click on another tile does not restore it. So after enrich.api.requests_per_session
         requests (default 22) the browser is closed BEFORE the block and reopened on the next proxy, and one
         401/403 restarts it at once (enrich.api.restart_after_blocks = 1). More browsers (browser.workers /
         --workers) hide the ~20 s restart time and multiply throughput. Needs STICKY (numbered) proxy logins.
  html - open every product page in CloakBrowser and read window.__INITIAL_CONFIG__ (slower, most reliable).
Speed also comes from parallelism: browser.workers x browser.tabs_per_browser pages at once.

Daily refresh: run phase 1, then phase 2, every day. Both reset themselves when the date changes
and resume within the same day.

Usage:
    python phase2_enrich.py --test-url https://www.nordstrom.com/s/<slug>/<id>   # one PDP, prints first row
    python phase2_enrich.py --dump-url  https://www.nordstrom.com/s/<slug>/<id>  # save the full config
    python phase2_enrich.py --limit 20            # smoke test
    python phase2_enrich.py                       # everything, resumes automatically
    python phase2_enrich.py --job women           # one category only
    python phase2_enrich.py --source api --api-per-session 22 --workers 4 --limit 200       # tune the API mode
    python phase2_enrich.py --compare-url https://www.nordstrom.com/s/<slug>/<id>   # api vs html parsed rows
    python phase2_enrich.py --fresh               # start a clean products.csv
"""
import argparse
import asyncio
import csv
import json
import re
import time
from collections import Counter
from datetime import datetime, timedelta

from nord_browser import PDP_JS, Crawler, Fetch, TaskQueue
from nord_core import (COLUMNS, ROOT, build_base_row, category_path, json_list_capped, load_config, log_err, log_head,
                       log_info, log_line, log_ok, log_warn, rpath, today)

MATERIAL_RE = re.compile(
    r"\b(cotton|polyester|nylon|wool|silk|linen|leather|suede|cashmere|viscose|rayon|spandex|"
    r"elastane|acrylic|lyocell|modal|denim|velvet|satin|textile|fabric|canvas|rubber|metal|"
    r"acetate|plastic|polyamide|polyurethane|down|fur|shearling|mesh|jersey|tweed|gold|silver|"
    r"steel|titanium|crystal|glass)\b|\d+\s?%", re.I)


# ── PDP parsing (unchanged) ───────────────────────────────────────────────
def product_id_from(url: str) -> str:
    return url.rstrip("/").split("?")[0].split("/")[-1]

def _first(lst):
    return (lst or [{}])[0] or {}

def _find_npt(obj, depth=0):
    """nptHierarchy ('npt.clothing.tops.sweaters') wherever the PDP keeps it (normally coreProducts[0])."""
    if depth > 6:
        return None
    if isinstance(obj, dict):
        v = obj.get("nptHierarchy")
        if isinstance(v, str) and v:
            return v
        for x in obj.values():
            if isinstance(x, (dict, list)):
                r = _find_npt(x, depth + 1)
                if r:
                    return r
    elif isinstance(obj, list):
        for x in obj[:5]:
            r = _find_npt(x, depth + 1)
            if r:
                return r
    return None

def parse_payload(payload: dict, url: str, base: dict, job=None, npt=None) -> list:
    """job / npt come from the listing record (women|men|kids + its nptHierarchy); the PDP's own nptHierarchy
    wins when present. category1 = gender (Women / Men / Kids), then clothing / tops / sweaters ..."""
    ent = payload["ent"]
    bc = payload.get("bc") or ent.get("breadcrumb") or []
    prop = _first(ent.get("propositions"))
    pav = prop.get("availability") or {}
    p_sell = prop.get("sellingRetailPriceRange") or {}
    p_base = prop.get("baseRetailPriceRange") or {}
    crumbs = [b["name"] for b in bc if b.get("name") and b["name"].strip().lower() != "home"][:10]
    feats = [f for f in (ent.get("copyFeatures") or []) if f]
    ptags = [t.get("type") for t in prop.get("enticementTags") or [] if t.get("type")]

    # core product id = defaultCoreChoice (same value as in the listing); falls back to the first colour's id
    default_choice = ent.get("defaultCoreChoice") or next(
        (ch.get("coreChoiceId") for c in ent.get("coreProducts") or [] for ch in c.get("coreChoices") or []
         if ch.get("coreChoiceId")), "")
    common = dict(base)
    common.update({
        "product_code": str(default_choice or ""),
        "brand": ent.get("labelDisplayName") or "",
        "product_title": ent.get("copyProductTitle") or "",
        "description": ent.get("copyDescription") or "",
        "specifications": " | ".join(feats),
        "material_info": " | ".join(f for f in feats if MATERIAL_RE.search(f)),
        "itemurl": url,
    })
    hierarchy = _find_npt(ent) or npt
    if hierarchy:
        cats = category_path(job, hierarchy)             # Women > Clothing > Tops > Sweaters
    elif job:
        cats = category_path(job, None) + crumbs[:9]     # no hierarchy anywhere: gender + breadcrumb (if any)
    else:
        cats = crumbs
    for n in range(1, 11):
        common[f"category{n}"] = cats[n - 1] if n <= len(cats) else ""
    fit = " ".join(ent.get("copyFitInfoDetails") or [])

    rows = []
    for core in ent.get("coreProducts") or []:
        core_id = core.get("coreProductId") or ""
        for ch in core.get("coreChoices") or []:
            images = [s["imageUrl"] for s in ch.get("orderedShots") or []
                      if s.get("shotName") != "swatch" and s.get("imageUrl")]
            color = ch.get("displayColorDescription") or ""
            for it in (ch.get("items") or [None]):        # None -> colour without size list
                row = dict(common)
                row["color_info"] = color
                row["main_image_url"] = images[0] if images else ""
                row["all_images"] = json_list_capped(images)

                sku_id, size, variant, gtin, tags, final_sale = "", "", "", "", list(ptags), None
                if it is None:
                    sprop, av = prop, pav
                    sellable = (prop.get("salability") or {}).get("status") == "SELLABLE"
                    price, full, ptype = p_sell.get("min"), p_base.get("min"), ""
                else:
                    sku = it.get("sku") or {}
                    sku_id = sku.get("skuId") or ""
                    sprop = _first(sku.get("propositions"))
                    av = sprop.get("availability") or {}
                    sellable = (sprop.get("salability") or {}).get("status") == "SELLABLE"
                    pricing = _first(sprop.get("pricings"))
                    sell, base_r = pricing.get("sellingRetail") or {}, pricing.get("baseRetail") or {}
                    price = sell.get("price") or p_sell.get("min")
                    full = base_r.get("price") or p_base.get("min")
                    ptype = sell.get("priceType") or ""
                    tags += [e.get("type") for e in sprop.get("enticements") or []
                             if isinstance(e, dict) and e.get("type")]
                    final_sale = sprop.get("isFinalSale")
                    size = it.get("concatenatedDisplaySize") or " x ".join(
                        d.get("label") for d in (it.get("sizeDimension1"), it.get("sizeDimension2"))
                        if d and d.get("label"))
                    widths = [(w.get("displayName") or {}).get("label") or w.get("label") or ""
                              for w in it.get("widthFinders") or []]
                    st = it.get("sizeType") or {}
                    variant = " | ".join(x for x in widths + [
                        st.get("label") if st.get("code") not in (None, "RG") else ""] if x)
                    gtin = next((u.get("upc") for u in it.get("upcs") or [] if u.get("isPrimary") in ("TRUE", True)),
                                next((u.get("upc") for u in it.get("upcs") or []), ""))

                qty = av.get("shipQuantity")
                if qty is None:
                    qty = av.get("marketPickQuantity")
                if qty is None:
                    in_stock, qty_out = (1 if (sellable and av.get("isAvailable")) else 0), ""
                else:
                    in_stock, qty_out = (1 if (sellable and qty > 0) else 0), int(qty)

                row.update({
                    "size_info": size, "variant_info": variant,
                    "in_stock": in_stock, "quantity": qty_out,
                    "price": price or "", "full_price": full or "",
                    "promotion_type": ptype if ptype and ptype != "REGULAR" else "",
                    "additional_tags": ", ".join(dict.fromkeys(t for t in tags if t)),
                    "additional_code_1": str(ent.get("id") or ""), "additional_code_1_type": "PRODUCT_ID" if ent.get("id") else "",
                    "additional_code_2": sku_id, "additional_code_2_type": "SKU" if sku_id else "",
                    "additional_code_3": gtin, "additional_code_3_type": "GTIN" if gtin else "",
                    "additional_content": json.dumps({
                        "web_style_id": ent.get("id"), "item_number": ent.get("legacyStyleGroupId"),
                        "core_choice_id": ch.get("coreChoiceId"), "core_product_id": core_id or None,
                        "color_family": (ch.get("colorFamily") or {}).get("label"),
                        "extra_title": ent.get("extraProductTitle"), "fit_info": fit or None,
                        "is_final_sale": final_sale, "as_of": sprop.get("asOf"),
                    }, ensure_ascii=False),
                })
                rows.append(row)
    return rows


def payload_from_api(text: str, pid: str):
    """PDP API response -> {"ent": entity, "bc": breadcrumb} (the shape PDP_JS returns for the html source).
    Finds the entity wherever the API wraps it: any dict holding a coreProducts list; the one whose id is
    `pid` wins. Returns None when there is no entity."""
    data = json.loads(text)
    ents, stack = {}, [data]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if isinstance(o.get("coreProducts"), list):
                ents[str(o.get("id") or o.get("webStyleId") or "")] = o
                continue
            stack.extend(v for v in o.values() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            stack.extend(v for v in o if isinstance(v, (dict, list)))
    ent = ents.get(str(pid)) or (next(iter(ents.values())) if ents else None)
    if not ent:
        return None

    def find_bc(root):
        st = [root]
        while st:
            o = st.pop()
            if isinstance(o, dict):
                bc = o.get("breadcrumb")
                if isinstance(bc, list) and bc and isinstance(bc[0], dict) and bc[0].get("name"):
                    return bc
                st.extend(v for v in o.values() if isinstance(v, (dict, list)))
            elif isinstance(o, list):
                st.extend(v for v in o if isinstance(v, (dict, list)))
        return None
    return {"ent": ent, "bc": find_bc(ent) or find_bc(data)}


# ── run ───────────────────────────────────────────────────────────────────
def load_items(cfg, job):
    src = rpath(cfg, "listing_jsonl")
    if not src.exists():
        raise SystemExit("No listing found - run phase1_listing.py first")
    items, seen = [], set()
    with open(src, encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            if job and r.get("job") != job:
                continue
            if r["product_id"] not in seen and r.get("url"):
                seen.add(r["product_id"])
                items.append((r["product_id"], r["url"], r.get("job"), r.get("npt_hierarchy")))
    return items


class Enrich:
    def __init__(self, cfg, args, base, writer, csv_f, done_f, total):
        self.cfg, self.args, self.base = cfg, args, base
        self.writer, self.csv_f, self.done_f, self.total = writer, csv_f, done_f, total
        self.E = cfg["enrich"]
        self.api = self.E.get("source", "html") == "api"
        self.api_delay = float((self.E.get("api") or {}).get("delay_ms", 0)) / 1000 if self.api else None
        self.block_limit = int((self.E.get("api") or {}).get("restart_after_blocks", 0) or 0) if self.api else 0
        self.crawler = None
        self.stats, self.failed, self.queue = Counter(), [], TaskQueue()
        self.t0, self.last_print, self.since_flush = time.time(), 0.0, 0
        self.deadline, self.stopped = None, False

    def finish_ok(self, pid, rows):
        self.writer.writerows(rows)
        self.done_f.write(pid + "\n")
        self.stats["ok"] += 1
        self.stats["rows"] += len(rows)
        self.since_flush += 1
        if self.since_flush >= int(self.E["flush_every"]):
            self.flush()

    def finish_gone(self, pid):
        self.done_f.write(pid + "\n")
        self.stats["gone"] += 1
        self.since_flush += 1

    def flush(self):
        self.csv_f.flush()
        self.done_f.flush()
        self.since_flush = 0

    async def handle(self, task, sess, tab):
        if self.deadline and time.time() >= self.deadline:
            if not self.stopped:
                self.stopped = True
                left = self.queue.drain()
                log_warn(f"stop time reached ({datetime.fromtimestamp(self.deadline):%Y-%m-%d %H:%M}): not starting "
                         f"{left + 1:,} remaining products; finishing the ones in flight")
            return                                       # not marked done: the next run continues from here
        pid, url = task["pid"], task["url"]
        if self.api:
            res = await sess.fetch_pdp_api(pid, tab=tab)
        else:
            res = await sess.fetch_config(url, PDP_JS, pid, tab=tab, expect=f"/{pid}")
        payload, note = None, ""
        if res.kind == "ok":
            try:
                payload = payload_from_api(res.data, pid) if self.api else json.loads(res.data)
            except ValueError:
                payload = None
            if not payload or not payload.get("ent"):
                payload, note = None, "no entity"
                res = Fetch("ok", detail="no entity")        # the page loaded fine: not a block
        kw = {"block_limit": self.block_limit} if self.block_limit else {}
        await sess.report(res, delay=self.api_delay, **kw)

        if payload:
            try:
                rows = parse_payload(payload, url, self.base, task.get("job"), task.get("npt"))
            except Exception as e:
                log_err(f"[{pid}] parse error: {type(e).__name__}: {e}")
                rows = None
            if rows is not None:
                self.finish_ok(pid, rows)
                self.progress()
                return
            note = "parse error"
        if res.kind == "gone":
            self.finish_gone(pid)
            self.progress()
            return

        if res.detail == "browser restarted during fetch" and task.get("soft", 0) < 40:
            task["soft"] = task.get("soft", 0) + 1       # the browser's fault, not the product's: no attempt used
            await asyncio.sleep(1.0)
            self.queue.put(task)
            return
        task["attempts"] += 1
        if note == "no entity" and task["attempts"] >= int(self.E["no_entity_attempts"]):
            self.finish_gone(pid)                            # product page without a product: delisted
            self.progress()
        elif task["attempts"] >= int(self.E["max_attempts"]):
            self.failed.append(pid)
            self.stats["failed"] += 1
            log_warn(f"[{pid}] given up after {task['attempts']} attempts ({note or res.kind}: {res.detail})")
        else:
            self.stats["retried"] += 1
            self.queue.put(task)

    def progress(self):
        if time.time() - self.last_print < 15:
            return
        self.last_print = time.time()
        n_done = self.stats["ok"] + self.stats["gone"] + self.stats["failed"]
        rate = n_done / max(time.time() - self.t0, 1)
        eta = (self.total - n_done) / rate / 60 if rate else 0
        sessions = sum(getattr(s, "generation", 0) + 1 for s in self.crawler.sessions) if self.crawler else 0
        log_line(f"progress: {n_done:,}/{self.total:,} | ok {self.stats['ok']:,} gone {self.stats['gone']:,} "
                 f"failed {self.stats['failed']:,} retried {self.stats['retried']:,} | rows {self.stats['rows']:,} | "
                 f"{rate:.2f} pdp/s | ETA {eta:.0f} min" + (f" | {sessions} browser sessions" if sessions else ""))


def stop_deadline(cfg, args):
    """enrich.stop_at ("HH:MM" server time, "" = off) and enrich.max_runtime_hours (0 = off); --stop-at / --no-stop
    override. Returns epoch seconds or None."""
    if args.no_stop:
        return None
    E, now, cands = cfg["enrich"], datetime.now(), []
    at = (args.stop_at if args.stop_at is not None else E.get("stop_at") or "").strip()
    if at:
        try:
            hh, mm = (int(x) for x in at.split(":")[:2])
        except ValueError:
            raise SystemExit(f"enrich.stop_at must look like 23:00, got {at!r}")
        cands.append(now.replace(hour=hh, minute=mm, second=0, microsecond=0))
    hrs = float(args.max_hours if args.max_hours is not None else E.get("max_runtime_hours") or 0)
    if hrs > 0:
        cands.append(now + timedelta(hours=hrs))
    return min(cands).timestamp() if cands else None


async def run_enrich(cfg, args):
    deadline = stop_deadline(cfg, args)
    if deadline and time.time() >= deadline:
        stop_txt = f"{datetime.fromtimestamp(deadline):%H:%M}"
        run_file = rpath(cfg, "enrich_run")
        try:
            last_run = json.loads(run_file.read_text()).get("run_date") if run_file.exists() else None
        except ValueError:
            last_run = None
        if last_run != today():
            # products.csv (if any) is from an earlier day: never let convert/upload pick it up as today's data
            log_err(f"past the stop time ({stop_txt}) and there is no enrichment from today (last run: {last_run or 'never'}); "
                    f"exiting with code 3 so main.py does not convert/upload stale data. Use --no-stop to run anyway.")
            raise SystemExit(3)
        log_warn(f"past the stop time ({stop_txt}) - skipping enrichment (--no-stop to run anyway); "
                 f"the next phases use today's products.csv as it is")
        return
    if deadline:
        log_info(f"enrichment stops at {datetime.fromtimestamp(deadline):%Y-%m-%d %H:%M} (server time)")
    base = build_base_row(cfg)
    csv_path, done_path = rpath(cfg, "products_csv"), rpath(cfg, "enrich_done")
    run_path, failed_path = rpath(cfg, "enrich_run"), rpath(cfg, "enrich_failed")

    run = json.loads(run_path.read_text()) if run_path.exists() else {}
    if args.fresh or (cfg["enrich"].get("auto_reset_daily", True) and run.get("run_date") not in (None, today())):
        log_info("New day / --fresh: starting a clean products.csv")
        for p in (csv_path, done_path, failed_path):
            p.unlink(missing_ok=True)
    run_path.write_text(json.dumps({"run_date": today()}))

    items = load_items(cfg, args.job)
    done = set(done_path.read_text().split()) if done_path.exists() else set()
    pending = [it for it in items if it[0] not in done]
    if args.limit:
        pending = pending[:args.limit]
    log_info(f"{len(items):,} products in listing, {len(done):,} already done, {len(pending):,} to fetch")
    if not pending:
        return

    write_header = not csv_path.exists()
    csv_f = open(csv_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_f, fieldnames=COLUMNS)
    if write_header:
        writer.writeheader()
    done_f = open(done_path, "a", encoding="utf-8")
    state = Enrich(cfg, args, base, writer, csv_f, done_f, len(pending))
    state.deadline = deadline
    for pid, url, job, npt in pending:
        state.queue.put({"pid": pid, "url": url, "job": job, "npt": npt, "attempts": 0})
    try:
        async with Crawler(cfg, single_tab=cfg["enrich"].get("source") == "api") as crawler:
            state.crawler = crawler
            A = cfg["enrich"].get("api") or {}
            log_info(f"{crawler.n} browser(s) x {crawler.tabs} tab(s) | source: {cfg['enrich'].get('source', 'html')}"
                     + (f" | delay {A.get('delay_ms', 0)} ms, new browser/proxy every "
                        f"{A.get('requests_per_session', 0) or 'never'} requests"
                        + (f" (mint: heart, reload+click every {A.get('reload_after_requests', 0) or 'never'}, "
                           f"new IP after {A.get('max_reloads_per_session', 0) or 'never'} reloads)"
                           if A.get("mint_mode") == "heart" else "")
                        + f", restart after "
                        f"{A.get('restart_after_blocks', 'default')} block(s)" if cfg['enrich'].get('source') == 'api' else ""))
            await crawler.run(state.queue, state.handle)
    finally:
        state.flush()
        csv_f.close()
        done_f.close()
        failed_path.write_text("\n".join(state.failed))
    s = state.stats
    if state.stopped:
        log_warn(f"Stopped at the configured time with {len(pending) - s['ok'] - s['gone'] - s['failed']:,} products "
                 f"left for the next run; moving on is safe, products.csv holds everything fetched so far")
    log_ok(f"Finished: {s['ok']:,} products -> {s['rows']:,} rows in {csv_path.name} "
           f"({s['gone']:,} gone, {s['failed']:,} failed -> {failed_path.name})")
    tried = s["ok"] + s["gone"] + s["failed"]
    ratio_limit = float(cfg["enrich"].get("fail_exit_ratio", 0.3) or 0)
    if ratio_limit and tried >= 50 and s["failed"] / tried > ratio_limit:
        log_err(f"{s['failed']:,} of {tried:,} products failed ({s['failed'] / tried:.0%} > enrich.fail_exit_ratio "
                f"{ratio_limit:.0%}): exiting with code 4 so the convert/upload phases do not run on a mostly empty "
                f"products.csv. Re-run phase 2: the failed ids are retried, finished ones are skipped.")
        raise SystemExit(4)


async def run_single(cfg, url, dump):
    pid = product_id_from(url)
    async with Crawler(cfg, workers=1, single_tab=cfg["enrich"].get("source") == "api") as crawler:
        sess = crawler.sessions[0]
        if dump:
            res = await sess.fetch_config(url, "() => JSON.stringify(window.__INITIAL_CONFIG__ || null)",
                                          expect=f"/{pid}")
            if res.kind != "ok":
                log_err(f"No config: {res.kind} {res.detail or res.status}")
                return
            cfg_json = json.loads(res.data)
            out = ROOT / cfg["output_dir"] / f"debug_{pid}.json"
            out.parent.mkdir(exist_ok=True)
            out.write_text(json.dumps(cfg_json, indent=1), encoding="utf-8")
            ents = ((cfg_json.get("productDisplay") or {}).get("productDisplaysById") or {}).get("entities") or {}
            ent = ents.get(pid) or next(iter(ents.values()), {})
            log_ok(f"Saved {out}")
            log_line(f"top-level keys: {list(cfg_json)}")
            log_line(f"entity keys: {list(ent)}")
            return
        api = cfg["enrich"].get("source") == "api"
        res = await (sess.fetch_pdp_api(pid) if api else sess.fetch_config(url, PDP_JS, pid, expect=f"/{pid}"))
        log_line(f"source={'api' if api else 'html'} result: {res.kind} {res.detail or res.status} {res.secs:.1f}s")
        if res.kind == "ok":
            payload = payload_from_api(res.data, pid) if api else json.loads(res.data)
            if not payload:
                log_err("response had no product entity")
                return
            rows = parse_payload(payload, url, build_base_row(cfg))
            log_line(f"{len(rows)} rows. First row:")
            print(json.dumps(rows[0], indent=2, ensure_ascii=False))


async def run_compare(cfg, url):
    """Fetch one product through BOTH sources, parse both with parse_payload, and list the columns that differ."""
    pid = product_id_from(url)
    base = build_base_row(cfg)
    async with Crawler(cfg, workers=1, single_tab=cfg["enrich"].get("source") == "api") as crawler:
        sess = crawler.sessions[0]
        h = await sess.fetch_config(url, PDP_JS, pid, expect=f"/{pid}")
        a = await sess.fetch_pdp_api(pid)
    if h.kind != "ok" or a.kind != "ok":
        log_err(f"need both to work: html={h.kind} {h.detail or h.status}, api={a.kind} {a.detail or a.status}")
        return
    rows_h = parse_payload(json.loads(h.data), url, base)
    pa = payload_from_api(a.data, pid)
    if not pa:
        log_err("API response has no product entity")
        return
    rows_a = parse_payload(pa, url, base)

    def key(r):
        return (r["additional_code_2"], r["color_info"], r["size_info"])

    def norm(r):
        r = dict(r)
        try:
            ac = json.loads(r.get("additional_content") or "{}")
            ac.pop("as_of", None)
            r["additional_content"] = json.dumps(ac, sort_keys=True)
        except ValueError:
            pass
        return r
    H, A = {key(r): norm(r) for r in rows_h}, {key(r): norm(r) for r in rows_a}
    log_line(f"rows: html {len(rows_h)}, api {len(rows_a)}, shared {len(set(H) & set(A))}")
    diffs = Counter()
    examples = {}
    for k in set(H) & set(A):
        for col in COLUMNS:
            if H[k].get(col) != A[k].get(col):
                diffs[col] += 1
                examples.setdefault(col, (H[k].get(col), A[k].get(col)))
    if not diffs:
        log_ok("every shared row is identical in all columns -> the API is a drop-in replacement")
    for col, n in diffs.most_common():
        log_warn(f"{col}: {n} rows differ, e.g. html={str(examples[col][0])[:70]!r} api={str(examples[col][1])[:70]!r}")
    for col in COLUMNS:
        if col in ("quantity", "in_stock", "size_info", "category1", "price") and not diffs[col]:
            log_line(f"{col}: identical")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    ap.add_argument("--job", help="only this category (women/men/kids)")
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--test-url")
    ap.add_argument("--dump-url")
    ap.add_argument("--compare-url", help="fetch one PDP through api AND html and diff the parsed rows")
    ap.add_argument("--source", choices=["api", "html"], help="override enrich.source")
    ap.add_argument("--api-delay-ms", type=int, help="override enrich.api.delay_ms (pause after each API request)")
    ap.add_argument("--api-per-page", type=int, help="override enrich.api.requests_per_page (0 = never reload)")
    ap.add_argument("--api-per-session", type=int, help="override enrich.api.requests_per_session (0 = never rotate)")
    ap.add_argument("--api-warm", type=float, help="override enrich.api.warm_interaction_seconds (0 = no mouse/scroll warm-up)")
    ap.add_argument("--workers", type=int, help="override browser.workers (parallel browsers, one proxy each)")
    ap.add_argument("--mint-mode", choices=["heart", "quickview"], help="override enrich.api.mint_mode")
    ap.add_argument("--reload-after", type=int, help="override enrich.api.reload_after_requests (heart mode: reload page + click heart after N requests)")
    ap.add_argument("--max-reloads", type=int, help="override enrich.api.max_reloads_per_session (then new browser/proxy; 0 = unlimited)")
    ap.add_argument("--stop-at", help="stop starting new products at HH:MM server time (overrides enrich.stop_at; '' = never)")
    ap.add_argument("--max-hours", type=float, help="stop after this many hours (overrides enrich.max_runtime_hours)")
    ap.add_argument("--no-stop", action="store_true", help="ignore enrich.stop_at / max_runtime_hours")
    ap.add_argument("--api-reopen-url", help="override enrich.api.reopen_url ('' = home page)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    E = cfg["enrich"]
    E.setdefault("api", {})
    if args.source:
        E["source"] = args.source
    if args.api_delay_ms is not None:
        E["api"]["delay_ms"] = args.api_delay_ms
    if args.api_per_page is not None:
        E["api"]["requests_per_page"] = args.api_per_page
    if args.api_reopen_url is not None:
        E["api"]["reopen_url"] = args.api_reopen_url
    if args.api_per_session is not None:
        E["api"]["requests_per_session"] = args.api_per_session
    if args.api_warm is not None:
        E["api"]["warm_interaction_seconds"] = args.api_warm
    if args.mint_mode:
        E["api"]["mint_mode"] = args.mint_mode
    if args.reload_after is not None:
        E["api"]["reload_after_requests"] = args.reload_after
    if args.max_reloads is not None:
        E["api"]["max_reloads_per_session"] = args.max_reloads
    if args.workers:
        cfg["browser"]["workers"] = args.workers
    log_head("Phase 2 - PDP enrichment")
    try:
        if args.compare_url:
            asyncio.run(run_compare(cfg, args.compare_url))
        elif args.test_url or args.dump_url:
            asyncio.run(run_single(cfg, args.test_url or args.dump_url, bool(args.dump_url)))
        else:
            asyncio.run(run_enrich(cfg, args))
    except KeyboardInterrupt:
        log_warn("Interrupted - progress saved, re-run to resume")

if __name__ == "__main__":
    main()
