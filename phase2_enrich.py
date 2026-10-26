#!/usr/bin/env python3
"""
Phase 2 - PDP enrichment -> output/products.csv (DBQ E0005, one row per colour x size).

Reads output/listing.jsonl (phase 1), opens every product page in CloakBrowser and reads
window.__INITIAL_CONFIG__ straight from the page. Parsing (sizes, quantities, categories, ...) is
your previous parse_payload(), unchanged.

Why only navigation: on this machine curl_cffi (even with the browser's cookies) and in-page XHR
never received the real PDP; real navigations from CloakBrowser did (10/10). Speed therefore comes
from parallelism: browser.workers x browser.tabs_per_browser pages at once, and from stopping each
page load as soon as the config script has run.

Daily refresh: run phase 1, then phase 2, every day. Both reset themselves when the date changes
and resume within the same day.

Usage:
    python phase2_enrich.py --test-url https://www.nordstrom.com/s/<slug>/<id>   # one PDP, prints first row
    python phase2_enrich.py --dump-url  https://www.nordstrom.com/s/<slug>/<id>  # save the full config
    python phase2_enrich.py --limit 20            # smoke test
    python phase2_enrich.py                       # everything, resumes automatically
    python phase2_enrich.py --job women           # one category only
    python phase2_enrich.py --fresh               # start a clean products.csv
"""
import argparse
import asyncio
import csv
import json
import re
import time
from collections import Counter

from nord_browser import PDP_JS, Crawler, Fetch, TaskQueue
from nord_core import (COLUMNS, ROOT, build_base_row, json_list_capped, load_config, log_err, log_head,
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

def parse_payload(payload: dict, url: str, base: dict) -> list:
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
    for n, name in enumerate(crumbs, 1):
        common[f"category{n}"] = name
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
                items.append((r["product_id"], r["url"]))
    return items


class Enrich:
    def __init__(self, cfg, args, base, writer, csv_f, done_f, total):
        self.cfg, self.args, self.base = cfg, args, base
        self.writer, self.csv_f, self.done_f, self.total = writer, csv_f, done_f, total
        self.E = cfg["enrich"]
        self.stats, self.failed, self.queue = Counter(), [], TaskQueue()
        self.t0, self.last_print, self.since_flush = time.time(), 0.0, 0

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
        pid, url = task["pid"], task["url"]
        res = await sess.fetch_config(url, PDP_JS, pid, tab=tab, expect=f"/{pid}")
        payload, note = None, ""
        if res.kind == "ok":
            try:
                payload = json.loads(res.data)
            except ValueError:
                payload = None
            if not payload or not payload.get("ent"):
                payload, note = None, "no entity"
                res = Fetch("ok", detail="no entity")        # the page loaded fine: not a block
        await sess.report(res)

        if payload:
            try:
                rows = parse_payload(payload, url, self.base)
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
        log_line(f"progress: {n_done:,}/{self.total:,} | ok {self.stats['ok']:,} gone {self.stats['gone']:,} "
                 f"failed {self.stats['failed']:,} retried {self.stats['retried']:,} | rows {self.stats['rows']:,} | "
                 f"{rate:.2f} pdp/s | ETA {eta:.0f} min")


async def run_enrich(cfg, args):
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
    pending = [(pid, u) for pid, u in items if pid not in done]
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
    for pid, url in pending:
        state.queue.put({"pid": pid, "url": url, "attempts": 0})
    try:
        async with Crawler(cfg) as crawler:
            log_info(f"{crawler.n} browser(s) x {crawler.tabs} tab(s)")
            await crawler.run(state.queue, state.handle)
    finally:
        state.flush()
        csv_f.close()
        done_f.close()
        failed_path.write_text("\n".join(state.failed))
    s = state.stats
    log_ok(f"Finished: {s['ok']:,} products -> {s['rows']:,} rows in {csv_path.name} "
           f"({s['gone']:,} gone, {s['failed']:,} failed -> {failed_path.name})")


async def run_single(cfg, url, dump):
    pid = product_id_from(url)
    async with Crawler(cfg, workers=1) as crawler:
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
        res = await sess.fetch_config(url, PDP_JS, pid, expect=f"/{pid}")
        log_line(f"result: {res.kind} {res.detail or res.status} {res.secs:.1f}s")
        if res.kind == "ok":
            payload = json.loads(res.data)
            rows = parse_payload(payload, url, build_base_row(cfg))
            log_line(f"{len(rows)} rows. First row:")
            print(json.dumps(rows[0], indent=2, ensure_ascii=False))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    ap.add_argument("--job", help="only this category (women/men/kids)")
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--test-url")
    ap.add_argument("--dump-url")
    args = ap.parse_args()

    cfg = load_config(args.config)
    log_head("Phase 2 - PDP enrichment")
    try:
        if args.test_url or args.dump_url:
            asyncio.run(run_single(cfg, args.test_url or args.dump_url, bool(args.dump_url)))
        else:
            asyncio.run(run_enrich(cfg, args))
    except KeyboardInterrupt:
        log_warn("Interrupted - progress saved, re-run to resume")

if __name__ == "__main__":
    main()
