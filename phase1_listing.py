#!/usr/bin/env python3
"""
Phase 1 - Nordstrom listing: ONE script for unfiltered AND brand-filtered crawling.
There is no separate brand phase any more.

What it does, per category (women / men / kids):
  1. Loads page 1 and reads resultCount + pageCount. Pagination is capped (~695 pages), so
     reach = pageCount x products-per-page.
  2. resultCount <= reach  -> crawl the category unfiltered (largest pages, fewest requests).
  3. resultCount >  reach  -> read the Brand facet from that same page and split the category into
     partitions that each fit under the cap. Brands are PACKED into groups
     (listing.group_brands) so every request returns a full page instead of 3 items from a tiny
     brand. The script first verifies that a multi-brand filter really returns the sum of the
     brands (it tries listing.group_styles); if none works it falls back to one brand per partition.
  4. Pages of all partitions go through one queue, fetched by browsers x tabs in parallel.

Where the data comes from (listing.source):
  page - navigate to /browse/<category>?...&page=N and read window.__INITIAL_CONFIG__ (default,
         no API key needed)
  api  - fetch() the sbn/v2 browse API from inside the page (top=150 -> up to 120 items per request)

A blocked page (redirect to siteclosed.nordstrom.com, Access Denied, empty page) is NEVER the end of a
job: it is backed off and retried, and the browser restarts on a new proxy when it keeps happening.

Usage:
    python phase1_listing.py --discover                  # page 1 per category, dumps structure + facets
    python phase1_listing.py --category women --max-pages 3   # smoke test
    python phase1_listing.py                             # everything, resumes automatically
    python phase1_listing.py --fresh                     # ignore progress and delete the output
    python phase1_listing.py --export-csv                # only rebuild listing.csv
"""
import argparse
import asyncio
import csv
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode

from nord_browser import LISTING_JS, Crawler, Fetch, TaskQueue
from nord_core import (COLUMNS, ROOT, build_base_row, json_list_capped, load_config, load_json,
                       log_err, log_head, log_info, log_line, log_ok, log_warn, rpath, save_json_atomic, today)


# ── JSON helpers (the nesting is not fully known, so search by key) ───────
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

def locate_products(obj):
    """-> (container dict, path). The container is the dict holding the MOST product entries
    (an entry = dict with 'webPathAlias'); used by --discover to show where products live."""
    best, best_n, best_path = None, 0, ""
    stack = [(obj, "")]
    while stack:
        o, path = stack.pop()
        if isinstance(o, dict):
            n = sum(1 for v in o.values() if isinstance(v, dict) and "webPathAlias" in v)
            if n > best_n:
                best, best_n, best_path = o, n, path or "/"
            stack.extend((v, f"{path}/{k}") for k, v in o.items() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            stack.extend((v, f"{path}[{i}]") for i, v in enumerate(o) if isinstance(v, (dict, list)))
    return best, best_path

def find_products(obj):
    """{product_id: product}: the biggest dict of product entries anywhere in the payload."""
    best, best_n = None, 0
    stack = [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            n = sum(1 for v in o.values() if isinstance(v, dict) and "webPathAlias" in v)
            if n > best_n:
                best, best_n = o, n
            stack.extend(v for v in o.values() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            stack.extend(v for v in o if isinstance(v, (dict, list)))
    if not best:
        return {}
    return {k: v for k, v in best.items() if isinstance(v, dict) and "webPathAlias" in v}

def strip_products(obj, products):
    if obj is products:
        return f"<{len(products)} products omitted>"
    if isinstance(obj, dict):
        return {k: strip_products(v, products) for k, v in obj.items()}
    if isinstance(obj, list):
        return [strip_products(v, products) for v in obj]
    return obj

def find_facets(obj):
    """[(name, n_values)] of every facet-like dict found (name + values list)."""
    out, stack = [], [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if isinstance(o.get("name"), str) and isinstance(o.get("values"), list):
                out.append((o["name"], len(o["values"])))
            stack.extend(v for v in o.values() if isinstance(v, (dict, list)))
        elif isinstance(o, list):
            stack.extend(v for v in o if isinstance(v, (dict, list)))
    return out

def find_brand_facet(obj):
    """Facet dict named 'Brand' with a values list (largest one if several)."""
    best, stack = None, [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, dict):
            if o.get("name") == "Brand" and isinstance(o.get("values"), list):
                if best is None or len(o["values"]) > len(best["values"]):
                    best = o
            stack.extend(o.values())
        elif isinstance(o, list):
            stack.extend(o)
    return best

def parse_brands(facet) -> tuple:
    brands, seen, skipped = [], set(), 0
    for v in facet["values"]:
        q = parse_qs((v.get("queryParam") or "").lstrip("&?"))
        slug = (q.get("filterByBrand") or [None])[0]
        if not slug or slug in seen:
            skipped += 1
            continue
        seen.add(slug)
        brands.append({"name": v.get("value"), "slug": slug, "count": v.get("productDisplayCount") or 0})
    return brands, skipped


# ── product parsing + DBQ listing rows (unchanged from your phase2_listing.py) ─────
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
        "core_product_id": dflt or (colors[0]["core_choice_id"] if colors else None),   # = defaultCoreChoice
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
    code = r.get("core_product_id") or (colors[0].get("core_choice_id") if colors else "") or ""
    # first 2 images per colour (round-robin), capped < Excel's 32,767-char cell limit
    per_color = [c.get("images", [])[:2] for c in colors]
    images = list(dict.fromkeys(i for grp in zip(*[g + [None] * (2 - len(g)) for g in per_color]) for i in grp if i)) if per_color else []
    row.update({
        "product_code": code,                                   # core product id (defaultCoreChoice)
        "additional_code_1": r.get("product_id") or "",
        "additional_code_1_type": "PRODUCT_ID" if r.get("product_id") else "",
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
            "web_style_id": r.get("product_id"), "item_number": r.get("style_group_id"), "price_max": r.get("price_max"),
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


# ── URLs ──────────────────────────────────────────────────────────────────
def brand_params(style, slugs):
    if not slugs:
        return []
    if style == "repeat":
        return [("filterByBrand", s) for s in slugs]
    if style == "tilde":
        return [("filterByBrand", "~".join(slugs))]
    if style == "comma":
        return [("filterByBrand", ",".join(slugs))]
    raise ValueError(f"unknown group style {style!r}")

def build_url(cfg, cat, page, slugs=None, style="repeat") -> str:
    L, site = cfg["listing"], cfg["site"]["url"]
    api = L["source"] == "api"
    params = []
    if api:
        params += [("top", L.get("top") or 120), ("include", "ads"), ("include", "enticements")]
    elif L.get("top"):
        params.append(("top", L["top"]))
    params += [("breadcrumb", cat["breadcrumb"]), ("origin", "topnav"), ("page", page)]
    for k, v in (cat.get("extra_params") or {}).items():
        for item in (v if isinstance(v, list) else [v]):
            params.append((k, item))
    params += brand_params(style, slugs)
    if api:
        params.append(("isDynamicFacetsEnabled", "true"))
        base = f"{site}/sbn/v2/category/query/browse/{cat['path']}"
    else:
        base = f"{site}/browse/{cat['path']}"
    return f"{base}?{urlencode(params)}"


# ── one request ───────────────────────────────────────────────────────────
async def fetch_listing(sess, cfg, url, tab=0, keep_facets=False, expect=None) -> Fetch:
    L = cfg["listing"]
    if L["source"] == "api":
        return await sess.fetch_api(url, tab=tab, keep_facets=keep_facets)
    return await sess.fetch_config(url, LISTING_JS, {"keys": L["config_keys"], "keepFacets": keep_facets},
                                   tab=tab, expect=expect)

def read_page(res: Fetch):
    try:
        data = json.loads(res.data)
    except (TypeError, ValueError):
        return None
    return data if find_products(data) else None

async def fetch_checked(sess, cfg, url, keep_facets=False, tab=0, expect=None, tries=None):
    """One page with retries; used for planning requests. An empty page counts as a soft block."""
    for _ in range(tries or cfg["listing"]["max_attempts"]):
        res = await fetch_listing(sess, cfg, url, tab, keep_facets, expect)
        data = None
        if res.kind == "ok":
            data = read_page(res)
            if data is None:
                res = Fetch("blocked", detail="empty page")
        await sess.report(res)
        if data is not None:
            return data
        if res.kind == "gone":
            return None
    return None


async def fetch_count(sess, cfg, url, tries=None):
    """resultCount of a listing URL. None = could not read it (blocked / failed / empty config).
    A REAL zero (filter understood, nothing matches) returns 0 - unlike fetch_checked, which treats an
    empty page as a soft block."""
    for _ in range(tries or cfg["listing"]["max_attempts"]):
        res = await fetch_listing(sess, cfg, url)
        count = None
        if res.kind == "ok":
            try:
                data = json.loads(res.data)
            except (TypeError, ValueError):
                data = None
            count = find_first(data, "resultCount") if data is not None else None
            if count is None:
                res = Fetch("blocked", detail="no resultCount in page")
        await sess.report(res)
        if count is not None:
            return int(count)
        if res.kind == "gone":
            return None
    return None


# ── planning: how to cover a category ────────────────────────────────────
def pack_groups(brands, max_products, max_brands):
    """Greedy packing of brands (in facet order) into groups of <= max_products / max_brands.
    A brand bigger than max_products stays alone (it gets a gap warning later)."""
    groups, cur, size = [], [], 0
    for b in brands:
        if b["count"] > max_products:
            if cur:
                groups.append(cur)
                cur, size = [], 0
            groups.append([b])
            continue
        if cur and (size + b["count"] > max_products or len(cur) >= max_brands):
            groups.append(cur)
            cur, size = [], 0
        cur.append(b)
        size += b["count"]
    if cur:
        groups.append(cur)
    return groups

async def verify_grouping(sess, cfg, cat, brands):
    """Does a multi-brand filter return the SUM of the brands?
    -> working style | "none" (tested conclusively, no style works) | None (inconclusive: blocked).
    The expected value is the sum of each sample brand's ACTUAL resultCount (fetched alone), not the
    facet count. A blocked/unreadable page is never treated as 'got 0'."""
    pool = [b for b in brands if 20 <= b["count"] <= 300] or [b for b in brands if 0 < b["count"] <= 800]
    sample = pool[:3]
    if len(sample) < 2:
        return "none"
    singles = []
    for b in sample:
        n = await fetch_count(sess, cfg, build_url(cfg, cat, 1, [b["slug"]]), tries=3)
        if n is None:
            log_warn(f"grouping test: could not read brand '{b['name']}' (blocked) - inconclusive")
            return None
        log_info(f"grouping test: brand '{b['name']}' facet={b['count']:,} actual={n:,}")
        singles.append(n)
    want = sum(singles)
    if want == 0:
        return None
    inconclusive = False
    for style in cfg["listing"]["group_styles"]:
        url = build_url(cfg, cat, 1, [b["slug"] for b in sample], style)
        got = await fetch_count(sess, cfg, url, tries=3)
        if got is None:
            log_warn(f"grouping test '{style}': could not read the page (blocked) - inconclusive, not a failure")
            inconclusive = True
            continue
        log_info(f"grouping test '{style}': {len(sample)} brands, singles sum {want:,} -> got {got:,}")
        if abs(got - want) <= max(2, 0.02 * want):
            return style
    return None if inconclusive else "none"

async def plan_category(sess, cfg, name, cat, args, progress=None):
    """-> (info, {partition_key: partition}, first_page_data_or_None)"""
    L = cfg["listing"]
    data = await fetch_checked(sess, cfg, build_url(cfg, cat, 1), keep_facets=True)
    if data is None:
        log_err(f"[{name}] could not load page 1 - run --discover to see what comes back")
        return None
    prods = find_products(data)
    result_count = int(find_first(data, "resultCount") or 0)
    page_count = int(find_first(data, "pageCount") or 1)
    per_page = len(prods)
    reach = page_count * per_page
    info = {"result_count": result_count, "page_count": page_count, "per_page": per_page, "reach": reach}
    log_info(f"[{name}] {result_count:,} products | {per_page} per page x {page_count} pages = reach {reach:,}")

    def part(label, slugs, expected, style="repeat"):
        return {"category": name, "label": label, "brands": slugs, "style": style, "expected": expected,
                "page_count": None, "per_page": None, "result_count": None, "done_pages": [],
                "fetched": 0, "done": False}

    if not result_count or result_count <= reach:
        info["mode"] = "unfiltered"
        log_ok(f"[{name}] fits under the pagination cap -> unfiltered crawl")
        return info, {f"{name}|all": part("all", None, result_count)}, data

    facet = find_brand_facet(data)
    if not facet:
        log_err(f"[{name}] {result_count:,} products exceed the cap but no 'Brand' facet was found. "
                f"Run --discover and send me output/discover_{name}.json. Crawling unfiltered (partial).")
        info["mode"] = "unfiltered-partial"
        return info, {f"{name}|all": part("all", None, result_count)}, data

    brands, skipped = parse_brands(facet)
    brands = [b for b in brands if b["count"] > 0]
    if args.limit_brands:
        brands = brands[:args.limit_brands]
    total = sum(b["count"] for b in brands)
    info.update({"brands": len(brands), "brand_total": total, "unbranded": max(result_count - total, 0)})
    log_info(f"[{name}] {len(brands):,} brands cover {total:,} products "
             f"({info['unbranded']:,} without a brand value are not reachable by brand filters)")

    style = None
    if L.get("group_brands", True) and not args.no_group:
        style = (progress or {}).get("group_style")
        if style:
            log_info(f"[{name}] using multi-brand style '{style}' verified earlier in this run")
        else:
            verdict = None
            for rnd in range(int(L.get("group_verify_rounds", 3))):
                verdict = await verify_grouping(sess, cfg, cat, brands)
                if verdict is not None:
                    break
                log_warn(f"[{name}] grouping test inconclusive (round {rnd + 1}) - trying again")
            if verdict is None:
                if not L.get("fallback_per_brand", False):
                    log_err(f"[{name}] grouping test stayed inconclusive (blocks). NOT falling back to "
                            f"{len(brands):,} single-brand partitions - re-run to retry "
                            f"(or set listing.fallback_per_brand=true)")
                    return None
                verdict = "none"
            style = None if verdict == "none" else verdict
            if style and progress is not None:
                progress["group_style"] = style
    parts = {}
    if style:
        max_products = int(reach * float(L["group_fill_ratio"]))
        groups = pack_groups(brands, max_products, int(L["group_max_brands"]))
        for i, g in enumerate(groups, 1):
            parts[f"{name}|g{i:03d}"] = part(f"group {i} ({len(g)} brands)", [b["slug"] for b in g],
                                             sum(b["count"] for b in g), style)
        info["mode"] = f"grouped:{style}"
        log_ok(f"[{name}] multi-brand filter works ('{style}') -> {len(groups)} partitions of <= {max_products:,} products")
    else:
        for i, b in enumerate(brands, 1):
            parts[f"{name}|b{i:04d}"] = part(b["name"], [b["slug"]], b["count"])
        info["mode"] = "per-brand"
        log_warn(f"[{name}] multi-brand filters do not work -> {len(parts):,} single-brand partitions "
                 f"(slower: every brand ends in a part-empty page)")
    return info, parts, None


# ── the crawl ─────────────────────────────────────────────────────────────
class Listing:
    def __init__(self, cfg, args, progress, seen, fh):
        self.cfg, self.args, self.progress, self.seen, self.fh = cfg, args, progress, seen, fh
        self.parts, self.gaps = progress["partitions"], progress["gaps"]
        self.stats, self.queue = Counter(), TaskQueue()
        self.t0, self.last_save, self.last_print = time.time(), time.time(), 0.0
        self.prog_file = rpath(cfg, "listing_progress")
        self.max_fails = int(cfg["listing"]["max_attempts"])
        self.max_empty = int(cfg["listing"]["empty_page_retries"])

    def save(self, force=False):
        if force or time.time() - self.last_save >= 5:
            save_json_atomic(self.prog_file, self.progress)
            self.last_save = time.time()

    def ingest(self, key, page, data):
        """Write the page's products; the first page of a partition also queues pages 2..pageCount."""
        P, site = self.parts[key], self.cfg["site"]["url"]
        prods = find_products(data)
        if P["page_count"] is None:
            P["page_count"] = int(find_first(data, "pageCount") or 1)
            P["result_count"] = find_first(data, "resultCount")
            P["per_page"] = len(prods)
            reach = P["page_count"] * len(prods)
            if P["result_count"] and P["result_count"] > reach:
                self.gaps[key] = f"over pagination cap: {P['result_count']:,} > {reach:,} ({P['label']})"
                log_warn(f"[{key}] {P['result_count']:,} products but pagination reaches ~{reach:,} "
                         f"({P['label']}) - needs a second split")
            for pg in range(2, P["page_count"] + 1):
                if pg not in P["done_pages"]:
                    self.queue.put({"key": key, "page": pg, "tries": 0, "empties": 0})
        ids_block = find_first(data, "productDisplayIds") or {}
        rank_of = {str(i): n + 1 for n, i in enumerate(ids_block.get("organic") or [])}
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        new = 0
        for pid, prod in prods.items():
            pid = str(pid)
            if pid in self.seen:
                continue
            rank = ((page - 1) * (P["per_page"] or len(prods)) + rank_of[pid]) if pid in rank_of else None
            rec = parse_product(pid, prod, site, P["category"], page, rank, pid in rank_of, now)
            rec["brand_filter"] = ",".join(P["brands"]) if P["brands"] else None
            self.fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.seen.add(pid)
            new += 1
        self.fh.flush()
        if page not in P["done_pages"]:
            P["done_pages"].append(page)
        P["fetched"] += len(prods)
        if len(P["done_pages"]) >= P["page_count"]:
            P["done"] = True
        return new

    async def handle(self, task, sess, tab):
        if self.args.max_pages and self.stats["pages"] >= self.args.max_pages:
            return                                     # test mode: leave the rest for the next run
        key, page = task["key"], task["page"]
        P = self.parts[key]
        if page in P["done_pages"]:
            return
        url = build_url(self.cfg, self.cfg["categories"][P["category"]], page, P["brands"], P["style"])
        res = await fetch_listing(sess, self.cfg, url, tab, False, expect=f"page={page}" if page > 1 else None)
        data = None
        if res.kind == "ok":
            data = read_page(res)
            if data is None:                           # empty page: a soft block, NOT the end
                res = Fetch("blocked", detail=f"empty page {page}")
                task["empties"] += 1
        await sess.report(res)
        if data is not None:
            new = self.ingest(key, page, data)
            self.stats["pages"] += 1
            self.stats["new"] += new
            self.save()
            self.tick()
            return
        task["tries"] += 1
        self.stats["failed_requests"] += 1
        if res.kind == "gone" or task["tries"] >= self.max_fails or task["empties"] > self.max_empty:
            self.gaps[f"{key}|p{page}"] = f"{res.kind}: {res.detail or res.status}"
            self.stats["stalled_pages"] += 1
            log_err(f"[{key}] page {page} given up after {task['tries']} tries ({res.kind}); "
                    f"NOT marked done - re-run to retry")
        else:
            self.queue.put(task)

    def tick(self):
        if time.time() - self.last_print < 15:
            return
        self.last_print = time.time()
        done = sum(1 for p in self.parts.values() if p["done"])
        rate = self.stats["pages"] / max(time.time() - self.t0, 1)
        remaining = sum(max((p["page_count"] or 1) - len(p["done_pages"]), 0) for p in self.parts.values()
                        if not p["done"])
        eta = remaining / rate / 60 if rate else 0
        log_line(f"progress: {done}/{len(self.parts)} partitions done | {len(self.seen):,} products | "
                 f"{self.stats['pages']} pages ({rate:.2f}/s) | failed requests {self.stats['failed_requests']} | "
                 f"~{remaining} pages left, ETA ~{eta:.0f} min")

    def initial_tasks(self, categories):
        n = 0
        for key, P in self.parts.items():
            if P["done"] or P["category"] not in categories:
                continue
            if P["page_count"] is None:
                self.queue.put({"key": key, "page": 1, "tries": 0, "empties": 0})
                n += 1
            else:
                for pg in range(1, P["page_count"] + 1):
                    if pg not in P["done_pages"]:
                        self.queue.put({"key": key, "page": pg, "tries": 0, "empties": 0})
                        n += 1
        return n


async def run_listing(cfg, args):
    jsonl, prog_file = rpath(cfg, "listing_jsonl"), rpath(cfg, "listing_progress")
    progress = load_json(prog_file, {}) or {}
    stale = cfg["listing"].get("auto_reset_daily", True) and progress.get("run_date") not in (None, today())
    if args.fresh or stale:
        if stale:
            log_info("New day: starting a fresh listing run")
        for p in (jsonl, prog_file):
            p.unlink(missing_ok=True)
        progress = {}
    progress["run_date"] = today()
    for k in ("plan", "partitions", "gaps"):
        progress.setdefault(k, {})
    categories = [n for n in cfg["categories"] if not args.category or n == args.category]
    if not categories:
        raise SystemExit(f"Unknown category {args.category!r}. Available: {', '.join(cfg['categories'])}")
    if args.replan:                                    # throw away a saved (bad) plan; products already saved stay
        for name in categories:
            progress["plan"].pop(name, None)
            for k in [k for k, P in progress["partitions"].items() if P["category"] == name]:
                progress["partitions"].pop(k)
                progress["gaps"].pop(k, None)
        progress.pop("group_style", None)
        log_info(f"Re-planning: {', '.join(categories)}")
    if args.reopen:
        for key, P in progress["partitions"].items():
            if args.reopen in ("all", key, P["category"]):
                P["done"] = False
    seen = load_seen_ids(jsonl)
    log_info(f"{len(seen):,} products already saved | source: {cfg['listing']['source']}")

    fh = open(jsonl, "a", encoding="utf-8")
    state = Listing(cfg, args, progress, seen, fh)
    try:
        async with Crawler(cfg) as crawler:
            sess = crawler.sessions[0]
            for name in categories:
                if name in progress["plan"]:
                    continue
                planned = await plan_category(sess, cfg, name, cfg["categories"][name], args, progress)
                if planned is None:
                    continue
                info, parts, first = planned
                progress["plan"][name] = info
                progress["partitions"].update(parts)
                if first is not None:                  # page 1 is already in hand
                    state.ingest(f"{name}|all", 1, first)
                state.save(force=True)
            n = state.initial_tasks(categories)
            est = sum(max((p["page_count"] or math.ceil(p["expected"] / max(
                    progress["plan"][p["category"]]["per_page"], 1))) - len(p["done_pages"]), 0)
                      for p in state.parts.values() if not p["done"] and p["category"] in categories)
            log_info(f"{n} queued tasks (~{est:,} pages to fetch) | {crawler.n} browser(s) x {crawler.tabs} tab(s)")
            if n:
                await crawler.run(state.queue, state.handle)
    finally:
        fh.close()
        state.save(force=True)

    # ── report ──
    for key, P in state.parts.items():
        if P["done"] and P["expected"] and P["fetched"] < 0.9 * P["expected"] and key not in state.gaps:
            state.gaps[key] = f"fetched {P['fetched']:,} of ~{P['expected']:,} expected ({P['label']})"
    state.save(force=True)
    counts = Counter()
    if jsonl.exists():
        with open(jsonl, encoding="utf-8") as f:
            for line in f:
                counts[json.loads(line).get("job")] += 1
    open_parts = [k for k, P in state.parts.items() if not P["done"] and P["category"] in categories]
    log_ok(f"{sum(counts.values()):,} unique products saved ({', '.join(f'{c}: {n:,}' for c, n in counts.items())})")
    (log_warn if open_parts else log_ok)(
        f"{len(state.parts) - len(open_parts):,}/{len(state.parts):,} partitions complete"
        + (f"; {len(open_parts)} unfinished - re-run to resume" if open_parts else ""))
    if state.gaps:
        (ROOT / cfg["files"]["listing_gaps"]).parent.mkdir(parents=True, exist_ok=True)
        (ROOT / cfg["files"]["listing_gaps"]).write_text(json.dumps(state.gaps, indent=2), encoding="utf-8")
        log_warn(f"{len(state.gaps)} gap(s) -> {cfg['files']['listing_gaps']}")


async def run_discover(cfg, args):
    out_dir = ROOT / cfg["output_dir"]
    out_dir.mkdir(exist_ok=True)
    async with Crawler(cfg, workers=1) as crawler:
        sess = crawler.sessions[0]
        for name, cat in cfg["categories"].items():
            if args.category and name != args.category:
                continue
            url = build_url(cfg, cat, 1)
            log_info(f"[{name}] {cfg['listing']['source']} -> {url}")
            res = await fetch_listing(sess, cfg, url, 0, True)
            await sess.report(res)
            if res.kind != "ok":
                log_err(f"[{name}] discover failed: {res.kind} {res.detail or res.status}")
                continue
            data = json.loads(res.data)
            container, path = locate_products(data)
            prods = find_products(data)
            dst = out_dir / f"discover_{name}.json"
            dst.write_text(json.dumps(strip_products(data, container), indent=2), encoding="utf-8")
            if prods:
                (out_dir / f"discover_{name}_sample_product.json").write_text(
                    json.dumps(next(iter(prods.values())), indent=2), encoding="utf-8")
            facets = find_facets(data)
            brand = find_brand_facet(data)
            log_ok(f"[{name}] products_on_page={len(prods)} (found at {path}) | "
                   f"pageCount={find_first(data, 'pageCount')} resultCount={find_first(data, 'resultCount')} | "
                   f"Brand facet: {len(brand['values']) if brand else 'NOT FOUND'} values")
            log_line(f"    config keys: {list(data)[:12]}")
            log_line(f"    facets: {facets[:12]}")
            log_line(f"    saved {dst.name}" + (" + sample product" if prods else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    ap.add_argument("--category", help="only this category (e.g. women)")
    ap.add_argument("--fresh", action="store_true", help="ignore progress and delete output")
    ap.add_argument("--reopen", help="'all', a category or a partition key: set done=false")
    ap.add_argument("--max-pages", type=int, default=0, help="stop after N pages this run (testing)")
    ap.add_argument("--limit-brands", type=int, default=0, help="only the first N brands per category (testing)")
    ap.add_argument("--replan", action="store_true", help="discard the saved plan for the selected categories and plan again (keeps saved products)")
    ap.add_argument("--no-group", action="store_true", help="never pack brands into groups")
    ap.add_argument("--top", type=int, help="override listing.top (items per request)")
    ap.add_argument("--source", choices=["page", "api"], help="override listing.source")
    ap.add_argument("--discover", action="store_true", help="dump page-1 structure and facets per category")
    ap.add_argument("--export-csv", action="store_true", help="only rebuild listing.csv from the JSONL")
    args = ap.parse_args()

    cfg = load_config(args.config)
    if args.top is not None:
        cfg["listing"]["top"] = args.top
    if args.source:
        cfg["listing"]["source"] = args.source
    if args.export_csv:
        export_csv(cfg)
        return
    log_head("Phase 1 - listing")
    try:
        asyncio.run(run_discover(cfg, args) if args.discover else run_listing(cfg, args))
    except KeyboardInterrupt:
        log_warn("Interrupted - progress saved, re-run to resume")
    finally:
        if not args.discover:
            export_csv(cfg)

if __name__ == "__main__":
    main()
