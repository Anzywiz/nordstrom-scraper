#!/usr/bin/env python3
"""
Phase 2 - PDP enrichment -> output/products.csv (DBQ E0005, one row per colour x size).

Reads output/products_listing.jsonl (phase 1), opens every product page and reads the
JSON in <script>window.__INITIAL_CONFIG__ = {...}.

Fetch modes (config "fetch_mode" / --mode):
  curl    - browser mints cookies once, curl_cffi threads fetch HTML.        fastest
  browser - one browser, concurrent fetch() inside the page.                 fast
  nav     - several real browsers (processes) that NAVIGATE to each PDP and  slowest,
            read window.__INITIAL_CONFIG__ directly.                         most robust
  auto    - probe curl -> browser -> nav and use the first that works.

Debug helpers:
  --dump-url URL   save the full __INITIAL_CONFIG__ to output/debug_<id>.json and print
                   where "breadcrumb" etc. live (send me this file if fields look wrong)
  --test-url URL   fetch + parse one PDP and print the first row

Daily refresh: run phase1 then phase2 each day; both reset themselves when the date
changes and resume within the same day.
"""
import argparse
import csv
import json
import os
import queue
import random
import re
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from common import (COLUMNS, ROOT, build_base_row, check_proxies, close_browser, get_cookies,
                    json_list_capped, load_config, log_err, log_info, log_ok, log_warn, next_proxy,
                    open_browser, proxy_for_curl, proxy_label, proxy_list, rpath, today)

MARK = "window.__INITIAL_CONFIG__"
MATERIAL_RE = re.compile(
    r"\b(cotton|polyester|nylon|wool|silk|linen|leather|suede|cashmere|viscose|rayon|spandex|"
    r"elastane|acrylic|lyocell|modal|denim|velvet|satin|textile|fabric|canvas|rubber|metal|"
    r"acetate|plastic|polyamide|polyurethane|down|fur|shearling|mesh|jersey|tweed|gold|silver|"
    r"steel|titanium|crystal|glass)\b|\d+\s?%", re.I)


# ── PDP JSON extraction / parsing ─────────────────────────────────────────
def product_id_from(url: str) -> str:
    return url.rstrip("/").split("?")[0].split("/")[-1]

def find_breadcrumb(cfg):
    """The breadcrumb is not on the product entity itself; search the whole config
    (productDisplay first)."""
    def dfs(o, depth=0):
        if depth > 14:
            return None
        if isinstance(o, dict):
            b = o.get("breadcrumb")
            if isinstance(b, list) and b and isinstance(b[0], dict) and b[0].get("name"):
                return b
            for v in o.values():
                r = dfs(v, depth + 1)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = dfs(v, depth + 1)
                if r:
                    return r
        return None
    return dfs(cfg.get("productDisplay") or {}) or dfs(cfg)

def extract_payload(html: str, pid: str):
    """-> ({'ent':..., 'bc':...} | None, err)"""
    i = html.find(MARK)
    if i < 0:
        return None, "no config"
    j = html.find("{", i)
    try:
        cfg, _ = json.JSONDecoder().raw_decode(html, j)
    except ValueError:
        return None, "bad json"
    ents = ((cfg.get("productDisplay") or {}).get("productDisplaysById") or {}).get("entities") or {}
    ent = ents.get(pid) or next(iter(ents.values()), None)
    if not ent:
        return None, "no entity"
    return {"ent": ent, "bc": find_breadcrumb(cfg)}, ""

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

    common = dict(base)
    common.update({
        "product_code": str(ent.get("legacyStyleGroupId") or ""),
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
                    "additional_code_1": gtin, "additional_code_1_type": "GTIN" if gtin else "",
                    "additional_code_2": sku_id, "additional_code_2_type": "SKU" if sku_id else "",
                    "additional_code_3": core_id, "additional_code_3_type": "CORE_PRODUCT_ID" if core_id else "",
                    "additional_content": json.dumps({
                        "web_style_id": ent.get("id"), "core_choice_id": ch.get("coreChoiceId"),
                        "color_family": (ch.get("colorFamily") or {}).get("label"),
                        "extra_title": ent.get("extraProductTitle"), "fit_info": fit or None,
                        "is_final_sale": final_sale, "as_of": sprop.get("asOf"),
                    }, ensure_ascii=False),
                })
                rows.append(row)
    return rows


# ── JS used in the browser-based modes ────────────────────────────────────
# slim(cfg, id) -> JSON string {ent, bc} with the heavy per-store availability stripped
SLIM_JS = r"""
function slim(cfg, id) {
  const strip = (k, v) => (k === 'fulfillmentChannels' || k === 'services' || k === 'nodes') ? undefined : v;
  const ents = ((cfg.productDisplay || {}).productDisplaysById || {}).entities || {};
  const ent = ents[id] || Object.values(ents)[0];
  if (!ent) return '';
  let bc = null;
  const dfs = (o, d) => {
    if (bc || !o || typeof o !== 'object' || d > 14) return;
    if (Array.isArray(o)) { for (const x of o) dfs(x, d + 1); return; }
    if (Array.isArray(o.breadcrumb) && o.breadcrumb.length && o.breadcrumb[0] && o.breadcrumb[0].name) { bc = o.breadcrumb; return; }
    for (const k in o) dfs(o[k], d + 1);
  };
  dfs(cfg.productDisplay || {}, 0);
  if (!bc) dfs(cfg, 0);
  return JSON.stringify({ent: ent, bc: bc}, strip);
}
"""

BATCH_JS = SLIM_JS + r"""
(function () {
  window.__b = null;
  const urls = __URLS__;
  const one = async (u) => {
    try {
      const r = await fetch(u, {credentials: 'include', headers: {'accept': 'text/html,application/xhtml+xml'}});
      if (r.status !== 200) return {u: u, s: r.status, d: null, e: ''};
      const t = await r.text();
      const m = t.match(/window\.__INITIAL_CONFIG__\s*=\s*(\{[\s\S]*?\})\s*;?\s*<\/script>/);
      if (!m) return {u: u, s: 200, d: null, e: 'no config'};
      const id = u.split('?')[0].replace(/\/$/, '').split('/').pop();
      const d = slim(JSON.parse(m[1]), id);
      return {u: u, s: 200, d: d || null, e: d ? '' : 'no entity'};
    } catch (e) { return {u: u, s: 0, d: null, e: String(e).slice(0, 80)}; }
  };
  Promise.all(urls.map(one)).then(res => { window.__b = res; });
  return true;
})()
"""

NAV_JS = SLIM_JS + r"""
(function () { try { return slim(window.__INITIAL_CONFIG__, __ID__) || ''; } catch (e) { return ''; } })()
"""
NAV_STATE_JS = ("(document.title||'').indexOf('Access Denied')>=0 ? 'D' : "
                "(typeof window.__INITIAL_CONFIG__ !== 'undefined' ? 'R' : '')")


# ── fetchers ──────────────────────────────────────────────────────────────
# fetch_batch(urls) -> list of (status, payload|None, err) aligned with urls
class CurlFetcher:
    name = "curl"

    def __init__(self, cfg, sample_url):
        self.cfg, self.sample_url = cfg, sample_url
        self.batch_size = cfg["batch_size"]
        self.cookies, self.tl, self.proxies = {}, threading.local(), None
        self.pool = ThreadPoolExecutor(max_workers=cfg["workers"])

    def start(self):
        self.remint()

    def remint(self):
        proxy = next_proxy()
        log_info(f"Minting session cookies via browser (proxy: {proxy_label(proxy)})...")
        sb = open_browser(self.cfg, proxy)
        try:
            sb.goto(self.cfg["site_url"] + self.cfg.get("warmup_path", "/"))
            time.sleep(self.cfg.get("warmup_seconds", 10))
            if self.sample_url:
                sb.goto(self.sample_url)
                time.sleep(6)
            self.cookies = get_cookies(sb)
        finally:
            close_browser(sb)
        # cookies are tied to the IP: fetch through the SAME proxy used to mint them
        self.proxies = {"http": proxy_for_curl(proxy), "https": proxy_for_curl(proxy)} if proxy else None
        self.tl = threading.local()
        log_ok(f"Got {len(self.cookies)} cookies")

    def stop(self):
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _one(self, url):
        from curl_cffi import requests as cr
        s = getattr(self.tl, "s", None)
        if s is None:
            s = self.tl.s = cr.Session(impersonate=self.cfg.get("impersonate", "chrome"))
        for attempt in range(2):
            try:
                r = s.get(url, cookies=self.cookies, proxies=self.proxies,
                          timeout=self.cfg["request_timeout"],
                          headers={"accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                                   "accept-language": "en-US,en;q=0.9"})
            except Exception as e:
                if attempt == 0:
                    time.sleep(1.5)
                    continue
                return 0, None, str(e)[:60]
            if r.status_code in (429, 500, 502, 503, 504) and attempt == 0:
                time.sleep(random.uniform(3, 6))
                continue
            if r.status_code != 200:
                return r.status_code, None, ""
            payload, err = extract_payload(r.text, product_id_from(url))
            return 200, payload, err
        return 0, None, "retry exhausted"

    def fetch_batch(self, urls):
        return list(self.pool.map(self._one, urls))


class BrowserFetcher:
    name = "browser"

    def __init__(self, cfg, sample_url):
        self.cfg, self.sb = cfg, None
        self.batch_size = cfg["batch_size"]

    def start(self):
        self.stop()
        proxy = next_proxy()
        log_info(f"Opening browser session (proxy: {proxy_label(proxy)})...")
        self.sb = open_browser(self.cfg, proxy)
        self.sb.goto(self.cfg["site_url"] + self.cfg.get("warmup_path", "/"))
        time.sleep(self.cfg.get("warmup_seconds", 10))
        log_ok("Browser session ready")

    def remint(self):
        time.sleep(random.uniform(2, 5))
        self.start()

    def stop(self):
        if self.sb:
            close_browser(self.sb)
            self.sb = None

    def _run_chunk(self, urls):
        try:
            self.sb.evaluate(BATCH_JS.replace("__URLS__", json.dumps(urls)))
            for _ in range(self.cfg["request_timeout"] * 4 * 3):
                raw = self.sb.evaluate("window.__b ? JSON.stringify(window.__b) : ''")
                if raw:
                    return {r["u"]: r for r in json.loads(raw)}
                time.sleep(0.25)
        except Exception as e:
            return {u: {"s": 0, "d": None, "e": f"browser error: {str(e)[:50]}"} for u in urls}
        return {u: {"s": 0, "d": None, "e": "timeout"} for u in urls}

    def fetch_batch(self, urls):
        c, out = self.cfg["browser_concurrency"], {}
        for i in range(0, len(urls), c):
            out.update(self._run_chunk(urls[i:i + c]))
        res = []
        for u in urls:
            r = out.get(u) or {"s": 0, "d": None, "e": "missing"}
            res.append((r["s"], json.loads(r["d"]) if r.get("d") else None, r.get("e") or ""))
        return res


def nav_worker(cfg, idx, in_q, out_q):
    """Process: own browser, navigates to each PDP and reads window.__INITIAL_CONFIG__."""
    sb, boots = None, 0

    def boot():
        nonlocal sb, boots
        if sb:
            close_browser(sb)
        pl = cfg.get("_proxies") or proxy_list()
        proxy = pl[(idx + boots) % len(pl)] if pl else None
        boots += 1
        sb = open_browser(cfg, proxy)
        sb.goto(cfg["site_url"] + cfg.get("warmup_path", "/"))
        time.sleep(cfg.get("warmup_seconds", 10))

    boot()
    bad = 0
    while True:
        url = in_q.get()
        if url is None:
            break
        pid = product_id_from(url)
        status, raw, err = 0, None, "timeout"
        try:
            sb.goto(url)
            t_end = time.time() + cfg["request_timeout"]
            while time.time() < t_end:
                st = sb.evaluate(NAV_STATE_JS)
                if st == "D":
                    status, err = 403, ""
                    break
                if st == "R":
                    raw = sb.evaluate(NAV_JS.replace("__ID__", json.dumps(pid)))
                    status, err = 200, ("" if raw else "no entity")
                    break
                time.sleep(0.4)
        except Exception as e:
            status, err = 0, str(e)[:60]
        out_q.put((url, status, raw or None, err))
        bad = bad + 1 if status in (0, 403) else 0
        if bad >= 3:                      # blocked repeatedly: new browser (+ next proxy)
            try:
                boot()
            except Exception:
                pass
            bad = 0
    close_browser(sb)


class NavFetcher:
    name = "nav"

    def __init__(self, cfg, sample_url):
        self.cfg, self.procs = cfg, []
        self.batch_size = cfg["nav_workers"] * 25

    def start(self):
        import multiprocessing as mp
        ctx = mp.get_context("spawn")
        self.in_q, self.out_q = ctx.Queue(), ctx.Queue()
        n = self.cfg["nav_workers"]
        self.cfg["_proxies"] = proxy_list()
        log_info(f"Starting {n} navigation browsers (each takes ~15s to warm up)...")
        for i in range(n):
            p = ctx.Process(target=nav_worker, args=(self.cfg, i, self.in_q, self.out_q), daemon=True)
            p.start()
            self.procs.append(p)

    def stop(self):
        for _ in self.procs:
            try:
                self.in_q.put(None)
            except Exception:
                pass
        for p in self.procs:
            p.join(timeout=10)
            if p.is_alive():
                p.terminate()
        self.procs = []

    def remint(self):
        self.stop()
        time.sleep(3)
        self.start()

    def fetch_batch(self, urls):
        for u in urls:
            self.in_q.put(u)
        res = {}
        n = max(len(self.procs), 1)
        deadline = time.time() + (len(urls) / n) * self.cfg["request_timeout"] * 1.5 + 90
        while len(res) < len(urls) and time.time() < deadline:
            try:
                u, s, raw, e = self.out_q.get(timeout=5)
            except queue.Empty:
                continue
            res[u] = (s, json.loads(raw) if raw else None, e)
        return [res.get(u, (0, None, "lost")) for u in urls]


FETCHERS = {"curl": CurlFetcher, "browser": BrowserFetcher, "nav": NavFetcher}


# ── debug dump ────────────────────────────────────────────────────────────
def dump_url(cfg, url):
    pid = product_id_from(url)
    proxy = next_proxy()
    sb = open_browser(cfg, proxy)
    try:
        sb.goto(cfg["site_url"] + "/")
        time.sleep(cfg.get("warmup_seconds", 10))
        sb.goto(url)
        for _ in range(60):
            if sb.evaluate("typeof window.__INITIAL_CONFIG__ !== 'undefined' ? 1 : 0"):
                break
            time.sleep(0.5)
        raw = sb.evaluate("JSON.stringify(window.__INITIAL_CONFIG__ || null)")
        title = sb.evaluate("document.title")
    finally:
        close_browser(sb)
    if not raw or raw == "null":
        log_err(f"No __INITIAL_CONFIG__ found (page title: {title!r})")
        return
    cfg_json = json.loads(raw)
    out = ROOT / cfg["output_dir"] / f"debug_{pid}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(cfg_json, indent=1), encoding="utf-8")
    log_ok(f"Saved {out}  (page title: {title!r})")
    print("top-level keys:", list(cfg_json))
    ents = ((cfg_json.get("productDisplay") or {}).get("productDisplaysById") or {}).get("entities") or {}
    ent = ents.get(pid) or next(iter(ents.values()), {})
    print("entity keys:", list(ent))

    paths = []
    def walk(o, path=""):
        if isinstance(o, dict):
            for k, v in o.items():
                if k == "breadcrumb":
                    paths.append(path + "/" + k)
                walk(v, path + "/" + k)
        elif isinstance(o, list):
            for i, v in enumerate(o[:50]):
                walk(v, f"{path}[{i}]")
    walk(cfg_json)
    print("breadcrumb found at:", paths or "NOWHERE")
    print("breadcrumb picked :", [b.get("name") for b in (find_breadcrumb(cfg_json) or [])])


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

def choose_fetcher(mode, cfg, sample_url, probe_urls):
    if mode != "auto":
        f = FETCHERS[mode](cfg, sample_url)
        f.start()
        return f
    for name in ("curl", "browser", "nav"):
        f = FETCHERS[name](cfg, sample_url)
        f.start()
        res = f.fetch_batch(probe_urls)
        ok = sum(1 for _, p, _ in res if p)
        log_info(f"probe {name}: {ok}/{len(probe_urls)} ok  "
                 f"({dict(Counter(f'{s}:{e}' if e else str(s) for s, _, e in res))})")
        if ok >= max(2, len(probe_urls) // 2):
            return f
        f.stop()
        log_warn(f"{name} mode blocked, trying next")
    raise SystemExit("All fetch modes blocked. Try a proxy (PROXY_LIST) or run --dump-url to inspect.")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    ap.add_argument("--job")
    ap.add_argument("--mode", choices=["auto", "curl", "browser", "nav"])
    ap.add_argument("--fresh", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--test-url")
    ap.add_argument("--dump-url")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    mode = args.mode or cfg.get("fetch_mode", "auto")
    base = build_base_row(cfg)
    if proxy_list():
        check_proxies()

    if args.dump_url:
        dump_url(cfg, args.dump_url)
        return

    if args.test_url:
        m = "browser" if mode == "auto" else mode
        cfg["nav_workers"] = 1
        f = FETCHERS[m](cfg, args.test_url)
        f.start()
        try:
            (status, payload, err), = f.fetch_batch([args.test_url])
        finally:
            f.stop()
        print("mode:", m, "| status:", status, "| err:", err)
        if payload:
            rows = parse_payload(payload, args.test_url, base)
            print(f"{len(rows)} rows. First row:")
            print(json.dumps(rows[0], indent=2, ensure_ascii=False))
        return

    csv_path, done_path = rpath(cfg, "products_csv"), rpath(cfg, "phase2_done_file")
    run_path, failed_path = rpath(cfg, "phase2_run_file"), rpath(cfg, "phase2_failed_file")

    run = json.loads(run_path.read_text()) if run_path.exists() else {}
    if args.fresh or (cfg.get("auto_reset_daily", True) and run.get("run_date") not in (None, today())):
        log_info("New day / --fresh: starting a clean products.csv")
        for p in (csv_path, done_path, failed_path):
            p.unlink(missing_ok=True)
    run_path.write_text(json.dumps({"run_date": today()}))

    items = load_items(cfg, args.job)
    done = set(done_path.read_text().split()) if done_path.exists() else set()
    pending = deque((pid, u) for pid, u in items if pid not in done)
    if args.limit:
        pending = deque(list(pending)[:args.limit])
    total = len(pending)
    log_info(f"{len(items):,} products in listing, {len(done):,} already done, {total:,} to fetch")
    if not total:
        return

    probe = [u for _, u in list(pending)[:4]]
    fetcher = choose_fetcher(mode, cfg, pending[0][1], probe)
    log_info(f"Fetch mode: {fetcher.name}")

    write_header = not csv_path.exists()
    csv_f = open(csv_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_f, fieldnames=COLUMNS)
    if write_header:
        writer.writeheader()
    done_f = open(done_path, "a", encoding="utf-8")

    attempts, failed, stats = Counter(), [], Counter()
    t0, batches = time.time(), 0
    try:
        while pending:
            batch = [pending.popleft() for _ in range(min(fetcher.batch_size, len(pending)))]
            results = fetcher.fetch_batch([u for _, u in batch])
            retry, codes = [], Counter()
            for (pid, url), (status, payload, err) in zip(batch, results):
                codes[f"{status}:{err}" if err else str(status)] += 1
                if status == 200 and payload:
                    rows = parse_payload(payload, url, base)
                    writer.writerows(rows)
                    stats["rows"] += len(rows)
                    stats["ok"] += 1
                    done_f.write(pid + "\n")
                elif status in (404, 410):
                    stats["gone"] += 1
                    done_f.write(pid + "\n")
                else:
                    attempts[pid] += 1
                    if attempts[pid] >= cfg["max_attempts"]:
                        failed.append(pid)
                        stats["failed"] += 1
                    else:
                        retry.append((pid, url))
                        stats["retried"] += 1
            csv_f.flush()
            done_f.flush()
            batches += 1

            n_done = stats["ok"] + stats["gone"] + stats["failed"]
            rate = n_done / max(time.time() - t0, 1)
            eta = (total - n_done) / rate / 60 if rate else 0
            print(f"  batch {batches}: {n_done:,}/{total:,} | ok {stats['ok']:,} gone {stats['gone']:,} "
                  f"failed {stats['failed']:,} | rows {stats['rows']:,} | {rate:.1f} pdp/s | ETA {eta:.0f} min"
                  f" | responses {dict(codes)}", flush=True)

            if retry:
                pending.extendleft(reversed(retry))
            if (retry and len(retry) / len(batch) >= 0.2) or batches % cfg["remint_every_batches"] == 0:
                fetcher.remint()
            elif retry:
                time.sleep(random.uniform(2, 5))
    except KeyboardInterrupt:
        log_warn("Interrupted - progress saved, re-run to resume")
    finally:
        csv_f.close()
        done_f.close()
        fetcher.stop()
        failed_path.write_text("\n".join(failed))
    log_ok(f"Finished: {stats['ok']:,} products -> {stats['rows']:,} rows in {csv_path.name} "
           f"({stats['gone']:,} gone, {stats['failed']:,} failed -> {failed_path.name})")

if __name__ == "__main__":
    main()
