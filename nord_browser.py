#!/usr/bin/env python3
"""
CloakBrowser layer shared by phase1_listing.py and phase2_enrich.py.

Why this design (from the tests on this machine, no proxy, same 10 PDPs):
    CloakBrowser navigation 10/10, SeleniumBase 4/10, Patchright 4/10, and neither curl_cffi
    (with the browser's cookies) nor in-page XHR could fetch the PDP HTML. So every page
    is a REAL navigation, and we read window.__INITIAL_CONFIG__ straight from the page.

Speed tricks:
    * goto(wait_until="commit") then poll: we stop as soon as the config script has run
      instead of waiting for images/analytics (window.stop() afterwards).
    * only the needed slice of the config crosses into Python (JS-side slimming).
    * several tabs per browser (browser.tabs_per_browser) and several browsers
      (browser.workers, each on its own proxy).

Failure handling: Nordstrom redirects some sessions to siteclosed.nordstrom.com. That is detected
within ~1s, backed off, retried, and after a few in a row the browser restarts on a new proxy
on a fresh browser. A block never ends the run.
"""
import asyncio
import json
import os
import platform
import random
import subprocess
import time
import traceback
import re
import uuid
from collections import deque
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

from nord_core import ProxyPool, log_err, log_info, log_ok, log_warn, proxy_list


# ── JS run inside the page ────────────────────────────────────────────────
# PDP: returns JSON {ent, bc} (entity + breadcrumb) with heavy per-store nodes removed.
# The pathname check guards against reading the PREVIOUS product's config after a failed goto.
PDP_JS = r"""
(id) => {
  if (!location.pathname.includes(id)) return '';
  const strip = (k, v) => (k === 'fulfillmentChannels' || k === 'services' || k === 'nodes') ? undefined : v;
  const cfg = window.__INITIAL_CONFIG__ || {};
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

# Listing page: returns JSON of the listed config keys (listing.config_keys, normally productResults);
# falls back to the whole config when that key does not exist. Facet lists are dropped unless asked.
LISTING_JS = r"""
(opt) => {
  const isFacet = v => v && typeof v === 'object' && !Array.isArray(v) &&
      Array.isArray(v.values) && typeof v.name === 'string' && ('productDisplayCount' in v);
  const strip = (k, v) => (k === 'fulfillmentChannels' || k === 'services' || k === 'nodes' ||
      (!opt.keepFacets && isFacet(v))) ? undefined : v;
  const cfg = window.__INITIAL_CONFIG__ || {};
  let out = cfg;
  if (cfg.productResults) {
    out = {};
    for (const k of opt.keys) if (cfg[k] !== undefined) out[k] = cfg[k];
  }
  return JSON.stringify(out, strip);
}
"""

# Optional: call the sbn/v2 browse API with fetch() from inside the page (listing.source = "api").
API_FETCH_JS = r"""
async (a) => {
  const isFacet = v => v && typeof v === 'object' && !Array.isArray(v) &&
      Array.isArray(v.values) && typeof v.name === 'string' && ('productDisplayCount' in v);
  const strip = (k, v) => (k === 'fulfillmentChannels' || k === 'services' || k === 'nodes' ||
      (!a.keepFacets && isFacet(v))) ? undefined : v;
  try {
    const r = await fetch(a.url, {method: 'GET', headers: a.headers, credentials: 'include'});
    const t = await r.text();
    if (r.status !== 200) return {s: r.status, t: t.slice(0, 300)};
    try { return {s: 200, t: JSON.stringify(JSON.parse(t), strip)}; }
    catch (e) { return {s: 200, t: t.slice(0, 300), bad: true}; }
  } catch (e) { return {s: 0, t: String(e).slice(0, 200)}; }
}
"""

# PDP details API (enrich.source = "api"): XMLHttpRequest from inside a nordstrom.com page, so the page's own
# bot-manager script decorates the request. Returns {s: status, t: body}.
QV_RE = re.compile(r"^\s*quick\s*view\s*$", re.I)
HEART_SEL = 'button[aria-label="Add to Wishlist"]'      # the wishlist heart on every listing tile
HEART_TILE_SEL = 'article:has(a[href^="/s/"]), li:has(a[href^="/s/"])'
_SKIP_HDR = {"host", "user-agent", "referer", "origin", "cookie", "connection", "content-length", "accept-encoding",
             "accept-language", "priority", "dnt", "upgrade-insecure-requests"}
_SENSOR_RE = re.compile(r"^x-[a-z0-9]{6,}-[a-z0-9]{1,2}$", re.I)      # bot-manager headers: the page adds them itself

PDP_API_JS = r"""
(a) => new Promise((resolve) => {
  const x = new XMLHttpRequest();
  x.open('GET', a.url);
  for (const [k, v] of Object.entries(a.headers)) { try { x.setRequestHeader(k, v); } catch (e) {} }
  x.timeout = a.timeout;
  x.onload = () => resolve({s: x.status, t: x.responseText});
  x.onerror = () => resolve({s: 0, t: 'xhr error'});
  x.ontimeout = () => resolve({s: 0, t: 'xhr timeout'});
  x.send();
})
"""

def state_js(title_markers):
    """'D' = block page title, 'R' = config present, '' = keep waiting."""
    markers = json.dumps([m.lower() for m in title_markers])
    return ("(() => { const t = (document.title || '').toLowerCase(); "
            f"if ({markers}.some(m => t.includes(m))) return 'D'; "
            "return typeof window.__INITIAL_CONFIG__ !== 'undefined' ? 'R' : ''; })()")



# listing API through XMLHttpRequest from inside the page: the bot-manager script adds its x-...-a/b/c/d/f/z headers
# to XHRs it sees (plain fetch() got 403). Same output shape as API_FETCH_JS.
LISTING_XHR_JS = r"""
(a) => new Promise((resolve) => {
  const isFacet = v => v && typeof v === 'object' && !Array.isArray(v) &&
      Array.isArray(v.values) && typeof v.name === 'string' && ('productDisplayCount' in v);
  const strip = (k, v) => (k === 'fulfillmentChannels' || k === 'services' || k === 'nodes' ||
      (!a.keepFacets && isFacet(v))) ? undefined : v;
  const x = new XMLHttpRequest();
  x.open('GET', a.url);
  for (const [k, v] of Object.entries(a.headers)) { try { x.setRequestHeader(k, v); } catch (e) {} }
  x.timeout = a.timeout || 40000;
  x.onload = () => {
    if (x.status !== 200) { resolve({s: x.status, t: (x.responseText || '').slice(0, 300)}); return; }
    try { resolve({s: 200, t: JSON.stringify(JSON.parse(x.responseText), strip)}); }
    catch (e) { resolve({s: 200, t: (x.responseText || '').slice(0, 300), bad: true}); }
  };
  x.onerror = () => resolve({s: 0, t: 'xhr error'});
  x.ontimeout = () => resolve({s: 0, t: 'xhr timeout'});
  x.send();
})
"""
LISTING_API_MARK = "/sbn/v2/category/query/browse/"


@dataclass
class Fetch:
    kind: str                 # ok | blocked | gone | error
    data: str = None          # JSON text when kind == "ok"
    detail: str = ""
    status: int = 0
    secs: float = 0.0


def proxy_label(proxy):
    """Log label for a proxy: 'username:***' (password redacted) instead of the shared host, so each
    login of a rotating pool (username-1 ... username-100) can be told apart in the log."""
    if not proxy:
        return "direct"
    user = proxy.get("username")
    if not user:
        try:
            user = urlparse(proxy.get("url") or "").username
        except Exception:
            user = None
    return f"{unquote(user)}:***" if user else (proxy.get("label") or "proxy")


# ── one browser (own proxy, several tabs) ─────────────────────────────────
class CloakSession:
    def __init__(self, cfg, tag, pool):
        self.cfg, self.b, self.r = cfg, cfg["browser"], cfg["retry"]
        self.tag, self.pool = f"[{tag}] ", pool
        self.url_markers = list(self.b.get("block_url_markers") or ["siteclosed"])
        self.title_markers = list(self.b.get("block_title_markers") or ["access denied"])
        self._state_js = state_js(self.title_markers)
        self._blocked_types = set(self.b.get("block_resources") or [])
        self._blocked_urls = [x.lower() for x in (self.b.get("block_url_patterns") or []) if x]
        self.proxy, self.browser, self.ctx, self.pages = None, None, None, []
        self.generation = 0
        self.consec, self.clean, self.slow = 0, 0, 1.0

    # lifecycle ------------------------------------------------------------
    async def start(self):
        self._lock, self._ready = asyncio.Lock(), asyncio.Event()
        await self._boot_until_healthy()
        self._ready.set()

    async def _boot_until_healthy(self):
        limit, n = int(self.b.get("session_attempts", 0) or 0), 0
        while True:
            n += 1
            ok = False
            try:
                self.proxy = self.pool.acquire(self.tag)
                await asyncio.wait_for(self._launch(self.proxy), 120)
                ok = await asyncio.wait_for(self._warmup(), 150)
            except Exception as e:
                log_warn(f"{self.tag}browser start failed: {type(e).__name__}: {str(e)[:120]}")
            if ok:
                return
            self.pool.fail(self.proxy)
            await self._close()
            if limit and n >= limit:
                raise RuntimeError(f"No accepted session after {n} attempts (browser.session_attempts={limit})")
            wait = float(self.b.get("session_retry_wait_seconds", 3)) + random.uniform(0, 2) + min(90.0, 5.0 * max(0, n - 2))
            log_warn(f"{self.tag}session attempt {n} rejected ({proxy_label(self.proxy)}); waiting {wait:.0f}s, then next proxy")
            await asyncio.sleep(wait)

    async def _launch(self, proxy):
        """Plain (non-persistent) browser + one fresh context. Every launch is a clean session."""
        from cloakbrowser import launch_async
        kwargs = {"headless": bool(self.b.get("headless", False))}
        kwargs.update(self.b.get("launch_kwargs") or {})
        # proxy: try the URL form first, then the Playwright dict form
        forms = [None]
        if proxy:
            forms = [proxy["url"], {k: proxy[k] for k in ("server", "username", "password") if k in proxy}]
        browser, last = None, None
        for form in forms:
            kw = dict(kwargs)
            if form is not None:
                kw["proxy"] = form
            try:
                browser = await launch_async(**kw)
                break
            except Exception as e:
                last = e
        if browser is None:
            raise last
        self.browser = browser
        self.ctx = await browser.new_context()
        if self._blocked_types or self._blocked_urls:
            await self.ctx.route("**/*", self._route)
        n_tabs = int(getattr(self, "n_tabs", 0) or 0) or max(1, int(self.b["tabs_per_browser"]))
        self.pages = [await self.ctx.new_page() for _ in range(n_tabs)]

    async def _route(self, route):
        req = route.request
        if req.resource_type in self._blocked_types or (
                self._blocked_urls and any(p in req.url.lower() for p in self._blocked_urls)):
            await route.abort()                               # images / media / fonts / trackers: pure proxy bandwidth
        else:
            await route.continue_()

    def _is_block(self, url, title=""):
        return (any(m in url for m in self.url_markers)
                or any(m in title.lower() for m in self.title_markers))

    async def _warmup(self):
        page = self.pages[0]
        site = self.cfg["site"]
        loaded = True
        try:
            await page.goto(site["url"] + site.get("warmup_path", "/"),
                            wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            loaded = False
            log_warn(f"{self.tag}warm-up navigation failed: {str(e)[:80]}")
        await asyncio.sleep(float(self.b.get("warmup_seconds", 4)))
        try:
            title = (await page.title()) or ""
        except Exception:
            title = ""
        label = proxy_label(self.proxy)
        if not loaded or self._is_block(page.url, title):
            log_warn(f"{self.tag}warm-up rejected (title {title!r}, url {page.url[:60]}, proxy {label})")
            return False
        try:
            persona = await page.evaluate("navigator.platform")      # Win32 / MacIntel / Linux x86_64
        except Exception:
            persona = "?"
        log_ok(f"{self.tag}browser ready (proxy: {label}, platform: {persona}, {len(self.pages)} tab(s))")
        return True

    async def _close(self):
        for obj in (self.ctx, self.browser):
            try:
                if obj:
                    await asyncio.wait_for(obj.close(), 20)
            except Exception:
                pass
        self.ctx = self.browser = None
        self.pages = []
        self.pool.release(self.proxy)

    async def stop(self):
        await self._close()

    async def restart(self, seen_generation=None):
        """New browser, next proxy. Safe to call from several tabs at once."""
        async with self._lock:
            if seen_generation is not None and seen_generation != self.generation:
                return                                   # another tab already restarted it
            self.generation += 1                         # in-flight fetches on the old browser notice at once
            self._ready.clear()
            log_warn(f"{self.tag}restarting browser (leaving {proxy_label(self.proxy)})...")
            self.pool.fail(self.proxy)
            await self._close()
            await asyncio.sleep(random.uniform(3, 6))
            try:
                await self._boot_until_healthy()
            except Exception as e:
                self.dead = True
                self._ready.set()
                log_err(f"{self.tag}could not start a new browser ({str(e)[:100]}); this browser is out of the run")
                raise
            self.consec = 0
            self.slow = 1.0              # a block belongs to the old session/IP
            self._ready.set()

    # fetching -------------------------------------------------------------
    async def fetch_config(self, url, js, arg=None, tab=0, expect=None) -> Fetch:
        """Navigate to url and return the JS result (a JSON string) once window.__INITIAL_CONFIG__ exists.
        `expect` is a substring the final URL must contain if the navigation call itself failed
        (protects against reading the previous page's config)."""
        await self._ready.wait()
        gen, t0 = self.generation, time.time()
        page = self.pages[tab % len(self.pages)]
        status, nav_failed, nav_err = 0, False, ""
        try:
            resp = await page.goto(url, wait_until="commit",
                                   timeout=int(float(self.b["nav_timeout_seconds"]) * 1000))
            status = resp.status if resp else 0
        except Exception as e:
            nav_failed, nav_err = True, f"{type(e).__name__}: {str(e)[:60]}"
        if status in (404, 410):
            return Fetch("gone", status=status, secs=time.time() - t0)
        if status in (403, 429):
            return Fetch("blocked", detail=f"HTTP {status}", status=status, secs=time.time() - t0)

        deadline = t0 + float(self.b["config_timeout_seconds"])
        while time.time() < deadline:
            if gen != self.generation:
                return Fetch("error", detail="browser restarted during fetch", secs=time.time() - t0)
            try:
                cur = page.url
                if self._is_block(cur):
                    return Fetch("blocked", detail=cur[:80], status=status, secs=time.time() - t0)
                state = await page.evaluate(self._state_js)
                if state == "D":
                    return Fetch("blocked", detail="access denied page", status=status, secs=time.time() - t0)
                if state == "R" and not (nav_failed and expect and expect not in cur):
                    raw = await page.evaluate(js, arg)
                    if self.b.get("stop_loading_after_config", True):
                        try:
                            await page.evaluate("window.stop()")
                        except Exception:
                            pass
                    if not raw:
                        return Fetch("error", detail="config present but no data", status=status,
                                     secs=time.time() - t0)
                    return Fetch("ok", data=raw, status=status or 200, secs=time.time() - t0)
            except Exception:
                pass                                    # page mid-navigation; poll again
            await asyncio.sleep(0.25)
        return Fetch("error", detail=nav_err or "timeout waiting for config", status=status,
                     secs=time.time() - t0)

    async def api_headers(self, tab=0) -> dict:
        h = dict(self.cfg["listing"].get("api_headers") or {})
        h["nord-context-id"] = str(uuid.uuid4())
        h["ads-nord-context-id"] = str(uuid.uuid4())
        h["eventcustomer"] = json.dumps({"idType": "SHOPPER_ID", "id": uuid.uuid4().hex})
        exp = None
        try:
            ck = await self.pages[tab % len(self.pages)].evaluate("document.cookie") or ""
            for part in ck.split(";"):
                if "ExperimentId=" in part:
                    exp = part.split("ExperimentId=")[1].strip()
        except Exception:
            pass
        h["experimentid"] = exp or str(uuid.uuid4())
        return {k: v for k, v in h.items() if v}

    # listing API (listing.source = "api"): Next-click mint + in-page XHR --------------
    def listing_api_cfg(self) -> dict:
        return self.cfg["listing"].get("api") or {}

    async def _find_page_next(self, page):
        """The pagination Next control (not the colour-swatch carousel arrows inside product tiles)."""
        cands = (page.get_by_role("link", name=re.compile(r"^\s*next", re.I)),
                 page.get_by_role("button", name=re.compile(r"^\s*next", re.I)),
                 page.locator('[aria-label*="next" i]'),
                 page.locator('a:has-text("Next"), button:has-text("Next")'))
        for loc in cands:
            try:
                n = await loc.count()
            except Exception:
                continue
            for i in range(n):
                el = loc.nth(i)
                try:
                    if not await el.is_visible():
                        continue
                    if await el.evaluate("e => !!e.closest('article')"):
                        continue
                    return el
                except Exception:
                    continue
        return None

    async def _mint_listing(self, page) -> str:
        """Open the mint listing page, scroll down, click the pagination Next: the page then fires its own
        /sbn/v2/category/query/browse/... call. Its request headers (this listing's nord-api-key, context id, ...)
        are kept for our XHRs. Returns '' on success, else a reason."""
        A, site = self.listing_api_cfg(), self.cfg["site"]
        u = (A.get("mint_url") or "/browse/women?breadcrumb=Home%2FWomen&origin=topnav").strip()
        url = site["url"] + u if u.startswith("/") else u
        seen = []

        def on_req(r):
            if r.resource_type in ("xhr", "fetch") and LISTING_API_MARK in r.url:
                seen.append(r)

        page.on("request", on_req)
        try:
            err = await self._goto_retry(page, url)
            if err:
                return f"navigation: {err}"
            tiles, t_end, found = page.locator(HEART_TILE_SEL), time.time() + float(A.get("mint_timeout_seconds", 40)), False
            while time.time() < t_end:
                try:
                    title = (await page.title()) or ""
                except Exception:
                    title = ""
                if self._is_block(page.url, title):
                    return f"block page ({title!r})"
                if await tiles.count() >= 3:
                    found = True
                    break
                await asyncio.sleep(0.5)
            if not found:
                return "no product tiles"
            nxt = None
            for _ in range(25):
                await page.mouse.wheel(0, 2500)
                await asyncio.sleep(0.7)
                await page.keyboard.press("End")
                nxt = await self._find_page_next(page)
                if nxt:
                    break
            if not nxt:
                return "pagination Next button not found"
            try:
                await nxt.scroll_into_view_if_needed(timeout=5000)
                await asyncio.sleep(0.5)
                seen.clear()
                await nxt.click(timeout=8000)
            except Exception as e:
                return f"Next click failed: {str(e)[:60]}"
            for _ in range(40):
                if seen:
                    break
                await asyncio.sleep(0.5)
            if not seen:
                return "the Next click did not fire the listing API"
            try:
                raw = {k.lower(): v for k, v in (await seen[-1].all_headers()).items()}
            except Exception:
                raw = {k.lower(): v for k, v in seen[-1].headers.items()}
            hdrs = {k: v for k, v in raw.items() if k not in _SKIP_HDR and not k.startswith(("sec-", ":"))
                    and not _SENSOR_RE.match(k)}
            try:
                resp = await asyncio.wait_for(seen[-1].response(), 20)
                status = resp.status if resp else 0
            except Exception:
                status = 0
            if status != 200:
                return f"the page's own listing call got HTTP {status}"
            if not hdrs.get("nord-api-key"):
                return "the page's listing call carried no nord-api-key"
            first = not self.__dict__.get("_lst_hdrs")
            self._lst_hdrs = hdrs
            if first:
                k = hdrs["nord-api-key"]
                log_info(f"{self.tag}listing API headers captured from the page ({len(hdrs)}), nord-api-key {k[:4]}...{k[-3:]}")
            return ""
        finally:
            try:
                page.remove_listener("request", on_req)
            except Exception:
                pass

    async def _lst_prepare(self, tab, why="") -> str:
        """Make sure this browser has a minted listing session within its request budget.
        Order: budget used -> reload+Next (same browser/IP) up to max_reloads, then a fresh browser on the next proxy."""
        A, gen = self.listing_api_cfg(), self.generation
        st = self.__dict__.setdefault("_lst", {"gen": gen, "n": 0, "ready": False, "reloads": 0})
        if st["gen"] != gen:
            st.update(gen=gen, n=0, ready=False, reloads=0)
            self._lst_hdrs = None
        ra, mr = int(A.get("reload_after_requests", 22) or 0), int(A.get("max_reloads_per_session", 6) or 0)
        lock = self.__dict__.setdefault("_mint_lock", asyncio.Lock())
        need = (not st["ready"]) or (ra and st["n"] >= ra) or bool(why)
        if not need:
            return ""
        if st["ready"] and mr and st["reloads"] >= mr:
            log_info(f"{self.tag}listing session budget used ({st['n']} requests, {st['reloads']} reloads on "
                     f"{proxy_label(self.proxy)}) -> fresh browser on the next proxy")
            await self.restart(gen)
            await self._ready.wait()
            gen = self.generation
            st.update(gen=gen, n=0, ready=False, reloads=0)
            self._lst_hdrs = None
        page = self.pages[tab % len(self.pages)]
        async with lock:
            t0 = time.time()
            reason = await self._mint_guard(self._mint_listing(page), float(A.get("mint_total_timeout_seconds", 120)))
        if reason:
            st["ready"] = False
            return reason
        if st["ready"]:
            st["reloads"] += 1
            log_info(f"{self.tag}listing reload+Next ok ({why or str(st['n']) + ' requests'}, {time.time() - t0:.1f}s, "
                     f"reload #{st['reloads']} on {proxy_label(self.proxy)})")
        st["ready"], st["n"] = True, 1
        return ""

    async def _fetch_api_next(self, url, tab=0, keep_facets=False) -> Fetch:
        t0 = time.time()
        A = self.listing_api_cfg()
        reason = await self._lst_prepare(tab)
        if reason:
            log_warn(f"{self.tag}listing mint failed ({reason}) on {proxy_label(self.proxy)}")
            return Fetch("blocked", detail=f"mint: {reason}", secs=time.time() - t0)
        st = self._lst
        page = self.pages[tab % len(self.pages)]
        arg = {"url": url, "keepFacets": keep_facets, "timeout": 40000}
        out = None
        for attempt in (1, 2):
            hdrs = dict(self.__dict__.get("_lst_hdrs") or await self.api_headers(tab))
            hdrs["nord-request-id"] = str(uuid.uuid4())
            arg["headers"] = hdrs
            try:
                out = await asyncio.wait_for(page.evaluate(LISTING_XHR_JS, arg),
                                             timeout=float(self.b["config_timeout_seconds"]) + 30)
            except Exception as e:
                return Fetch("error", detail=f"{type(e).__name__}: {str(e)[:80]}", secs=time.time() - t0)
            st["n"] += 1
            if out.get("s") in (403, 429) and attempt == 1 and A.get("reload_on_block", True) \
                    and not (int(A.get("max_reloads_per_session", 6) or 0) and st["reloads"] >= int(A.get("max_reloads_per_session", 6))):
                reason = await self._lst_prepare(tab, why=f"HTTP {out.get('s')}")
                if reason:
                    log_warn(f"{self.tag}listing reload+Next failed ({reason})")
                    return Fetch("blocked", detail=f"HTTP {out.get('s')} + mint: {reason}", status=out.get("s"),
                                 secs=time.time() - t0)
                continue
            break
        s = out.get("s", 0)
        if s == 200 and not out.get("bad"):
            return Fetch("ok", data=out["t"], status=200, secs=time.time() - t0)
        if s == 401:
            log_warn(f"{self.tag}listing API HTTP 401: nord headers rejected - not a used-up session, browser kept")
            return Fetch("error", detail="HTTP 401 nord headers rejected", status=s, secs=time.time() - t0)
        if s in (403, 429):
            return Fetch("blocked", detail=f"HTTP {s}", status=s, secs=time.time() - t0)
        if s == 404:
            return Fetch("gone", status=s, secs=time.time() - t0)
        return Fetch("error", detail=f"HTTP {s} {str(out.get('t'))[:60]!r}", status=s, secs=time.time() - t0)

    async def fetch_api(self, url, tab=0, keep_facets=False) -> Fetch:
        """sbn/v2 browse API from inside a nordstrom.com page (same origin, real cookies).
        listing.api.mint_mode "next" (default): Next-click mint + XHR with the page's headers; "plain": old fetch()."""
        await self._ready.wait()
        if (self.listing_api_cfg().get("mint_mode") or "next") == "next":
            return await self._fetch_api_next(url, tab, keep_facets)
        t0 = time.time()
        page = self.pages[tab % len(self.pages)]
        site = self.cfg["site"]
        try:
            if urlparse(page.url).netloc != urlparse(site["url"]).netloc or self._is_block(page.url):
                await page.goto(site["url"] + site.get("warmup_path", "/"),
                                wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
            arg = {"url": url, "headers": await self.api_headers(tab), "keepFacets": keep_facets}
            out = await asyncio.wait_for(page.evaluate(API_FETCH_JS, arg),
                                         timeout=float(self.b["config_timeout_seconds"]) + 30)
        except Exception as e:
            return Fetch("error", detail=f"{type(e).__name__}: {str(e)[:80]}", secs=time.time() - t0)
        s = out.get("s", 0)
        if s == 200 and not out.get("bad"):
            return Fetch("ok", data=out["t"], status=200, secs=time.time() - t0)
        if s in (403, 429):
            return Fetch("blocked", detail=f"HTTP {s}", status=s, secs=time.time() - t0)
        if s == 404:
            return Fetch("gone", status=s, secs=time.time() - t0)
        return Fetch("error", detail=f"HTTP {s} {str(out.get('t'))[:60]!r}", status=s, secs=time.time() - t0)

    # PDP details API ------------------------------------------------------
    def api_cfg(self) -> dict:
        return self.cfg["enrich"].get("api") or {}

    def pdp_api_headers(self) -> dict:
        A = self.api_cfg()
        h = {"accept": "*/*", "content-type": "application/json",
             "nord-channel-brand": "NORDSTROM", "nord-client-id": "APP01196",
             "nord-country-code": "US", "nord-customer-experience": "DESKTOP_WEB"}
        h.update({k: v for k, v in (A.get("headers") or {}).items() if v})   # an unset ${ENV} must not blank a default
        if A.get("use_page_headers", True) and self.__dict__.get("_page_hdrs"):
            h.update(self._page_hdrs)                         # headers of the page's own call win over config
        h["nord-request-id"] = str(uuid.uuid4())
        return {k: v for k, v in h.items() if v}

    def reopen_target(self) -> str:
        """enrich.api.reopen_url: absolute URL, a /path on the site, or '' for the warm-up (home) page."""
        site, u = self.cfg["site"], (self.api_cfg().get("reopen_url") or "").strip()
        if not u:
            return site["url"] + site.get("warmup_path", "/")
        return site["url"] + u if u.startswith("/") else u

    async def reopen_page(self, tab=0) -> bool:
        """Reload the reopen URL in this tab (fresh page state for the site's scripts). False if it landed on a block page."""
        page = self.pages[tab % len(self.pages)]
        A = self.api_cfg()
        await page.goto(self.reopen_target(), wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(float(A.get("reopen_wait_seconds", 2)))
        try:
            title = (await page.title()) or ""
        except Exception:
            title = ""
        return not self._is_block(page.url, title)

    async def _pdp_xhr(self, page, url) -> dict:
        A = self.api_cfg()
        t = float(A.get("timeout_seconds", 25))
        arg = {"url": url, "headers": self.pdp_api_headers(), "timeout": int(t * 1000)}
        return await asyncio.wait_for(page.evaluate(PDP_API_JS, arg), timeout=t + 10)

    async def _close_quickview(self, page):
        """Close the Quick View modal: Escape, then its close button, then a click on the backdrop corner."""
        try:
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.4)
            dlg = page.locator('[role="dialog"]')
            if await dlg.count() and await dlg.first.is_visible():
                close = page.locator('[role="dialog"] button[aria-label*="close" i], [role="dialog"] button:has-text("Close")')
                if await close.count():
                    await close.first.click(timeout=2000)
                else:
                    await page.mouse.click(8, 8)
                await asyncio.sleep(0.4)
        except Exception:
            pass

    async def _mint_session(self, page) -> str:
        """Start of every browser session: open the sale listing, hover a tile and click 'Quick View' so the page
        itself makes a /pdp/product-display/ call. Only after a real interaction like that did the site's
        bot-manager script attach its headers to our own in-page requests. Returns '' on success, else a reason."""
        A, site = self.api_cfg(), self.cfg["site"]
        u = (A.get("mint_url") or "/browse/sale").strip()
        url = site["url"] + u if u.startswith("/") else u
        seen = []

        def on_req(r):
            if r.resource_type in ("xhr", "fetch") and "/pdp/product-display/" in r.url:
                seen.append(r)

        page.on("request", on_req)
        try:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            except Exception as e:
                return f"navigation: {str(e)[:60]}"
            tiles = page.locator('li:has(a[href^="/s/"])')
            t_end, found = time.time() + float(A.get("mint_timeout_seconds", 40)), False
            while time.time() < t_end:
                try:
                    title = (await page.title()) or ""
                except Exception:
                    title = ""
                if self._is_block(page.url, title):
                    return f"block page ({title!r})"
                if await tiles.count() >= 3:
                    found = True
                    break
                await asyncio.sleep(0.5)
            if not found:
                return "no product tiles"
            await page.mouse.wheel(0, 700)
            await asyncio.sleep(1.2)
            n = await tiles.count()
            for i in range(min(int(A.get("mint_tiles", 6)), n)):
                tile = tiles.nth(i)
                try:
                    await tile.scroll_into_view_if_needed(timeout=5000)
                    box = await tile.locator("img").first.bounding_box()
                    if box:                                   # real mouse move so :hover / mouseenter fire
                        await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=8)
                    else:
                        await tile.hover(timeout=5000)
                    await asyncio.sleep(0.5)
                    btn = tile.get_by_role("button", name=QV_RE)
                    if await btn.count() == 0:
                        btn = tile.get_by_text(QV_RE)
                    if await btn.count() == 0:
                        continue
                    seen.clear()
                    await btn.first.click(timeout=5000)
                    for _ in range(24):                       # ~12 s for the page's own call
                        if seen:
                            try:
                                resp = await asyncio.wait_for(seen[0].response(), 10)
                                status = resp.status if resp else 0
                            except Exception:
                                status = 0
                            await asyncio.sleep(0.5)
                            await self._close_quickview(page)
                            return "" if status == 200 else f"the page's own Quick View call got HTTP {status}"
                        await asyncio.sleep(0.5)
                    await self._close_quickview(page)
                except Exception:
                    await self._close_quickview(page)
            return "Quick View did not fire the PDP call"
        finally:
            try:
                page.remove_listener("request", on_req)
            except Exception:
                pass

    async def _goto_retry(self, page, url, reload=False, timeout=45000) -> str:
        """goto / reload with up to 3 tries on the transient 'net::ERR_ABORTED / frame was detached' errors.
        Returns '' on success, else the error text (no retry once the browser itself is gone)."""
        last = ""
        for attempt in range(3):
            try:
                if reload and attempt == 0:
                    await page.reload(wait_until="domcontentloaded", timeout=timeout)
                else:
                    await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                return ""
            except Exception as e:
                last, low = f"{type(e).__name__}: {str(e)[:60]}", str(e).lower()
                if "target" in low and "closed" in low:
                    return last
                if not ("err_aborted" in low or "detached" in low or "interrupted" in low):
                    return last
                await asyncio.sleep(1.5 + attempt * 1.5)
        return last

    async def _mint_guard(self, coro, secs=None) -> str:
        """Run a mint coroutine with an overall time limit; any failure becomes a reason string ('' = ok)."""
        secs = float(secs or self.api_cfg().get("mint_total_timeout_seconds", 120))
        try:
            return await asyncio.wait_for(coro, secs)
        except asyncio.TimeoutError:
            return f"mint timed out after {secs:.0f}s"
        except Exception as e:
            return f"{type(e).__name__}: {str(e)[:70]}"

    async def _mint_heart(self, page, reload=False) -> str:
        """Open (or reload) a listing page and click the wishlist heart on a tile. The page then makes its own
        /pdp/product-display/ call, which is what switches on the bot-manager script for our later in-page XHRs.
        Measured by hand: after ~22 requests the session starts answering 403; reloading the page and clicking a
        heart again restores it (first call slow, then normal). Returns '' on success, else a reason."""
        A, site = self.api_cfg(), self.cfg["site"]
        u = (A.get("heart_url") or "/browse/women/clothing/petite-size"
             "?breadcrumb=Home%2FWomen%2FPetite-Size%20Clothing&origin=topnav").strip()
        url = site["url"] + u if u.startswith("/") else u
        seen = []

        def on_req(r):
            if r.resource_type in ("xhr", "fetch") and "/pdp/product-display/" in r.url:
                seen.append(r)

        page.on("request", on_req)
        try:
            same = urlparse(page.url).path == urlparse(url).path
            err = await self._goto_retry(page, url, reload=reload and same)
            if err:
                return f"navigation: {err}"
            hearts, tiles = page.locator(HEART_SEL), page.locator(HEART_TILE_SEL)
            t_end, found = time.time() + float(A.get("mint_timeout_seconds", 40)), False
            while time.time() < t_end:
                try:
                    title = (await page.title()) or ""
                except Exception:
                    title = ""
                if self._is_block(page.url, title):
                    return f"block page ({title!r})"
                if await hearts.count() >= 3 or await tiles.count() >= 3:
                    found = True
                    break
                await asyncio.sleep(0.5)
            if not found:
                return "no product tiles / heart buttons"
            await asyncio.sleep(float(A.get("heart_settle_seconds", 1.5)))
            for attempt in range(int(A.get("heart_tries", 3))):
                hearts = page.locator(HEART_SEL)           # a clicked heart no longer says "Add to Wishlist"
                n = await hearts.count()
                if n == 0:
                    break
                el = hearts.nth(min(attempt, n - 1))
                try:
                    await el.scroll_into_view_if_needed(timeout=5000)
                    await asyncio.sleep(0.4)
                    seen.clear()
                    await el.click(timeout=8000)
                except Exception:
                    await self._close_quickview(page)
                    continue
                for _ in range(40):                        # ~20 s: the first call after a reload is slow
                    if seen:
                        await self._remember_page_headers(seen[0])
                        try:
                            resp = await asyncio.wait_for(seen[0].response(), 15)
                            status = resp.status if resp else 0
                        except Exception:
                            status = 0
                        await asyncio.sleep(0.5)
                        await self._close_quickview(page)
                        return "" if status == 200 else f"the page's own heart call got HTTP {status}"
                    await asyncio.sleep(0.5)
                await self._close_quickview(page)
            return "the heart click did not fire the PDP call"
        finally:
            try:
                page.remove_listener("request", on_req)
            except Exception:
                pass

    async def _remember_page_headers(self, req):
        """Keep the request headers of the page's OWN PDP call (nord-api-key, nord-context-id, ...). Our in-page
        XHRs reuse them, so the PDP key no longer has to be right in .env (a wrong/listing key gives HTTP 401)."""
        try:
            raw = {k.lower(): v for k, v in (await req.all_headers()).items()}
        except Exception:
            raw = {k.lower(): v for k, v in req.headers.items()}
        hdrs = {k: v for k, v in raw.items() if k not in _SKIP_HDR and not k.startswith(("sec-", ":"))
                and not _SENSOR_RE.match(k)}
        if not hdrs.get("nord-api-key"):
            return
        old = self.__dict__.get("_page_hdrs") or {}
        self._page_hdrs = hdrs
        cfgkey = (self.api_cfg().get("headers") or {}).get("nord-api-key") or ""
        k = hdrs["nord-api-key"]
        if not old and cfgkey and cfgkey != k:
            log_warn(f"{self.tag}the page's PDP nord-api-key ({k[:4]}...{k[-3:]}) differs from NORD_API_KEY_PDP in .env "
                     f"({cfgkey[:4]}...{cfgkey[-3:]}); using the page's key")
        elif not old:
            log_info(f"{self.tag}using the page's own request headers ({len(hdrs)}), nord-api-key {k[:4]}...{k[-3:]}")

    async def _cycle(self, tab, bs, why) -> bool:
        """Reload the listing page and click a heart again (fresh request budget on the SAME browser and IP).
        Resets the request counter; False if it failed (caller then restarts the browser on the next proxy)."""
        page = self.pages[tab % len(self.pages)]
        lock = self.__dict__.setdefault("_mint_lock", asyncio.Lock())
        async with lock:
            t0 = time.time()
            reason = await self._mint_guard(self._mint_heart(page, reload=True))
        if reason:
            log_warn(f"{self.tag}reload+heart failed ({reason}) on {proxy_label(self.proxy)}")
            return False
        bs["n"] = 1                                           # the page's own call counts
        bs["reloads"] = bs.get("reloads", 0) + 1
        log_info(f"{self.tag}reload+heart ok ({why}, {time.time() - t0:.1f}s, reload #{bs['reloads']} "
                 f"on {proxy_label(self.proxy)})")
        return True

    async def _interact(self, page, secs):
        """A few mouse moves and scrolls so the site's bot-manager script has real input events to score."""
        try:
            vp = page.viewport_size or {"width": 1280, "height": 720}
            w, h = int(vp["width"]), int(vp["height"])
            x, y, t_end = random.randint(w // 4, 3 * w // 4), random.randint(h // 4, 3 * h // 4), time.time() + secs
            while time.time() < t_end:
                x = max(5, min(w - 5, x + random.randint(-220, 220)))
                y = max(5, min(h - 5, y + random.randint(-160, 160)))
                await page.mouse.move(x, y, steps=random.randint(6, 14))
                if random.random() < 0.4:
                    await page.mouse.wheel(0, random.randint(150, 500))
                await asyncio.sleep(random.uniform(0.15, 0.45))
        except Exception:
            pass

    async def fetch_pdp_api(self, pid, tab=0) -> Fetch:
        """GET /pdp/product-display/<pid> from inside the page.
        Measured: a browser session (IP + cookies) is cut off after about 28 requests of any kind, and neither a
        reload nor a click on another tile gives it a fresh budget. So after enrich.api.requests_per_session
        requests the browser is closed BEFORE the block and reopened on the next proxy (a clean exit keeps the
        IP's reputation; a burst of 403s does not). A 401/403/429 is not retried on the same session."""
        await self._ready.wait()
        gen, t0 = self.generation, time.time()
        A = self.api_cfg()
        budget = int(A.get("requests_per_session", 0) or 0)
        bs = self.__dict__.setdefault("_api_budget", {"gen": gen, "n": 0, "warm": False})
        if bs["gen"] != gen:
            bs.update(gen=gen, n=0, warm=False, reloads=0)
        heart_mode = (A.get("mint_mode") or "quickview") == "heart"
        reload_after = int(A.get("reload_after_requests", 0) or 0) if heart_mode else 0
        max_reloads = int(A.get("max_reloads_per_session", 0) or 0)
        reload_ok = bool(reload_after) and not (max_reloads and bs.get("reloads", 0) >= max_reloads)
        if reload_after and reload_ok and bs["warm"] and bs["n"] >= reload_after:
            if not await self._cycle(tab, bs, f"{bs['n']} requests"):
                await self.restart(gen)
                await self._ready.wait()
                gen = self.generation
                bs.update(gen=gen, n=0, warm=False, reloads=0)
        elif (budget and bs["n"] >= budget) or (reload_after and not reload_ok and bs["n"] >= reload_after):
            log_info(f"{self.tag}session budget used ({bs['n']} requests, {bs.get('reloads', 0)} reloads on "
                     f"{proxy_label(self.proxy)}) -> fresh browser on the next proxy")
            await self.restart(gen)
            await self._ready.wait()
            gen = self.generation
            bs.update(gen=gen, n=0, warm=False, reloads=0)
        if not bs["warm"]:
            wpage = self.pages[tab % len(self.pages)] if self.pages else None
            if wpage and A.get("mint_click", True):
                why = await self._mint_guard(self._mint_heart(wpage) if heart_mode else self._mint_session(wpage))
                if why:
                    log_warn(f"{self.tag}session mint failed ({why}) on {proxy_label(self.proxy)}")
                    if gen != self.generation or "closed" in why.lower():
                        return Fetch("error", detail="browser restarted during fetch", secs=time.time() - t0)
                    blockish = why.startswith(("block page", "the page's own", "no product tiles"))
                    return Fetch("blocked" if blockish else "error", detail=f"mint failed: {why}", secs=time.time() - t0)
                bs["warm"] = True
                bs["n"] += 1                                  # the page's own call counts against the budget
            else:
                bs["warm"] = True
                secs = float(A.get("warm_interaction_seconds", 0) or 0)
                if secs > 0 and wpage:
                    await self._interact(wpage, secs)
        state = self.__dict__.setdefault("_api_state", {})
        st = state.setdefault(tab, {"gen": gen, "n": 0})
        if st["gen"] != gen:                                   # new browser: warm-up page is already fresh
            st["gen"], st["n"] = gen, 0
        per_page = int(A.get("requests_per_page", 20) or 0)
        site = self.cfg["site"]
        url = f"{site['url']}/pdp/product-display/{pid}"
        try:
            page = self.pages[tab % len(self.pages)]
            if (urlparse(page.url).netloc != urlparse(site["url"]).netloc or self._is_block(page.url)
                    or (per_page and st["n"] >= per_page)):
                await self.reopen_page(tab)
                st["n"] = 0
            bs["n"] += 1
            out = await self._pdp_xhr(page, url)
            st["n"] += 1
            if out.get("s") in (403, 429) and heart_mode and A.get("reload_on_block", True) \
                    and not (max_reloads and bs.get("reloads", 0) >= max_reloads):
                if await self._cycle(tab, bs, f"HTTP {out.get('s')}"):
                    bs["n"] += 1
                    out = await self._pdp_xhr(page, url)
                    st["n"] += 1
            elif out.get("s") in (401, 403, 429) and A.get("reopen_on_block", False):
                ok = await self.reopen_page(tab)
                st["n"] = 0
                if ok:
                    bs["n"] += 1
                    out = await self._pdp_xhr(page, url)
                    st["n"] += 1
        except Exception as e:
            if gen != self.generation:
                return Fetch("error", detail="browser restarted during fetch", secs=time.time() - t0)
            return Fetch("error", detail=f"{type(e).__name__}: {str(e)[:80]}", secs=time.time() - t0)
        s, body = out.get("s", 0), out.get("t") or ""
        if s == 200 and not body.lstrip().startswith("<"):
            return Fetch("ok", data=body, status=200, secs=time.time() - t0)
        if s == 401 and heart_mode:
            log_warn(f"{self.tag}PDP API HTTP 401: the nord-* headers were rejected (wrong nord-api-key?) - this is "
                     f"not a used-up session, so the browser is kept. Body: {body[:80]!r}")
            return Fetch("error", detail="HTTP 401 nord headers rejected", status=s, secs=time.time() - t0)
        if s in (401, 403, 429) or (s == 200 and "access denied" in body[:600].lower()):
            log_warn(f"{self.tag}PDP API HTTP {s} after {bs['n']} requests in this session ({proxy_label(self.proxy)})"
                     f" - if this is below requests_per_session, lower it")
            return Fetch("blocked", detail=f"HTTP {s}", status=s, secs=time.time() - t0)
        if s == 404:
            return Fetch("gone", status=s, secs=time.time() - t0)
        return Fetch("error", detail=f"HTTP {s} {body[:60]!r}", status=s, secs=time.time() - t0)

    # pacing / block handling ---------------------------------------------
    async def report(self, res: Fetch, delay=None, block_limit=None):
        """Call after EVERY fetch. Success -> polite delay. Block -> after retry.restart_after_blocks
        in a row (default 2) the browser is closed and reopened on the next proxy IMMEDIATELY (no long
        backoff first). Other errors use retry.restart_after_failures (default 5)."""
        if res.kind in ("ok", "gone"):
            self.consec, self.clean = 0, self.clean + 1
            self.pool.ok(self.proxy)
            if self.clean % 20 == 0:
                self.slow = max(1.0, self.slow * 0.85)
            d = float(self.cfg["retry"]["delay_seconds"]) if delay is None else float(delay)
            await asyncio.sleep(random.uniform(d * 0.8, d * 1.3) * self.slow)
            return
        if res.detail == "browser restarted during fetch":
            return                                   # another tab restarted it: not a new failure
        self.consec += 1
        self.clean = 0
        is_block = res.kind == "blocked"
        if is_block:
            self.slow = min(self.slow * 1.6, 6.0)
        limit = int(block_limit or self.r.get("restart_after_blocks", 2)) if is_block \
            else int(self.r.get("restart_after_failures", 5))
        gen = self.generation
        if self.consec >= limit:
            log_warn(f"{self.tag}{res.kind} ({res.detail or res.status}) x{self.consec} "
                     f"[{proxy_label(self.proxy)}] -> closing browser, reopening on next proxy")
            await self.restart(gen)
            return
        wait = float(self.r.get("block_pause_seconds", 2)) + random.uniform(0, 2)
        log_warn(f"{self.tag}{res.kind} ({res.detail or res.status}) x{self.consec} "
                 f"[{proxy_label(self.proxy)}] -> pausing {wait:.0f}s")
        await asyncio.sleep(wait)


SESSION_CLASS = CloakSession          # tests swap in a fake

def make_session(cfg, tag, pool):
    return SESSION_CLASS(cfg, tag, pool)


# ── work queue + crawler ──────────────────────────────────────────────────
class TaskQueue:
    """FIFO of tasks. get() returns None only when the queue is empty AND nothing is in flight,
    so handlers can add follow-up tasks (retries, next pages) while others are still running."""

    def __init__(self, items=()):
        self.q, self.inflight, self.completed = deque(items), 0, 0

    def put(self, task, front=False):
        (self.q.appendleft if front else self.q.append)(task)

    def __len__(self):
        return len(self.q)

    def drain(self):
        """Drop everything that has not started yet (clean stop: in-flight tasks finish)."""
        n = len(self.q)
        self.q.clear()
        return n

    async def get(self):
        while True:
            if self.q:
                self.inflight += 1
                return self.q.popleft()
            if self.inflight == 0:
                return None
            await asyncio.sleep(0.2)

    def done(self):
        self.inflight -= 1
        self.completed += 1


def ensure_display(cfg):
    """Headed Chrome on a Linux server needs a display: start Xvfb if there is none."""
    if platform.system() != "Linux" or os.environ.get("DISPLAY") or cfg["browser"].get("headless"):
        return None
    try:
        proc = subprocess.Popen(["Xvfb", ":99", "-screen", "0", "1920x1080x24", "-ac"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        log_warn("No DISPLAY and Xvfb is not installed: `sudo apt install xvfb`, or set browser.headless=true")
        return None
    time.sleep(1.5)
    os.environ["DISPLAY"] = ":99"
    return proc


class Crawler:
    """    async with Crawler(cfg) as crawler:
               first = crawler.sessions[0]          # direct use (planning requests)
               await crawler.run(queue, handler)    # parallel: browsers x tabs
    handler(task, session, tab) fetches + processes one task and may queue.put() follow-ups."""

    def __init__(self, cfg, workers=None, single_tab=False):
        self.cfg = cfg
        b = cfg["browser"]
        self.n = max(1, int(workers or b["workers"]))
        self.tabs = max(1, int(b["tabs_per_browser"]))
        if single_tab and self.tabs > 1:
            # the API modes mint (heart / Next click) and reload one page; several tabs fight over the same
            # navigations ("ERR_ABORTED ... frame was detached") and over the shared request budget
            log_warn(f"API mode uses ONE tab per browser (browser.tabs_per_browser={self.tabs} ignored): "
                     f"raise browser.workers for parallelism instead (each worker = own browser + own proxy)")
            self.tabs = 1
        self.pool = ProxyPool(proxy_list(), cooldown=float(b.get("proxy_cooldown_seconds", 120)))
        start = int(os.getenv("PROXY_START", "") or b.get("proxy_start_index", 0) or 0)
        order = (os.getenv("PROXY_ORDER", "") or b.get("proxy_order", "random")).strip().lower()
        if start and len(self.pool):                       # explicit: begin at this entry of PROXY_LIST (0-based)
            k = start % len(self.pool)
            self.pool.items = self.pool.items[k:] + self.pool.items[:k]
            self.order_note = f"in order from entry {k}"
        elif order == "random" and len(self.pool):         # default: a different order on every run
            random.shuffle(self.pool.items)
            self.order_note = "in random order"
        else:
            self.order_note = "in list order"
        self.sessions, self._xvfb = [], None

    async def __aenter__(self):
        self._xvfb = ensure_display(self.cfg)
        if len(self.pool) and len(self.pool) < self.n:
            log_warn(f"Only {len(self.pool)} proxies for {self.n} browsers - some browsers will share a proxy")
        if not len(self.pool):
            log_info("No PROXY_LIST set: connecting directly")
        else:
            log_info(f"{len(self.pool)} proxies loaded, rotating {self.order_note} "
                     f"(first: {proxy_label(self.pool.items[0])})")
            rot = [proxy_label(p) for p in self.pool.items if "rotate" in proxy_label(p).lower()]
            if rot:
                log_warn(f"{len(rot)} proxy login(s) look like ROTATING exits ({rot[0]}...): the site's cookies are "
                         "bound to one IP, so sessions on them are blocked. Use numbered (sticky) logins only.")
            if len(self.pool) == 1:
                log_warn("PROXY_LIST holds only ONE proxy - nothing to rotate to. Separate entries with commas.")
        self.sessions = [make_session(self.cfg, f"b{i + 1}", self.pool) for i in range(self.n)]
        for sess in self.sessions:
            sess.n_tabs = self.tabs
        stagger = float(self.cfg["browser"].get("start_stagger_seconds", 8))

        async def boot(i, s):
            await asyncio.sleep(i * stagger)
            await s.start()

        await asyncio.gather(*(boot(i, s) for i, s in enumerate(self.sessions)))
        return self

    async def __aexit__(self, *exc):
        await asyncio.gather(*(s.stop() for s in self.sessions), return_exceptions=True)
        if self._xvfb:
            self._xvfb.terminate()

    async def run(self, queue: TaskQueue, handler):
        async def worker(sess, tab):
            while True:
                task = await queue.get()
                if task is None:
                    return
                if getattr(sess, "dead", False):
                    queue.put(task, front=True)             # hand the task back to the browsers that still work
                    queue.done()
                    return
                try:
                    await handler(task, sess, tab)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log_err(f"handler crashed on {str(task)[:100]}: {traceback.format_exc(limit=3)}")
                finally:
                    queue.done()

        async def safe_restart(sess):
            try:
                await sess.restart()
            except Exception as e:
                log_err(f"{sess.tag}forced restart failed: {str(e)[:100]}")

        async def watchdog():
            stall = float(self.cfg["browser"].get("stall_restart_seconds", 300) or 0)
            if stall <= 0:
                return
            seen, since = -1, time.time()
            while True:
                await asyncio.sleep(min(15.0, max(1.0, stall / 4)))
                if queue.completed != seen or not (queue.q or queue.inflight):
                    seen, since = queue.completed, time.time()
                    continue
                if time.time() - since < stall:
                    continue
                since = time.time()
                for sess in self.sessions:
                    if sess._ready.is_set() and not getattr(sess, "dead", False):
                        log_warn(f"{sess.tag}no task finished for {stall:.0f}s -> forcing a browser restart "
                                 f"(a page call is probably hung)")
                        asyncio.create_task(safe_restart(sess))

        workers = [asyncio.create_task(worker(s, t)) for s in self.sessions for t in range(self.tabs)]
        dog = asyncio.create_task(watchdog())
        try:
            await asyncio.gather(*workers)
        finally:
            dog.cancel()
            for w in workers:
                w.cancel()
