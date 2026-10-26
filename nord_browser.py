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
import re
import subprocess
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass
from urllib.parse import unquote, urlparse

from nord_core import log_err, log_info, log_ok, log_warn, proxy_list


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

def state_js(title_markers):
    """'D' = block page title, 'R' = config present, '' = keep waiting."""
    markers = json.dumps([m.lower() for m in title_markers])
    return ("(() => { const t = (document.title || '').toLowerCase(); "
            f"if ({markers}.some(m => t.includes(m))) return 'D'; "
            "return typeof window.__INITIAL_CONFIG__ !== 'undefined' ? 'R' : ''; })()")


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


def load_proxies():
    """Proxy dicts built from nord_core.proxy_list(). Any entry that still holds several proxies
    (comma / semicolon / whitespace / newline separated) is split, duplicates are dropped."""
    urls, seen = [], set()
    for p in proxy_list() or []:
        raw = p.get("url") if isinstance(p, dict) else str(p)
        for part in re.split(r"[,;\s]+", (raw or "").strip()):
            if part and part not in seen:
                seen.add(part)
                urls.append(part)
    out = []
    for u in urls:
        full = u if "://" in u else f"http://{u}"
        pr = urlparse(full)
        if not pr.hostname:
            continue
        d = {"url": full,
             "server": f"{pr.scheme}://{pr.hostname}" + (f":{pr.port}" if pr.port else ""),
             "label": f"{pr.hostname}:{pr.port or ''}"}
        if pr.username:
            d["username"] = unquote(pr.username)
        if pr.password:
            d["password"] = unquote(pr.password)
        out.append(d)
    return out


class RotatingPool:
    """Round-robin proxy pool. acquire() always moves on to the NEXT proxy that is not cooling down and
    not held by another browser; fail() puts a proxy in cooldown (browser.proxy_cooldown_seconds).
    If every proxy is cooling down, the one that cools down soonest is used (never blocks)."""

    def __init__(self, proxies, cooldown=120.0):
        self.items, self.cooldown, self.ptr = list(proxies), float(cooldown), 0
        self.cool_until, self.in_use = {}, {}

    def __len__(self):
        return len(self.items)

    def acquire(self, tag=None):
        if not self.items:
            return None
        now, n = time.time(), len(self.items)
        order = [self.items[(self.ptr + k) % n] for k in range(n)]
        ready = [p for p in order if self.cool_until.get(p["url"], 0) <= now and not self.in_use.get(p["url"])]
        if ready:
            pick = ready[0]
        else:
            free = [p for p in order if not self.in_use.get(p["url"])] or order
            pick = min(free, key=lambda p: self.cool_until.get(p["url"], 0))
        self.ptr = (self.items.index(pick) + 1) % n
        self.in_use[pick["url"]] = self.in_use.get(pick["url"], 0) + 1
        return pick

    def release(self, proxy):
        if proxy and self.in_use.get(proxy["url"]):
            self.in_use[proxy["url"]] -= 1

    def fail(self, proxy):
        if proxy:
            self.cool_until[proxy["url"]] = time.time() + self.cooldown

    def ok(self, proxy):
        pass


# ── one browser (own proxy, several tabs) ─────────────────────────────────
class CloakSession:
    def __init__(self, cfg, tag, pool):
        self.cfg, self.b, self.r = cfg, cfg["browser"], cfg["retry"]
        self.tag, self.pool = f"[{tag}] ", pool
        self.url_markers = list(self.b.get("block_url_markers") or ["siteclosed"])
        self.title_markers = list(self.b.get("block_title_markers") or ["access denied"])
        self._state_js = state_js(self.title_markers)
        self._blocked_types = set(self.b.get("block_resources") or [])
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
                await self._launch(self.proxy)
                ok = await self._warmup()
            except Exception as e:
                log_warn(f"{self.tag}browser start failed: {type(e).__name__}: {str(e)[:120]}")
            if ok:
                return
            self.pool.fail(self.proxy)
            await self._close()
            if limit and n >= limit:
                raise RuntimeError(f"No accepted session after {n} attempts (browser.session_attempts={limit})")
            wait = float(self.b.get("session_retry_wait_seconds", 3)) + random.uniform(0, 2)
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
        if self._blocked_types:
            await self.ctx.route("**/*", self._route)
        n_tabs = max(1, int(self.b["tabs_per_browser"]))
        self.pages = [await self.ctx.new_page() for _ in range(n_tabs)]

    async def _route(self, route):
        if route.request.resource_type in self._blocked_types:
            await route.abort()
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
        log_ok(f"{self.tag}browser ready (proxy: {label}, {len(self.pages)} tab(s))")
        return True

    async def _close(self):
        for obj in (self.ctx, self.browser):
            try:
                if obj:
                    await obj.close()
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
            self._ready.clear()
            log_warn(f"{self.tag}restarting browser (leaving {proxy_label(self.proxy)})...")
            self.pool.fail(self.proxy)
            await self._close()
            await asyncio.sleep(random.uniform(3, 6))
            await self._boot_until_healthy()
            self.generation += 1
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

    async def fetch_api(self, url, tab=0, keep_facets=False) -> Fetch:
        """fetch() the sbn/v2 browse API from inside a nordstrom.com page (same origin, real cookies)."""
        await self._ready.wait()
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

    # pacing / block handling ---------------------------------------------
    async def report(self, res: Fetch):
        """Call after EVERY fetch. Success -> polite delay. Block -> after retry.restart_after_blocks
        in a row (default 2) the browser is closed and reopened on the next proxy IMMEDIATELY (no long
        backoff first). Other errors use retry.restart_after_failures (default 5)."""
        if res.kind in ("ok", "gone"):
            self.consec, self.clean = 0, self.clean + 1
            self.pool.ok(self.proxy)
            if self.clean % 20 == 0:
                self.slow = max(1.0, self.slow * 0.85)
            d = float(self.cfg["retry"]["delay_seconds"])
            await asyncio.sleep(random.uniform(d * 0.8, d * 1.3) * self.slow)
            return
        if res.detail == "browser restarted during fetch":
            return                                   # another tab restarted it: not a new failure
        self.consec += 1
        self.clean = 0
        is_block = res.kind == "blocked"
        if is_block:
            self.slow = min(self.slow * 1.6, 6.0)
        limit = int(self.r.get("restart_after_blocks", 2)) if is_block \
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
        self.q, self.inflight = deque(items), 0

    def put(self, task, front=False):
        (self.q.appendleft if front else self.q.append)(task)

    def __len__(self):
        return len(self.q)

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

    def __init__(self, cfg, workers=None):
        self.cfg = cfg
        b = cfg["browser"]
        self.n = max(1, int(workers or b["workers"]))
        self.tabs = max(1, int(b["tabs_per_browser"]))
        self.pool = RotatingPool(load_proxies(), cooldown=float(b.get("proxy_cooldown_seconds", 120)))
        self.sessions, self._xvfb = [], None

    async def __aenter__(self):
        self._xvfb = ensure_display(self.cfg)
        if len(self.pool) and len(self.pool) < self.n:
            log_warn(f"Only {len(self.pool)} proxies for {self.n} browsers - some browsers will share a proxy")
        if not len(self.pool):
            log_info("No PROXY_LIST set: connecting directly")
        else:
            log_info(f"{len(self.pool)} proxies loaded, rotating in order: "
                     f"{proxy_label(self.pool.items[0])} ... {proxy_label(self.pool.items[-1])}")
            if len(self.pool) == 1:
                log_warn("PROXY_LIST holds only ONE proxy - nothing to rotate to. Separate entries with commas.")
        self.sessions = [make_session(self.cfg, f"b{i + 1}", self.pool) for i in range(self.n)]
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
                try:
                    await handler(task, sess, tab)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log_err(f"handler crashed on {str(task)[:100]}: {traceback.format_exc(limit=3)}")
                finally:
                    queue.done()

        workers = [asyncio.create_task(worker(s, t)) for s in self.sessions for t in range(self.tabs)]
        try:
            await asyncio.gather(*workers)
        finally:
            for w in workers:
                w.cancel()
