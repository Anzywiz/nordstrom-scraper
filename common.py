"""Shared helpers for the Nordstrom scraper (E0005 schema, logging, browser mint)."""
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# ── logging ──
def _ts(): return datetime.now().strftime("%H:%M:%S")
def log_info(m): print(f"\033[36m[{_ts()}] i  {m}\033[0m", flush=True)
def log_ok(m):   print(f"\033[32m[{_ts()}] ok {m}\033[0m", flush=True)
def log_warn(m): print(f"\033[33m[{_ts()}] !  {m}\033[0m", flush=True)
def log_err(m):  print(f"\033[31m[{_ts()}] x  {m}\033[0m", file=sys.stderr, flush=True)

# ── DBQ E0005 columns, in schema order ──
COLUMNS = [
    "dbq_prd_type", "website_name", "competence_date", "country_code", "currency_code",
    "store_id", "store_name", "store_address", "store_zip", "store_url",
    "merchant_id", "merchant_name", "merchant_url", "merchant_image", "merchant_descr",
    "product_code", "additional_code_1", "additional_code_1_type",
    "additional_code_2", "additional_code_2_type", "additional_code_3", "additional_code_3_type",
    "size_info", "brand", "color_info", "material_info", "variant_info",
    "description", "product_title", "specifications", "delivery", "in_stock", "quantity",
    "category1", "category2", "category3", "category4", "category5",
    "category6", "category7", "category8", "category9", "category10",
    "full_price", "price", "ppu", "unit_type", "promotion_type", "promotion_end_date",
    "package_desc", "additional_tags", "additional_content",
    "itemurl", "main_image_url", "all_images",
    "contract_id", "seller_id", "delivery_id",
]

def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def load_config(path: Path) -> dict:
    cfg = json.loads(Path(path).read_text(encoding="utf-8"))
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

def build_base_row(cfg) -> dict:
    """Row with every E0005 column blank + fixed values + competence_date + delivery_id."""
    row = {c: "" for c in COLUMNS}
    row.update(cfg["fixed_values"])
    row["competence_date"] = today()
    row["delivery_id"] = f"{time.time():.7f}"
    return row

# ── proxies (PROXY_LIST = comma separated; ROTATING_PROXY kept as a fallback name) ──
import random

_proxy_idx = {"i": None}

_good_proxies = None   # set by check_proxies()

def all_proxies() -> list:
    raw = os.getenv("PROXY_LIST", "").strip() or os.getenv("ROTATING_PROXY", "").strip()
    return [p.strip() for p in raw.split(",") if p.strip()]

def proxy_list() -> list:
    return all_proxies() if _good_proxies is None else _good_proxies

def check_proxies(timeout: int = 15, verbose: bool = True) -> list:
    """Test every proxy once (exit IP + nordstrom.com reachability) and keep the working
    ones. Dead/slow proxies are what makes a browser hang, so drop them up front."""
    global _good_proxies
    pl = all_proxies()
    if not pl:
        return []
    from concurrent.futures import ThreadPoolExecutor
    from curl_cffi import requests as cr

    def test(p):
        px = {"http": proxy_for_curl(p), "https": proxy_for_curl(p)}
        t = time.time()
        try:
            r = cr.get("https://api.ipify.org?format=json", proxies=px, timeout=timeout,
                       impersonate="chrome")
            if r.status_code != 200:
                return p, False, f"ipify HTTP {r.status_code}", None, None
            ip = r.json().get("ip")
        except Exception as e:
            return p, False, str(e)[:70], None, None
        dt = time.time() - t
        try:
            n = cr.get("https://www.nordstrom.com/", proxies=px, timeout=timeout,
                       impersonate="chrome").status_code
        except Exception as e:
            n = f"err {str(e)[:30]}"
        return p, True, "", ip, (dt, n)

    log_info(f"Checking {len(pl)} proxies (timeout {timeout}s)...")
    good = []
    with ThreadPoolExecutor(max_workers=min(10, len(pl))) as ex:
        for p, ok, err, ip, extra in ex.map(test, pl):
            if ok:
                good.append(p)
                if verbose:
                    log_ok(f"proxy {proxy_label(p)} -> exit IP {ip} ({extra[0]:.1f}s, nordstrom.com HTTP {extra[1]})")
            else:
                log_warn(f"proxy {proxy_label(p)} DEAD/unusable: {err}")
    if not good:
        raise SystemExit("No working proxies in PROXY_LIST - fix them or clear PROXY_LIST to run direct.")
    _good_proxies = good
    log_info(f"{len(good)}/{len(pl)} proxies usable")
    return good

def next_proxy():
    """Round-robin through PROXY_LIST (random start). None if no proxies configured."""
    pl = proxy_list()
    if not pl:
        return None
    i = _proxy_idx["i"]
    _proxy_idx["i"] = random.randrange(len(pl)) if i is None else (i + 1) % len(pl)
    return pl[_proxy_idx["i"]]

def proxy_for_browser(p: str) -> str:
    return p.replace("http://", "").replace("https://", "") if p else p

def proxy_for_curl(p: str) -> str:
    return p if "://" in p else "http://" + p

def proxy_label(p) -> str:
    return p.split("@")[-1] if p else "direct"

# Chrome + authenticated proxies is a common cause of hangs (auth prompt that never
# resolves). So credentials never go to Chrome: a tiny local relay on 127.0.0.1 adds the
# Proxy-Authorization header and Chrome talks to the relay without auth.
import asyncio
import base64
import threading
from urllib.parse import urlsplit

_relays = {}

def ensure_relay(proxy: str) -> str:
    """-> 'host:port' for Chrome. Starts a local relay if the proxy has credentials."""
    u = urlsplit(proxy_for_curl(proxy))
    if not u.username:
        return proxy_for_browser(proxy)
    if u.scheme.startswith("socks"):
        log_warn("SOCKS proxies with auth are not supported by the relay; use an http proxy")
        return proxy_for_browser(proxy)
    if proxy in _relays:
        return _relays[proxy]
    from urllib.parse import unquote
    token = base64.b64encode(f"{unquote(u.username)}:{unquote(u.password or '')}".encode()).decode().encode()
    host, port = u.hostname, u.port or 80
    ready, holder = threading.Event(), {}

    async def pipe(r, w):
        try:
            while True:
                d = await r.read(65536)
                if not d:
                    break
                w.write(d)
                await w.drain()
        except Exception:
            pass
        finally:
            try:
                w.close()
            except Exception:
                pass

    async def handle(reader, writer):
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            ur, uw = await asyncio.wait_for(asyncio.open_connection(host, port), 20)
            first, rest = head.split(b"\r\n", 1)
            uw.write(first + b"\r\nProxy-Authorization: Basic " + token + b"\r\n" + rest)
            await uw.drain()
            await asyncio.gather(pipe(reader, uw), pipe(ur, writer))
        except Exception:
            try:
                writer.close()
            except Exception:
                pass

    def runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def main():
            server = await asyncio.start_server(handle, "127.0.0.1", 0)
            holder["port"] = server.sockets[0].getsockname()[1]
            ready.set()
            async with server:
                await server.serve_forever()
        loop.run_until_complete(main())

    threading.Thread(target=runner, daemon=True).start()
    if not ready.wait(10):
        raise RuntimeError("local proxy relay failed to start")
    _relays[proxy] = f"127.0.0.1:{holder['port']}"
    return _relays[proxy]

# Excel silently splits any cell longer than 32,767 chars into the following rows
# when it opens a CSV, so cap JSON list cells below that.
def json_list_capped(items, limit=30000) -> str:
    out, size = [], 2
    for it in items:
        add = len(json.dumps(it)) + 2
        if size + add > limit:
            break
        out.append(it)
        size += add
    return json.dumps(out)

# ── browser helpers ──
def open_browser(cfg, proxy=None):
    from seleniumbase import sb_cdp
    kw = dict(locale="en", guest=True)
    if cfg.get("headless"):
        kw["headless"] = True
    if proxy:
        kw["proxy"] = ensure_relay(proxy)
    log_info(f"Launching Chrome (proxy: {proxy_label(proxy)})...")
    sb = sb_cdp.Chrome(**kw)
    log_info("Chrome launched")
    return sb

def close_browser(sb):
    try:
        sb.driver.stop()
    except Exception:
        pass

def get_cookies(sb) -> dict:
    out = {}
    try:
        for c in sb.get_all_cookies():
            name = c.get("name") if isinstance(c, dict) else getattr(c, "name", None)
            val = c.get("value") if isinstance(c, dict) else getattr(c, "value", None)
            if name:
                out[name] = val
    except Exception:
        pass
    try:
        for part in (sb.evaluate("document.cookie") or "").split(";"):
            k, _, v = part.strip().partition("=")
            if k and k not in out:
                out[k] = v
    except Exception:
        pass
    return out
