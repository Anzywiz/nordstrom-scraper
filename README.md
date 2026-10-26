# Nordstrom scraper (CloakBrowser edition)

## Files

| File | What it is |
|---|---|
| `config.json` | Every setting, each one explained in the `_help` blocks. |
| `.env.example` | Copy to `.env`: `PROXY_LIST`, optional `NORD_API_KEY`, AWS keys. |
| `nord_core.py` | Config loader, logging, proxy pool, DBQ columns. Does not import your old `common.py`. |
| `nord_browser.py` | CloakBrowser sessions: navigate, read `window.__INITIAL_CONFIG__`, block detection, backoff, restart, tabs. |
| `phase1_listing.py` | Listing. Unfiltered when the category fits under the pagination cap, otherwise brand groups. No brand phase. |
| `phase2_enrich.py` | PDP enrichment, one row per colour x size. Parser is your previous `parse_payload`, copied unchanged. |

Not needed any more: `phase1_brands.py`, `phase2_listing.py`, old `common.py` (leave them or delete them).
Phase 3 (convert) and phase 4 (upload) are untouched. Copy `fixed_values` (contract/seller ids) and `upload`
from your old `config.json` into this one, and check phase 3/4 read the keys they expect.

## Run order

```bash
pip install -r requirements.txt
cp .env.example .env                      # add PROXY_LIST if you use proxies

python phase1_listing.py --discover       # 1. check the structure (see below)
python phase1_listing.py --category women --max-pages 3
python phase2_enrich.py --test-url https://www.nordstrom.com/s/<slug>/<id>
python phase2_enrich.py --limit 20

python phase1_listing.py                  # full run, resumes after a crash or Ctrl+C
python phase2_enrich.py
```

`--discover` is important: I could not see the listing page's `__INITIAL_CONFIG__`, so product,
`pageCount`, `resultCount` and the Brand facet are located by searching the JSON, like your old code did.
It prints where the products were found and how many, whether a Brand facet exists, and saves
`output/discover_<category>.json` plus one sample product. If something says NOT FOUND, send me those files.

## How phase 1 decides what to do

1. Page 1 of a category gives `resultCount` and `pageCount`; reach = `pageCount x products per page`.
2. `resultCount <= reach`: crawl unfiltered.
3. Otherwise read the Brand facet and pack brands into groups of at most `group_fill_ratio x reach` products.
   Before relying on it, the script requests a few brands together and checks the result equals the sum
   (it tries `filterByBrand=a&filterByBrand=b`, then `a~b`, then `a,b`). If none works it falls back to one
   brand per partition and says so.
4. All pages of all partitions go through one queue, so browsers and tabs stay busy.

`listing.source` = `page` (default) reads the browse page's config; `api` calls the sbn/v2 API from inside
the page (up to 120 items per request, needs `NORD_API_KEY`). Try both on a small run
(`--max-pages 5 --source api`) and compare speed and the failed-request counter.

## Speed and scale (rough numbers, measure on your machine)

* A navigation took about 3.5 s in your test, plus `retry.delay_seconds` (2 s): ~0.2 pages/s per tab.
* Listing, 200k products at 72 per page: about 2,800 pages, about 4 hours on one tab.
* Enrichment is the hard part: 200k PDPs at ~5.5 s each is about 300 hours on one tab. Daily full refreshes
  need roughly 15 or more tabs in parallel (tabs x browsers), or a smaller scope.
* Raise `tabs_per_browser` first (1, 2, 3), watching `failed requests` in the progress line; then `workers`.
* CloakBrowser's free binary shows "1 concurrent session": more `workers` probably needs a paid key.
  Check their terms; tabs inside one browser may be fine, but I have not verified that against their licence.

## Things I could not verify (check these first)

* The listing page config shape (see `--discover`).
* The `kids` path/breadcrumb in `config.json`.
* Proxy arguments for `launch_persistent_context_async`: it tries the URL form, then the dict form. If both
  fail for your installed version, copy the call from your editorialist phase 1b and tell me.
* Whether `top` is honoured on the page URL: `--discover --top 150`, then read `products_on_page`.
