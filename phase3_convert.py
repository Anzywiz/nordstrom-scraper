"""
convert_to_txt.py  (low-memory, two-pass version)
─────────────────────────────────────────────────
Reads a product CSV, applies DBQ upload rules, and writes a
semicolon-delimited .txt file (all fields quoted, no header row).

Same rules and outputs as the original in-memory version, but the CSV is
streamed in chunks so RAM stays flat no matter how many rows there are.

  Pass 1  reads ONLY the identity columns (+ competence_date), hashes each
          row's identity key, and works out which row is the last occurrence
          of every key. Only a 1-D array of 64-bit hashes is held in memory.
  Pass 2  re-reads the full CSV chunk by chunk, drops rows that aren't the
          last occurrence, applies every upload rule, appends to the .txt,
          and accumulates brand counts / fill rates on the fly.

Rules applied:
  • all_images capped at 1000 characters
  • all string columns capped at 1000 characters
  • numeric price fields sanitised (max 99 999 999.99)
  • competence_date set to latest date in file
  • fixed_values from config.json stamped on every row
  • rank column dropped when dbq_prd_type == E0005
  • Duplicates removed on identity columns only (CONSTANT_COLS and
    VOLATILE_COLS excluded from the key). Keeps last occurrence.

Outputs:
  • data_file.txt          — DBQ upload file
  • brand_counts.csv       — per-brand product counts
  • fill_rate_report.csv   — per-column fill rates (non-constant cols only)

Usage:
    python convert_to_txt.py                        # uses config.json defaults
    python convert_to_txt.py --input my.csv         # override input file
    python convert_to_txt.py --input my.csv --output out/upload.txt
    python convert_to_txt.py --chunksize 50000      # smaller chunks = less RAM
"""

import argparse
import csv
import json
import logging
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# ── Coloured logging ──────────────────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
COLOURS = {
    logging.DEBUG:    "\033[36m",   # cyan
    logging.INFO:     "\033[32m",   # green
    logging.WARNING:  "\033[33m",   # yellow
    logging.ERROR:    "\033[31m",   # red
    logging.CRITICAL: "\033[35m",   # magenta
}

class ColourFormatter(logging.Formatter):
    def format(self, record):
        colour = COLOURS.get(record.levelno, RESET)
        record.levelname = f"{colour}{BOLD}{record.levelname:<7}{RESET}"
        record.msg       = f"{colour}{record.msg}{RESET}"
        return super().format(record)

def _setup_logger(name: str) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ColourFormatter(
        fmt="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

log = _setup_logger("convert_to_txt")

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_CONFIG = "config.json"
DEFAULT_CHUNK  = 100_000

NUMERIC_COLS = {"full_price", "price", "ppu", "rank", "in_stock", "quantity", "delivery_id"}
PRICE_COLS   = ["full_price", "price", "ppu"]
MAX_PRICE    = 99_999_999.99

# Columns that are constant per upload — excluded from fill-rate analysis
# and from the dedup key.
CONSTANT_COLS = {
    "dbq_prd_type", "website_name", "competence_date",
    "country_code", "currency_code",
    "contract_id", "seller_id", "delivery_id",
}

# Columns expected to fluctuate between scrapes of the SAME real-world item.
# Excluded from the dedup key. Columns absent from a schema are skipped.
VOLATILE_COLS = {
    "quantity", "in_stock", "rank",
    "price", "full_price", "ppu",
    "promotion_type", "promotion_end_date", "delivery",
    "description", "specifications", "additional_content",
    "all_images", "main_image_url", "package_desc", "additional_tags",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    if not Path(path).exists():
        log.warning(f"Config not found at '{path}' — using empty config.")
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def cap_images(val, max_len=1000):
    """Keep as many pipe-separated image URLs as fit within max_len chars."""
    if not val or pd.isna(val):
        return None
    urls = re.findall(r"https?://[^\s\",|]+", str(val))
    result = ""
    for url in urls:
        candidate = result + ("|" if result else "") + url
        if len(candidate) > max_len:
            break
        result = candidate
    return result or None


def section(title: str):
    log.info(f"── {title} {'─' * max(0, 55 - len(title))}")


def cap_strings(df: pd.DataFrame, cols) -> None:
    """Truncate string columns to 1000 chars in place (numeric + all_images skipped)."""
    for col in cols:
        if col not in NUMERIC_COLS and col != "all_images":
            df[col] = df[col].astype(str).str[:1000]


def read_chunks(path, chunksize, usecols=None):
    return pd.read_csv(path, dtype=str, keep_default_na=False,
                       chunksize=chunksize, usecols=usecols)

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Convert product CSV to DBQ upload txt.")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Path to config.json")
    parser.add_argument("--input",  help="Override input CSV path")
    parser.add_argument("--output", help="Override output TXT path")
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNK,
                        help=f"Rows per chunk (default {DEFAULT_CHUNK:,})")
    args = parser.parse_args()

    cfg = load_config(args.config)

    in_csv  = Path(args.input  or cfg.get("input_csv",  "output/products.csv"))
    out_txt = Path(args.output or cfg.get("phase4_output_txt", "output/data_file.txt"))
    out_dir = out_txt.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    fixed_values  = cfg.get("fixed_values", {})
    dbq_prd_type  = fixed_values.get("dbq_prd_type", cfg.get("dbq_prd_type", ""))
    chunksize     = max(1, args.chunksize)

    # ── Inspect header ────────────────────────────────────────────────────────
    section("Loading data")
    if not in_csv.exists():
        log.error(f"Input file not found: {in_csv}")
        sys.exit(1)

    in_columns = list(pd.read_csv(in_csv, dtype=str, nrows=0).columns)
    log.info(f"  Input   : {in_csv}  ({len(in_columns)} columns)")
    log.info(f"  Columns : {', '.join(in_columns)}")
    log.info(f"  Chunk   : {chunksize:,} rows")

    drop_rank = dbq_prd_type == "E0005" and "rank" in in_columns
    out_columns = [c for c in in_columns if not (drop_rank and c == "rank")]

    # Identity columns = everything except constant-per-upload and volatile.
    dedup_cols = [c for c in out_columns if c not in CONSTANT_COLS and c not in VOLATILE_COLS]

    # ── PASS 1: hash identity key + find latest competence_date ───────────────
    section("Pass 1/2 — scanning for duplicates")
    pass1_cols = list(dedup_cols)
    if "competence_date" in in_columns and "competence_date" not in pass1_cols:
        pass1_cols.append("competence_date")

    hash_parts   = []
    latest_date  = pd.NaT
    total_rows   = 0

    if pass1_cols:
        for chunk in read_chunks(in_csv, chunksize, usecols=pass1_cols):
            total_rows += len(chunk)
            if "competence_date" in chunk.columns:
                d = pd.to_datetime(chunk["competence_date"], errors="coerce").max()
                if pd.notna(d) and (pd.isna(latest_date) or d > latest_date):
                    latest_date = d
            if dedup_cols:
                key = chunk[dedup_cols].copy()
                cap_strings(key, dedup_cols)
                hash_parts.append(
                    pd.util.hash_pandas_object(key, index=False).to_numpy()
                )
            del chunk
    else:
        for chunk in read_chunks(in_csv, chunksize, usecols=[in_columns[0]]):
            total_rows += len(chunk)

    latest_str = latest_date.strftime("%Y-%m-%d") if pd.notna(latest_date) else ""
    if "competence_date" in in_columns:
        log.info(f"  competence_date = {latest_str or '(none found)'}")

    if dedup_cols and hash_parts:
        hashes = np.concatenate(hash_parts)
        del hash_parts
        keep = ~pd.Series(hashes).duplicated(keep="last").to_numpy()
        del hashes
        log.info(f"  Dedup key ({len(dedup_cols)} cols): {', '.join(dedup_cols)}")
    else:
        keep = np.ones(total_rows, dtype=bool)
        log.warning("  No identity columns found — skipping dedup")

    kept_total = int(keep.sum())
    removed    = total_rows - kept_total
    log.info(f"  {total_rows:,} → {kept_total:,} rows  ({removed:,} duplicates removed)")

    # ── PASS 2: transform, write, accumulate stats ────────────────────────────
    section("Pass 2/2 — transforming and writing")
    has_brand = "brand" in out_columns and "product_code" in out_columns
    analyse_cols = [c for c in out_columns if c not in CONSTANT_COLS and c != "rank"]

    filled_counts = {c: 0 for c in analyse_cols}
    brand_counter = Counter()
    seen_codes    = set()
    brands_seen   = set()
    rows_written  = 0
    offset        = 0
    price_warned  = Counter()

    tmp_txt = out_txt.with_name(out_txt.name + ".tmp")
    with open(tmp_txt, "w", encoding="utf-8", newline="") as fout:
        for chunk in read_chunks(in_csv, chunksize):
            mask = keep[offset:offset + len(chunk)]
            offset += len(chunk)
            chunk = chunk[mask]
            if chunk.empty:
                continue
            chunk = chunk.copy()

            # image cap
            if "all_images" in chunk.columns:
                chunk["all_images"] = chunk["all_images"].apply(cap_images)

            # string cap
            cap_strings(chunk, chunk.columns)

            # price sanitise
            for col in PRICE_COLS:
                if col not in chunk.columns:
                    continue
                numeric = pd.to_numeric(chunk[col], errors="coerce").astype(float)  # force float so formatting is identical in every chunk
                price_warned[col] += int((numeric > MAX_PRICE).sum())
                chunk[col] = (numeric.where(numeric <= MAX_PRICE)
                              .round(2).astype(str).replace("nan", ""))

            # competence date (latest over whole file)
            if "competence_date" in chunk.columns:
                chunk["competence_date"] = latest_str

            # fixed values
            for key, val in fixed_values.items():
                if key in chunk.columns:
                    chunk[key] = val

            # drop rank for E0005
            if drop_rank:
                chunk = chunk.drop(columns=["rank"])

            # stats: fill rate
            for col in analyse_cols:
                s = chunk[col]
                filled_counts[col] += int((s.notna() & (s != "")).sum())

            # stats: brand counts (first occurrence of each product_code)
            if has_brand:
                h = pd.util.hash_pandas_object(chunk["product_code"], index=False).to_numpy()
                first_in_chunk = ~pd.Series(h).duplicated().to_numpy()
                not_seen = np.fromiter((x not in seen_codes for x in h.tolist()),
                                       dtype=bool, count=len(h))
                fresh = first_in_chunk & not_seen
                seen_codes.update(h[fresh].tolist())
                brand_counter.update(chunk.loc[fresh, "brand"].tolist())
            if "brand" in chunk.columns:
                brands_seen.update(chunk["brand"].unique().tolist())

            # write
            chunk.to_csv(fout, sep=";", index=False, header=False,
                         quoting=csv.QUOTE_ALL)
            rows_written += len(chunk)
            log.info(f"  …{rows_written:,} / {kept_total:,} rows written")
            del chunk

    tmp_txt.replace(out_txt)

    for col, bad in price_warned.items():
        if bad:
            log.warning(f"  {col}: {bad:,} value(s) exceed {MAX_PRICE:,.2f} → nulled")
    if drop_rank:
        log.info("  dbq_prd_type=E0005 → rank column dropped")
    log.info(f"  Fixed values applied: {list(fixed_values.keys()) or '(none)'}")
    log.info(f"  ✓  {rows_written:,} rows  →  {out_txt}")

    # ── Summary ───────────────────────────────────────────────────────────────
    section("Summary")
    log.info(f"  Total rows      : {rows_written:,}")
    if "brand" in out_columns:
        log.info(f"  Unique brands   : {len(brands_seen):,}")
    else:
        log.info("  Unique brands   : n/a")
    if "product_code" in out_columns and has_brand:
        log.info(f"  Unique prod codes: {len(seen_codes):,}")

    # ── Brand counts ──────────────────────────────────────────────────────────
    section("Brand counts")
    if has_brand:
        brand_counts = (
            pd.DataFrame(list(brand_counter.items()), columns=["brand", "product_count"])
            .sort_values("product_count", ascending=False)
            .reset_index(drop=True)
        )
        counts_path = out_dir / "brand_counts.csv"
        brand_counts.to_csv(counts_path, index=False, encoding="utf-8")

        n         = len(brand_counts)
        max_count = brand_counts["product_count"].max() if n else 1
        max_count = max_count or 1

        def print_brands(rows, label):
            log.info(f"  {label}:")
            for _, row in rows.iterrows():
                bar = "█" * min(int(row["product_count"] / max_count * 20), 20)
                print(f"    {row['brand'][:35]:35s}  {row['product_count']:>6}  {bar}")

        mid = max(0, n // 2 - 2)
        print_brands(brand_counts.head(5),           "Top 5 brands")
        print_brands(brand_counts.iloc[mid:mid + 5], "Mid brands")
        print_brands(brand_counts.tail(5),           "Bottom 5 brands")
        log.info(f"  brand_counts.csv saved  ({n} brands)  →  {counts_path}")

    # ── Fill rate report ──────────────────────────────────────────────────────
    section("Fill rate report")
    fill_rows = []
    total = rows_written
    print(f"\n  {'Column':<30}  {'Fill rate':>10}  {'Filled':>8}  {'Total':>8}")
    print(f"  {'─'*30}  {'─'*10}  {'─'*8}  {'─'*8}")
    for col in analyse_cols:
        filled  = filled_counts[col]
        rate    = filled / total if total else 0.0
        pct_str = f"{rate * 100:.1f}%"

        if rate >= 0.95:
            colour, flag = "\033[32m", ""       # green
        elif rate >= 0.5:
            colour, flag = "\033[33m", "  ⚠"   # yellow
        else:
            colour, flag = "\033[31m", "  ✗"   # red

        if rate > 0:  # 0% columns hidden from table — still written to CSV
            print(f"  {colour}{col:<30}  {pct_str:>10}  {filled:>8,}  {total:>8,}{flag}{RESET}")
        fill_rows.append({"column": col, "fill_rate_pct": round(rate * 100, 2),
                          "filled": filled, "total": total})

    fill_path = out_dir / "fill_rate_report.csv"
    pd.DataFrame(fill_rows).to_csv(fill_path, index=False, encoding="utf-8")
    log.info(f"\n  fill_rate_report.csv saved  →  {fill_path}")


if __name__ == "__main__":
    main()