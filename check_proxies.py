#!/usr/bin/env python3
"""Quick proxy health check: python check_proxies.py  (reads PROXY_LIST from .env)"""
from common import all_proxies, check_proxies, log_err

if not all_proxies():
    log_err("PROXY_LIST is empty in .env")
else:
    check_proxies(timeout=15)
