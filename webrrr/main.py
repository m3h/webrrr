#!/usr/bin/env python3
"""Scrape the links of the internet using webrrr."""

import asyncio
import time

import webrrr.v1
import webrrr.v2
import webrrr.v3


async def main() -> None:
    start_url = "http://start.localhost:8000"

    pool_size = 10
    limit = 2_000_000

    results = {}
    for version_name, version_fn in [
        ("v1", webrrr.v1.orchestrate_url_visits),
        ("v2", webrrr.v2.orchestrate_url_visits),
        ("v3", webrrr.v3.orchestrate_url_visits),
    ]:
        s = time.time()
        await version_fn(f"data_{version_name}.db", start_url, pool_size, limit)
        results[version_name] = time.time() - s



asyncio.run(main())
