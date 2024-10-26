#!/usr/bin/env python3
"""Scrape the links of the internet using webrrr."""

import asyncio
import time

import webrrr.v1
import webrrr.v2


async def main() -> None:
    start_url = "https://docs.aiohttp.org/en/stable/migration_to_2xx.html"
    pool_size = 5
    limit = 100

    results = {}
    for version_name, version_fn in [
        ("v1", webrrr.v1.orchestrate_url_visits),
        ("v2", webrrr.v2.orchestrate_url_visits),
    ]:
        s = time.time()
        await version_fn(f"data_{version_name}.db", start_url, pool_size, limit)
        results[version_name] = time.time() - s


asyncio.run(main())
