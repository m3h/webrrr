#!/usr/bin/env python3
"""Scrape the links of the internet using webrrr."""

import asyncio
import time
from pprint import pprint

import webrrr.v1
import webrrr.v2
import webrrr.v3


async def main() -> None:
    start_url = "https://docs.aiohttp.org/en/stable/migration_to_2xx.html"
    start_url = "https://nl.wikipedia.org/wiki/Wiki"
    pool_size = 400
    limit = 2000

    results = {}
    for version_name, version_fn in [
        # ("v1", webrrr.v1.orchestrate_url_visits),
        # ("v2", webrrr.v2.orchestrate_url_visits),
        ("v3", webrrr.v3.orchestrate_url_visits),
    ]:
        s = time.time()
        await version_fn(f"data_{version_name}.db", start_url, pool_size, limit)
        results[version_name] = time.time() - s

    pprint(results)


asyncio.run(main())
