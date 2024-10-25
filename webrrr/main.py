#!/usr/bin/env python3
"""Scrape the links of the internet using webrrr."""

from __future__ import annotations

import asyncio
import http
import re
import sqlite3
import time
import urllib.parse
import urllib.robotparser
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult

import aiohttp

if TYPE_CHECKING:
    from collections.abc import Iterable

USERAGENT = "webrrr"


# https://docs.python.org/3/library/sqlite3.html#sqlite3-adapter-converter-recipes
def adapt_datetime_iso(val: datetime) -> str:
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


sqlite3.register_adapter(datetime, adapt_datetime_iso)


def convert_datetime(val: str) -> datetime:
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.fromisoformat(val.decode())


sqlite3.register_converter("datetime", convert_datetime)


def now(tz: timezone | None = None) -> datetime:
    """UTC-defaulted version of datetime.now()."""
    if tz is None:
        tz = timezone.utc
    return datetime.now(tz=tz)


def urlparse(url: str) -> ParseResult:
    """Parse URLs and strip fragments."""
    # parse URL, stripping fragment
    o = urllib.parse.urlparse(url=url)
    return ParseResult(
        scheme=o.scheme,
        netloc=o.netloc,
        path=o.path,
        params=o.params,
        query=o.query,
        fragment="",
    )


class DB:
    def __init__(self, connection_string: str = "data.db") -> None:
        self.connection_string = connection_string
        self.con = sqlite3.connect(self.connection_string)

        self.create_tables()

    def close(self) -> None:
        self.con.close()

    def _fetch_one_object(
        self,
        sql: str,
        parameters: Iterable[Any] = (),
    ) -> object | None:
        cur = self.con.cursor()
        cur.execute(sql, parameters)
        res = cur.fetchone()
        if res:
            assert len(res) >= 1
            return res[0]
        return None

    def create_tables(self) -> None:
        cur = self.con.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS
                URL(
                    url text primary key,
                    netloc text,
                    last_visited datetime,
                    error text,
                    foreign key (netloc) REFERENCES Robots(netloc)
                )
            """,
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS
                Robots(
                    netloc text primary key,
                    robots_txt text
                )
            """,
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS
                Links(
                    parent_url text,
                    child_url text,
                    primary key (parent_url, child_url),
                    foreign key (parent_url) REFERENCES URL(url),
                    foreign key (child_url) REFERENCES URL(url)
                )
        """
        )

        self.con.commit()

    def add_urls(self, urls: Iterable[ParseResult]) -> None:
        last_visited = None

        cur = self.con.cursor()
        cur.executemany(
            """
            INSERT INTO URL (url, netloc, last_visited)
            SELECT ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM URL WHERE url = ?
            )
        """,
            [[url.geturl(), url.netloc, last_visited, url.geturl()] for url in urls],
        )
        self.con.commit()

    def add_links(self, parent_url: ParseResult, urls: Iterable[ParseResult]) -> None:
        cur = self.con.cursor()

        cur.executemany(
            """
            INSERT INTO Links (parent_url, child_url)
            SELECT ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM Links WHERE parent_url = ? AND child_url = ?
            )
            """,
            [
                [parent_url.geturl(), url.geturl(), parent_url.geturl(), url.geturl()]
                for url in urls
            ],
        )
        self.con.commit()

    def get_unvisited_url(self, exclude_netlocs: Iterable[str]) -> ParseResult:
        # min(NULL, x) = NULL
        url = self._fetch_one_object(
            f"""
            SELECT url, netloc, last_visited
            FROM URL
            WHERE
                last_visited is NULL
                and netloc NOT IN ({','.join('?' * len(exclude_netlocs))})
            ORDER BY (
              SELECT MIN(last_visited)
              FROM URL as URL2
              where URL2.netloc = URL.netloc
            )
        """,  # noqa: S608
            list(exclude_netlocs),
        )
        if url is not None:
            return urlparse(url)
        return None

    def visit_url(self, url: ParseResult, error_str: str) -> None:
        self.add_urls([url])

        last_visited = now()

        cur = self.con.cursor()
        cur.execute(
            "UPDATE URL SET last_visited = ?, error = ? WHERE url = ?",
            [last_visited, error_str, url.geturl()],
        )
        assert cur.lastrowid
        self.con.commit()

    def count_visited(self) -> int:
        return self._fetch_one_object(
            "SELECT COUNT(1) FROM URL WHERE last_visited is not NULL",
        )

    def count_unvisited(self) -> int:
        return self._fetch_one_object(
            "SELECT COUNT(1) FROM URL WHERE last_visited is NULL",
        )

    def is_visited(self, url: ParseResult) -> bool:
        return (
            self._fetch_one_object(
                "SELECT COUNT(1) FROM URL WHERE last_visited is not NULL AND url = ?",
                [url.geturl()],
            )
            == 1
        )

    def get_robots_txt_text(self, netloc: str) -> str:
        return self._fetch_one_object(
            "SELECT robots_txt FROM Robots WHERE netloc = ?",
            [netloc],
        )

    def insert_robots_txt(self, netloc: str, robots_txt: str) -> None:
        cur = self.con.cursor()
        cur.execute("INSERT INTO Robots Values (?, ?)", [netloc, robots_txt])
        self.con.commit()


class PoolManager:
    def __init__(self, max_pool_size: int) -> None:
        self.max_pool_size = max_pool_size

        self.pool = set()
        self.netlocs_currently_processing = set()

    async def add_to_pool(self, url: ParseResult, task: asyncio.Task) -> bool:
        while len(self.pool) >= self.max_pool_size:
            await self.wait()

        self.pool.add(task)
        self.netlocs_currently_processing.add(url.netloc)

    def busy(self) -> bool:
        return len(self.pool) > 0

    def _pop_from_pool(self, url: ParseResult, task: asyncio.Task) -> None:
        self.pool.remove(task)
        self.netlocs_currently_processing.remove(url.netloc)

    async def wait(self) -> None:
        done_tasks, _pending_tasks = await asyncio.wait(
            self.pool,
            return_when="FIRST_COMPLETED",
        )
        for done_task in done_tasks:
            done_url: ParseResult = await done_task
            self._pop_from_pool(done_url, done_task)


class Fetcher:
    def __init__(self) -> None:
        self.start = time.time()

        self.processing = set()

        self.session = aiohttp.ClientSession()

        self.db = DB()

    def close(self) -> None:
        self.session.close()
        self.db.close()

    async def robots_txt_allows_visit(self, url: ParseResult) -> bool:
        if not (robots_txt_contents := self.db.get_robots_txt_text(url.netloc)):
            robots_url = urllib.parse.urlunparse(
                (url.scheme, url.netloc, "/robots.txt", "", "", ""),
            )
            try:
                async with self.session.get(
                    robots_url,
                    allow_redirects=True,
                ) as robots_response:
                    # no robots.txt - implicit allow
                    if robots_response.status in (
                        http.HTTPStatus.NOT_FOUND,
                        http.HTTPStatus.FORBIDDEN,
                    ):
                        return True
                    robots_response.raise_for_status()
                    robots_txt_contents = await robots_response.text()
            except (aiohttp.ClientError, UnicodeDecodeError):
                return False

            self.db.insert_robots_txt(url.netloc, robots_txt_contents)

        robot_file_parser = urllib.robotparser.RobotFileParser()
        robot_file_parser.parse(robots_txt_contents.splitlines())

        return robot_file_parser.can_fetch(useragent="webrrr", url=url.geturl())

    def fetch_and_process_url_task(self, url: ParseResult) -> asyncio.Task:
        return asyncio.create_task(self._fetch_and_process_url(url))

    async def _fetch_and_process_url(self, url: ParseResult) -> None:
        new_links, err_str = await self.fetch_children(url)
        if err_str is not None:
            assert len(new_links) == 0

        self.db.add_urls(new_links)
        self.db.add_links(url, new_links)
        self.db.visit_url(url, err_str)

        return url

    def get_canonical_url(self, url_part: str, base_url: ParseResult) -> ParseResult:
        o = urlparse(url_part)

        if o.scheme == "" and o.netloc == "":
            return ParseResult(
                base_url.scheme, base_url.netloc, o.path, o.params, o.query, o.fragment
            )
        return o

    async def fetch_children(
        self,
        url: ParseResult,
    ) -> tuple[set[ParseResult], str | None]:
        if not (await self.robots_txt_allows_visit(url)):
            return set(), "blocked by robots.txt"

        try:
            async with self.session.get(url.geturl(), allow_redirects=True) as response:
                if not response.ok:
                    return set(), await response.text()
                response.raise_for_status()

                if response.content_type.lower() != "text/html":
                    return set(), response.content_type

                html_text = await response.text()
        except aiohttp.ClientError as client_error:
            return set(), str(client_error)

        match_iter = re.finditer(r'href="(?P<href>[^"]+)"', html_text)
        urls = {self.get_canonical_url(m.groups()[0], url) for m in match_iter}
        urls = {*filter(lambda url: url.scheme in {"http", "https"}, urls)}
        return urls, None


async def orchestrate_url_visits(start_url: str, max_pool_size: int = 5) -> None:
    fetcher = Fetcher()
    pool_manager = PoolManager(max_pool_size)

    unvisited_url = urlparse(start_url)

    while unvisited_url or pool_manager.busy():
        if unvisited_url:
            task = fetcher.fetch_and_process_url_task(unvisited_url)
            await pool_manager.add_to_pool(unvisited_url, task)
        else:
            await pool_manager.wait()

        unvisited_url = fetcher.db.get_unvisited_url(
            exclude_netlocs=pool_manager.netlocs_currently_processing
        )

    fetcher.close()


async def main() -> None:
    await orchestrate_url_visits(
        "https://docs.aiohttp.org/en/stable/migration_to_2xx.html",
    )


asyncio.run(main())
