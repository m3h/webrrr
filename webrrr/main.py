#!/usr/bin/env python3

import urllib.robotparser
import asyncio
from datetime import datetime, timezone
import re
import urllib.parse
import urllib.robotparser
import time
import sqlite3


import aiohttp

USERAGENT = "webrrr"


def now():
    return datetime.now(tz=timezone.utc)


class DB:
    def __init__(self, connection_string="data.db"):
        self.connection_string = connection_string
        self.con = sqlite3.connect(self.connection_string)

        self.create_tables()

    def close(self):
        self.con.close()

    def create_tables(self):
        cur = self.con.cursor()

        cur.execute(
            """CREATE TABLE IF NOT EXISTS URL(url text primary key, netloc text, last_visited datetime, error text, foreign key (netloc) REFERENCES Robots(netloc))"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS Robots(netloc text primary key, robots_txt text)"""
        )
        cur.execute("""
            CREATE TABLE IF NOT EXISTS
                Links(
                    parent_url text,
                    child_url text,
                    primary key (parent_url, child_url),
                    foreign key (parent_url) REFERENCES URL(url),
                    foreign key (child_url) REFERENCES URL(url)
                )
        """)

        self.con.commit()

    def add_urls(self, parent_url, urls):
        last_visited = None

        cur = self.con.cursor()
        cur.executemany(
            """INSERT INTO URL (url, netloc, last_visited)
            SELECT ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM URL WHERE url = ?
            )
        """,
            [
                [url, urllib.parse.urlparse(url).netloc, last_visited, url]
                for url in urls
            ],
        )

        cur.executemany(
            "INSERT INTO Links VALUES (?, ?)", [[parent_url, url] for url in urls]
        )
        self.con.commit()

    def get_unvisited_url(self, exclude_netlocs):
        cur = self.con.cursor()

        # min(NULL, x)  == NULL
        cur.execute(
            f"""
            SELECT url, netloc, last_visited
            FROM URL
            WHERE last_visited is NULL and netloc NOT IN ({','.join('?' * len(exclude_netlocs))})
            ORDER BY (
              SELECT MIN(last_visited)
              FROM URL as URL2
              where URL2.netloc = URL.netloc
            )
        """,
            list(exclude_netlocs),
        )
        res = cur.fetchone()
        if res:
            return res[0]
        else:
            return None

    def get_unvisited_url_count(self, exclude_netlocs={}):
        cur = self.con.cursor()

        # min(NULL, x)  == NULL
        cur.execute(
            f"""
            SELECT COUNT(1), url, netloc, last_visited
            FROM URL
            WHERE last_visited is NULL and netloc NOT IN ({','.join('?' * len(exclude_netlocs))})
            ORDER BY (
              SELECT MIN(last_visited)
              FROM URL as URL2
              where URL2.netloc = URL.netloc
            )
        """,
            exclude_netlocs,
        )
        res = cur.fetchone()
        if res:
            return res[0]
        else:
            return None

    def visit_url(self, url, error_str):
        last_visited = now()

        cur = self.con.cursor()
        cur.execute(
            "UPDATE URL SET last_visited = ?, error = ? WHERE url = ?",
            [last_visited, error_str, url],
        )
        self.con.commit()

    def count_visited(self):
        cur = self.con.cursor()
        cur.execute("SELECT COUNT(1) FROM URL WHERE last_visited is not NULL")
        res = cur.fetchone()
        if res:
            return res[0]
        else:
            return None

    def is_visited(self, url):
        cur = self.con.cursor()
        cur.execute(
            "SELECT COUNT(1) FROM URL WHERE last_visited is not NULL AND url = ?", [url]
        )
        return cur.fetchone() is not None

    def get_robots_txt_text(self, netloc):
        cur = self.con.cursor()
        cur.execute("SELECT robots_txt FROM Robots WHERE netloc = ?", [netloc])
        res = cur.fetchone()
        if res:
            return res[0]
        return None

    def insert_robots_txt(self, netloc, robots_txt):
        # last_visited = now()

        cur = self.con.cursor()
        cur.execute("INSERT INTO Robots Values (?, ?)", [netloc, robots_txt])
        self.con.commit()


class Fetcher:
    def __init__(self, start_url: str, num_workers: int = 5):
        self.num_workers = num_workers

        self.start = time.time()

        self.processing = set()

        self.session = aiohttp.ClientSession()

        self.db = DB()

        self.db.add_urls(None, [start_url])

    def close(self):
        self.session.close()
        self.db.close()

    def count_visited(self):
        return self.db.count_visited()

    async def robots_txt_allows_visit(self, url: str):
        o = urllib.parse.urlparse(url)

        if not (robots_txt_contents := self.db.get_robots_txt_text(o.netloc)):
            robots_url = urllib.parse.urlunparse(
                (o.scheme, o.netloc, "/robots.txt", "", "", "")
            )
            try:
                async with self.session.get(
                    robots_url, allow_redirects=True
                ) as robots_response:
                    # no robots.txt - implicit allow
                    if 400 <= robots_response.status <= 499:
                        return True
                    robots_response.raise_for_status()
                    robots_txt_contents = await robots_response.text()
            except aiohttp.ClientError as client_error:
                print(client_error)
                return False

            self.db.insert_robots_txt(o.netloc, robots_txt_contents)

        robot_file_parser = urllib.robotparser.RobotFileParser()
        robot_file_parser.parse(robots_txt_contents.splitlines())

        return robot_file_parser.can_fetch(useragent="webrrr", url=url)

    def get_next_url(self):
        return self.db.get_unvisited_url(self.processing)

    async def process_next_url(self):
        next_url = self.get_next_url()
        if next_url is None:
            return None
        o = urllib.parse.urlparse(next_url)
        self.processing.add(o.netloc)

        new_links, err_str = await self.fetch_children(next_url)
        if err_str is not None:
            assert len(new_links) == 0
            print(f"[{datetime.now().isoformat()}] ERROR: [{next_url}] [{err_str}]")

        self.db.add_urls(next_url, new_links)
        self.db.visit_url(next_url, err_str)

        self.processing.remove(o.netloc)

        print(f"visited: {self.count_visited()}")

        t = time.time() - self.start

        print(f"total time: {t}, rate = {self.count_visited() / t}")

    async def orchestrate_url_visits(self):
        # pool = {asyncio.create_task(self.process_next_url())}
        pool = set()

        pool_size = len(pool)

        while self.db.get_unvisited_url_count():
            done, pending = await asyncio.wait(pool, return_when="FIRST_COMPLETED")
            for task in done:
                await task

            pool = pending
            while len(pool) < pool_size and self.db.get_unvisited_url_count():
                pool.add(asyncio.create_task(self.process_next_url()))
            # slowly ramp up pool size so that we don't overwhelm single netloc at beginning
            pool_size = min(self.num_workers, pool_size + 1)

    def get_canonical_url(self, url_part: str, base_url: str):
        o = urllib.parse.urlparse(url_part)

        fragment = ""
        if o.scheme == "" and o.netloc == "":
            base_o = urllib.parse.urlparse(base_url)
            return urllib.parse.urlunparse(
                (base_o.scheme, base_o.netloc, o.path, o.params, o.query, fragment)
            )
        else:
            return urllib.parse.urlunparse(
                (o.scheme, o.netloc, o.path, o.params, o.query, fragment)
            )

    async def fetch_children(self, url: str):
        try:
            if not (await self.robots_txt_allows_visit(url)):
                return set(), "blocked by robots.txt"

            print(f"[{datetime.now().isoformat()}] FETCH {url}")

            async with self.session.get(url, allow_redirects=True) as response:
                response.raise_for_status()

                html_text = await response.text()

            match_iter = re.finditer(r'href="(?P<href>[^"]+)"', html_text)
            urls = set(self.get_canonical_url(m.groups()[0], url) for m in match_iter)
            return urls, None
        except Exception as client_error:
            return set(), str(client_error)


async def main():
    # create_tables()
    # return
    f = Fetcher("https://docs.aiohttp.org/en/stable/migration_to_2xx.html")

    await f.orchestrate_url_visits()


asyncio.run(main())
