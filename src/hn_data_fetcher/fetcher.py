"""Fetch Hacker News items straight into a Hive-partitioned Parquet store.

Pipeline: HN Firebase API -> worker pool -> in-memory chunk -> ``Store.merge``
(atomic per-month Parquet rewrite). There is no intermediate database: the
newest stored item id *is* the resume point, and a chunk is only committed once
every id in it has been fetched, so an interrupted or failed run never skips ids.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime

import aiohttp
import orjson
from tqdm import tqdm

from .store import STORE_TZ, Store, rows_to_table

API = "https://hacker-news.firebaseio.com/"  # paths below are relative to it
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


class FetchError(RuntimeError):
    """An id could not be fetched after all retries."""


@dataclass
class Config:
    """Tunables for a fetch run."""

    data_dir: str = "data"
    concurrency: int = 2500  # measured: ~3.4k ids/s; >=6000 makes HN reset connections
    chunk_size: int = 200_000
    timeout: float = 30.0
    retries: int = 6


@dataclass
class Stats:
    """Counters reported at the end of a run."""

    ids: int = 0
    items: int = 0
    retries: int = 0
    partitions: dict = field(default_factory=dict)
    seconds: float = 0.0

    def report(self) -> str:
        rate = self.ids / self.seconds if self.seconds else 0.0
        parts = ", ".join(f"{y}-{m:02d}={n:,}" for (y, m), n in sorted(self.partitions.items()))
        return (
            f"ids={self.ids:,} items={self.items:,} retries={self.retries:,} "
            f"duration={self.seconds:.1f}s speed={rate:,.0f} ids/s\n"
            f"partitions written: {parts or 'none'}"
        )


class Fetcher:
    """Async HN API client that commits to a :class:`Store`."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.store = Store(config.data_dir)
        self.stats = Stats()

    # -- HTTP --------------------------------------------------------------
    def session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            base_url=API,
            connector=aiohttp.TCPConnector(
                limit=self.config.concurrency, ttl_dns_cache=600, keepalive_timeout=60
            ),
            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            headers={"User-Agent": "hn-data-fetcher"},
        )

    async def _get(self, session: aiohttp.ClientSession, path: str) -> bytes:
        """GET with exponential backoff on transient errors."""
        delay = 0.5
        for attempt in range(self.config.retries + 1):
            try:
                async with session.get(path) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    if resp.status not in RETRY_STATUS:
                        raise FetchError(f"{path}: HTTP {resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError):
                pass
            if attempt == self.config.retries:
                break
            self.stats.retries += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)
        raise FetchError(f"{path}: gave up after {self.config.retries} retries")

    async def max_item(self, session: aiohttp.ClientSession) -> int:
        """Current highest item id on HN."""
        return int(await self._get(session, "v0/maxitem.json"))

    async def _item_time(self, session: aiohttp.ClientSession, item_id: int) -> int:
        """Timestamp of the first existing item at or after ``item_id``."""
        for i in range(item_id, item_id + 1000):
            item = orjson.loads(await self._get(session, f"v0/item/{i}.json"))
            if isinstance(item, dict) and "time" in item:
                return item["time"]
        raise FetchError(f"no item with a time near id {item_id}")

    async def find_id_at(self, session: aiohttp.ClientSession, epoch: int, lo: int, hi: int) -> int:
        """First id in ``[lo, hi]`` whose item is at or after ``epoch`` (binary search;
        ids grow with time)."""
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if await self._item_time(session, mid) < epoch:
                lo = mid
            else:
                hi = mid
        return hi

    # -- fetching ----------------------------------------------------------
    async def _worker(
        self,
        session: aiohttp.ClientSession,
        ids: Iterator[int],
        rows: list[dict],
        failed: list[int],
        bar: tqdm,
    ) -> None:
        for item_id in ids:  # a shared iterator: each id is handed out exactly once
            try:
                body = await self._get(session, f"v0/item/{item_id}.json")
            except FetchError:
                failed.append(item_id)
                continue
            if body != b"null":
                item = orjson.loads(body)
                if isinstance(item, dict):
                    rows.append(item)
            bar.update(1)

    async def _fetch_ids(
        self, session: aiohttp.ClientSession, lo: int, hi: int, bar: tqdm
    ) -> list[dict]:
        """Fetch every id in ``[lo, hi)``; raise if any stay unreachable."""
        rows: list[dict] = []
        failed: list[int] = []
        ids = iter(range(lo, hi))
        await asyncio.gather(
            *(self._worker(session, ids, rows, failed, bar) for _ in range(self.config.concurrency))
        )
        if failed:  # one gentler second pass before giving up on the chunk
            retry = iter(failed)
            failed = []
            await asyncio.gather(
                *(self._worker(session, retry, rows, failed, bar) for _ in range(20))
            )
        if failed:
            raise FetchError(
                f"{len(failed)} ids failed in [{lo}, {hi}), e.g. {failed[:5]}; nothing committed"
            )
        return rows

    async def run(self, ranges: list[tuple[int, int]]) -> Stats:
        """Fetch id ranges ``[lo, hi)`` and merge them into the store, chunk by chunk.

        Merging runs in a worker thread while the next chunk downloads.
        """
        ranges = [(lo, hi) for lo, hi in ranges if hi > lo]
        total = sum(hi - lo for lo, hi in ranges)
        loop = asyncio.get_running_loop()
        pending: asyncio.Future | None = None
        started = time.monotonic()

        def commit(rows: list[dict]) -> list[tuple[int, int, int]]:
            return self.store.merge(rows_to_table(rows))

        async def settle() -> None:
            nonlocal pending
            if pending is not None:
                for y, m, n in await pending:
                    self.stats.partitions[(y, m)] = n
                pending = None

        try:
            with tqdm(total=total, unit="id", smoothing=0.05) as bar:
                async with self.session() as session:
                    for lo, hi in ranges:
                        for start in range(lo, hi, self.config.chunk_size):
                            end = min(start + self.config.chunk_size, hi)
                            rows = await self._fetch_ids(session, start, end, bar)
                            await settle()
                            self.stats.ids += end - start
                            self.stats.items += len(rows)
                            pending = loop.run_in_executor(None, commit, rows)
        finally:
            await settle()
            self.stats.seconds = time.monotonic() - started
        return self.stats


# -- CLI -------------------------------------------------------------------
def _epoch(date: str) -> int:
    """Midnight (store time, UTC+7) at the start of a ``YYYY-MM-DD`` date."""
    d = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=STORE_TZ)
    return int(d.timestamp())


async def plan(fetcher: Fetcher, args: argparse.Namespace) -> list[tuple[int, int]]:
    """Turn CLI arguments into id ranges ``[lo, hi)``."""
    async with fetcher.session() as session:
        newest = await fetcher.max_item(session)
        end = newest + 1
        if args.end_id:
            end = min(end, args.end_id)
        if args.until:
            end = min(end, await fetcher.find_id_at(session, _epoch(args.until), 1, newest))

        if args.mode == "repair":
            return [(lo, min(hi, end)) for lo, hi in fetcher.store.gaps()]
        if args.mode == "update":
            start = fetcher.store.max_id() + 1
        elif args.mode == "overwrite":
            start = args.start_id
        else:  # overwrite-from-date
            start = await fetcher.find_id_at(session, _epoch(args.start_date), 1, newest)
        return [(start, end)]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Hacker News -> Parquet fetcher")
    p.add_argument(
        "--mode",
        choices=["update", "overwrite", "overwrite-from-date", "repair"],
        default="update",
        help="update: continue after the newest stored id; overwrite: refetch from "
        "--start-id; overwrite-from-date: refetch from --start-date; "
        "repair: refetch ids missing between partitions",
    )
    p.add_argument("--start-id", type=int)
    p.add_argument("--start-date", help="YYYY-MM-DD, store time (UTC+7)")
    p.add_argument("--end-id", type=int, help="stop before this id (exclusive)")
    p.add_argument("--until", help="stop before this date, YYYY-MM-DD (UTC+7)")
    p.add_argument("--data-dir", default="data", help="Parquet root (default: ./data)")
    p.add_argument("--concurrency", type=int, default=Config.concurrency)
    p.add_argument(
        "--chunk-size",
        type=int,
        default=Config.chunk_size,
        help="ids per commit; bounds memory and work lost on a crash",
    )
    p.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.mode == "overwrite" and args.start_id is None:
        parser.error("--start-id is required for overwrite mode")
    if args.mode == "overwrite-from-date" and not args.start_date:
        parser.error("--start-date is required for overwrite-from-date mode")

    fetcher = Fetcher(Config(args.data_dir, args.concurrency, args.chunk_size))

    async def go() -> int:
        ranges = await plan(fetcher, args)
        total = sum(max(0, hi - lo) for lo, hi in ranges)
        print(
            f"plan: {len(ranges)} range(s), {total:,} ids"
            + (f" [{ranges[0][0]}..{ranges[-1][1]})" if ranges else "")
        )
        if args.dry_run or not total:
            return 0
        print((await fetcher.run(ranges)).report())
        return 0

    try:
        try:
            import uvloop  # optional speed-up

            run = uvloop.run
        except ImportError:
            run = asyncio.run
        return run(go())
    except KeyboardInterrupt:
        print("\ninterrupted; completed chunks are saved, rerun to continue", file=sys.stderr)
        return 130
    except FetchError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
