from dataclasses import dataclass
from typing import Optional, Literal
import asyncio
import aiohttp
from tqdm import tqdm
import json
import sqlite3
import queue
import threading
from datetime import datetime
import argparse
from aiohttp import TCPConnector


@dataclass
class Config:
    """Configuration for the HN data fetcher."""

    db_name: str = "hn2.db"
    concurrent_requests: int = 1000
    progress_update_interval: int = 1000
    db_queue_size: int = 1000
    db_commit_interval: int = 1000
    tcp_limit: int = 0


class HNDatabase:
    """Handles database operations for HN items."""

    def __init__(self, db_name: str):
        self.db_name = db_name
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database with required tables and optimizations."""
        with sqlite3.connect(self.db_name) as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS hn_items(
                    id INTEGER PRIMARY KEY,
                    item_json BLOB,
                    time TEXT
                )"""
            )
            db.execute("CREATE INDEX IF NOT EXISTS idx_hn_items_time ON hn_items(time)")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA cache_size=10000")

    def get_boundary_id(self, boundary: Literal["min", "max"]) -> int:
        """Get the minimum or maximum item ID from the database."""
        with sqlite3.connect(self.db_name) as db:
            cursor = db.execute(f"SELECT {boundary}(id) FROM hn_items")
            result = cursor.fetchone()[0]
            return int(result) if result else 0

    def get_id_from_date(self, target_date: str) -> int:
        """Find the earliest item ID from a given date."""
        date_start = f"{target_date.split('T')[0]}T00:00:00"
        date_end = f"{target_date.split('T')[0]}T23:59:59"

        with sqlite3.connect(self.db_name) as db:
            cursor = db.execute(
                "SELECT MIN(id) FROM hn_items WHERE time >= ? AND time <= ?",
                (date_start, date_end),
            )
            result = cursor.fetchone()[0]
            return int(result) if result else 0

    def get_current_time(self, order: Literal["asc", "desc"]) -> str:
        """Get the timestamp of the most recent or oldest item."""
        with sqlite3.connect(self.db_name) as db:
            cursor = db.execute(
                f"SELECT time FROM hn_items ORDER BY id {order} LIMIT 1"
            )
            result = cursor.fetchone()
            return result[0] if result else ""


class HNFetcher:
    """Handles fetching and processing of HN items."""

    def __init__(self, config: Config):
        self.config = config
        self.db = HNDatabase(config.db_name)
        self.db_queue: queue.Queue = queue.Queue(maxsize=config.db_queue_size)

    async def get_max_item_id(self) -> int:
        """Fetch the current maximum item ID from HN API."""
        async with aiohttp.ClientSession(connector=TCPConnector(limit=0)) as session:
            async with session.get(
                "https://hacker-news.firebaseio.com/v0/maxitem.json"
            ) as response:
                return int(await response.text())

    def db_writer(self) -> None:
        """Database writer worker that processes items from the queue."""
        with sqlite3.connect(self.config.db_name, isolation_level=None) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA cache_size=10000")
            db.execute("BEGIN")

            count = 0
            while True:
                data = self.db_queue.get()
                if data is None:
                    db.execute("COMMIT")
                    break

                item_id, item_json = data
                try:
                    json_data = json.loads(item_json)
                    if isinstance(json_data, dict) and "time" in json_data:
                        iso_time = datetime.fromtimestamp(json_data["time"]).isoformat()
                        db.execute(
                            "INSERT OR REPLACE INTO hn_items(id, item_json, time) VALUES(?, ?, ?)",
                            (item_id, item_json, iso_time),
                        )
                        count += 1
                        if count % self.config.db_commit_interval == 0:
                            db.execute("COMMIT")
                            db.execute("BEGIN")
                except (json.JSONDecodeError, TypeError):
                    continue

    async def fetch_item(
        self, session: aiohttp.ClientSession, sem: asyncio.Semaphore, item_id: int
    ) -> None:
        """Fetch a single HN item."""
        try:
            async with sem, session.get(
                f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
            ) as response:
                if response.status == 200:
                    text = await response.text()
                    if text and text.strip() and text.strip().lower() != "null":
                        self.db_queue.put((item_id, text))
        except Exception as e:
            print(f"Error fetching item {item_id}: {e}")

    async def process_items(
        self,
        mode: Literal["backfill", "update", "overwrite", "overwrite-from-date"],
        start_id: Optional[int] = None,
        start_date: Optional[str] = None,
    ) -> None:
        """Process items based on the specified mode."""
        max_id = await self.get_max_item_id()

        # Determine start and end IDs based on mode
        if mode == "update":
            start = self.db.get_boundary_id("max") + 1
            end = max_id
            step = 1
        elif mode == "backfill":
            start = self.db.get_boundary_id("min") - 1
            end = 1
            step = -1
        elif mode == "overwrite":
            if not start_id:
                raise ValueError("start_id required for overwrite mode")
            start = start_id
            end = max_id
            step = 1
        else:  # overwrite-from-date
            if not start_date:
                raise ValueError("start_date required for overwrite-from-date mode")
            start = self.db.get_id_from_date(start_date)
            if start == 0:
                raise ValueError(f"No items found from date {start_date}")
            end = max_id
            step = 1

        # Start database writer thread
        db_thread = threading.Thread(target=self.db_writer)
        db_thread.start()

        try:
            sem = asyncio.Semaphore(self.config.concurrent_requests)
            async with aiohttp.ClientSession(
                connector=TCPConnector(limit=self.config.tcp_limit)
            ) as session:
                tasks = []
                active_tasks = set()

                for item_id in (pbar := tqdm(range(start, end + step, step))):
                    if item_id % self.config.progress_update_interval == 0:
                        order = "desc" if step > 0 else "asc"
                        current_time = self.db.get_current_time(order)
                        pbar.set_description(f"Processing: {current_time}")

                    # Create the task
                    task = asyncio.create_task(self.fetch_item(session, sem, item_id))
                    tasks.append(task)
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)

                    # Wait if we've reached the concurrent request limit
                    while len(active_tasks) >= self.config.concurrent_requests:
                        await asyncio.sleep(0.01)

                # Wait for all remaining tasks to complete
                await asyncio.gather(*tasks)
        finally:
            while not self.db_queue.empty():
                await asyncio.sleep(0.1)
            self.db_queue.put(None)
            db_thread.join()


def main() -> None:
    """Main entry point for the HN data fetcher."""
    parser = argparse.ArgumentParser(description="Hacker News data fetcher")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["backfill", "update", "overwrite", "overwrite-from-date"],
        default="update",
        help="Operation mode",
    )
    parser.add_argument("--start-id", type=int, help="Starting ID for overwrite mode")
    parser.add_argument("--start-date", type=str, help="Starting date (YYYY-MM-DD)")
    parser.add_argument("--db-name", type=str, help="SQLite database file path")
    parser.add_argument(
        "--concurrent-requests", type=int, help="Max concurrent API requests"
    )
    parser.add_argument("--update-interval", type=int, help="Progress update interval")
    parser.add_argument("--db-queue-size", type=int, help="Database queue size")
    parser.add_argument(
        "--db-commit-interval", type=int, help="Database commit interval"
    )
    parser.add_argument(
        "--tcp-limit", type=int, help="Max TCP connections (0=unlimited)"
    )

    args = parser.parse_args()

    if args.mode == "overwrite" and args.start_id is None:
        parser.error("--start-id required for overwrite mode")

    # Create config with CLI overrides
    config = Config()
    for key, value in vars(args).items():
        if value is not None and hasattr(config, key):
            setattr(config, key, value)

    try:
        fetcher = HNFetcher(config)
        asyncio.run(fetcher.process_items(args.mode, args.start_id, args.start_date))
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        print("Completed")


if __name__ == "__main__":
    main()
