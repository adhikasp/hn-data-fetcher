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

# Default constants
DEFAULT_DB_NAME = "hn2.db"
DEFAULT_CONCURRENT_REQUESTS = 1000
DEFAULT_PROGRESS_UPDATE_INTERVAL = 1000
DEFAULT_DB_QUEUE_SIZE = 1000
DEFAULT_DB_COMMIT_INTERVAL = 1000
DEFAULT_TCP_LIMIT = 0


def create_db(db_name):
    with sqlite3.connect(db_name) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS hn_items(id int PRIMARY KEY, item_json blob, time text)"
        )
        db.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode at DB creation
        db.execute("PRAGMA synchronous=NORMAL")  # Optimize write performance
        db.execute("PRAGMA cache_size=10000")  # Increase cache size
        db.commit()


def get_last_id(db_name):
    with sqlite3.connect(db_name) as db:
        cursor = db.execute("select max(id) from hn_items")
        rows = cursor.fetchall()
        return int(rows[0][0]) if rows[0][0] else 0

def get_first_id(db_name) -> int:
    with sqlite3.connect(db_name) as db:
        cursor = db.execute("select min(id) from hn_items")
        rows = cursor.fetchall()
        return int(rows[0][0]) - 1 if rows[0][0] else 0

async def get_max_id():
    async with aiohttp.ClientSession(connector=TCPConnector(limit=0)) as session:
        async with session.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json"
        ) as response:
            text = await response.text()
    return json.loads(text)


def db_writer_worker(db_name, input_queue, commit_interval):
    with sqlite3.connect(db_name, isolation_level=None) as db:
        db.execute('pragma journal_mode=wal;')
        db.execute('pragma synchronous=normal;')  # Changed from 1 to normal
        db.execute('pragma cache_size=10000;')
        db.execute('BEGIN;')  # Start transaction
        count = 0
        while True:
            data = input_queue.get()
            if data is None:
                db.execute('COMMIT;')  # Commit final transaction
                break
            item, item_json = data
            try:
                json_data = json.loads(item_json)
                if json_data and isinstance(json_data, dict) and "time" in json_data:
                    time = json_data["time"]
                    iso_time = datetime.fromtimestamp(time).isoformat()
                    db.execute(
                        """INSERT OR REPLACE INTO hn_items(id, item_json, time) 
                        VALUES(?, ?, ?)""", 
                        (item, item_json, iso_time)
                    )
                    count += 1
                    if count % commit_interval == 0:
                        db.execute('COMMIT;')
                        db.execute('BEGIN;')
            except (json.JSONDecodeError, TypeError):
                # Skip items with invalid JSON or None values
                continue

def get_current_processed_time(db_name: str, id: str, order) -> str:
    with sqlite3.connect(db_name) as db:
        r = db.execute(f"select time from hn_items order by id {order} limit 1")
        rows = r.fetchall()
        return rows[0][0] if len(rows) > 0 else ""

async def fetch_and_save(session, db_queue, sem, id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                text = await response.text()
                # Only queue valid JSON responses
                if text and text.strip() and text.strip().lower() != "null":
                    db_queue.put((id, text))
            else:
                print(f"Error fetching item {id}: HTTP {response.status}")
    except Exception as e:
        print(f"Exception fetching item {id}: {e}")
    finally:
        sem.release()


async def run(db_queue, db_name: str, concurrent_requests: int, update_interval: int, tcp_limit: int, mode: str = "backfill", start_id: int = None):
    """
    Args:
        db_queue: Queue for database operations
        db_name: Name of the SQLite database file
        concurrent_requests: Number of concurrent API requests
        update_interval: Progress update interval
        tcp_limit: Maximum number of TCP connections
        mode: Operation mode - 'backfill', 'update', or 'overwrite'
        start_id: Starting ID for overwrite mode
    """
    create_db(db_name)
    if mode == "update":
        last_id = get_last_id(db_name)
        max_id = await get_max_id()
    elif mode == "backfill":
        max_id = get_first_id(db_name)
        first_id = 1
    elif mode == "overwrite":
        if start_id is None:
            raise ValueError("start_id must be provided for overwrite mode")
        max_id = await get_max_id()
        first_id = start_id

    sem = asyncio.Semaphore(concurrent_requests)

    async with aiohttp.ClientSession(connector=TCPConnector(limit=tcp_limit)) as session:
        tasks = []
        if mode == "backfill":
            for id in (pbar := tqdm(range(max_id, first_id, -1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(db_name, str(id), order="asc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                task = asyncio.create_task(fetch_and_save(session, db_queue, sem, id))
                tasks.append(task)
        elif mode == "update":
            for id in (pbar := tqdm(range(last_id + 1, max_id, 1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(db_name, str(id), order="desc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                task = asyncio.create_task(fetch_and_save(session, db_queue, sem, id))
                tasks.append(task)
        elif mode == "overwrite":
            for id in (pbar := tqdm(range(first_id, max_id + 1, 1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(db_name, str(id), order="desc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                task = asyncio.create_task(fetch_and_save(session, db_queue, sem, id))
                tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        for i in range(concurrent_requests):
            await sem.acquire()


import signal
import asyncio

def signal_handler(sig, frame):
    print("\nCtrl+C pressed. Terminating...")
    loop = asyncio.get_running_loop()
    loop.stop()

signal.signal(signal.SIGINT, signal_handler)

async def main(db_name: str, concurrent_requests: int, update_interval: int, db_queue_size: int, db_commit_interval: int, tcp_limit: int, mode: str, start_id: int = 0):
    if mode not in ["backfill", "update", "overwrite"]:
        raise ValueError(f"Invalid mode: {mode}. Must be one of: backfill, update, overwrite")

    if mode == "overwrite" and start_id == 0:
        raise ValueError("start_id must be provided when mode is 'overwrite'")

    db_queue = queue.Queue(maxsize=db_queue_size)
    db_thread = threading.Thread(target=db_writer_worker, args=(db_name, db_queue, db_commit_interval))
    db_thread.start()

    try:
        await run(db_queue, db_name, concurrent_requests, update_interval, tcp_limit, mode=mode, start_id=start_id)
    except KeyboardInterrupt:
        print("\nCtrl+C pressed. Terminating...")
    except asyncio.CancelledError:
        print("\nAsyncio tasks cancelled. Cleaning up...")
    finally:
        # Cancel all running tasks
        for task in asyncio.all_tasks():
            if task is not asyncio.current_task():
                task.cancel()
        
        # Wait for all items in the queue to be processed
        while not db_queue.empty():
            await asyncio.sleep(0.1)
        db_queue.put(None)
        db_thread.join()
        print("Cleanup complete. Exiting.")

def cli() -> None:
    """Command line interface for the Hacker News data fetcher."""
    parser = argparse.ArgumentParser(description='Hacker News data fetcher')
    parser.add_argument('--mode', type=str, choices=['backfill', 'update', 'overwrite'],
                       default='update', 
                       help='Operation mode: update (fetch new items), backfill (fetch historical items), or overwrite (update existing items from start_id)')
    parser.add_argument('--start-id', type=int,
                       help='Starting ID for overwrite mode')
    parser.add_argument('--db-name', type=str, default=DEFAULT_DB_NAME,
                       help=f'Path to SQLite database file to store HN items (default: {DEFAULT_DB_NAME})')
    parser.add_argument('--concurrent-requests', type=int, default=DEFAULT_CONCURRENT_REQUESTS,
                       help=f'Maximum number of concurrent API requests to HN. Higher values speed up fetching but may hit rate limits (default: {DEFAULT_CONCURRENT_REQUESTS})')
    parser.add_argument('--update-interval', type=int, default=DEFAULT_PROGRESS_UPDATE_INTERVAL,
                       help=f'How often to update the progress bar, in number of items processed. Lower values give more frequent updates but may impact performance (default: {DEFAULT_PROGRESS_UPDATE_INTERVAL})')
    parser.add_argument('--db-queue-size', type=int, default=DEFAULT_DB_QUEUE_SIZE,
                       help=f'Maximum size of database operation queue (default: {DEFAULT_DB_QUEUE_SIZE})')
    parser.add_argument('--db-commit-interval', type=int, default=DEFAULT_DB_COMMIT_INTERVAL,
                       help=f'How often to commit database transactions, in number of items (default: {DEFAULT_DB_COMMIT_INTERVAL})')
    parser.add_argument('--tcp-limit', type=int, default=DEFAULT_TCP_LIMIT,
                       help=f'Maximum number of TCP connections. 0 means unlimited (default: {DEFAULT_TCP_LIMIT})')
    args = parser.parse_args()

    if args.mode == 'overwrite' and args.start_id is None:
        parser.error("--start-id is required when mode is 'overwrite'")

    try:
        asyncio.run(main(args.db_name, args.concurrent_requests, args.update_interval, args.db_queue_size, args.db_commit_interval, args.tcp_limit, args.mode, args.start_id))
    except RuntimeError:
        print("An error occurred while running the event loop.")
    finally:
        print("Script execution completed.")

if __name__ == "__main__":
    cli()