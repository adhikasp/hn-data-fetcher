import asyncio
import aiohttp
from tqdm import tqdm
import json
import sqlite3
import queue
import threading
from datetime import datetime
import argparse

# Default constants
DEFAULT_DB_NAME = "hn2.db"
DEFAULT_CONCURRENT_REQUESTS = 300
DEFAULT_PROGRESS_UPDATE_INTERVAL = 1000


def create_db(db_name):
    with sqlite3.connect(db_name) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS hn_items(id int PRIMARY KEY, item_json blob, time text)"
        )
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
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json"
        ) as response:
            text = await response.text()
    return json.loads(text)


def db_writer_worker(db_name, input_queue):
    with sqlite3.connect(db_name, isolation_level=None) as db:
        db.execute('pragma journal_mode=wal;')
        db.execute('pragma synchronous=1;')
        while True:
            data = input_queue.get()
            if data is None:
                break
            item, item_json = data
            if "time" in json.loads(item_json):
                time = json.loads(item_json)["time"]
                iso_time = datetime.fromtimestamp(time).isoformat()
                db.execute(
                    """INSERT OR REPLACE INTO hn_items(id, item_json, time) 
                    VALUES(?, ?, ?)""", 
                    (item, item_json, iso_time)
                )

def get_current_processed_time(db_name: str, id: str, order) -> str:
    with sqlite3.connect(db_name) as db:
        r = db.execute(f"select time from hn_items order by id {order} limit 1")
        return r.fetchall()[0][0]

async def fetch_and_save(session, db_queue, sem, id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
    try:
        async with session.get(url) as response:
            text = await response.text()
            db_queue.put((id, text))
    except Exception as e:
        print(e)
    finally:
        sem.release()


async def run(db_queue, db_name: str, concurrent_requests: int, update_interval: int, mode: str = "backfill", start_id: int = None):
    """
    Args:
        db_queue: Queue for database operations
        db_name: Name of the SQLite database file
        concurrent_requests: Number of concurrent API requests
        update_interval: Progress update interval
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

    async with aiohttp.ClientSession() as session:
        if mode == "backfill":
            for id in (pbar := tqdm(range(max_id, first_id, -1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(db_name, str(id), order="asc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                asyncio.create_task(fetch_and_save(session, db_queue, sem, id))
        elif mode == "update":
            for id in (pbar := tqdm(range(last_id + 1, max_id, 1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(db_name, str(id), order="desc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                asyncio.create_task(fetch_and_save(session, db_queue, sem, id))
        elif mode == "overwrite":
            for id in (pbar := tqdm(range(first_id, max_id + 1, 1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(db_name, str(id), order="desc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                asyncio.create_task(fetch_and_save(session, db_queue, sem, id))

        # Wait for all tasks to complete
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
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

async def main(db_name: str, concurrent_requests: int, update_interval: int, mode: str, start_id: int = None):
    db_queue = queue.Queue()
    db_thread = threading.Thread(target=db_writer_worker, args=(db_name, db_queue))
    db_thread.start()

    try:
        await run(db_queue, db_name, concurrent_requests, update_interval, mode=mode, start_id=start_id)
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

if __name__ == "__main__":
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
    args = parser.parse_args()

    if args.mode == 'overwrite' and args.start_id is None:
        parser.error("--start-id is required when mode is 'overwrite'")

    try:
        asyncio.run(main(args.db_name, args.concurrent_requests, args.update_interval, args.mode, args.start_id))
    except RuntimeError:
        print("An error occurred while running the event loop.")
    finally:
        print("Script execution completed.")
