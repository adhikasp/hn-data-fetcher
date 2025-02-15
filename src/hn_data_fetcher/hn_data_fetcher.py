import asyncio
import aiohttp
from tqdm import tqdm
import json
import queue
import threading
from datetime import datetime
import argparse
from aiohttp import TCPConnector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional, Dict, Any, List

# Default constants
DEFAULT_DATA_DIR = "hn_data"
DEFAULT_CONCURRENT_REQUESTS = 1000
DEFAULT_PROGRESS_UPDATE_INTERVAL = 1000
DEFAULT_BATCH_SIZE = 1000
DEFAULT_TCP_LIMIT = 0

def ensure_data_dir(data_dir: str) -> None:
    """Ensure the data directory exists.
    
    Args:
        data_dir: Path to the data directory
    """
    Path(data_dir).mkdir(parents=True, exist_ok=True)

def get_parquet_files(data_dir: str) -> List[Path]:
    """Get all parquet files in the data directory.
    
    Args:
        data_dir: Path to the data directory
        
    Returns:
        List of parquet file paths
    """
    return sorted(Path(data_dir).glob("*.parquet"))

def get_last_id(data_dir: str) -> int:
    """Get the last ID from parquet files.
    
    Args:
        data_dir: Path to the data directory
        
    Returns:
        The highest ID found in parquet files, or 0 if no files exist
    """
    files = get_parquet_files(data_dir)
    if not files:
        return 0
    
    # Read the last file's metadata
    last_file = files[-1]
    table = pq.read_table(last_file, columns=['id'])
    if len(table) == 0:
        return 0
    return table['id'].to_numpy().max()

def get_first_id(data_dir: str) -> int:
    """Get the first ID from parquet files.
    
    Args:
        data_dir: Path to the data directory
        
    Returns:
        The lowest ID found in parquet files minus 1, or 0 if no files exist
    """
    files = get_parquet_files(data_dir)
    if not files:
        return 0
    
    # Read the first file's metadata
    first_file = files[0]
    table = pq.read_table(first_file, columns=['id'])
    if len(table) == 0:
        return 0
    return table['id'].to_numpy().min() - 1

async def get_max_id() -> int:
    """Get the maximum item ID from Hacker News API.
    
    Returns:
        The maximum item ID
    """
    async with aiohttp.ClientSession(connector=TCPConnector(limit=0)) as session:
        async with session.get(
            "https://hacker-news.firebaseio.com/v0/maxitem.json"
        ) as response:
            text = await response.text()
    return json.loads(text)

def parquet_writer_worker(data_dir: str, input_queue: queue.Queue, batch_size: int) -> None:
    """Worker thread that writes data to parquet files in Hive partitioning format.
    
    Args:
        data_dir: Path to the data directory
        input_queue: Queue containing items to write
        batch_size: Number of items to write in each batch
    """
    ensure_data_dir(data_dir)
    batch: List[Dict[str, Any]] = []
    current_partition: Optional[str] = None
    current_file: Optional[Path] = None
    
    while True:
        data = input_queue.get()
        if data is None:
            # Write remaining batch
            if batch:
                df = pd.DataFrame(batch)
                table = pa.Table.from_pandas(df)
                if current_file:
                    pq.write_table(table, current_file)
            break
            
        item_id, item_json = data
        try:
            item_data = json.loads(item_json)
            if isinstance(item_data, dict) and "time" in item_data:
                # Convert Unix timestamp to datetime
                item_time = datetime.fromtimestamp(item_data['time'])
                item_data['iso_time'] = item_time.isoformat()
                
                # Create partition path: year=YYYY/month=MM
                partition = f"year={item_time.year}/month={item_time.month:02d}"
                
                # If we're starting a new partition or batch is full, write current batch
                if partition != current_partition or len(batch) >= batch_size:
                    if batch:
                        df = pd.DataFrame(batch)
                        table = pa.Table.from_pandas(df)
                        if current_file:
                            pq.write_table(table, current_file)
                        batch = []
                    
                    # Setup new partition
                    current_partition = partition
                    partition_dir = Path(data_dir) / partition
                    partition_dir.mkdir(parents=True, exist_ok=True)
                    current_file = partition_dir / f"data.parquet"
                
                batch.append(item_data)
                
        except json.JSONDecodeError:
            print(f"Failed to decode JSON for item {item_id}")
        except Exception as e:
            print(f"Error processing item {item_id}: {e}")

def get_current_processed_time(data_dir: str, order: str = "desc") -> str:
    """Get the timestamp of the most recently processed item.
    
    Args:
        data_dir: Path to the data directory
        order: Sort order ('asc' or 'desc')
        
    Returns:
        ISO formatted timestamp of the most recent item
    """
    files = get_parquet_files(data_dir)
    if not files:
        return ""
        
    file_to_check = files[-1] if order == "desc" else files[0]
    try:
        table = pq.read_table(file_to_check, columns=['iso_time'])
        if len(table) == 0:
            return ""
        times = table['iso_time'].to_numpy()
        return str(times[-1] if order == "desc" else times[0])
    except:
        return ""

async def fetch_and_save(session: aiohttp.ClientSession, data_queue: queue.Queue, sem: asyncio.Semaphore, id: int) -> None:
    """Fetch an item from HN API and save to queue.
    
    Args:
        session: aiohttp client session
        data_queue: Queue for saving fetched items
        sem: Semaphore for controlling concurrent requests
        id: Item ID to fetch
    """
    url = f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
    try:
        async with session.get(url) as response:
            text = await response.text()
            data_queue.put((id, text))
    except Exception as e:
        print(f"Error fetching item {id}: {e}")
    finally:
        sem.release()

async def run(
    data_queue: queue.Queue,
    data_dir: str,
    concurrent_requests: int,
    update_interval: int,
    tcp_limit: int,
    mode: str = "backfill",
    start_id: Optional[int] = None
) -> None:
    """Run the data fetcher.
    
    Args:
        data_queue: Queue for data operations
        data_dir: Path to the data directory
        concurrent_requests: Number of concurrent API requests
        update_interval: Progress update interval
        tcp_limit: Maximum number of TCP connections
        mode: Operation mode ('backfill', 'update', or 'overwrite')
        start_id: Starting ID for overwrite mode
    """
    ensure_data_dir(data_dir)
    
    if mode == "update":
        last_id = get_last_id(data_dir)
        max_id = await get_max_id()
        first_id = last_id + 1
    elif mode == "backfill":
        max_id = get_first_id(data_dir)
        first_id = 1
    elif mode == "overwrite":
        if start_id is None:
            raise ValueError("start_id must be provided for overwrite mode")
        max_id = await get_max_id()
        first_id = start_id
    else:
        raise ValueError(f"Invalid mode: {mode}")

    sem = asyncio.Semaphore(concurrent_requests)
    
    async with aiohttp.ClientSession(connector=TCPConnector(limit=tcp_limit)) as session:
        tasks = []
        if mode == "backfill":
            for id in (pbar := tqdm(range(max_id, first_id - 1, -1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(data_dir, order="asc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                task = asyncio.create_task(fetch_and_save(session, data_queue, sem, id))
                tasks.append(task)
        else:  # update or overwrite
            for id in (pbar := tqdm(range(first_id, max_id + 1))):
                if id % update_interval == 0:
                    current_time = get_current_processed_time(data_dir, order="desc")
                    pbar.set_description(f"Processed item: {current_time}")
                await sem.acquire()
                task = asyncio.create_task(fetch_and_save(session, data_queue, sem, id))
                tasks.append(task)

        await asyncio.gather(*tasks)
        
        for i in range(concurrent_requests):
            await sem.acquire()

def signal_handler(sig, frame):
    """Handle Ctrl+C signal."""
    print("\nCtrl+C pressed. Terminating...")
    loop = asyncio.get_running_loop()
    loop.stop()

async def main(
    data_dir: str,
    concurrent_requests: int,
    update_interval: int,
    batch_size: int,
    tcp_limit: int,
    mode: str,
    start_id: int = 0
) -> None:
    """Main function to run the HN data fetcher.
    
    Args:
        data_dir: Path to the data directory
        concurrent_requests: Number of concurrent API requests
        update_interval: Progress update interval
        batch_size: Number of items to write in each batch
        tcp_limit: Maximum number of TCP connections
        mode: Operation mode ('backfill', 'update', or 'overwrite')
        start_id: Starting ID for overwrite mode
    """
    if mode not in ["backfill", "update", "overwrite"]:
        raise ValueError(f"Invalid mode: {mode}. Must be one of: backfill, update, overwrite")

    if mode == "overwrite" and start_id == 0:
        raise ValueError("start_id must be provided when mode is 'overwrite'")

    data_queue = queue.Queue(maxsize=batch_size * 2)
    writer_thread = threading.Thread(
        target=parquet_writer_worker,
        args=(data_dir, data_queue, batch_size)
    )
    writer_thread.start()

    try:
        await run(
            data_queue,
            data_dir,
            concurrent_requests,
            update_interval,
            tcp_limit,
            mode=mode,
            start_id=start_id
        )
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
        while not data_queue.empty():
            await asyncio.sleep(0.1)
        data_queue.put(None)
        writer_thread.join()
        print("Cleanup complete. Exiting.")

def cli() -> None:
    """Command line interface for the Hacker News data fetcher."""
    parser = argparse.ArgumentParser(description='Hacker News data fetcher')
    parser.add_argument(
        '--mode',
        type=str,
        choices=['backfill', 'update', 'overwrite'],
        default='update',
        help='Operation mode: update (fetch new items), backfill (fetch historical items), or overwrite (update existing items from start_id)'
    )
    parser.add_argument(
        '--start-id',
        type=int,
        help='Starting ID for overwrite mode'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default=DEFAULT_DATA_DIR,
        help=f'Directory to store Parquet files (default: {DEFAULT_DATA_DIR})'
    )
    parser.add_argument(
        '--concurrent-requests',
        type=int,
        default=DEFAULT_CONCURRENT_REQUESTS,
        help=f'Maximum number of concurrent API requests to HN (default: {DEFAULT_CONCURRENT_REQUESTS})'
    )
    parser.add_argument(
        '--update-interval',
        type=int,
        default=DEFAULT_PROGRESS_UPDATE_INTERVAL,
        help=f'How often to update the progress bar, in number of items processed (default: {DEFAULT_PROGRESS_UPDATE_INTERVAL})'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'Number of items to write in each Parquet batch (default: {DEFAULT_BATCH_SIZE})'
    )
    parser.add_argument(
        '--tcp-limit',
        type=int,
        default=DEFAULT_TCP_LIMIT,
        help=f'Maximum number of TCP connections. 0 means unlimited (default: {DEFAULT_TCP_LIMIT})'
    )
    args = parser.parse_args()

    if args.mode == 'overwrite' and args.start_id is None:
        parser.error("--start-id is required when mode is 'overwrite'")

    try:
        asyncio.run(main(
            args.data_dir,
            args.concurrent_requests,
            args.update_interval,
            args.batch_size,
            args.tcp_limit,
            args.mode,
            args.start_id
        ))
    except RuntimeError:
        print("An error occurred while running the event loop.")
    finally:
        print("Script execution completed.")

if __name__ == "__main__":
    cli()