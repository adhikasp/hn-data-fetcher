# Hacker News Data Fetcher

A high-performance asynchronous tool for fetching and storing Hacker News items in a SQLite database.

## Features

- **Asynchronous Processing**: Uses Python's asyncio and aiohttp for efficient concurrent API requests
- **Flexible Operation Modes**:
  - `update`: Fetch new items since last run
  - `backfill`: Fetch historical items
  - `overwrite`: Update existing items from a specified ID
- **Performance Optimized**:
  - Configurable concurrent requests
  - SQLite WAL mode for better write performance
  - Batched database commits
  - Progress tracking with ETA
- **Robust Error Handling**:
  - Graceful shutdown on Ctrl+C
  - Connection error recovery
  - Transaction management

## Usage

To use the Hacker News Data Fetcher, follow these steps:

1. **Clone the Repository**:
    ```sh
    git clone git@github.com:adhikasp/hn-data-fetcher.git
    cd hn-data-fetcher
    ```

2. **Set Up the Environment**:
    - Ensure you have Python 3.7+ installed.
    - Create a virtual environment and activate it:
      ```sh
      python -m venv venv
      source venv/bin/activate  # On Windows use `venv\Scripts\activate`
      ```
    - Install the required dependencies:
      ```sh
      pip install -r requirements.txt
      ```

3. **Run the Script**:
    - The script can be run in three different modes: `update`, `backfill`, and `overwrite`.
    - Use the following command to run the script:
      ```sh
      python hn_async2.py --mode <mode> [--start-id <start_id>] [--db-name <db_name>] [--concurrent-requests <concurrent_requests>] [--update-interval <update_interval>] [--db-queue-size <db_queue_size>] [--db-commit-interval <db_commit_interval>] [--tcp-limit <tcp_limit>]
      ```
    - **Parameters**:
      - `--mode`: Operation mode. Choices are `update`, `backfill`, or `overwrite`.
      - `--start-id`: Starting ID for `overwrite` mode (required if mode is `overwrite`).
      - `--db-name`: Path to the SQLite database file to store HN items (default: `hn2.db`).
      - `--concurrent-requests`: Maximum number of concurrent API requests to HN (default: `1000`).
      - `--update-interval`: How often to update the progress bar, in number of items processed (default: `1000`).
      - `--db-queue-size`: Maximum size of the database operation queue (default: `1000`).
      - `--db-commit-interval`: How often to commit database transactions, in number of items (default: `1000`).
      - `--tcp-limit`: Maximum number of TCP connections. `0` means unlimited (default: `0`).

    - **Examples**:
      - To update the database with new items:
        ```sh
        python hn_async2.py --mode update
        ```
      - To backfill the database with historical items:
        ```sh
        python hn_async2.py --mode backfill
        ```
      - To overwrite existing items starting from a specific ID:
        ```sh
        python hn_async2.py --mode overwrite --start-id 1000
        ```

4. **Monitor Progress**:
    - The script provides a progress bar with an estimated time of arrival (ETA) for completion.
    - It also handles errors gracefully and ensures that the database is updated correctly.

5. **Graceful Shutdown**:
    - You can stop the script at any time by pressing `Ctrl+C`. The script will handle the shutdown gracefully, ensuring that all ongoing transactions are completed.


