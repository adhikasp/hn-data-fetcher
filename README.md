# Hacker News Data Fetcher

A tool to fetch and store Hacker News data in a SQLite database.

## Installation

To install the Hacker News Data Fetcher, follow these steps:

1. **Install**:
    ```sh
    pip install hn-data-fetcher
    ```

2. **Run the Script**:
    - The script can be run in four different modes: `update`, `backfill`, `overwrite`, and `overwrite-from-date`.
    - Use the following command to run the script:
      ```sh
      hn_data_fetcher --mode <mode> [--start-id <start_id>] [--start-date <start_date>] [--db-name <db_name>] [--concurrent-requests <concurrent_requests>] [--update-interval <update_interval>] [--db-queue-size <db_queue_size>] [--db-commit-interval <db_commit_interval>] [--tcp-limit <tcp_limit>]
      ```
    - **Parameters**:
      - `--mode`: Operation mode. Choices are `update`, `backfill`, `overwrite`, or `overwrite-from-date`.
      - `--start-id`: Starting ID for `overwrite` mode (required if mode is `overwrite`).
      - `--start-date`: Starting date for `overwrite-from-date` mode in YYYY-MM-DD format (required if mode is `overwrite-from-date`).
      - `--db-name`: Path to the SQLite database file to store HN items (default: `hn2.db`).
      - `--concurrent-requests`: Maximum number of concurrent API requests to HN (default: `1000`).
      - `--update-interval`: How often to update the progress bar, in number of items processed (default: `1000`).
      - `--db-queue-size`: Maximum size of the database operation queue (default: `1000`).
      - `--db-commit-interval`: How often to commit database transactions, in number of items (default: `1000`).
      - `--tcp-limit`: Maximum number of TCP connections. `0` means unlimited (default: `0`).

    - **Examples**:
      - To update the database with new items:
        ```sh
        hn-data-fetcher --mode update
        ```
      - To backfill the database with historical items:
        ```sh
        hn-data-fetcher --mode backfill
        ```
      - To overwrite existing items starting from a specific ID:
        ```sh
        hn-data-fetcher --mode overwrite --start-id 1000
        ```
      - To overwrite existing items starting from a specific date:
        ```sh
        hn-data-fetcher --mode overwrite-from-date --start-date 2024-01-01
        ```

3. **Monitor Progress**:
    - The script provides a progress bar with an estimated time of arrival (ETA) for completion.
    - It also handles errors gracefully and ensures that the database is updated correctly.

4. **Graceful Shutdown**:
    - You can stop the script at any time by pressing `Ctrl+C`. The script will handle the shutdown gracefully, ensuring that all ongoing transactions are completed.



## Local Development

1. **Install Development Dependencies**:
    - Install the package in editable mode and development dependencies:
      ```sh
      pip install -e .
      pip install -r requirements-dev.txt
      ```

2. **Run Tests**:
    - Execute the test suite:
      ```sh
      pytest tests/ -v
      ```
