# Hacker News Data Fetcher

A high-performance tool to fetch and store Hacker News data in Parquet format.

## Installation

To install the Hacker News Data Fetcher, follow these steps:

1. **Install**:
    ```sh
    pip install hn-data-fetcher
    ```

2. **Run the Script**:
    - The script can be run in three different modes: `update`, `backfill`, and `overwrite`.
    - Use the following command to run the script:
      ```sh
      hn-data-fetcher --mode <mode> [--start-id <start_id>] [--data-dir <data_dir>] [--concurrent-requests <concurrent_requests>] [--update-interval <update_interval>] [--batch-size <batch_size>] [--tcp-limit <tcp_limit>]
      ```
    - **Parameters**:
      - `--mode`: Operation mode. Choices are `update`, `backfill`, or `overwrite`.
      - `--start-id`: Starting ID for `overwrite` mode (required if mode is `overwrite`).
      - `--data-dir`: Directory to store Parquet files (default: `hn_data`).
      - `--concurrent-requests`: Maximum number of concurrent API requests to HN (default: `1000`).
      - `--update-interval`: How often to update the progress bar, in number of items processed (default: `1000`).
      - `--batch-size`: Number of items to write in each Parquet batch (default: `1000`).
      - `--tcp-limit`: Maximum number of TCP connections. `0` means unlimited (default: `0`).

    - **Examples**:
      - To update with new items:
        ```sh
        hn-data-fetcher --mode update
        ```
      - To backfill with historical items:
        ```sh
        hn-data-fetcher --mode backfill
        ```
      - To overwrite existing items starting from a specific ID:
        ```sh
        hn-data-fetcher --mode overwrite --start-id 1000
        ```

3. **Monitor Progress**:
    - The script provides a progress bar with an estimated time of arrival (ETA) for completion.
    - It also handles errors gracefully and ensures that the data is written correctly.

4. **Graceful Shutdown**:
    - You can stop the script at any time by pressing `Ctrl+C`. The script will handle the shutdown gracefully, ensuring that all ongoing transactions are completed.

## Data Storage

The fetched data is stored in Parquet format, which offers several advantages:

- Efficient columnar storage for better query performance
- Compression to reduce storage space
- Schema evolution support
- Compatibility with big data tools (Spark, Dask, etc.)

Each Parquet file contains a batch of HN items with the following schema:
- `id`: Item ID
- `time`: Unix timestamp
- `iso_time`: ISO formatted timestamp
- Other HN item fields (type, title, url, etc.)

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
