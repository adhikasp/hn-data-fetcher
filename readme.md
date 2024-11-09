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
