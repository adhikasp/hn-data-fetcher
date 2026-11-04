# Hacker News Data Fetcher

Fetches Hacker News items from the official Firebase API straight into
Hive-partitioned Parquet files (`data/year=YYYY/month=M/data.parquet`), and can
sync them to a Tigris/S3 bucket. No intermediate database.

```sh
pip install -e '.[fast]'        # 'fast' adds uvloop
```

## Fetch

```sh
hn-data-fetcher                              # update: continue after the newest stored id
hn-data-fetcher --until 2025-04-16           # ...but stop at a date (a bounded test run)
hn-data-fetcher --mode overwrite-from-date --start-date 2025-03-01
hn-data-fetcher --mode overwrite --start-id 43372773
hn-data-fetcher --mode repair                # refetch ids missing between partitions
hn-data-fetcher --dry-run                    # show the plan only
```

Options: `--data-dir` (default `./data`), `--concurrency` (default 2500; HN starts
resetting connections around 6000), `--chunk-size` (ids per commit, default 200k),
`--end-id`, `--until`.

- **Resume** is automatic: the highest stored id is the resume point. A chunk is
  committed only after *every* id in it was fetched (transient errors are retried
  with backoff), and each partition is replaced atomically, so Ctrl+C or a crash
  loses at most one chunk and never leaves a torn file.
- **Time** is stored as a naive timestamp in **UTC+7** and partitioned by it, to match
  the original dataset. `dead` / `deleted` are booleans (null when false).
- Throughput is about 3,400-3,900 ids/s on a home connection (~320k ids/month of HN).

## Upload

```sh
cp .env.example .env     # AWS_*, S3_BUCKET_NAME
upload-to-tigris         # uploads partitions that are missing or differ in size
upload-to-tigris --dry-run
delete-from-tigris --dry-run   # destructive; wipes the bucket without --dry-run
```

## Development

```sh
pip install -e '.[fast,dev]'
pytest          # offline, uses a fake HN API
ruff check . && ruff format .
```
