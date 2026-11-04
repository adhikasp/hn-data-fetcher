"""Sync the local Parquet store to a Tigris/S3 bucket.

A partition is uploaded when it is missing remotely or its size differs, so the
command is idempotent and picks up every changed month (not just the newest two).
Credentials come from the environment / ``.env``:
``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_ENDPOINT_URL_S3``,
``AWS_REGION`` and ``S3_BUCKET_NAME``.
"""

from __future__ import annotations

import argparse
import os
import sys

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from dotenv import load_dotenv
from rich.console import Console

from .store import Partition, Store

console = Console()


def get_s3_client():
    """S3 client for Tigris, configured from the environment."""
    load_dotenv()
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL_S3"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION") or "auto",
        config=Config(s3={"addressing_style": "path"}),
    )


def remote_sizes(s3_client, bucket: str) -> dict[str, int]:
    """Map of object key -> size for everything in the bucket."""
    sizes: dict[str, int] = {}
    for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            sizes[obj["Key"]] = obj["Size"]
    return sizes


def pending_uploads(parts: list[Partition], remote: dict[str, int]) -> list[Partition]:
    """Partitions that are missing from, or differ in size from, the bucket."""
    return [p for p in parts if remote.get(p.s3_key) != p.path.stat().st_size]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upload Parquet partitions to Tigris")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--dry-run", action="store_true", help="list what would be uploaded")
    args = parser.parse_args(argv)

    load_dotenv()
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        console.print("[red]S3_BUCKET_NAME must be set[/red]")
        return 2

    parts = Store(args.data_dir).partitions()
    if not parts:
        console.print(f"[yellow]No parquet files under {args.data_dir}[/yellow]")
        return 0

    s3 = get_s3_client()
    todo = pending_uploads(parts, remote_sizes(s3, bucket))
    console.print(f"{len(parts)} local partitions, {len(todo)} to upload to [bold]{bucket}[/bold]")
    transfer = TransferConfig(multipart_threshold=64 * 1024**2, max_concurrency=8)
    failed = 0
    for part in todo:
        console.print(
            f"[cyan]{'would upload' if args.dry_run else 'uploading'} {part.s3_key}[/cyan]"
        )
        if args.dry_run:
            continue
        try:
            s3.upload_file(str(part.path), bucket, part.s3_key, Config=transfer)
        except Exception as e:  # noqa: BLE001 - report and continue with the rest
            console.print(f"[red]failed {part.s3_key}: {e}[/red]")
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
