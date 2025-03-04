#!/usr/bin/env python3

import os
from pathlib import Path
from typing import List, NamedTuple
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

console = Console()


class ParquetFile(NamedTuple):
    """Represents a parquet file with its partition information."""
    path: Path
    year: str
    month: str
    
    @property
    def date(self) -> datetime:
        """Get datetime object for sorting."""
        return datetime.strptime(f"{self.year}-{self.month}", "%Y-%m")
    
    @property
    def s3_key(self) -> str:
        """Get the S3 key for this file."""
        return f"year={self.year}/month={self.month}/data.parquet"


def get_s3_client():
    """
    Create and return an S3 client configured with Tigris credentials.
    
    Returns:
        boto3.client: Configured S3 client for Tigris
    """
    load_dotenv()
    
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL_S3"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
        config=Config(s3={"addressing_style": "path"}),
    )


def list_parquet_files(directory: str = "data_parquet") -> List[ParquetFile]:
    """
    List all parquet files in the specified directory, preserving Hive partition structure.
    
    Args:
        directory: Base directory containing Hive-partitioned parquet files
        
    Returns:
        List of ParquetFile objects containing file paths and partition info, sorted by date descending
    """
    data_dir = Path(directory)
    parquet_files = []
    
    # Walk through the directory structure
    for year_dir in data_dir.glob("year=*"):
        year = year_dir.name.split("=")[1]
        for month_dir in year_dir.glob("month=*"):
            month = month_dir.name.split("=")[1]
            for parquet_file in month_dir.glob("*.parquet"):
                parquet_files.append(ParquetFile(
                    path=parquet_file,
                    year=year,
                    month=month
                ))
    
    # Sort by date descending (newest first)
    return sorted(parquet_files, key=lambda x: x.date, reverse=True)


def should_upload_file(s3_client, parquet_file: ParquetFile, bucket: str) -> bool:
    """
    Check if file should be uploaded based on existence and if it's the newest.
    
    Args:
        s3_client: Boto3 S3 client
        parquet_file: ParquetFile containing file path and partition info
        bucket: Target bucket name
        
    Returns:
        bool: True if file should be uploaded
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=parquet_file.s3_key)
        # File exists, only upload if it's the newest or last month
        newest_date = max(parquet_files, key=lambda x: x.date).date
        # Get the second newest date (last month)
        sorted_dates = sorted(set(pf.date for pf in parquet_files), reverse=True)
        last_month_date = sorted_dates[1] if len(sorted_dates) > 1 else newest_date
        return parquet_file.date == newest_date or parquet_file.date == last_month_date
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # File doesn't exist, should upload
            return True
        else:
            # Some other error occurred
            console.print(f"[red]Error checking file existence: {str(e)}[/red]")
            return False


def upload_file(s3_client, parquet_file: ParquetFile, bucket: str = "hacker-news") -> bool:
    """
    Upload a single file to the specified S3 bucket, maintaining Hive partition structure.
    
    Args:
        s3_client: Boto3 S3 client
        parquet_file: ParquetFile containing file path and partition info
        bucket: Target bucket name
        
    Returns:
        bool: True if upload was successful
    """
    try:
        s3_client.upload_file(str(parquet_file.path), bucket, parquet_file.s3_key)
        return True
    except Exception as e:
        console.print(f"[red]Error uploading {parquet_file.path}: {str(e)}[/red]")
        return False


def main():
    """Main function to handle the upload process."""
    console.print("[bold blue]Starting upload to Tigris bucket...[/bold blue]")
    
    # Get S3 client
    s3_client = get_s3_client()
    
    # List parquet files with partition info
    global parquet_files  # Make it accessible in should_upload_file
    parquet_files = list_parquet_files()
    if not parquet_files:
        console.print("[yellow]No parquet files found in data_parquet directory[/yellow]")
        return
    
    console.print(f"[green]Found {len(parquet_files)} parquet files to process[/green]")
    
    # Upload files with progress bar
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing files...", total=len(parquet_files))
        
        for parquet_file in parquet_files:
            progress.update(
                task, 
                description=f"Processing year={parquet_file.year}/month={parquet_file.month}"
            )
            
            if should_upload_file(s3_client, parquet_file, "hacker-news"):
                console.print(f"[cyan]Uploading {parquet_file.s3_key}[/cyan]")
                upload_file(s3_client, parquet_file)
            else:
                console.print(f"[yellow]Skipping {parquet_file.s3_key} - already exists[/yellow]")
            
            progress.advance(task)
    
    console.print("[bold green]Process completed![/bold green]")


if __name__ == "__main__":
    main() 