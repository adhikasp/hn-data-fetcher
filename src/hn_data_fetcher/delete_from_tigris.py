#!/usr/bin/env python3

import os
from typing import List, Optional
from datetime import datetime

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.prompt import Confirm

console = Console()


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


def list_objects(s3_client, bucket: str = "hacker-news") -> List[dict]:
    """
    List all objects in the specified S3 bucket.
    
    Args:
        s3_client: Boto3 S3 client
        bucket: Target bucket name
        
    Returns:
        List of object information dictionaries
    """
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        objects = []
        
        for page in paginator.paginate(Bucket=bucket):
            if 'Contents' in page:
                objects.extend(page['Contents'])
        
        return objects
    except ClientError as e:
        console.print(f"[red]Error listing objects: {str(e)}[/red]")
        return []


def delete_objects(
    s3_client,
    objects: List[dict],
    bucket: str = "hacker-news",
    dry_run: bool = False
) -> bool:
    """
    Delete multiple objects from the specified S3 bucket.
    
    Args:
        s3_client: Boto3 S3 client
        objects: List of object information dictionaries
        bucket: Target bucket name
        dry_run: If True, only simulate deletion
        
    Returns:
        bool: True if all deletions were successful
    """
    if not objects:
        console.print("[yellow]No objects to delete[/yellow]")
        return True
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Deleting objects...", total=len(objects))
        
        # Process objects in batches of 1000 (S3 delete_objects limit)
        batch_size = 1000
        success = True
        
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]
            delete_keys = {'Objects': [{'Key': obj['Key']} for obj in batch]}
            
            try:
                if not dry_run:
                    response = s3_client.delete_objects(
                        Bucket=bucket,
                        Delete=delete_keys
                    )
                    
                    if 'Errors' in response:
                        for error in response['Errors']:
                            console.print(
                                f"[red]Error deleting {error['Key']}: "
                                f"{error['Code']} - {error['Message']}[/red]"
                            )
                            success = False
                
                progress.advance(task, len(batch))
            except ClientError as e:
                console.print(f"[red]Error in batch deletion: {str(e)}[/red]")
                success = False
                progress.advance(task, len(batch))
    
    return success


def main():
    """Main function to handle the deletion process."""
    console.print("[bold red]WARNING: This script will delete ALL data from the Tigris bucket![/bold red]")
    console.print("It is recommended to run with --dry-run first to preview changes.")
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Delete all data from Tigris bucket')
    parser.add_argument('--dry-run', action='store_true', help='Simulate deletion without actually deleting')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompt')
    args = parser.parse_args()
    
    if args.dry_run:
        console.print("[yellow]Running in dry-run mode - no actual deletions will occur[/yellow]")
    
    # Get S3 client
    s3_client = get_s3_client()
    
    # List all objects
    console.print("[blue]Listing objects in bucket...[/blue]")
    objects = list_objects(s3_client)
    
    if not objects:
        console.print("[yellow]No objects found in bucket[/yellow]")
        return
    
    console.print(f"[green]Found {len(objects)} objects to delete[/green]")
    
    # Show sample of objects to be deleted
    console.print("\nSample of objects to be deleted:")
    for obj in objects[:5]:
        console.print(f"- {obj['Key']}")
    if len(objects) > 5:
        console.print(f"... and {len(objects) - 5} more")
    
    # Confirm deletion
    if not args.force and not args.dry_run:
        if not Confirm.ask("\n[bold red]Are you sure you want to delete all these objects?[/bold red]"):
            console.print("[yellow]Deletion cancelled[/yellow]")
            return
    
    # Perform deletion
    if delete_objects(s3_client, objects, dry_run=args.dry_run):
        status = "simulated" if args.dry_run else "completed"
        console.print(f"[bold green]Deletion {status} successfully![/bold green]")
    else:
        console.print("[bold red]Some errors occurred during deletion[/bold red]")


if __name__ == "__main__":
    main() 