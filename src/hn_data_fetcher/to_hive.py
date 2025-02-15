import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import calendar

# Connect to SQLite database
engine = create_engine("sqlite:////mnt/e/Workspace/script/hn/hn2.db")


# Create base output directory
base_dir = "data_parquet"
os.makedirs(base_dir, exist_ok=True)

# Start from current date
current_date = datetime.now()
start_date = current_date.replace(day=1)  # First day of current month

while True:
    year = start_date.year
    month = start_date.month

    # Create partition directory path
    partition_dir = f"{base_dir}/year={year}/month={month}"
    partition_path = f"{partition_dir}/data.parquet"

    # Process current month and last month if within 7 days
    is_processing_current_month = (
        current_date.year == year and current_date.month == month
    )
    is_processing_last_month = current_date.month - 1 == month and current_date.day <= 7
    if not os.path.exists(partition_path) or (
        is_processing_current_month or is_processing_last_month
    ):
        print(f"Processing {year}-{month:02d}")
        # Calculate start and end timestamps for the month
        _, last_day = calendar.monthrange(year, month)
        end_date = start_date.replace(day=last_day)

        # Query data for this month using date strings
        query = f"""
        SELECT 
            id,
            time,
            json_extract(item_json, '$.by') as by,
            json_extract(item_json, '$.title') as title,
            json_extract(item_json, '$.url') as url,
            json_extract(item_json, '$.text') as text,
            json_extract(item_json, '$.score') as score,
            json_extract(item_json, '$.type') as type,
            json_extract(item_json, '$.parent') as parent,
            json_extract(item_json, '$.poll') as poll,
            json_extract(item_json, '$.parts') as parts,
            json_extract(item_json, '$.descendants') as descendants,
            json_extract(item_json, '$.dead') as dead,
            json_extract(item_json, '$.deleted') as deleted,
            json_extract(item_json, '$.kids') as kids
        FROM hn_items 
        WHERE time >= '{start_date.strftime('%Y-%m-%d')}'
        AND time <= '{end_date.strftime('%Y-%m-%d')}'
        """

        df = pd.read_sql_query(query, engine)

        # Stop processing if no data found
        if df.empty:
            print(f"No data found for {year}-{month:02d}")
            break

        # Convert JSON string arrays to actual lists
        df["kids"] = df["kids"].apply(lambda x: eval(x) if pd.notna(x) else None)
        df["parts"] = df["parts"].apply(lambda x: eval(x) if pd.notna(x) else None)

        # Convert numeric fields
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        df["descendants"] = pd.to_numeric(df["descendants"], errors="coerce")
        df["parent"] = pd.to_numeric(df["parent"], errors="coerce")
        df["poll"] = pd.to_numeric(df["poll"], errors="coerce")
        # Convert time column to datetime
        df["time"] = pd.to_datetime(df["time"])

        # Convert boolean fields
        df["dead"] = df["dead"].map({"1": True, "0": False})
        df["deleted"] = df["deleted"].map({"1": True, "0": False})

        print(f"Number of records: {len(df)}")

        if not df.empty:
            # Create partition directory
            os.makedirs(partition_dir, exist_ok=True)

            # Write parquet file
            df.to_parquet(partition_path, index=False)
            print(f"Saved partition for {year}-{month:02d}")
        else:
            print(f"No data found for {year}-{month:02d}")
    else:
        print(f"Partition already exists for {year}-{month:02d}")

    # Move to previous month
    start_date = (start_date - timedelta(days=1)).replace(day=1)

print("Data has been partitioned and saved in Hive format")
