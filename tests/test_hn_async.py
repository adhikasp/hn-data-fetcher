import pytest
import os
import sqlite3
import json
import time
from hn_async2 import main, get_max_id

TEST_DB = "test_hn.db"

@pytest.fixture(autouse=True)
def cleanup():
    """Remove test database before and after tests"""
    if os.path.exists(TEST_DB):
        try:
            os.remove(TEST_DB)
        except PermissionError:
            # Wait a bit and try again if file is locked
            time.sleep(1)
            os.remove(TEST_DB)
    yield
    if os.path.exists(TEST_DB):
        for attempt in range(10):
            try:
                os.remove(TEST_DB)
                break
            except PermissionError:
                # Wait a bit and try again if file is locked
                time.sleep(1)
                if attempt == 9:  # Last attempt
                    os.remove(TEST_DB)  # Final try, let exception propagate if it fails

@pytest.mark.asyncio
async def test_fetch_last_10_items():
    """Test fetching the last 10 items from HN API"""
    # Get the current max item ID
    max_id = await get_max_id()
    start_id = max_id - 10
    
    # Run the main function to fetch 10 items
    await main(
        db_name=TEST_DB,
        concurrent_requests=5,
        update_interval=2,
        db_queue_size=100,
        db_commit_interval=5,
        tcp_limit=5,
        mode="overwrite",
        start_id=start_id
    )
    
    # Verify the results
    assert os.path.exists(TEST_DB), "Database file was not created"
    
    # Wait a moment for DB operations to complete
    time.sleep(1)
    
    # Check database contents
    try:
        conn = sqlite3.connect(TEST_DB)
        cursor = conn.cursor()
        
        # Count the number of items
        cursor.execute("SELECT COUNT(*) FROM hn_items")
        count = cursor.fetchone()[0]
        assert count > 0, "No items were fetched"
        
        # Verify items have required fields
        cursor.execute("SELECT item_json FROM hn_items WHERE item_json IS NOT NULL LIMIT 1")
        row = cursor.fetchone()
        assert row is not None, "No valid items found in database"
        
        item = json.loads(row[0])
        assert "id" in item, "Item missing 'id' field"
        assert "time" in item, "Item missing 'time' field"
        
    finally:
        cursor.close()
        conn.close() 