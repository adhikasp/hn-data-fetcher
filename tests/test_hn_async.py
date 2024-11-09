import pytest
import os
import sqlite3
import json
import time
from hn_async2 import main, get_max_id
from unittest.mock import AsyncMock, patch
import aiohttp
from typing import AsyncGenerator
from pytest_mock.plugin import MockerFixture
import queue
import threading

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

@pytest.fixture
async def mock_session(mocker: MockerFixture) -> AsyncGenerator[AsyncMock, None]:
    """Fixture to mock aiohttp ClientSession"""
    mock = AsyncMock()
    with patch('aiohttp.ClientSession', return_value=mock):
        yield mock

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

@pytest.mark.asyncio
async def test_get_max_id_success():
    """Test successful max ID fetch"""
    max_id = await get_max_id()
    assert isinstance(max_id, int)
    assert max_id > 0

@pytest.mark.asyncio
async def test_get_max_id_connection_error(mock_session: AsyncMock):
    """Test handling of connection error in get_max_id"""
    mock_session.get.side_effect = aiohttp.ClientError()
    
    with pytest.raises(aiohttp.ClientError):
        await get_max_id()

@pytest.mark.asyncio
async def test_different_modes():
    """Test different operation modes"""
    modes = ["update", "backfill", "overwrite"]
    
    for mode in modes:
        start_id = 1000 if mode == "overwrite" else None
        await main(
            db_name=TEST_DB,
            concurrent_requests=2,
            update_interval=1,
            db_queue_size=10,
            db_commit_interval=2,
            tcp_limit=2,
            mode=mode,
            start_id=start_id
        )
        
        # Verify database has expected content
        conn = sqlite3.connect(TEST_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hn_items")
        count = cursor.fetchone()[0]
        assert count > 0
        conn.close()

@pytest.mark.asyncio
async def test_invalid_mode():
    """Test handling of invalid mode"""
    with pytest.raises(ValueError):
        await main(
            db_name=TEST_DB,
            concurrent_requests=2,
            update_interval=1,
            db_queue_size=10,
            db_commit_interval=2,
            tcp_limit=2,
            mode="invalid_mode"
        )

@pytest.mark.asyncio
async def test_overwrite_without_start_id():
    """Test overwrite mode without start_id"""
    with pytest.raises(ValueError):
        await main(
            db_name=TEST_DB,
            concurrent_requests=2,
            update_interval=1,
            db_queue_size=10,
            db_commit_interval=2,
            tcp_limit=2,
            mode="overwrite"
        )

@pytest.mark.asyncio
async def test_db_writer_worker():
    """Test database writer functionality"""
    test_queue = queue.Queue()
    test_data = {"id": 1234, "time": int(time.time()), "title": "Test Item"}
    
    # Start db writer thread
    db_thread = threading.Thread(
        target=db_writer_worker,
        args=(TEST_DB, test_queue, 1)
    )
    db_thread.start()
    
    # Add test item
    test_queue.put((test_data["id"], json.dumps(test_data)))
    test_queue.put(None)  # Signal to stop
    db_thread.join()
    
    # Verify item was written
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT item_json FROM hn_items WHERE id = ?", (test_data["id"],))
    row = cursor.fetchone()
    assert row is not None
    stored_item = json.loads(row[0])
    assert stored_item["id"] == test_data["id"]
    assert stored_item["title"] == test_data["title"]
    conn.close()
