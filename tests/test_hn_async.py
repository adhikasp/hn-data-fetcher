import pytest
import os
import time
import shutil
from pathlib import Path
import pyarrow.parquet as pq
from hn_data_fetcher import main, get_max_id
from unittest.mock import AsyncMock, patch
from typing import AsyncGenerator
from pytest_mock.plugin import MockerFixture

TEST_DATA_DIR = "test_hn_data"


@pytest.fixture(autouse=True)
def cleanup():
    """Remove test data directory before and after tests"""
    if os.path.exists(TEST_DATA_DIR):
        try:
            shutil.rmtree(TEST_DATA_DIR)
        except PermissionError:
            # Wait a bit and try again if directory is locked
            time.sleep(1)
            shutil.rmtree(TEST_DATA_DIR)
    yield
    if os.path.exists(TEST_DATA_DIR):
        for attempt in range(10):
            try:
                shutil.rmtree(TEST_DATA_DIR)
                break
            except PermissionError:
                # Wait a bit and try again if directory is locked
                time.sleep(1)
                if attempt == 9:  # Last attempt
                    shutil.rmtree(
                        TEST_DATA_DIR
                    )  # Final try, let exception propagate if it fails


@pytest.fixture
async def mock_session(mocker: MockerFixture) -> AsyncGenerator[AsyncMock, None]:
    """Fixture to mock aiohttp ClientSession"""
    mock = AsyncMock()
    with patch("aiohttp.ClientSession", return_value=mock):
        yield mock


@pytest.mark.asyncio
async def test_fetch_last_10_items():
    """Test fetching the last 10 items from HN API"""
    # Get the current max item ID
    max_id = await get_max_id()
    start_id = max_id - 10

    # Run the main function to fetch 10 items
    await main(
        data_dir=TEST_DATA_DIR,
        concurrent_requests=5,
        update_interval=2,
        batch_size=100,
        tcp_limit=5,
        mode="overwrite",
        start_id=start_id,
    )

    # Verify the results
    assert os.path.exists(TEST_DATA_DIR), "Data directory was not created"

    # Wait a moment for write operations to complete
    time.sleep(1)

    # Check parquet file contents
    parquet_files = list(Path(TEST_DATA_DIR).rglob("*.parquet"))
    assert len(parquet_files) > 0, "No parquet files were created"

    # Read and verify data from parquet files
    total_items = 0
    for parquet_file in parquet_files:
        table = pq.read_table(parquet_file)
        df = table.to_pandas()
        total_items += len(df)

        # Verify items have required fields
        assert "id" in df.columns, "Items missing 'id' field"
        assert "time" in df.columns, "Items missing 'time' field"
        assert "iso_time" in df.columns, "Items missing 'iso_time' field"

    assert total_items > 0, "No items were fetched"


@pytest.mark.asyncio
async def test_get_max_id_success():
    """Test successful max ID fetch"""
    max_id = await get_max_id()
    assert isinstance(max_id, int)
    assert max_id > 0


@pytest.mark.asyncio
async def test_invalid_mode():
    """Test handling of invalid mode"""
    with pytest.raises(ValueError):
        await main(
            data_dir=TEST_DATA_DIR,
            concurrent_requests=2,
            update_interval=1,
            batch_size=10,
            tcp_limit=2,
            mode="invalid_mode",
        )


@pytest.mark.asyncio
async def test_overwrite_without_start_id():
    """Test overwrite mode without start_id"""
    with pytest.raises(ValueError):
        await main(
            data_dir=TEST_DATA_DIR,
            concurrent_requests=2,
            update_interval=1,
            batch_size=10,
            tcp_limit=2,
            mode="overwrite",
        )
