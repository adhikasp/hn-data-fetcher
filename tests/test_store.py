"""Tests for the Parquet store (no network)."""

from pathlib import Path

import pyarrow.compute as pc

from hn_data_fetcher.store import SCHEMA, Store, local_time, rows_to_table

MAR_31_LOCAL = 1743400000  # 2025-03-31 05:46:40 UTC -> 12:46 local
APR_1_LOCAL = 1743444000  # 2025-03-31 18:00:00 UTC -> 2025-04-01 01:00 local


def item(i: int, t: int, **kw) -> dict:
    return {"id": i, "time": t, "type": "story", "by": "x", **kw}


def test_local_time_is_utc_plus_7() -> None:
    assert str(local_time(APR_1_LOCAL)) == "2025-04-01 01:00:00"


def test_rows_to_table_schema_and_flags() -> None:
    t = rows_to_table(
        [item(1, MAR_31_LOCAL, dead=True, kids=[2, 3]), item(2, MAR_31_LOCAL), {"id": 3}]
    )
    assert t.schema == SCHEMA and t.num_rows == 2  # item without time dropped
    assert t["dead"].to_pylist() == [True, None]
    assert t["kids"].to_pylist() == [[2, 3], None]


def test_merge_partitions_by_local_month_and_last_day_kept(tmp_path: Path) -> None:
    s = Store(tmp_path)
    written = s.merge(rows_to_table([item(1, MAR_31_LOCAL), item(2, APR_1_LOCAL)]))
    assert written == [(2025, 3, 1), (2025, 4, 1)]  # 31 Mar not dropped, 1 Apr 01:00 is April
    assert [p.key for p in s.partitions()] == [(2025, 3), (2025, 4)]


def test_merge_replaces_same_id_and_sorts(tmp_path: Path) -> None:
    s = Store(tmp_path)
    s.merge(rows_to_table([item(5, MAR_31_LOCAL, title="old"), item(3, MAR_31_LOCAL)]))
    s.merge(rows_to_table([item(5, MAR_31_LOCAL, title="new"), item(4, MAR_31_LOCAL)]))
    t = s.read(s.partitions()[0])
    assert t["id"].to_pylist() == [3, 4, 5]
    assert t.filter(pc.equal(t["id"], 5))["title"].to_pylist() == ["new"]
    assert not list(tmp_path.rglob("*.tmp"))


def test_max_id_and_gaps(tmp_path: Path) -> None:
    s = Store(tmp_path)
    assert s.max_id() == 0
    s.merge(rows_to_table([item(1, MAR_31_LOCAL), item(10, MAR_31_LOCAL)]))
    s.merge(rows_to_table([item(30, APR_1_LOCAL), item(40, APR_1_LOCAL)]))
    assert s.max_id() == 40
    assert s.gaps() == [(11, 30)]  # ids between months, not holes inside one
