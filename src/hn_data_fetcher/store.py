"""Hive-partitioned Parquet store for Hacker News items.

Layout (identical to the Tigris bucket)::

    <root>/year=YYYY/month=M/data.parquet

One row per HN item. ``time`` is a *naive* timestamp expressed in UTC+7
(``STORE_UTC_OFFSET``): that is what the original SQLite-based scraper stored
(it ran in Asia/Jakarta), and keeping it means old and new partitions line up.
Partitioning is by that same local time, so a month's partition holds exactly
the items posted during that calendar month.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

STORE_UTC_OFFSET = timedelta(hours=7)
STORE_TZ = timezone(STORE_UTC_OFFSET)

# Canonical schema. Numeric columns stay float64 (not int64) on purpose: that
# is how the existing partitions were written, and a dataset only reads cleanly
# when every partition agrees. ``dead``/``deleted`` were always null in the old
# files (a conversion bug); they are real booleans now.
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("time", pa.timestamp("ns")),
        ("by", pa.string()),
        ("title", pa.string()),
        ("url", pa.string()),
        ("text", pa.string()),
        ("score", pa.float64()),
        ("type", pa.string()),
        ("parent", pa.float64()),
        ("poll", pa.float64()),
        ("parts", pa.list_(pa.int64())),
        ("descendants", pa.float64()),
        ("dead", pa.bool_()),
        ("deleted", pa.bool_()),
        ("kids", pa.list_(pa.int64())),
    ]
)

_PARTITION_RE = re.compile(r"year=(\d+)[/\\]month=(\d+)[/\\]data\.parquet$")


@dataclass(frozen=True)
class Partition:
    """A single ``year=/month=`` partition file."""

    year: int
    month: int
    path: Path

    @property
    def key(self) -> tuple[int, int]:
        return (self.year, self.month)

    @property
    def s3_key(self) -> str:
        return f"year={self.year}/month={self.month}/data.parquet"


def local_time(epoch: int) -> datetime:
    """Convert a unix timestamp to the store's naive UTC+7 datetime."""
    return datetime.fromtimestamp(epoch, STORE_TZ).replace(tzinfo=None)


def month_start_epoch(year: int, month: int) -> int:
    """Unix timestamp of the first instant of a store-local month."""
    return int(datetime(year, month, 1, tzinfo=STORE_TZ).timestamp())


def rows_to_table(rows: list[dict]) -> pa.Table:
    """Build a table in ``SCHEMA`` from raw HN item dicts.

    Items without ``id``/``time`` are dropped. ``dead``/``deleted`` become
    ``True`` or null (the API only ever sends them as ``true``).
    """
    rows = [r for r in rows if "id" in r and "time" in r]
    offset = int(STORE_UTC_OFFSET.total_seconds())
    cols = {
        "id": [r["id"] for r in rows],
        "time": pa.array([r["time"] + offset for r in rows], pa.int64())
        .cast(pa.timestamp("s"))
        .cast(pa.timestamp("ns")),
        "dead": [True if r.get("dead") else None for r in rows],
        "deleted": [True if r.get("deleted") else None for r in rows],
    }
    for name in (
        "by",
        "title",
        "url",
        "text",
        "type",
        "score",
        "parent",
        "poll",
        "descendants",
        "parts",
        "kids",
    ):
        cols[name] = [r.get(name) for r in rows]
    arrays = [
        cols[f.name] if isinstance(cols[f.name], pa.Array) else pa.array(cols[f.name], f.type)
        for f in SCHEMA
    ]
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


class Store:
    """Read/merge/write access to the Hive-partitioned Parquet directory."""

    def __init__(self, root: os.PathLike | str) -> None:
        self.root = Path(root)

    # -- discovery ---------------------------------------------------------
    def partitions(self) -> list[Partition]:
        """All partitions, oldest first."""
        found = []
        for p in self.root.glob("year=*/month=*/data.parquet"):
            m = _PARTITION_RE.search(str(p))
            if m:
                found.append(Partition(int(m[1]), int(m[2]), p))
        return sorted(found, key=lambda x: x.key)

    def path_for(self, year: int, month: int) -> Path:
        return self.root / f"year={year}" / f"month={month}" / "data.parquet"

    # -- reading -----------------------------------------------------------
    def read(self, part: Partition, columns: list[str] | None = None) -> pa.Table:
        """Read a partition. Without ``columns`` the result is cast to ``SCHEMA``
        so partitions written by older versions (null-typed columns) merge cleanly."""
        table = pq.read_table(part.path, columns=columns)
        if columns is None:
            table = table.select(SCHEMA.names).cast(SCHEMA)
        return table

    def id_bounds(self, part: Partition) -> tuple[int, int]:
        """(min id, max id) of a partition, reading only the id column."""
        ids = pq.read_table(part.path, columns=["id"]).column("id")
        mm = pc.min_max(ids)
        return mm["min"].as_py(), mm["max"].as_py()

    def max_id(self) -> int:
        """Highest stored item id (0 for an empty store)."""
        for part in reversed(self.partitions()):
            _, hi = self.id_bounds(part)
            if hi is not None:
                return hi
        return 0

    def gaps(self) -> list[tuple[int, int]]:
        """Id ranges ``[lo, hi)`` that lie between consecutive partitions.

        Ids inside a month that never made it into a file (deleted items) are
        not gaps. Ids *between* months are: they're items a month-boundary bug
        in the old converter silently dropped.
        """
        out: list[tuple[int, int]] = []
        prev_hi: int | None = None
        for part in self.partitions():
            lo, hi = self.id_bounds(part)
            if prev_hi is not None and lo > prev_hi + 1:
                out.append((prev_hi + 1, lo))
            prev_hi = hi if prev_hi is None else max(prev_hi, hi)
        return out

    # -- writing -----------------------------------------------------------
    def merge(self, table: pa.Table) -> list[tuple[int, int, int]]:
        """Merge ``table`` into the store, one partition per month.

        New rows replace stored rows with the same id. Each touched partition is
        rewritten atomically (temp file + rename), so an interrupted run never
        leaves a torn file. Returns ``(year, month, rows_in_partition)`` tuples.
        """
        if table.num_rows == 0:
            return []
        table = table.cast(SCHEMA)
        key = pc.add(pc.multiply(pc.year(table["time"]), 100), pc.month(table["time"]))
        written = []
        for k in sorted(pc.unique(key).to_pylist()):
            year, month = divmod(k, 100)
            new = table.filter(pc.equal(key, k))
            path = self.path_for(year, month)
            if path.exists():
                old = self.read(Partition(year, month, path))
                old = old.filter(pc.invert(pc.is_in(old["id"], value_set=new["id"])))
                new = pa.concat_tables([old, new])
            new = new.take(pc.sort_indices(new["id"]))
            self._write(path, new)
            written.append((year, month, new.num_rows))
        return written

    @staticmethod
    def _write(path: Path, table: pa.Table) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd", compression_level=3)
        os.replace(tmp, path)
