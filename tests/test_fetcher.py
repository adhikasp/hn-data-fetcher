"""Fetcher tests against a local fake HN API."""

from pathlib import Path

import pytest
from aiohttp import web

from hn_data_fetcher import fetcher as fx
from hn_data_fetcher.store import Store

T = 1743400000  # 2025-03-31


@pytest.fixture
async def fake_api(monkeypatch: pytest.MonkeyPatch):
    flaky = {"hits": 0}

    async def item(request: web.Request) -> web.Response:
        i = int(request.match_info["i"])
        if i == 3:  # fail once, then succeed: exercises retry
            flaky["hits"] += 1
            if flaky["hits"] == 1:
                return web.Response(status=503)
        if i % 5 == 0:  # deleted/missing item
            return web.Response(text="null")
        return web.json_response({"id": i, "time": T + i * 60, "type": "comment", "by": "u"})

    app = web.Application()
    app.router.add_get("/v0/item/{i}.json", item)

    async def maxitem(request: web.Request) -> web.Response:
        return web.Response(text="20")

    app.router.add_get("/v0/maxitem.json", maxitem)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    monkeypatch.setattr(fx, "API", f"http://127.0.0.1:{port}/")
    yield
    await runner.cleanup()


async def test_run_writes_items_and_resumes(tmp_path: Path, fake_api) -> None:
    f = fx.Fetcher(fx.Config(data_dir=str(tmp_path), concurrency=8, chunk_size=7))
    stats = await f.run([(1, 11)])
    assert stats.ids == 10 and stats.items == 8 and stats.retries >= 1
    store = Store(tmp_path)
    assert store.max_id() == 9  # 10 is a null item

    # update mode continues after the newest stored id
    args = fx.build_parser().parse_args(["--data-dir", str(tmp_path)])
    ranges = await fx.plan(fx.Fetcher(fx.Config(data_dir=str(tmp_path))), args)
    assert ranges == [(10, 21)]


async def test_find_id_at(tmp_path: Path, fake_api) -> None:
    f = fx.Fetcher(fx.Config(data_dir=str(tmp_path)))
    async with f.session() as s:
        assert await f.find_id_at(s, T + 12 * 60, 1, 20) == 12


async def test_unreachable_ids_commit_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake_api
) -> None:
    f = fx.Fetcher(fx.Config(data_dir=str(tmp_path), retries=0, chunk_size=100))
    monkeypatch.setattr(f, "_get", lambda *a, **k: (_ for _ in ()).throw(fx.FetchError("boom")))
    with pytest.raises(fx.FetchError):
        await f.run([(1, 6)])
    assert Store(tmp_path).partitions() == []
