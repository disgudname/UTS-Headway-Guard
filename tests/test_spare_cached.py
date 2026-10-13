import asyncio

import pytest
from fastapi import HTTPException

import app


def test_failed_fetch_serves_last_good_list():
    async def run():
        cache = app.TTLCache(30)
        trips = [{"id": "a", "status": "accepted"}]

        async def good():
            return trips

        async def bad():
            raise RuntimeError("spare timed out")

        assert await app._spare_cached(cache, good, "requests") == trips
        cache.ts = 0.0  # expired, as after a requestStatus webhook
        assert await app._spare_cached(cache, bad, "requests") == trips

    asyncio.run(run())


def test_failed_fetch_is_retried_once():
    async def run():
        cache = app.TTLCache(30)
        calls = []

        async def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("connection reset")
            return [{"id": "b"}]

        assert await app._spare_cached(cache, flaky, "requests") == [{"id": "b"}]
        assert len(calls) == 2

    asyncio.run(run())


def test_failed_fetch_with_nothing_cached_is_502_not_empty():
    async def run():
        cache = app.TTLCache(30)

        async def bad():
            raise RuntimeError("spare down")

        with pytest.raises(HTTPException) as err:
            await app._spare_cached(cache, bad, "requests")
        assert err.value.status_code == 502

    asyncio.run(run())
