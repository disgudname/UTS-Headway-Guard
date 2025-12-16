"""
Tests for cache concurrency correctness.

Validates:
1. TTLCache singleflight prevents duplicate fetches
2. StaleWhileRevalidateCache cold-cache concurrency
3. Never returns None from cold cache
"""
import asyncio
import time
from typing import Any, Dict, List, Optional


class TTLCache:
    """Copy of TTLCache from app.py for isolated testing."""

    def __init__(self, ttl: float):
        self.ttl = ttl
        self.value: Any = None
        self.ts: float = 0.0
        self.lock = asyncio.Lock()
        self._inflight: Optional[asyncio.Task] = None

    async def get(self, fetcher):
        async with self.lock:
            now = time.time()
            if self.value is not None and now - self.ts < self.ttl:
                return self.value
            # Singleflight: reuse in-flight fetch task
            if self._inflight is not None:
                inflight_task = self._inflight
            else:
                inflight_task = asyncio.create_task(fetcher())
                self._inflight = inflight_task

        try:
            data = await inflight_task
        except Exception:
            async with self.lock:
                if self._inflight is inflight_task:
                    self._inflight = None
            raise

        async with self.lock:
            if self._inflight is inflight_task:
                self.value = data
                self.ts = time.time()
                self._inflight = None
        return data


class StaleWhileRevalidateCache:
    """Copy of StaleWhileRevalidateCache from app.py for isolated testing."""

    def __init__(self, ttl: float):
        self.ttl = ttl
        self.value: Any = None
        self.ts: float = 0.0
        self.refresh_task: Optional[asyncio.Task] = None
        self.lock = asyncio.Lock()

    async def get(self, fetcher):
        async with self.lock:
            if self.value is None:
                if self.refresh_task is None:
                    self.refresh_task = asyncio.create_task(fetcher())
                seed_task = self.refresh_task
                seed = True
            else:
                seed = False
                seed_task = None
                value = self.value
                ts = self.ts
                refresh_task = self.refresh_task

        if seed:
            try:
                data = await seed_task
            except Exception as exc:
                async with self.lock:
                    if self.refresh_task is seed_task:
                        self.refresh_task = None
                print(f"[cache] seed failed: {exc}")
                return {}, "seed_failed"

            if data is None:
                data = {}

            async with self.lock:
                if self.value is None:
                    self.value = data
                    self.ts = time.time()
                value = self.value
                if self.refresh_task is seed_task:
                    self.refresh_task = None

            return value, "seed"

        now = time.time()
        is_fresh = now - ts < self.ttl
        if (not is_fresh) and (refresh_task is None or refresh_task.done()):
            async with self.lock:
                if self.refresh_task is None or self.refresh_task.done():
                    self.refresh_task = asyncio.create_task(self._refresh(fetcher))

        assert value is not None
        return value, "fresh" if is_fresh else "stale"

    async def _refresh(self, fetcher):
        try:
            data = await fetcher()
        except Exception as exc:
            print(f"[cache] refresh failed: {exc}")
            return

        async with self.lock:
            self.value = data
            self.ts = time.time()


class PerKeyStaleWhileRevalidateCache:
    """Copy of PerKeyStaleWhileRevalidateCache from app.py for isolated testing."""

    def __init__(self, ttl: float, max_keys: int = 100):
        self.ttl = ttl
        self.max_keys = max_keys
        self._caches: Dict[Any, StaleWhileRevalidateCache] = {}
        self._access_order: List[Any] = []
        self._lock = asyncio.Lock()

    async def get(self, key: Any, fetcher):
        async with self._lock:
            cache = self._caches.get(key)
            if cache is None:
                while len(self._caches) >= self.max_keys and self._access_order:
                    oldest = self._access_order.pop(0)
                    self._caches.pop(oldest, None)
                cache = StaleWhileRevalidateCache(self.ttl)
                self._caches[key] = cache
                self._access_order.append(key)
            else:
                if key in self._access_order:
                    self._access_order.remove(key)
                self._access_order.append(key)
        return await cache.get(fetcher)


# Test utilities
fetch_count = 0


async def slow_fetcher():
    """Simulates a slow fetch that takes 100ms."""
    global fetch_count
    fetch_count += 1
    await asyncio.sleep(0.1)
    return {"data": "value", "fetch_num": fetch_count}


async def failing_fetcher():
    """Simulates a fetcher that fails."""
    await asyncio.sleep(0.05)
    raise RuntimeError("Fetch failed")


# Tests


async def test_ttl_cache_singleflight():
    """TTLCache should only call fetcher once for concurrent requests."""
    global fetch_count
    fetch_count = 0

    cache = TTLCache(ttl=10.0)

    # Launch 10 concurrent requests
    tasks = [cache.get(slow_fetcher) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # All results should be the same
    assert all(r == results[0] for r in results), "All results should match"
    # Fetcher should only be called once
    assert fetch_count == 1, f"Expected 1 fetch, got {fetch_count}"
    print("[PASS] TTLCache singleflight")


async def test_swr_cache_cold_never_none():
    """StaleWhileRevalidateCache should never return None on cold cache."""
    global fetch_count
    fetch_count = 0

    cache = StaleWhileRevalidateCache(ttl=10.0)

    # Launch 10 concurrent requests on cold cache
    tasks = [cache.get(slow_fetcher) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # No result should be None
    for i, (value, state) in enumerate(results):
        assert value is not None, f"Result {i} value is None"
        assert isinstance(value, dict), f"Result {i} is not a dict"
    print("[PASS] SWR cold cache never None")


async def test_swr_cache_singleflight():
    """StaleWhileRevalidateCache should only call fetcher once for cold cache."""
    global fetch_count
    fetch_count = 0

    cache = StaleWhileRevalidateCache(ttl=10.0)

    # Launch 10 concurrent requests
    tasks = [cache.get(slow_fetcher) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # Fetcher should only be called once
    assert fetch_count == 1, f"Expected 1 fetch, got {fetch_count}"
    # All results should have data
    for value, state in results:
        assert "data" in value
    print("[PASS] SWR singleflight")


async def test_swr_cache_seed_failure():
    """StaleWhileRevalidateCache returns {} on seed failure, not None."""
    cache = StaleWhileRevalidateCache(ttl=10.0)

    value, state = await cache.get(failing_fetcher)

    assert value == {}, f"Expected empty dict on failure, got {value}"
    assert state == "seed_failed"
    print("[PASS] SWR seed failure returns {}")


async def test_perkey_cache_lru_eviction():
    """PerKeyStaleWhileRevalidateCache evicts old keys at max_keys."""
    global fetch_count
    fetch_count = 0

    cache = PerKeyStaleWhileRevalidateCache(ttl=10.0, max_keys=3)

    # Add 3 keys
    for key in ["a", "b", "c"]:
        await cache.get(key, slow_fetcher)

    assert len(cache._caches) == 3

    # Add 4th key - should evict "a"
    await cache.get("d", slow_fetcher)

    assert len(cache._caches) == 3
    assert "a" not in cache._caches
    assert "d" in cache._caches
    print("[PASS] PerKey LRU eviction")


async def test_perkey_cache_lru_access_order():
    """PerKeyStaleWhileRevalidateCache updates access order on get."""
    global fetch_count
    fetch_count = 0

    cache = PerKeyStaleWhileRevalidateCache(ttl=10.0, max_keys=3)

    # Add 3 keys
    for key in ["a", "b", "c"]:
        await cache.get(key, slow_fetcher)

    # Access "a" again to make it most recently used
    await cache.get("a", slow_fetcher)

    # Add 4th key - should evict "b" (oldest after "a" was accessed)
    await cache.get("d", slow_fetcher)

    assert len(cache._caches) == 3
    assert "b" not in cache._caches
    assert "a" in cache._caches
    assert "d" in cache._caches
    print("[PASS] PerKey LRU access order")


async def main():
    """Run all tests."""
    print("=" * 50)
    print("Cache Concurrency Tests")
    print("=" * 50)

    await test_ttl_cache_singleflight()
    await test_swr_cache_cold_never_none()
    await test_swr_cache_singleflight()
    await test_swr_cache_seed_failure()
    await test_perkey_cache_lru_eviction()
    await test_perkey_cache_lru_access_order()

    print("=" * 50)
    print("All tests passed!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
