"""Token-bucket rate limiter for API compliance."""

from __future__ import annotations

import threading
import time

from loguru import logger


class RateLimiter:
    """
    Thread-safe token-bucket rate limiter.

    Usage:
        limiter = RateLimiter(calls_per_minute=60)
        limiter.acquire()  # blocks until a token is available
        make_api_call()
    """

    def __init__(self, calls_per_minute: int) -> None:
        self.calls_per_minute = calls_per_minute
        self.interval = 60.0 / calls_per_minute if calls_per_minute > 0 else 0
        self._lock = threading.Lock()
        self._last_call: float = 0.0

    def acquire(self) -> None:
        """Block until the next call is allowed under the rate limit."""
        if self.interval <= 0:
            return

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.interval:
                sleep_time = self.interval - elapsed
                logger.debug(f"Rate limiter: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            self._last_call = time.monotonic()

    def __repr__(self) -> str:
        return f"RateLimiter(calls_per_minute={self.calls_per_minute})"
