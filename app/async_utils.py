"""Async/sync bridge for Streamlit's synchronous execution context.

Streamlit rerenders are synchronous. We run async dbt SL operations in a
dedicated thread with its own event loop to avoid conflicting with any loop
Streamlit's internals may have running.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")

# One thread pool shared for the process lifetime — keeps overhead low.
_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="sl_async")


def run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Execute an async coroutine from Streamlit's synchronous context."""

    def _run() -> T:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    future = _pool.submit(_run)
    return future.result()
