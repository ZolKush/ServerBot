"""Drain then cancel application jobs before PTB closes clients and persistence."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from functools import wraps
from typing import Any

from telegram.ext import JobQueue

from ..config import logger


class ManagedJobQueue(JobQueue):
    def __init__(self, *, drain_timeout: float = 10.0) -> None:
        super().__init__()
        self._drain_timeout = drain_timeout
        self._active_callbacks: set[asyncio.Task] = set()
        self._closing = False

    def managed(self, callback: Callable[[Any], Awaitable[Any]]) -> Callable[[Any], Coroutine[Any, Any, None]]:
        @wraps(callback)
        async def run(context: Any) -> None:
            if self._closing:
                return
            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("managed job callback requires an asyncio task")
            self._active_callbacks.add(task)
            try:
                await callback(context)
            finally:
                self._active_callbacks.discard(task)

        return run

    async def start(self) -> None:
        self._closing = False
        await super().start()

    async def stop(self, wait: bool = True) -> None:
        self._closing = True
        if self.scheduler.running:
            self.scheduler.pause()
        pending = set(self._active_callbacks)
        if pending and wait:
            _, pending = await asyncio.wait(pending, timeout=self._drain_timeout)
        if pending:
            logger.info("Cancelling %s background jobs during shutdown", len(pending))
            for task in pending:
                task.cancel()
            # Await finally blocks: subprocesses, sockets, and in-flight atomic
            # storage commits must settle before PTB persistence is flushed.
            await asyncio.gather(*pending, return_exceptions=True)
        if self.scheduler.running:
            await super().stop(wait=wait)
