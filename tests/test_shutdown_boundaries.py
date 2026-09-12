from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from telegram.ext import ApplicationBuilder, ExtBot
from telegram.request import BaseRequest

from app.bot.job_queue import ManagedJobQueue
from app.runtime import process


class LocalRequest(BaseRequest):
    read_timeout = 5

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        assert url.endswith("/getMe")
        result = {"ok": True, "result": {"id": 123, "is_bot": True, "first_name": "Local"}}
        return 200, json.dumps(result).encode()


@pytest.mark.asyncio
async def test_ptb_application_stop_cancels_shielded_scheduled_callback():
    queue = ManagedJobQueue(drain_timeout=0.01)
    bot = ExtBot("123:LOCAL_ONLY", request=LocalRequest(), get_updates_request=LocalRequest())
    application = ApplicationBuilder().bot(bot).job_queue(queue).build()
    started = asyncio.Event()
    closed = asyncio.Event()

    async def callback(context):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    queue.run_once(queue.managed(callback), when=0.01)
    async with application:
        await application.start()
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
        finally:
            await asyncio.wait_for(application.stop(), timeout=1)
    assert closed.is_set()
    assert not queue.scheduler.running


@pytest.mark.asyncio
async def test_shutdown_drains_completed_jobs_and_rejects_new_callbacks():
    queue = ManagedJobQueue(drain_timeout=0.1)
    called = []

    async def callback(context):
        await asyncio.sleep(0)
        called.append(context)

    task = asyncio.create_task(queue.managed(callback)("before"))
    await asyncio.sleep(0)
    await queue.stop()
    await task
    await queue.managed(callback)("after")
    assert called == ["before"]
    assert not queue._active_callbacks


@pytest.mark.asyncio
async def test_shutdown_cancels_stuck_jobs_and_awaits_resource_cleanup():
    queue = ManagedJobQueue(drain_timeout=0.01)
    started = asyncio.Event()
    closed = asyncio.Event()

    async def callback(context):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.01)
            closed.set()

    task = asyncio.create_task(queue.managed(callback)(None))
    await started.wait()
    await asyncio.wait_for(queue.stop(), timeout=1)
    assert task.cancelled()
    assert closed.is_set()
    assert not queue._active_callbacks


@pytest.mark.asyncio
async def test_unreapable_process_has_a_bounded_wait(monkeypatch):
    cancelled = asyncio.Event()

    async def wait():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    proc = SimpleNamespace(pid=42, returncode=None, kill=lambda: None, wait=wait)
    monkeypatch.setattr(process.os, "killpg", lambda *args: None, raising=False)
    monkeypatch.setattr(process, "_REAP_TIMEOUT", 0.01)
    await asyncio.wait_for(process._kill_process_group(proc), timeout=1)
    assert cancelled.is_set()


@pytest.mark.asyncio
async def test_cancellation_during_timeout_reap_does_not_leave_pipe_tasks(monkeypatch):
    reaping = asyncio.Event()
    waiter_cancelled = asyncio.Event()

    async def wait():
        try:
            await asyncio.Event().wait()
        finally:
            waiter_cancelled.set()

    async def spawn(*args, **kwargs):
        return SimpleNamespace(stdout=None, stderr=None, wait=wait)

    async def reap(proc):
        reaping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(process.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(process, "_kill_process_group", reap)
    task = asyncio.create_task(process.run_exec(["synthetic"], timeout=0.01))
    await asyncio.wait_for(reaping.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert waiter_cancelled.is_set()
