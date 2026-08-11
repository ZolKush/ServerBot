from __future__ import annotations

from datetime import datetime, timezone

from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.main import build_app


def test_tls_network_runs_at_startup_then_weekly_with_local_daily_deadline_check() -> None:
    before = datetime.now(timezone.utc)
    application = build_app()
    after = datetime.now(timezone.utc)
    assert application.job_queue is not None
    jobs = {job.name: job for job in application.job_queue.jobs()}

    startup = jobs["tls_certificate_check_startup"].job.trigger
    weekly = jobs["tls_certificate_check"].job.trigger
    deadline = jobs["tls_deadline_evaluation"].job.trigger

    assert isinstance(startup, DateTrigger)
    assert before.timestamp() + 9 <= startup.run_date.timestamp() <= after.timestamp() + 11
    assert isinstance(weekly, IntervalTrigger)
    assert weekly.interval.total_seconds() == 7 * 24 * 60 * 60
    assert isinstance(deadline, IntervalTrigger)
    assert deadline.interval.total_seconds() == 24 * 60 * 60
