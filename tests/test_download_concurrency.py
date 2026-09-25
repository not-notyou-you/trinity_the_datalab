# tests/test_download_concurrency.py
"""Batas koneksi per sumber, prioritas Dataset Saya di atas Live, dan
pemulihan scene Live yang macet PROCESSING setelah proses mati.

Tanpa database: sesi DB pemulihan diganti tiruan kecil.
"""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

from etl import download_guard as dg


def _wait_until(pred, timeout=2.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.005)
    return False


def test_priority_semaphore_caps_concurrency():
    sem = dg.PrioritySemaphore(2)
    active, peak, lock = [0], [0], threading.Lock()

    def work():
        sem.acquire()
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.02)
        with lock:
            active[0] -= 1
        sem.release()

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak[0] == 2


def test_normal_waiter_beats_low_waiter():
    """Slot yang lepas jatuh ke Dataset Saya walau Live menunggu lebih dulu."""
    sem = dg.PrioritySemaphore(1)
    sem.acquire()
    order: list[str] = []

    def take(name, low):
        sem.acquire(low=low)
        order.append(name)
        sem.release()

    live = threading.Thread(target=take, args=("live", True))
    live.start()
    time.sleep(0.05)  # Live sudah menunggu duluan
    user = threading.Thread(target=take, args=("user", False))
    user.start()
    assert _wait_until(lambda: sem._waiting_normal == 1)
    sem.release()
    live.join(2)
    user.join(2)
    assert order == ["user", "live"]


def test_low_waiter_not_starved_once_normal_queue_empties():
    sem = dg.PrioritySemaphore(2)
    sem.acquire()
    sem.acquire()
    got = []
    t_user = threading.Thread(target=lambda: (sem.acquire(), got.append("user")))
    t_live = threading.Thread(target=lambda: (sem.acquire(low=True), got.append("live")))
    t_user.start()
    assert _wait_until(lambda: sem._waiting_normal == 1)
    t_live.start()
    time.sleep(0.02)
    sem.release()
    sem.release()  # dua slot kosong: user mengambil satu, live yang lain
    t_user.join(2)
    t_live.join(2)
    assert sorted(got) == ["live", "user"]


def test_source_slot_uses_thread_priority(monkeypatch):
    calls = []

    class Spy:
        def acquire(self, low=False):
            calls.append(low)

        def release(self):
            pass

    monkeypatch.setitem(dg._SOURCE_SLOTS, dg.CDSE, Spy())
    with dg.source_slot(dg.CDSE):
        pass
    with dg.low_priority():
        with dg.source_slot(dg.CDSE):
            pass
    with dg.source_slot(dg.CDSE):
        pass
    assert calls == [False, True, False]


def test_low_priority_is_thread_local():
    seen = []
    with dg.low_priority():
        t = threading.Thread(target=lambda: seen.append(dg.is_low_priority()))
        t.start()
        t.join()
        assert dg.is_low_priority()
    assert seen == [False]
    assert not dg.is_low_priority()


# ---------------------------------------------------------------------------
# pemulihan scene Live PROCESSING
# ---------------------------------------------------------------------------

class _FakeSession:
    def __init__(self, area, scenes, jobs):
        self._area, self._results = area, [scenes, jobs]

    def get(self, model, key):
        return self._area

    def scalars(self, stmt):
        rows = self._results.pop(0)
        return SimpleNamespace(all=lambda: rows)


def test_recover_interrupted_ingest_marks_failed_and_counts_attempt():
    from etl import live_cycle as lc

    stuck = SimpleNamespace(scene_date=date(2026, 9, 20), status="PROCESSING",
                            source_status={"sentinel1": {"attempts": 1}}, updated_at=None)
    job = SimpleNamespace(job_id=77, status="DOWNLOADING", completed_at=None)
    area = SimpleNamespace(dataset_id=5)
    sess = _FakeSession(area, [stuck], [job])
    logs = []

    @contextmanager
    def session():
        yield sess

    mon = SimpleNamespace(_db=SimpleNamespace(session=session),
                          log=lambda *a, **k: logs.append(a[1:3]))
    lc._recover_interrupted_ingest(mon, 1)

    assert stuck.status == "FAILED"
    assert stuck.source_status["sentinel1"]["attempts"] == 2
    assert stuck.source_status["sentinel1"]["status"] == "FAILED"
    assert job.status == "FAILED" and job.completed_at is not None
    assert ("RECOVER", "WARNING") in logs



# ---------------------------------------------------------------------------
# jeda retry bersama
# ---------------------------------------------------------------------------

def test_retry_after_is_capped():
    assert dg.retry_delay(1, "99999") == dg.MAX_RETRY_AFTER_S
    assert dg.retry_delay(1, "7") == 7.0
    assert dg.retry_delay(3) <= 9.0


def test_sleep_or_cancel_wakes_on_cancel():
    ev = threading.Event()
    threading.Timer(0.05, ev.set).start()
    t0 = time.monotonic()
    try:
        dg.sleep_or_cancel(30, ev)
    except dg.DownloadCancelled:
        pass
    else:
        raise AssertionError("seharusnya DownloadCancelled")
    assert time.monotonic() - t0 < 5


# ---------------------------------------------------------------------------
# token CDSE bersama
# ---------------------------------------------------------------------------

def _reset_token(m1):
    m1._token_cache.update(token=None, expires_at=0.0)


def test_cdse_token_shared_across_threads(monkeypatch):
    from etl import module1_download as m1

    _reset_token(m1)
    logins = []

    def fake_fetch(user, pwd):
        logins.append(1)
        time.sleep(0.02)
        return f"tok{len(logins)}", 600

    monkeypatch.setattr(m1, "_fetch_cdse_token", fake_fetch)
    got = []
    threads = [threading.Thread(target=lambda: got.append(m1._get_cdse_token("u", "p")))
               for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(logins) == 1 and set(got) == {"tok1"}

    # 401 dengan token lama: satu login ulang, thread berikut ikut token baru.
    assert m1._get_cdse_token("u", "p", "tok1") == "tok2"
    assert m1._get_cdse_token("u", "p", "tok1") == "tok2"
    assert len(logins) == 2
    _reset_token(m1)


def test_cdse_login_retries_on_429_then_fails_fast_on_bad_password(monkeypatch):
    import requests
    from etl import module1_download as m1

    class R:
        def __init__(self, code, body=None):
            self.status_code, self._body, self.text, self.headers = code, body, "", {}

        def json(self):
            return self._body

    seq = [R(429), R(503), R(200, {"access_token": "t", "expires_in": 600})]
    monkeypatch.setattr(requests, "post", lambda *a, **k: seq.pop(0))
    monkeypatch.setattr(m1.time, "sleep", lambda s: None)
    assert m1._fetch_cdse_token("u", "p") == ("t", 600.0)

    calls = []
    monkeypatch.setattr(requests, "post", lambda *a, **k: calls.append(1) or R(401))
    try:
        m1._fetch_cdse_token("u", "p")
    except RuntimeError as exc:
        assert "401" in str(exc)
    assert len(calls) == 1


# ---------------------------------------------------------------------------
# error NASA
# ---------------------------------------------------------------------------

def test_nasa_401_fails_fast_and_is_recorded(monkeypatch, tmp_path):
    import requests
    from etl import module8_gpm_download as m8

    calls = []

    class R:
        status_code = 401
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setenv("NASA_EARTHDATA_TOKEN", "x")
    monkeypatch.setattr(requests, "get", lambda *a, **k: calls.append(1) or R())
    monkeypatch.setattr(m8.time, "sleep", lambda s: None)
    monkeypatch.setattr(dg, "reuse_granule", lambda *a, **k: False)
    t0 = time.time()
    try:
        m8._download_with_retry("http://x/g.nc4", tmp_path / "g.nc4", item_label="g")
    except dg.NasaAuthError as exc:
        assert dg.NASA_AUTH_MESSAGE in str(exc)
    else:
        raise AssertionError("seharusnya NasaAuthError")
    assert len(calls) == 1
    assert "GESDISC" in dg.auth_failures_since(t0)


# ---------------------------------------------------------------------------
# antrean job Dataset Saya
# ---------------------------------------------------------------------------

def test_dataset_jobs_queue_beyond_max_active(monkeypatch):
    from etl import dataset_manager as dm

    monkeypatch.setattr(dm, "MAX_ACTIVE_JOBS", 2)
    monkeypatch.setattr(dm, "_running_jobs", set())
    monkeypatch.setattr(dm, "_pending_jobs", [])
    started = []
    mgr = dm.DatasetManager(None)
    monkeypatch.setattr(mgr, "_start_job_thread", lambda j: started.append(j) or True)
    cancelled = {3}
    monkeypatch.setattr(mgr, "_job_still_queued", lambda j: j not in cancelled)

    for j in (1, 2, 3, 4, 5):
        mgr._spawn_job_runner(j)
    mgr._spawn_job_runner(4)  # ganda: tidak diantrekan dua kali
    assert started == [1, 2]
    assert mgr.queue_position(4) == 2 and mgr.queue_position(1) is None

    mgr._job_slot_done(1)  # 3 sudah dibatalkan -> dilewati, 4 mulai
    assert started == [1, 2, 4]
    mgr._job_slot_done(2)
    assert started == [1, 2, 4, 5]
    assert dm._pending_jobs == []


# ---------------------------------------------------------------------------
# gerbang siklus Live
# ---------------------------------------------------------------------------

def test_cycle_gate_serves_oldest_checked_first():
    from etl.live_monitor import _CycleGate

    gate = _CycleGate()
    order = []
    release = threading.Event()

    def first():
        with gate.hold(0):
            release.wait(2)

    def waiter(name, key):
        with gate.hold(key):
            order.append(name)

    t0 = threading.Thread(target=first)
    t0.start()
    assert _wait_until(gate.busy)
    ts = [threading.Thread(target=waiter, args=(n, k))
          for n, k in (("baru", 300.0), ("lama", 100.0), ("sedang", 200.0))]
    for t in ts:
        t.start()
        time.sleep(0.02)
    assert _wait_until(lambda: len(gate._waiters) == 3)
    release.set()
    for t in [t0, *ts]:
        t.join(2)
    assert order == ["lama", "sedang", "baru"]
    assert not gate.busy()


def test_retry_scene_refused_while_area_busy(monkeypatch):
    from etl import live_monitor as lm

    mon = lm.LiveMonitor.__new__(lm.LiveMonitor)
    monkeypatch.setattr(mon, "get_area", lambda a: {})
    busy = threading.Event()
    t = threading.Thread(target=busy.wait, args=(2,))
    t.start()
    monkeypatch.setitem(lm._area_threads, 42, t)
    try:
        assert mon.retry_scene(42, date(2026, 9, 20)) is False
    finally:
        busy.set()
        t.join()


# ---------------------------------------------------------------------------
# loading bar Live
# ---------------------------------------------------------------------------

def _progress_mon(monkeypatch, running, event):
    from etl import live_monitor as lm

    mon = lm.LiveMonitor.__new__(lm.LiveMonitor)
    monkeypatch.setattr(mon, "is_running", lambda a: running)

    @contextmanager
    def session():
        yield SimpleNamespace(scalar=lambda stmt: event)

    mon._db = SimpleNamespace(session=session)
    monkeypatch.setattr(mon, "_cycle_timing", lambda a: {"elapsed_s": 5})
    return mon


def test_progress_waiting_and_idle(monkeypatch):
    mon = _progress_mon(monkeypatch, False, None)
    assert mon._progress(SimpleNamespace(area_id=1, status="WAITING", dataset_id=5)) == \
        {"phase": "Menunggu giliran", "percent": None, "waiting": None, "timing": {"elapsed_s": 5}}
    assert mon._progress(SimpleNamespace(area_id=1, status="ACTIVE", dataset_id=5)) is None


def test_progress_ingest_uses_job_counters(monkeypatch):
    from etl import dataset_manager as dm

    ev = SimpleNamespace(step="INGEST", status="STARTED")
    mon = _progress_mon(monkeypatch, True, ev)
    monkeypatch.setattr(dm.DatasetManager, "get_progress", lambda self, ds: {
        "total_scenes": 4, "processed_count": 1, "failed_count": 1, "progress_percent": 55})
    p = mon._progress(SimpleNamespace(area_id=1, status="BACKFILLING", dataset_id=5))
    assert p == {"phase": "Mengunduh & memproses", "percent": 55,
                 "ok": 1, "failed": 1, "total": 4, "waiting": None, "timing": {"elapsed_s": 5}}


def test_progress_other_phases_are_indeterminate(monkeypatch):
    ev = SimpleNamespace(step="SCENE", status="OK")
    mon = _progress_mon(monkeypatch, True, ev)
    p = mon._progress(SimpleNamespace(area_id=1, status="RUNNING", dataset_id=5))
    assert p == {"phase": "Menyusun metrik & preview", "percent": None, "waiting": None, "timing": {"elapsed_s": 5}}


# ---------------------------------------------------------------------------
# hambatan: jeda server & token NASA
# ---------------------------------------------------------------------------

def test_wait_is_recorded_per_dataset_and_expires(monkeypatch):
    monkeypatch.setattr(dg.time, "sleep", lambda s: None)
    dg.set_context(None)
    dg.backoff_wait(dg.CDSE, 1, "tanpa konteks", retry_after="30")
    assert dg.current_wait(11) is None

    dg.set_context(11)
    try:
        dg.backoff_wait(dg.CDSE, 2, "dibatasi server (429)", retry_after="30", max_attempts=8)
    finally:
        dg.set_context(None)
    w = dg.current_wait(11)
    assert w["source_label"] == "CDSE (Sentinel-1)" and w["attempt"] == 2
    assert w["max_attempts"] == 8 and 0 < w["remaining_s"] <= 30
    assert dg.current_wait(12) is None

    with dg._waits_lock:
        dg._waits[11]["until"] = time.time() - 1
    assert dg.current_wait(11) is None


def test_auth_failure_cleared_by_success():
    dg.record_auth_failure("LAADS", "token ditolak")
    assert "LAADS" in dg.active_auth_failures()
    dg.clear_auth_failure("LAADS")
    assert "LAADS" not in dg.active_auth_failures()


def test_obstacles_only_for_active_jobs():
    from etl import dataset_manager as dm

    dg.record_auth_failure("GESDISC", "token ditolak")
    try:
        assert dm._obstacles(1, "COMPLETED") == {"waiting": None, "alerts": []}
        got = dm._obstacles(1, "DOWNLOADING")
        assert {"source": "GESDISC", "message": "token ditolak"} in got["alerts"]
    finally:
        dg.clear_auth_failure("GESDISC")


# ---------------------------------------------------------------------------
# durasi, "tidak ada kemajuan", ringkasan siklus
# ---------------------------------------------------------------------------

def test_timing_flags_stall_and_activity_resets_it(monkeypatch):
    monkeypatch.setattr(dg, "PROGRESS_STALL_AFTER_S", 60)
    started = time.time() - 600
    with dg._activity_lock:
        dg._activity.pop(77, None)
    t = dg.timing(77, started)
    assert t["stalled"] and 590 <= t["elapsed_s"] <= 610

    dg.note_activity(77)
    t = dg.timing(77, started)
    assert not t["stalled"] and t["idle_s"] <= 1


def test_stallguard_bytes_count_as_activity():
    dg.set_context(78)
    try:
        dg.StallGuard().update(1024)
    finally:
        dg.set_context(None)
    assert dg.last_activity(78) is not None


def test_queued_job_timing_is_never_stalled():
    from datetime import datetime, timedelta, timezone
    from etl import dataset_manager as dm

    old = datetime.now(timezone.utc) - timedelta(hours=5)
    t = dm._job_timing(1, {"status": "QUEUED", "created_at": old, "resumed_at": None})
    assert t["stalled"] is False and t["elapsed_s"] >= 5 * 3600 - 5
    assert dm._job_timing(1, {"status": "COMPLETED"}) is None


def _result_mon(rows):
    @contextmanager
    def session():
        yield SimpleNamespace(scalars=lambda stmt: SimpleNamespace(all=lambda: rows))

    return SimpleNamespace(_db=SimpleNamespace(session=session))


def test_cycle_result_distinguishes_full_partial_failed(monkeypatch):
    from etl import live_cycle as lc

    monkeypatch.setattr(dg, "_auth_failures", {})

    rows = [
        SimpleNamespace(status="READY", source_status={}),
        SimpleNamespace(status="PARTIAL", source_status={
            "sentinel1": {"status": "OK"}, "gpm": {"status": "FAILED"}}),
        SimpleNamespace(status="FAILED", source_status={}),
    ]
    d = [date(2026, 9, i) for i in (1, 2, 3)]
    r = lc._cycle_result(_result_mon(rows), 1, d, 5, 6)
    assert r["level"] == "warn"
    assert r["text"] == ("Selesai: 3 scene baru \u2014 1 lengkap, 1 sebagian (GPM gagal), "
                         "1 gagal Sentinel-1")


def test_cycle_result_quiet_cycle_is_ok(monkeypatch):
    from etl import live_cycle as lc

    monkeypatch.setattr(dg, "_auth_failures", {})

    r = lc._cycle_result(_result_mon([]), 1, [], 6, 6)
    assert r == {"level": "ok", "text": "Selesai: tidak ada scene baru (6/6 tersimpan)"}
