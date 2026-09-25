# tests/test_orchestrator_preview_stage.py
"""
Tests tahap penyelesaian per-tanggal di etl/module5_orchestrator.

PREVIEW dan FUSION tidak jalan di dalam pipeline scene: keduanya menulis
berkas yang namanya cuma memuat tanggal, sementara satu tanggal bisa punya
beberapa scene Sentinel-1. Yang diuji di sini:

    1. Cancel/pause dicek ulang sebelum render dimulai.
    2. PNG yang sudah ditulis tetap tercatat walau level berikutnya gagal.
    3. Satu tanggal difinalisasi SEKALI, di atas mosaik seluruh frame-nya.

Unit test murni: module10, module9, dan mosaik di-stub; _JobContext diganti
namespace berisi atribut yang memang dibaca jalur itu.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from etl import folder_manager as fm
from etl import module5_orchestrator as m5

PID_A = "S1A_IW_GRDH_TEST_20240305T111502"
PID_B = "S1A_IW_GRDH_TEST_20240305T111532"
ACQ = datetime(2024, 3, 5, 22, 50, tzinfo=timezone.utc)
DAY = date(2024, 3, 5)


class _Stage:
    def output(self, **_):
        pass


class _FakePlog:
    @contextmanager
    def stage(self, *args, **kwargs):
        yield _Stage()

    def log_event(self, *args, **kwargs):
        pass


class _RecordingDsmgr:
    def __init__(self):
        self.states: list[tuple[str, str, str]] = []

    def upsert_scene_job_state(self, job_id, pid, **fields):
        self.states.append(
            (pid, fields.get("current_stage"), fields.get("stage_status"))
        )

    def increment_job_counters(self, *args, **kwargs):
        pass


def _make_jc(levels=("PROCESSED",), skip=("FUSION",)):
    pause = threading.Event()
    pause.set()
    return SimpleNamespace(
        job_id=1, dataset_id=7, dataset_name="Preview Stage Test",
        db=None, region_id=1, bbox_tuple=(106.4, -6.7, 107.2, -5.9),
        plog=_FakePlog(), dsmgr=_RecordingDsmgr(), meta=SimpleNamespace(
            fail_open_jobs=lambda exc: None
        ),
        plan=SimpleNamespace(
            output_levels=lambda: list(levels), source_count=1,
            fusion_eligible=lambda strategy: False,
            source_levels_for_run=lambda level: {},
        ),
        skip_stages=set(skip), fusion_strategy=None, preview_options=None,
        pause_event=pause, cancel_event=threading.Event(),
        expected_pids_by_date={},
    )


def _member(pid: str, levels=("PROCESSED",), scene_id: int = 11) -> m5._SceneResult:
    return m5._SceneResult(
        pid=pid, scene_id=scene_id, acquisition_date=DAY,
        produced_tiers=["COG"], produced_files={"COG": [f"{pid}_vv.tif"]},
        s1_files_by_level={
            level: {"VV": f"{pid}_{level}_vv.tif", "VH": f"{pid}_{level}_vh.tif"}
            for level in levels
        },
    )


@pytest.fixture
def stubbed(monkeypatch, tmp_path):
    """Stub module10 dan mosaik; kembalikan dict yang mencatat panggilan."""
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
    calls: dict = {"render": [], "mosaic": []}

    def fake_render(dataset_id, dataset_name, acq_date, *, processing_level, **kw):
        calls["render"].append((processing_level, kw.get("s1_files")))
        behaviour = calls.get(processing_level)
        if isinstance(behaviour, Exception):
            raise behaviour
        return {
            "files": [f"/preview/{processing_level}/s1_vv.png"], "total_size_mb": 0.1,
            "counts": {"grayscale": 1, "colored": 0, "composite": 0, "skipped": 0},
        }

    def fake_mosaic(frames, out_dir, *, date_key, level):
        calls["mosaic"].append((date_key, level, len(frames)))
        if len(frames) == 1:
            return dict(frames[0])
        return {"VV": f"/mosaic/{date_key}_{level}_VV.tif",
                "VH": f"/mosaic/{date_key}_{level}_VH.tif"}

    monkeypatch.setattr(m5, "generate_previews", fake_render)
    monkeypatch.setattr(m5, "mosaic_frames", fake_mosaic)
    return calls


class TestCancelPauseGate:
    def test_cancel_before_finalize_skips_render(self, stubbed):
        jc = _make_jc()
        jc.cancel_event.set()

        m5._finalize_date(jc, [_member(PID_A)])

        assert stubbed["render"] == []
        assert jc.dsmgr.states == []

    def test_pause_holds_render_until_resume(self, stubbed):
        jc = _make_jc()
        jc.pause_event.clear()
        rendered_while_paused: list[bool] = []

        original = m5.generate_previews

        def spy(*args, **kwargs):
            rendered_while_paused.append(not jc.pause_event.is_set())
            return original(*args, **kwargs)

        m5.generate_previews = spy
        threading.Timer(0.2, jc.pause_event.set).start()
        try:
            m5._finalize_date(jc, [_member(PID_A)])
        finally:
            m5.generate_previews = original

        assert rendered_while_paused == [False]

    def test_cancel_while_paused_skips_render(self, stubbed):
        jc = _make_jc()
        jc.pause_event.clear()

        def cancel_and_release():
            # Urutan dataset_manager saat Cancel: set cancel, lalu lepas pause.
            jc.cancel_event.set()
            jc.pause_event.set()

        threading.Timer(0.2, cancel_and_release).start()
        m5._finalize_date(jc, [_member(PID_A)])

        assert stubbed["render"] == []


class TestProducedFilesRecording:
    def test_first_level_files_survive_second_level_failure(self, stubbed):
        jc = _make_jc(levels=("RAW", "PROCESSED"))
        stubbed["PROCESSED"] = RuntimeError("render PROCESSED rusak")
        member = _member(PID_A, levels=("RAW", "PROCESSED"))

        m5._finalize_date(jc, [member])

        assert [lvl for lvl, _ in stubbed["render"]] == ["RAW", "PROCESSED"]
        assert member.produced_files["PREVIEW"] == ["/preview/RAW/s1_vv.png"]
        # Gagal preview tidak menandai scene FAILED maupun COMPLETED.
        assert (PID_A, "PREVIEW", "COMPLETED") not in jc.dsmgr.states
        assert all(status != "FAILED" for _, _, status in jc.dsmgr.states)

    def test_all_levels_recorded_on_success(self, stubbed):
        jc = _make_jc(levels=("RAW", "PROCESSED"))
        member = _member(PID_A, levels=("RAW", "PROCESSED"))

        m5._finalize_date(jc, [member])

        assert member.produced_files["PREVIEW"] == [
            "/preview/RAW/s1_vv.png", "/preview/PROCESSED/s1_vv.png",
        ]
        assert (PID_A, "PREVIEW", "COMPLETED") in jc.dsmgr.states


class TestOneFinalizationPerDate:
    """Inti perbaikan 22_try6: dua frame satu tanggal tidak boleh saling
    menimpa keluaran tanggal itu."""

    def test_two_frames_render_once_from_the_mosaic(self, stubbed):
        jc = _make_jc()
        members = [_member(PID_B, scene_id=12), _member(PID_A, scene_id=11)]

        m5._finalize_date(jc, members)

        assert len(stubbed["render"]) == 1, "preview dirender lebih dari sekali"
        level, s1_files = stubbed["render"][0]
        assert stubbed["mosaic"] == [("20240305", "PROCESSED", 2)]
        assert s1_files == {"VV": "/mosaic/20240305_PROCESSED_VV.tif",
                            "VH": "/mosaic/20240305_PROCESSED_VH.tif"}

    def test_single_frame_uses_its_own_raster(self, stubbed):
        jc = _make_jc()
        member = _member(PID_A)

        m5._finalize_date(jc, [member])

        _, s1_files = stubbed["render"][0]
        assert s1_files == member.s1_files_by_level["PROCESSED"]

    def test_output_attached_to_lowest_pid_regardless_of_order(self, stubbed):
        """Scene utama dipilih dari pid, bukan dari urutan selesai -- kalau
        tidak, hasilnya kembali bergantung pada balapan thread."""
        first = [_member(PID_B, scene_id=12), _member(PID_A, scene_id=11)]
        second = [_member(PID_A, scene_id=11), _member(PID_B, scene_id=12)]

        for members in (first, second):
            jc = _make_jc()
            m5._finalize_date(jc, members)
            owner = [m.pid for m in members if "PREVIEW" in m.produced_files]
            assert owner == [PID_A], owner

    def test_every_frame_gets_its_own_stage_state(self, stubbed):
        """Dua scene, satu render: keduanya tetap harus terlihat maju ke
        PREVIEW di UI, bukan cuma scene utama."""
        jc = _make_jc()

        m5._finalize_date(jc, [_member(PID_A), _member(PID_B, scene_id=12)])

        for pid in (PID_A, PID_B):
            assert (pid, "PREVIEW", "RUNNING") in jc.dsmgr.states
            assert (pid, "PREVIEW", "COMPLETED") in jc.dsmgr.states

    def test_empty_member_list_is_a_noop(self, stubbed):
        jc = _make_jc()
        m5._finalize_date(jc, [])
        assert stubbed["render"] == []


class TestReconcileStragglerFrame:
    """Insiden fusion_20250111_hybrid_processed.h5: frame A CLEANUP di run
    sebelumnya (tanggalnya sempat di-drain parsial karena frame B waktu itu
    gagal unduh); job di-resume, frame B akhirnya berhasil tapi
    `_download_one` melewatkan frame A (sudah `scene_is_done`) sehingga
    `pending` run baru cuma berisi frame B sendirian. Tanpa rekonsiliasi,
    drain menimpa stack dengan hanya frame B -- inilah yang diuji di sini
    tidak boleh terjadi lagi."""

    def test_frame_from_previous_run_is_merged_back_in(self, stubbed, monkeypatch):
        import queue

        from etl import refusion as rf

        jc = _make_jc()
        # Antrean RUN INI cuma membawa frame B (frame A sudah selesai & di-
        # skip _download_one di run sebelumnya).
        straggler_only = [_member(PID_B, scene_id=12)]

        def fake_scene_results_for_date(db, job_id, jc_arg, date_key):
            # Disk+DB (etl.refusion) masih ingat frame A dari run sebelumnya.
            return [_member(PID_A, scene_id=11)]

        monkeypatch.setattr(rf, "scene_results_for_date", fake_scene_results_for_date)

        m5._flush_date(jc, "20240305", straggler_only, queue.Queue())

        assert stubbed["mosaic"] == [("20240305", "PROCESSED", 2)], (
            "frame A dari run sebelumnya tidak ikut mosaik -- stack akan "
            "menimpa dirinya sendiri jadi cuma satu frame"
        )

    def test_reconcile_is_noop_when_nothing_missing(self, stubbed, monkeypatch):
        """Run normal, tanpa resume/retry: rekonsiliasi tidak boleh mengubah
        apa pun -- disk+DB melihat persis anggota yang sama."""
        import queue

        from etl import refusion as rf

        jc = _make_jc()
        members = [_member(PID_A, scene_id=11), _member(PID_B, scene_id=12)]
        monkeypatch.setattr(
            rf, "scene_results_for_date",
            lambda db, job_id, jc_arg, date_key: list(members),
        )

        m5._flush_date(jc, "20240305", members, queue.Queue())

        assert stubbed["mosaic"] == [("20240305", "PROCESSED", 2)]

    def test_reconcile_failure_falls_back_to_live_members(self, stubbed, monkeypatch):
        """Kalau rekonsiliasi sendiri gagal (mis. DB tidak terjangkau),
        finalize tetap jalan dengan apa yang ada di antrean run ini --
        tidak boleh menjatuhkan seluruh finalisasi tanggal."""
        import queue

        from etl import refusion as rf

        jc = _make_jc()
        members = [_member(PID_A, scene_id=11)]

        def boom(*_args, **_kwargs):
            raise RuntimeError("db tidak terjangkau")

        monkeypatch.setattr(rf, "scene_results_for_date", boom)

        m5._flush_date(jc, "20240305", members, queue.Queue())

        assert stubbed["mosaic"] == [("20240305", "PROCESSED", 1)]
