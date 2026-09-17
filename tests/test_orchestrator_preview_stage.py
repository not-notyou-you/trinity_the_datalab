# tests/test_orchestrator_preview_stage.py
"""
Tests untuk blok PREVIEW di etl/module5_orchestrator._process_scene.

Unit test murni: rantai S1, input aux, dan module10 di-stub, _JobContext
diganti namespace berisi atribut yang memang dibaca blok itu. Yang diuji
hanya dua kontrak orkestrasi:

    1. Cancel/pause dicek ulang SESUDAH input aux, sebelum render dimulai.
    2. PNG yang sudah ditulis tetap tercatat di produced_files walau level
       berikutnya gagal.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from etl import folder_manager as fm
from etl import module5_orchestrator as m5

PID = "S1A_IW_GRDH_TEST_20240305"
ACQ = datetime(2024, 3, 5, 22, 50, tzinfo=timezone.utc)


class _Stage:
    def output(self, **_):
        pass


class _FakePlog:
    @contextmanager
    def stage(self, *args, **kwargs):
        yield _Stage()


class _RecordingDsmgr:
    def __init__(self):
        self.states: list[tuple[str, str]] = []

    def upsert_scene_job_state(self, job_id, pid, **fields):
        self.states.append((fields.get("current_stage"), fields.get("stage_status")))


def _make_jc(levels=("PROCESSED",)):
    pause = threading.Event()
    pause.set()
    return SimpleNamespace(
        job_id=1, dataset_id=7, dataset_name="Preview Stage Test",
        db=None, region_id=1, bbox_tuple=(106.4, -6.7, 107.2, -5.9),
        plog=_FakePlog(), dsmgr=_RecordingDsmgr(),
        plan=SimpleNamespace(output_levels=lambda: list(levels), source_count=1),
        # FUSION dilewati supaya _process_scene berhenti tepat setelah PREVIEW.
        skip_stages={"FUSION"}, fusion_strategy=None, preview_options=None,
        pause_event=pause, cancel_event=threading.Event(),
    )


@pytest.fixture
def stubbed(monkeypatch, tmp_path):
    """Stub rantai S1 dan module10; kembalikan dict yang mencatat panggilan."""
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
    calls = {"render": [], "aux_hook": None}

    monkeypatch.setattr(
        m5, "_run_s1_chain",
        lambda jc, meta, dl: (11, ["COG"], {"COG": ["vv.tif", "vh.tif"]},
                              {"VV": "vv.tif", "VH": "vh.tif"}),
    )

    def fake_aux(*args, **kwargs):
        if calls["aux_hook"]:
            calls["aux_hook"]()
        return {}

    monkeypatch.setattr(m5, "ensure_aux_inputs_for_date", fake_aux)

    def fake_render(dataset_id, dataset_name, acq_date, *, processing_level, **kw):
        calls["render"].append(processing_level)
        behaviour = calls.get(processing_level)
        if isinstance(behaviour, Exception):
            raise behaviour
        files = [f"/preview/{processing_level}/s1_vv.png"]
        return {
            "files": files, "total_size_mb": 0.1,
            "counts": {"grayscale": 1, "colored": 0, "composite": 0, "skipped": 0},
        }

    monkeypatch.setattr(m5, "generate_previews", fake_render)
    return calls


def _run(jc):
    return m5._process_scene(
        jc, {"product_identifier": PID}, SimpleNamespace(acquisition_datetime=ACQ)
    )


class TestCancelPauseGate:
    def test_cancel_during_aux_skips_render(self, stubbed):
        jc = _make_jc()
        # Cancel ditekan selagi MODIS/GPM masih diunduh.
        stubbed["aux_hook"] = jc.cancel_event.set

        _, _, produced_files = _run(jc)

        assert stubbed["render"] == []
        assert "PREVIEW" not in produced_files
        assert ("PREVIEW", "RUNNING") not in jc.dsmgr.states

    def test_pause_during_aux_holds_render_until_resume(self, stubbed):
        jc = _make_jc()
        rendered_while_paused: list[bool] = []

        def pause_then_resume_later():
            jc.pause_event.clear()
            threading.Timer(0.2, jc.pause_event.set).start()

        stubbed["aux_hook"] = pause_then_resume_later

        original = m5.generate_previews

        def spy(*args, **kwargs):
            rendered_while_paused.append(not jc.pause_event.is_set())
            return original(*args, **kwargs)

        m5.generate_previews = spy
        try:
            _run(jc)
        finally:
            m5.generate_previews = original

        assert rendered_while_paused == [False]

    def test_cancel_while_paused_skips_render(self, stubbed):
        jc = _make_jc()

        def pause_then_cancel():
            jc.pause_event.clear()

            def cancel_and_release():
                # Urutan dataset_manager saat Cancel: set cancel, lalu lepas pause.
                jc.cancel_event.set()
                jc.pause_event.set()

            threading.Timer(0.2, cancel_and_release).start()

        stubbed["aux_hook"] = pause_then_cancel

        _run(jc)

        assert stubbed["render"] == []


class TestProducedFilesRecording:
    def test_first_level_files_survive_second_level_failure(self, stubbed):
        jc = _make_jc(levels=("RAW", "PROCESSED"))
        stubbed["PROCESSED"] = RuntimeError("render PROCESSED rusak")

        _, _, produced_files = _run(jc)

        assert stubbed["render"] == ["RAW", "PROCESSED"]
        assert produced_files["PREVIEW"] == ["/preview/RAW/s1_vv.png"]
        # Gagal preview tidak menandai scene FAILED maupun COMPLETED.
        assert ("PREVIEW", "COMPLETED") not in jc.dsmgr.states
        assert all(status != "FAILED" for _, status in jc.dsmgr.states)

    def test_all_levels_recorded_on_success(self, stubbed):
        jc = _make_jc(levels=("RAW", "PROCESSED"))

        _, _, produced_files = _run(jc)

        assert produced_files["PREVIEW"] == [
            "/preview/RAW/s1_vv.png", "/preview/PROCESSED/s1_vv.png",
        ]
        assert ("PREVIEW", "COMPLETED") in jc.dsmgr.states
