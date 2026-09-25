"""Tes pemetaan raster S1 yang dipakai perakitan ulang (etl/refusion.py).

Bagian inilah yang menentukan perbaikan berjalan tanpa unduhan: kalau raster
PROCESSED di disk gagal dicocokkan dengan product_identifier-nya, frame itu
lenyap dari mosaik dan stack hasil perbaikan cuma memuat sebagian AOI — persis
penyakit yang s1_mosaic dibuat untuk menyembuhkan.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from etl.refusion import _match_cogs, _s1_cogs_by_pid, scene_results_for_date

PID_A = "S1A_IW_GRDH_1SDV_20251204T222544_20251204T222609_062171_07C81E_541C.SAFE"
PID_B = "S1A_IW_GRDH_1SDV_20251204T222609_20251204T222637_062171_07C81E_4B33.SAFE"


@pytest.fixture
def proc_dir(tmp_path, monkeypatch):
    from etl import folder_manager as fm

    root = tmp_path / "27_JAWA_A"
    proc = root / "sentinel-1" / "PROCESSED"
    proc.mkdir(parents=True)
    for stem in ("S1A_IW_GRDH_1SDV_20251204T222544_20",
                 "S1A_IW_GRDH_1SDV_20251204T222609_20"):
        for band in ("VV", "VH"):
            (proc / f"{stem}_calibrated_{band}_lee.tif").write_bytes(b"x")
    monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: root)
    return proc


def test_kedua_band_tiap_frame_terpetakan(proc_dir):
    cogs = _s1_cogs_by_pid(27, "JAWA_A")
    assert len(cogs) == 2
    for bands in cogs.values():
        assert set(bands) == {"VV", "VH"}


def test_pid_utuh_cocok_dengan_nama_berkas_yang_terpotong(proc_dir):
    """Nama COG cuma memuat potongan pid, jadi pencocokannya lewat awalan."""
    cogs = _s1_cogs_by_pid(27, "JAWA_A")
    bands = _match_cogs(PID_A, cogs)
    assert set(bands) == {"VV", "VH"}
    assert "20251204T222544" in bands["VV"]


def test_dua_frame_satu_tanggal_tidak_saling_tertukar(proc_dir):
    """Frame satu tanggal beda hanya di detik akuisisi; tertukar di sini
    berarti mosaiknya menyusun frame yang salah."""
    cogs = _s1_cogs_by_pid(27, "JAWA_A")
    a = _match_cogs(PID_A, cogs)
    b = _match_cogs(PID_B, cogs)
    assert "20251204T222544" in a["VV"]
    assert "20251204T222609" in b["VV"]
    assert a["VV"] != b["VV"]


def test_pid_tanpa_raster_mengembalikan_kosong(proc_dir):
    hilang = "S1A_IW_GRDH_1SDV_20251299T000000_20251299T000030_000000_000000_0000.SAFE"
    assert _match_cogs(hilang, _s1_cogs_by_pid(27, "JAWA_A")) == {}


def test_folder_processed_tidak_ada_bukan_error(tmp_path, monkeypatch):
    from etl import folder_manager as fm

    monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: tmp_path / "kosong")
    assert _s1_cogs_by_pid(99, "TIDAK_ADA") == {}


class TestQueryLintasJob:
    """`scene_results_for_date` dulu di-scope ke satu job_id: frame yang
    tercatat di job lain (retry/resume yang dapat job_id baru) hilang dari
    daftar walau COG-nya lengkap di disk -- persis penyebab
    `fusion_20250111_hybrid_processed.h5` cuma memuat satu dari dua frame."""

    @pytest.fixture
    def dua_job(self, db_client, sample_dataset):
        from etl.database_client import DatasetJob

        with db_client.session() as sess:
            job_lama = DatasetJob(dataset_id=sample_dataset, job_type="CREATE", status="COMPLETED")
            job_baru = DatasetJob(dataset_id=sample_dataset, job_type="RESUME", status="COMPLETED")
            sess.add_all([job_lama, job_baru])
            sess.flush()
            return job_lama.job_id, job_baru.job_id

    @pytest.fixture
    def proc_dir_dua_frame(self, tmp_path, monkeypatch):
        from etl import folder_manager as fm

        root = tmp_path / "27_JAWA_A"
        proc = root / "sentinel-1" / "PROCESSED"
        proc.mkdir(parents=True)
        for stem in ("S1A_IW_GRDH_1SDV_20251204T222544_20",
                     "S1A_IW_GRDH_1SDV_20251204T222609_20"):
            for band in ("VV", "VH"):
                (proc / f"{stem}_calibrated_{band}_lee.tif").write_bytes(b"x")
        monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: root)
        return proc

    def test_frame_di_job_lain_tetap_ikut(
        self, db_client, sample_dataset, dua_job, proc_dir_dua_frame
    ):
        """Frame A dicatat di job lama, frame B di job baru (skenario retry
        yang dapat job_id baru). Merakit ulang lewat job LAMA harus tetap
        mengikutkan frame B, bukan cuma frame A."""
        from etl.database_client import SceneJobState

        job_lama, job_baru = dua_job
        with db_client.session() as sess:
            sess.add(SceneJobState(job_id=job_lama, product_identifier=PID_A))
            sess.add(SceneJobState(job_id=job_baru, product_identifier=PID_B))

        jc = SimpleNamespace(dataset_id=sample_dataset, dataset_name="JAWA_A")
        out = scene_results_for_date(db_client, job_lama, jc, "20251204")

        assert {m.pid for m in out} == {PID_A, PID_B}

    def test_baris_job_yang_diminta_menang_saat_pid_sama_di_dua_job(
        self, db_client, sample_dataset, dua_job, proc_dir_dua_frame
    ):
        """Kalau pid yang sama tercatat di dua job (mis. retry menulis ulang
        state-nya sendiri), baris dari job_id yang diminta yang dipakai."""
        from etl.database_client import SceneJobState

        job_lama, job_baru = dua_job
        with db_client.session() as sess:
            sess.add(SceneJobState(job_id=job_lama, product_identifier=PID_A, scene_id=None))
            sess.add(SceneJobState(job_id=job_baru, product_identifier=PID_A, scene_id=None))

        jc = SimpleNamespace(dataset_id=sample_dataset, dataset_name="JAWA_A")
        out = scene_results_for_date(db_client, job_baru, jc, "20251204")

        assert len(out) == 1
        assert out[0].pid == PID_A

    def test_cog_tanpa_baris_db_di_job_manapun_tidak_meledak(
        self, db_client, sample_dataset, dua_job, proc_dir_dua_frame
    ):
        """COG frame B ada di disk tapi tidak pernah dicatat di SceneJobState
        job manapun -- harus tetap hanya mengembalikan frame A yang tercatat,
        bukan error, dan tidak diam-diam mengarang frame B."""
        from etl.database_client import SceneJobState

        job_lama, _job_baru = dua_job
        with db_client.session() as sess:
            sess.add(SceneJobState(job_id=job_lama, product_identifier=PID_A))

        jc = SimpleNamespace(dataset_id=sample_dataset, dataset_name="JAWA_A")
        out = scene_results_for_date(db_client, job_lama, jc, "20251204")

        assert {m.pid for m in out} == {PID_A}


class TestTierRAWIkutTerekonstruksi:
    """`fusion_<tanggal>_hybrid_raw.h5` sumbernya sentinel-1/RAW/ (crop
    sebelum Lee filter), BUKAN sentinel-1/PROCESSED/. Ditemukan saat
    memverifikasi perbaikan dataset 35: tier PROCESSED pulih ke valid_fraction
    0.9996 sesudah refuse_date, tapi tier RAW-nya tetap 0.3578 karena
    scene_results_for_date cuma pernah mengisi s1_files_by_level["PROCESSED"]."""

    @pytest.fixture
    def satu_job(self, db_client, sample_dataset):
        from etl.database_client import DatasetJob

        with db_client.session() as sess:
            job = DatasetJob(dataset_id=sample_dataset, job_type="CREATE", status="COMPLETED")
            sess.add(job)
            sess.flush()
            return job.job_id

    def _tulis_raster(self, root, subfolder, suffix):
        d = root / "sentinel-1" / subfolder
        d.mkdir(parents=True, exist_ok=True)
        for band in ("VV", "VH"):
            (d / f"S1A_IW_GRDH_1SDV_20251204T222544_20_calibrated_{band}_{suffix}.tif").write_bytes(b"x")

    def test_raw_dan_processed_dua_duanya_terisi(
        self, tmp_path, db_client, sample_dataset, satu_job, monkeypatch
    ):
        from etl import folder_manager as fm
        from etl.database_client import SceneJobState

        root = tmp_path / "35_JAWA_A"
        self._tulis_raster(root, "RAW", "crop")
        self._tulis_raster(root, "PROCESSED", "lee")
        monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: root)

        with db_client.session() as sess:
            sess.add(SceneJobState(job_id=satu_job, product_identifier=PID_A))

        jc = SimpleNamespace(dataset_id=sample_dataset, dataset_name="JAWA_A")
        out = scene_results_for_date(db_client, satu_job, jc, "20251204")

        assert len(out) == 1
        assert set(out[0].s1_files_by_level.keys()) == {"RAW", "PROCESSED"}
        assert "_crop.tif" in out[0].s1_files_by_level["RAW"]["VV"]
        assert "_lee.tif" in out[0].s1_files_by_level["PROCESSED"]["VV"]

    def test_tanpa_raster_raw_di_disk_processed_tetap_jalan(
        self, tmp_path, db_client, sample_dataset, satu_job, monkeypatch
    ):
        """Dataset yang cuma menyimpan tier PROCESSED (RAW sudah disapu
        cleanup): tidak ada raster RAW di disk sama sekali -- perilaku lama
        (cuma PROCESSED) tetap harus jalan, bukan error."""
        from etl import folder_manager as fm
        from etl.database_client import SceneJobState

        root = tmp_path / "35_JAWA_A"
        self._tulis_raster(root, "PROCESSED", "lee")
        monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: root)

        with db_client.session() as sess:
            sess.add(SceneJobState(job_id=satu_job, product_identifier=PID_A))

        jc = SimpleNamespace(dataset_id=sample_dataset, dataset_name="JAWA_A")
        out = scene_results_for_date(db_client, satu_job, jc, "20251204")

        assert len(out) == 1
        assert set(out[0].s1_files_by_level.keys()) == {"PROCESSED"}
