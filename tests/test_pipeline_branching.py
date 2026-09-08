"""
Integration tests: run_dataset_job() dengan konfigurasi per-satelit.

Menjalankan orchestrator ujung-ke-ujung terhadap database uji, dengan HANYA
lapisan jaringan/raster yang di-stub (CDSE, LAADS, GES DISC, kalibrasi, crop,
Lee filter, ekspor COG). Semua keputusan percabangan, penulisan berkas, dan
registrasi `data_products` berjalan apa adanya — itulah yang diuji.

Skenario (sesuai DOCS/ETL.md "Pipeline Branching Logic"):
    sentinel1[RAW]              -> BRONZE saja, tidak ada SILVER/GOLD
    sentinel1[RAW,PROCESSED]    -> BRONZE (RAW) + SILVER/GOLD (PROCESSED)
    modis[PROCESSED]            -> FLOOD + NDVI + NDWI
    gpm[RAW]                    -> satu berkas rainfall, tanpa 72h/7d
    plus: data_products.processing_level ditandai benar di tiap tier

Run:
    pytest tests/test_pipeline_branching.py -v
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text

from etl import folder_manager as fm
from etl import module5_orchestrator as m5
from etl import module7_modis_download as m7
from etl import module8_gpm_download as m8
from etl import module9_fusion as m9
from etl.database_client import Dataset, DatasetJob
from etl.module1_download import DownloadResult

ACQ = datetime(2024, 3, 5, 22, 50, tzinfo=timezone.utc)
DATE_KEY = "20240305"
BBOX_WKT = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))"
PID = f"S1A_IW_GRDH_TEST_{DATE_KEY}"


def _touch(path, payload: bytes = b"raster") -> str:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return str(path)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def job_factory(db_client, sample_region, tmp_path, monkeypatch):
    """Buat dataset + dataset_job dengan konfigurasi sumber tertentu, dan
    arahkan seluruh penulisan berkas ke tmp_path."""
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")

    def make(sources: dict, fusion_strategy=None, required_tiers=None):
        from etl.database_client import derive_required_tiers, normalize_source_configs

        configs = normalize_source_configs(sources)
        tiers = required_tiers or derive_required_tiers(
            configs, with_fusion=bool(fusion_strategy)
        )
        with db_client.session() as sess:
            ds = Dataset(
                name=f"BRANCH_{datetime.now().timestamp()}",
                location_label="Test AOI",
                region_id=sample_region,
                bbox=f"SRID=4326;{BBOX_WKT}",
                bbox_wkt=BBOX_WKT,
                date_start=date(2024, 3, 5),
                date_end=date(2024, 3, 5),
                required_tiers=tiers,
                fusion_strategy=fusion_strategy,
                dataset_kind="STANDARD",
                status="QUEUED",
                generate_preview=False,
            )
            sess.add(ds)
            sess.flush()
            dataset_id, dataset_name = ds.dataset_id, ds.name
            job = DatasetJob(
                dataset_id=dataset_id, job_type="CREATE", status="QUEUED",
                date_range_start=date(2024, 3, 5), date_range_end=date(2024, 3, 5),
            )
            sess.add(job)
            sess.flush()
            job_id = job.job_id

        for source, levels in configs.items():
            db_client.upsert_dataset_source_config(dataset_id, source, levels)
        return dataset_id, dataset_name, job_id

    return make


@pytest.fixture
def stub_sentinel1(monkeypatch, tmp_path):
    """Stub discovery + download + tiap tahap raster S1."""

    def fake_discover(bbox_wkt, date_from, date_to, max_results=200):
        return [{"product_identifier": PID, "size_mb": 1.0, "cloud_cover": 0}]

    def fake_download(scene_meta, output_dir, keep_raw=True, progress_cb=None):
        out = Path(output_dir)
        return DownloadResult(
            product_identifier=PID,
            zip_path=_touch(out / f"{PID}.SAFE.zip"),
            vv_tif_path=_touch(out / f"{PID}_vv.tif"),
            vh_tif_path=_touch(out / f"{PID}_vh.tif"),
            file_size_mb=1.0,
            checksum_md5="0" * 32,
            acquisition_datetime=ACQ,
            orbit_direction="ASCENDING",
            orbit_number=1,
            relative_orbit=1,
            cloud_cover=0.0,
            incidence_near=30.0,
            incidence_far=45.0,
            download_url="https://example.invalid/scene",
        )

    def fake_calibrate(zip_path, vv, vh, out_dir):
        d = Path(out_dir)
        return _touch(d / "cal_vv.tif"), _touch(d / "cal_vh.tif")

    def fake_crop(vv, vh, out_dir, bbox):
        d = Path(out_dir)
        return _touch(d / "crop_vv.tif"), _touch(d / "crop_vh.tif")

    def fake_lee(vv, vh, out_dir, window_size=7, looks=1):
        d = Path(out_dir)
        return _touch(d / "lee_vv.tif"), _touch(d / "lee_vh.tif")

    def fake_gold(dataset_id, dataset_name, source, scene_key, silver_files):
        d = fm.ensure_scene_dir(dataset_id, dataset_name, "gold", source, scene_key)
        return {band: _touch(d / f"{source}_{scene_key}_{band}.tif")
                for band in silver_files}

    # dataclass, bukan kelas biasa: orchestrator memanggil asdict() atasnya.
    @dataclass
    class _Metrics:
        total_pixels: int = 100
        valid_pixels: int = 100
        nodata_pixels: int = 0
        quality_score: float = 90.0
        backscatter_mean_db: float = -12.0
        backscatter_std_db: float = 2.0
        backscatter_min_db: float = -30.0
        backscatter_max_db: float = 0.0
        radiometric_consistency: bool = True
        speckle_index: float = 0.2
        quality_flag: str = "PASS"

    monkeypatch.setattr(m5, "discover_scenes", fake_discover)
    monkeypatch.setattr(m5, "download_scene", fake_download)
    monkeypatch.setattr(m5, "calibrate_run", fake_calibrate)
    monkeypatch.setattr(m5, "crop_run", fake_crop)
    monkeypatch.setattr(m5, "lee_run", fake_lee)
    monkeypatch.setattr(m5, "export_scene_to_gold", fake_gold)
    monkeypatch.setattr(m5, "compute_band_metrics",
                        lambda path, band, min_quality_score=60.0: _Metrics())
    monkeypatch.setattr(m5, "create_fusion_stack",
                        lambda *a, **kw: pytest.fail("FUSION seharusnya tidak jalan"))


@pytest.fixture
def stub_aux(monkeypatch):
    """Stub lapisan jaringan MODIS/GPM + ekspor COG-nya. Registrasi
    data_products di module9 sengaja TIDAK di-stub — itu yang diuji."""

    def fake_modis_build(*, band, product, date, date_key, tiles, raw_dir, out_path,
                         aoi_bbox, plog, dataset_id, scene_label):
        _touch(out_path, b"modis:" + band.encode())
        return {"band": band, "product": product, "path": str(out_path),
                "checksum_md5": "0" * 32, "source_tiles": {}, "skipped": False,
                "degraded": False, "failed_tiles": []}

    def fake_accumulate(end_date, num_days, raw_dir, *, plog=None, dataset_id=None,
                        scene_id="", window_name=""):
        return object(), object(), "EPSG:4326", {
            "2024-03-05": {"checksum_md5": "0" * 32, "run": "F"}
        }

    def fake_reproject(accum, src_transform, src_crs, aoi_bbox, output_path):
        return Path(_touch(output_path, b"gpm"))

    def fake_gold(dataset_id, dataset_name, source, scene_key, silver_files):
        d = fm.ensure_scene_dir(dataset_id, dataset_name, "gold", source, scene_key)
        return {band: _touch(d / Path(path).name)
                for band, path in silver_files.items()}

    monkeypatch.setattr(m7, "_build_band_for_date", fake_modis_build)
    monkeypatch.setattr(m7, "_md5", lambda p, chunk=0: "0" * 32)
    monkeypatch.setattr(m8, "_accumulate_window", fake_accumulate)
    monkeypatch.setattr(m8, "_reproject_and_crop_to_s1_grid", fake_reproject)
    monkeypatch.setattr(m8, "_md5", lambda p, chunk=0: "0" * 32)
    monkeypatch.setattr(m9.m4, "export_scene_to_gold", fake_gold)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def products(db_client, dataset_id, source=None) -> list[tuple]:
    """(tier, processing_level, band, file_name) tiap data_products dataset."""
    sql = """
        SELECT product_tier, processing_level, band_name, file_name
        FROM data_products WHERE dataset_id = :d
    """
    params = {"d": dataset_id}
    if source:
        sql += " AND source = :s"
        params["s"] = source
    sql += " ORDER BY product_tier, band_name"
    with db_client.session() as sess:
        return [tuple(r) for r in sess.execute(text(sql), params)]


def tiers(rows) -> set[str]:
    return {r[0] for r in rows}


def files_in(dataset_id, dataset_name, tier, source, scene_key) -> list[str]:
    d = fm.get_dataset_root(dataset_id, dataset_name) / tier / source / scene_key
    return sorted(p.name for p in d.iterdir()) if d.exists() else []


# ---------------------------------------------------------------------------
# Sentinel-1
# ---------------------------------------------------------------------------
class TestSentinel1Branching:
    def test_raw_only_stops_at_bronze(self, db_client, job_factory, stub_sentinel1):
        dataset_id, name, job_id = job_factory({"sentinel1": ["RAW"]})

        m5.run_dataset_job(db_client, job_id)

        rows = products(db_client, dataset_id)
        assert tiers(rows) == {"RAW", "BRONZE"}, rows
        assert all(level == "RAW" for _, level, _, _ in rows), rows

        assert len(files_in(dataset_id, name, "bronze", "sentinel1", PID)) == 2
        assert files_in(dataset_id, name, "silver", "sentinel1", PID) == []
        assert files_in(dataset_id, name, "gold", "sentinel1", PID) == []

    def test_raw_plus_processed_produces_both(self, db_client, job_factory, stub_sentinel1):
        dataset_id, name, job_id = job_factory({"sentinel1": ["RAW", "PROCESSED"]})

        m5.run_dataset_job(db_client, job_id)

        rows = products(db_client, dataset_id)
        assert tiers(rows) == {"RAW", "BRONZE", "SILVER", "GOLD"}, rows

        by_tier = {tier: {level for t, level, _, _ in rows if t == tier}
                   for tier in tiers(rows)}
        # BRONZE adalah deliverable RAW; SILVER/GOLD milik jalur PROCESSED.
        assert by_tier["BRONZE"] == {"RAW"}
        assert by_tier["SILVER"] == {"PROCESSED"}
        assert by_tier["GOLD"] == {"PROCESSED"}

        assert len(files_in(dataset_id, name, "bronze", "sentinel1", PID)) == 2
        assert len(files_in(dataset_id, name, "silver", "sentinel1", PID)) == 3  # +metadata_qa
        assert len(files_in(dataset_id, name, "gold", "sentinel1", PID)) == 2

    def test_processed_only_tags_every_tier_processed(
        self, db_client, job_factory, stub_sentinel1
    ):
        """Tanpa level RAW, BRONZE cuma langkah antara jalur penuh."""
        dataset_id, _, job_id = job_factory({"sentinel1": ["PROCESSED"]})

        m5.run_dataset_job(db_client, job_id)

        rows = products(db_client, dataset_id)
        assert tiers(rows) == {"RAW", "BRONZE", "SILVER", "GOLD"}
        assert all(level == "PROCESSED" for _, level, _, _ in rows), rows

    def test_quality_metrics_only_for_processed(self, db_client, job_factory, stub_sentinel1):
        raw_id, _, raw_job = job_factory({"sentinel1": ["RAW"]})
        proc_id, _, proc_job = job_factory({"sentinel1": ["PROCESSED"]})

        m5.run_dataset_job(db_client, raw_job)
        m5.run_dataset_job(db_client, proc_job)

        def qa_count(dataset_id):
            with db_client.session() as sess:
                return sess.scalar(text("""
                    SELECT COUNT(*) FROM quality_metrics q
                    JOIN data_products p ON p.product_id = q.product_id
                    WHERE p.dataset_id = :d
                """), {"d": dataset_id})

        assert qa_count(raw_id) == 0, "QA analytics adalah tahap PROCESSED"
        assert qa_count(proc_id) == 2


# ---------------------------------------------------------------------------
# MODIS / GPM (jalur aux-only: dataset tanpa Sentinel-1)
# ---------------------------------------------------------------------------
class TestAuxBranching:
    def test_modis_processed_produces_flood_ndvi_ndwi(
        self, db_client, job_factory, stub_aux
    ):
        dataset_id, name, job_id = job_factory({"modis": ["PROCESSED"]})

        m5.run_dataset_job(db_client, job_id)

        assert files_in(dataset_id, name, "silver", "modis", DATE_KEY) == [
            "modis_20240305_flood.tif",
            "modis_20240305_ndvi.tif",
            "modis_20240305_ndwi.tif",
        ]
        assert files_in(dataset_id, name, "bronze", "modis", DATE_KEY) == []

        rows = products(db_client, dataset_id, source="MODIS")
        assert tiers(rows) == {"SILVER", "GOLD"}
        assert all(level == "PROCESSED" for _, level, _, _ in rows), rows
        assert {band for _, _, band, _ in rows} == {"FLOOD", "NDVI", "NDWI"}

    def test_modis_raw_produces_flood_only_at_bronze(
        self, db_client, job_factory, stub_aux
    ):
        dataset_id, name, job_id = job_factory({"modis": ["RAW"]})

        m5.run_dataset_job(db_client, job_id)

        assert files_in(dataset_id, name, "bronze", "modis", DATE_KEY) == [
            "modis_20240305_flood.tif"
        ]
        assert files_in(dataset_id, name, "silver", "modis", DATE_KEY) == []
        rows = products(db_client, dataset_id, source="MODIS")
        assert tiers(rows) == {"BRONZE"}
        assert {band for _, _, band, _ in rows} == {"FLOOD"}
        assert all(level == "RAW" for _, level, _, _ in rows), rows

    def test_gpm_raw_produces_single_rainfall_file(
        self, db_client, job_factory, stub_aux
    ):
        dataset_id, name, job_id = job_factory({"gpm": ["RAW"]})

        m5.run_dataset_job(db_client, job_id)

        assert files_in(dataset_id, name, "bronze", "gpm", DATE_KEY) == [
            "gpm_rain_24h_20240305.tif"
        ]
        assert files_in(dataset_id, name, "silver", "gpm", DATE_KEY) == []
        assert files_in(dataset_id, name, "gold", "gpm", DATE_KEY) == []

        rows = products(db_client, dataset_id, source="GPM")
        assert {band for _, _, band, _ in rows} == {"RAIN_24H"}, "tidak ada 72h/7d"
        assert tiers(rows) == {"BRONZE"}
        assert all(level == "RAW" for _, level, _, _ in rows), rows

    def test_gpm_processed_produces_all_windows(self, db_client, job_factory, stub_aux):
        dataset_id, name, job_id = job_factory({"gpm": ["PROCESSED"]})

        m5.run_dataset_job(db_client, job_id)

        assert len(files_in(dataset_id, name, "silver", "gpm", DATE_KEY)) == 3
        rows = products(db_client, dataset_id, source="GPM")
        assert {band for _, _, band, _ in rows} == {"RAIN_24H", "RAIN_72H", "RAIN_7D"}
        assert tiers(rows) == {"SILVER", "GOLD"}


# ---------------------------------------------------------------------------
# Isolasi antar-sumber
# ---------------------------------------------------------------------------
class TestSourceIsolation:
    def test_unconfigured_source_is_never_downloaded(
        self, db_client, job_factory, stub_aux
    ):
        """Dataset yang cuma memilih GPM tidak boleh menarik MODIS."""
        dataset_id, name, job_id = job_factory({"gpm": ["RAW"]})

        m5.run_dataset_job(db_client, job_id)

        assert files_in(dataset_id, name, "bronze", "modis", DATE_KEY) == []
        assert files_in(dataset_id, name, "silver", "modis", DATE_KEY) == []
        assert products(db_client, dataset_id, source="MODIS") == []

    def test_s1_raw_does_not_cut_off_processed_aux_source(
        self, db_client, job_factory, stub_sentinel1, stub_aux
    ):
        """Level satu sumber tidak boleh memutus sumber lain: S1 berhenti di
        BRONZE, MODIS tetap harus sampai NDVI/NDWI."""
        dataset_id, name, job_id = job_factory(
            {"sentinel1": ["RAW"], "modis": ["PROCESSED"]}
        )

        m5.run_dataset_job(db_client, job_id)

        s1_rows = products(db_client, dataset_id, source="SENTINEL1")
        assert tiers(s1_rows) == {"RAW", "BRONZE"}
        assert all(level == "RAW" for _, level, _, _ in s1_rows)

        modis_rows = products(db_client, dataset_id, source="MODIS")
        assert {band for _, _, band, _ in modis_rows} == {"FLOOD", "NDVI", "NDWI"}
        assert all(level == "PROCESSED" for _, level, _, _ in modis_rows)

    def test_fusion_skipped_for_single_source(self, db_client, job_factory, stub_sentinel1):
        """stub_sentinel1 membuat create_fusion_stack gagal keras kalau
        dipanggil; satu sumber tidak boleh memicunya."""
        _, _, job_id = job_factory({"sentinel1": ["PROCESSED"]})
        m5.run_dataset_job(db_client, job_id)
