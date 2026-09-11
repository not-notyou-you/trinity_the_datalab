"""
Unit tests: percabangan pipeline per-satelit (DOCS/ETL.md).

Coverage:
    - etl/processing_plan.py — SourcePlan/ProcessingPlan, pemetaan level ->
      tahap S1, band MODIS, window GPM, dan tag data_products.processing_level
    - etl/module7_modis_download.py — RAW cuma FLOOD ke bronze/, PROCESSED
      FLOOD+NDVI+NDWI ke silver/, RAW+PROCESSED keduanya
    - etl/module8_gpm_download.py — RAW cuma window 24h (1 granule harian),
      PROCESSED 24h/72h/7d (7 granule)
    - etl/module5_orchestrator.py — skip_stages hasil union required_tiers +
      level Sentinel-1, dan gerbang FUSION
    - data_products.processing_level tersimpan dan ter-CHECK di database

Jaringan tidak disentuh: tahap unduh/mosaic/reproject di-patch, yang diuji
adalah KEPUTUSAN percabangannya (band/window/tier mana yang diminta), bukan
implementasi raster-nya.

Run:
    pytest tests/test_processing_plan.py -v
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etl import folder_manager as fm
from etl import module7_modis_download as m7
from etl import module8_gpm_download as m8
from etl import processing_plan as pp
from etl.dataset_manager import compute_skip_stages
from etl.processing_plan import (
    GPM,
    MODIS,
    PROCESSED,
    RAW,
    SENTINEL1,
    SourcePlan,
    plan_from_configs,
)


def s1(*levels) -> SourcePlan:
    return SourcePlan(SENTINEL1, tuple(levels))


def modis(*levels) -> SourcePlan:
    return SourcePlan(MODIS, tuple(levels))


def gpm(*levels) -> SourcePlan:
    return SourcePlan(GPM, tuple(levels))


# ---------------------------------------------------------------------------
# SourcePlan
# ---------------------------------------------------------------------------
class TestSourcePlan:
    def test_normalize_levels_orders_and_dedups(self):
        assert pp.normalize_levels(["PROCESSED", "raw", "RAW"]) == (RAW, PROCESSED)
        assert pp.normalize_levels("processed") == (PROCESSED,)
        assert pp.normalize_levels([]) == ()

    def test_normalize_levels_drops_unknown(self):
        """Nilai asing dibuang, bukan menjatuhkan job."""
        assert pp.normalize_levels(["RAW", "BRONZE"]) == (RAW,)

    def test_s1_raw_only_skips_processed_stages(self):
        assert s1(RAW).s1_skip_stages() == {
            "LEE_FILTER", "QUALITY_ANALYTICS", "GOLD_EXPORT",
        }

    def test_s1_processed_skips_nothing(self):
        assert s1(PROCESSED).s1_skip_stages() == set()
        assert s1(RAW, PROCESSED).s1_skip_stages() == set()

    def test_download_calibrate_crop_never_skipped(self):
        """Nilai DN tanpa LUT sigma-nought tidak punya arti fisik, jadi RAW
        untuk SAR tetap berarti terkalibrasi + ter-crop."""
        for stage in pp.S1_BASE_STAGES:
            assert stage not in s1(RAW).s1_skip_stages()

    def test_tiers_per_level(self):
        assert s1(RAW).tiers() == {"RAW", "BRONZE"}
        assert s1(PROCESSED).tiers() == {"RAW", "BRONZE", "SILVER", "GOLD"}
        assert s1(RAW).max_tier == "BRONZE"
        assert s1(RAW, PROCESSED).max_tier == "GOLD"

    @pytest.mark.parametrize(
        "levels,tier,expected",
        [
            ((RAW,), "RAW", RAW),
            ((RAW,), "BRONZE", RAW),
            # Murni PROCESSED: RAW/BRONZE cuma langkah antara jalur penuh.
            ((PROCESSED,), "RAW", PROCESSED),
            ((PROCESSED,), "BRONZE", PROCESSED),
            ((PROCESSED,), "SILVER", PROCESSED),
            ((PROCESSED,), "GOLD", PROCESSED),
            # Keduanya: BRONZE adalah deliverable RAW, SILVER/GOLD PROCESSED.
            ((RAW, PROCESSED), "BRONZE", RAW),
            ((RAW, PROCESSED), "SILVER", PROCESSED),
            ((RAW, PROCESSED), "GOLD", PROCESSED),
        ],
    )
    def test_level_for_tier(self, levels, tier, expected):
        assert s1(*levels).level_for_tier(tier) == expected

    def test_modis_bands(self):
        assert modis(RAW).modis_bands() == ("FLOOD",)
        assert modis(PROCESSED).modis_bands() == ("FLOOD", "NDVI", "NDWI")
        assert modis(RAW, PROCESSED).modis_bands() == ("FLOOD", "NDVI", "NDWI")

    def test_gpm_windows_and_days(self):
        assert gpm(RAW).gpm_windows() == ("24h",)
        assert gpm(RAW).gpm_days() == 1
        assert gpm(PROCESSED).gpm_windows() == ("24h", "72h", "7d")
        assert gpm(PROCESSED).gpm_days() == 7

    def test_targets_raw_only(self):
        assert modis(RAW).targets() == {"FLOOD": (("BRONZE", RAW),)}
        assert gpm(RAW).targets() == {"24h": (("BRONZE", RAW),)}

    def test_targets_processed_only(self):
        targets = modis(PROCESSED).targets()
        assert set(targets) == {"FLOOD", "NDVI", "NDWI"}
        assert all(t == (("SILVER", PROCESSED),) for t in targets.values())

    def test_targets_both_levels_coexist(self):
        """DOCS/ETL.md: artefak RAW dan PROCESSED hidup berdampingan."""
        targets = modis(RAW, PROCESSED).targets()
        assert targets["FLOOD"] == (("BRONZE", RAW), ("SILVER", PROCESSED))
        assert targets["NDVI"] == (("SILVER", PROCESSED),)

    def test_targets_rejects_sentinel1(self):
        with pytest.raises(ValueError, match="MODIS/GPM"):
            s1(RAW).targets()


# ---------------------------------------------------------------------------
# ProcessingPlan
# ---------------------------------------------------------------------------
class TestProcessingPlan:
    def test_plan_from_configs_normalizes(self):
        plan = plan_from_configs(1, {"sentinel1": ["raw"], "GPM": "PROCESSED"})
        assert plan.get(SENTINEL1).levels == (RAW,)
        assert plan.get("gpm").levels == (PROCESSED,)
        assert plan.get(MODIS) is None
        assert plan.is_configured("SENTINEL1")
        assert not plan.is_configured("MODIS")

    def test_empty_levels_dropped(self):
        plan = plan_from_configs(1, {"SENTINEL1": [], "MODIS": ["RAW"]})
        assert plan.source_names == (MODIS,)

    def test_source_order_is_canonical(self):
        plan = plan_from_configs(1, {"GPM": ["RAW"], "SENTINEL1": ["RAW"]})
        assert plan.source_names == (SENTINEL1, GPM)

    def test_aux_sources_excludes_s1(self):
        plan = plan_from_configs(1, {"SENTINEL1": ["RAW"], "GPM": ["RAW"]})
        assert plan.aux_sources() == (GPM,)

    def test_required_tiers_is_union(self):
        plan = plan_from_configs(1, {"SENTINEL1": ["RAW"], "MODIS": ["PROCESSED"]})
        assert plan.required_tiers() == {"RAW", "BRONZE", "SILVER", "GOLD"}

    def test_fusion_needs_two_sources_and_strategy(self):
        one = plan_from_configs(1, {"SENTINEL1": ["PROCESSED"]})
        two = plan_from_configs(1, {"SENTINEL1": ["PROCESSED"], "GPM": ["RAW"]})
        assert not one.fusion_eligible("FULL_COVERAGE")
        assert not two.fusion_eligible(None)
        assert two.fusion_eligible("FULL_COVERAGE")

    def test_load_falls_back_to_legacy_when_unconfigured(self, db_client, sample_dataset):
        """Dataset tanpa baris konfigurasi tidak boleh menghasilkan plan kosong
        (job "berhasil" tanpa satu pun produk)."""
        with db_client.session() as sess:
            sess.execute(
                text("DELETE FROM dataset_source_config WHERE dataset_id = :d"),
                {"d": sample_dataset},
            )
        plan = pp.load_processing_plan(db_client, sample_dataset)
        assert plan.summary() == {
            SENTINEL1: [PROCESSED], MODIS: [PROCESSED], GPM: [PROCESSED],
        }

    def test_load_reads_configured_rows(self, db_client, sample_dataset):
        db_client.upsert_dataset_source_config(sample_dataset, "sentinel1", ["RAW"])
        db_client.upsert_dataset_source_config(sample_dataset, "gpm", ["RAW", "PROCESSED"])
        plan = pp.load_processing_plan(db_client, sample_dataset)
        assert plan.get(SENTINEL1).levels == (RAW,)
        assert plan.get(GPM).levels == (RAW, PROCESSED)


# ---------------------------------------------------------------------------
# Orchestrator: keputusan tahap
# ---------------------------------------------------------------------------
class TestOrchestratorStageDecisions:
    """Union dua sumber pembatas tahap yang dipakai run_dataset_job."""

    @staticmethod
    def _skips(required_tiers, plan):
        skips = compute_skip_stages(required_tiers)
        s1_plan = plan.get(SENTINEL1)
        if s1_plan is not None:
            skips |= s1_plan.s1_skip_stages()
        return skips

    def test_s1_raw_only_stops_after_crop(self):
        plan = plan_from_configs(1, {"SENTINEL1": ["RAW"]})
        skips = self._skips(["RAW", "BRONZE"], plan)
        assert "CROP" not in skips
        assert {"LEE_FILTER", "QUALITY_ANALYTICS", "GOLD_EXPORT"} <= skips

    def test_s1_raw_still_skipped_when_other_source_needs_gold(self):
        """required_tiers dataset bersifat global: MODIS PROCESSED menariknya
        sampai GOLD, tapi Sentinel-1 RAW tetap harus berhenti di CROP."""
        plan = plan_from_configs(1, {"SENTINEL1": ["RAW"], "MODIS": ["PROCESSED"]})
        skips = self._skips(sorted(plan.required_tiers()), plan)
        assert {"LEE_FILTER", "QUALITY_ANALYTICS", "GOLD_EXPORT"} <= skips

    def test_s1_processed_runs_full_chain(self):
        plan = plan_from_configs(1, {"SENTINEL1": ["RAW", "PROCESSED"]})
        skips = self._skips(sorted(plan.required_tiers()), plan)
        assert not {"LEE_FILTER", "QUALITY_ANALYTICS", "GOLD_EXPORT"} & skips


# ---------------------------------------------------------------------------
# MODIS
# ---------------------------------------------------------------------------
@pytest.fixture
def modis_stub(monkeypatch, tmp_path):
    """Ganti tahap jaringan/raster module7 dengan penulis file kosong.

    Mengembalikan daftar band yang benar-benar dibangun, supaya tes bisa
    memeriksa keputusan percabangan tanpa menyentuh LAADS."""
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
    built: list[str] = []

    def fake_build(*, band, product, date, date_key, tiles, raw_dir, out_path,
                   aoi_bbox, plog, dataset_id, scene_label):
        built.append(band)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"tif:" + band.encode())
        return {
            "band": band, "product": product, "path": str(out_path),
            "checksum_md5": "0" * 32, "source_tiles": {},
            "skipped": False, "degraded": False, "failed_tiles": [],
        }

    monkeypatch.setattr(m7, "_build_band_for_date", fake_build)
    monkeypatch.setattr(m7, "_md5", lambda p, chunk=0: "0" * 32)
    return built


def _run_modis(levels, tmp_path):
    day = datetime(2024, 3, 5, tzinfo=timezone.utc)
    return m7.download_modis_scene(
        7, "plan_test", day, day, (106.4, -6.7, 107.2, -5.9),
        processing_levels=levels,
    )


def _written(dataset_id, name, tier, source, date_key):
    d = fm.get_dataset_root(dataset_id, name) / date_key / tier / source
    return sorted(p.name for p in d.iterdir() if p.is_file()) if d.exists() else []


class TestModisBranching:
    def test_raw_builds_flood_only_at_bronze(self, modis_stub, tmp_path):
        _, meta = _run_modis(["RAW"], tmp_path)

        assert modis_stub == ["FLOOD"], "NDVI/NDWI adalah indeks turunan"
        assert _written(7, "plan_test", "bronze", "modis", "20240305") == [
            "modis_20240305_flood.tif"
        ]
        assert _written(7, "plan_test", "silver", "modis", "20240305") == []
        target = meta["outputs"][0]["bands"]["FLOOD"]["targets"]
        assert set(target) == {"BRONZE"}
        assert target["BRONZE"]["processing_level"] == RAW

    def test_raw_does_not_touch_reflectance_product(self, modis_stub, tmp_path):
        """MOD09GA tidak diunduh sama sekali di level RAW."""
        _, meta = _run_modis(["RAW"], tmp_path)
        assert meta["products"] == [m7.MODIS_FLOOD_PRODUCT]

    def test_processed_builds_three_bands_at_silver(self, modis_stub, tmp_path):
        _, meta = _run_modis(["PROCESSED"], tmp_path)

        assert modis_stub == ["FLOOD", "NDVI", "NDWI"]
        assert _written(7, "plan_test", "silver", "modis", "20240305") == [
            "modis_20240305_flood.tif",
            "modis_20240305_ndvi.tif",
            "modis_20240305_ndwi.tif",
        ]
        assert _written(7, "plan_test", "bronze", "modis", "20240305") == []
        for band in ("FLOOD", "NDVI", "NDWI"):
            targets = meta["outputs"][0]["bands"][band]["targets"]
            assert set(targets) == {"SILVER"}
            assert targets["SILVER"]["processing_level"] == PROCESSED

    def test_both_levels_write_flood_twice_but_build_once(self, modis_stub, tmp_path):
        _, meta = _run_modis(["RAW", "PROCESSED"], tmp_path)

        # Dibangun sekali; salinan BRONZE adalah copy file, bukan build ulang.
        assert modis_stub == ["FLOOD", "NDVI", "NDWI"]
        assert _written(7, "plan_test", "bronze", "modis", "20240305") == [
            "modis_20240305_flood.tif"
        ]
        assert len(_written(7, "plan_test", "silver", "modis", "20240305")) == 3

        flood = meta["outputs"][0]["bands"]["FLOOD"]["targets"]
        assert flood["BRONZE"]["processing_level"] == RAW
        assert flood["SILVER"]["processing_level"] == PROCESSED


# ---------------------------------------------------------------------------
# GPM
# ---------------------------------------------------------------------------
@pytest.fixture
def gpm_stub(monkeypatch, tmp_path):
    """Ganti akumulasi + reproyeksi module8. Mengembalikan daftar
    (window, num_days) yang diminta — itulah jumlah granule yang diunduh."""
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
    accumulated: list[tuple[str, int]] = []

    def fake_accumulate(end_date, num_days, raw_dir, *, plog=None, dataset_id=None,
                        scene_id="", window_name=""):
        accumulated.append((window_name, num_days))
        return object(), object(), "EPSG:4326", {
            "2024-03-05": {"checksum_md5": "0" * 32, "run": "F"}
        }

    def fake_reproject(accum, src_transform, src_crs, aoi_bbox, output_path):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"tif")
        return output_path

    monkeypatch.setattr(m8, "_accumulate_window", fake_accumulate)
    monkeypatch.setattr(m8, "_reproject_and_crop_to_s1_grid", fake_reproject)
    monkeypatch.setattr(m8, "_md5", lambda p, chunk=0: "0" * 32)
    return accumulated


def _run_gpm(levels):
    return m8.download_gpm_scene(
        8, "plan_test", datetime(2024, 3, 5, tzinfo=timezone.utc),
        (106.4, -6.7, 107.2, -5.9), processing_levels=levels,
    )


class TestGpmBranching:
    def test_raw_downloads_single_day_only(self, gpm_stub):
        _, meta = _run_gpm(["RAW"])

        assert gpm_stub == [("24h", 1)], "level RAW tidak boleh menarik hari tetangga"
        assert set(meta["windows"]) == {"24h"}
        assert _written(8, "plan_test", "bronze", "gpm", "20240305") == [
            "gpm_rain_24h_20240305.tif"
        ]
        assert _written(8, "plan_test", "silver", "gpm", "20240305") == []
        assert meta["windows"]["24h"]["targets"]["BRONZE"]["processing_level"] == RAW

    def test_processed_builds_all_windows(self, gpm_stub):
        _, meta = _run_gpm(["PROCESSED"])

        assert gpm_stub == [("24h", 1), ("72h", 3), ("7d", 7)]
        assert set(meta["windows"]) == {"24h", "72h", "7d"}
        assert _written(8, "plan_test", "silver", "gpm", "20240305") == [
            "gpm_rain_24h_20240305.tif",
            "gpm_rain_72h_20240305.tif",
            "gpm_rain_7d_20240305.tif",
        ]
        assert _written(8, "plan_test", "bronze", "gpm", "20240305") == []

    def test_both_levels_put_daily_rain_in_both_tiers(self, gpm_stub):
        _, meta = _run_gpm(["RAW", "PROCESSED"])

        assert gpm_stub == [("24h", 1), ("72h", 3), ("7d", 7)]
        assert _written(8, "plan_test", "bronze", "gpm", "20240305") == [
            "gpm_rain_24h_20240305.tif"
        ]
        assert len(_written(8, "plan_test", "silver", "gpm", "20240305")) == 3
        targets = meta["windows"]["24h"]["targets"]
        assert targets["BRONZE"]["processing_level"] == RAW
        assert targets["SILVER"]["processing_level"] == PROCESSED


# ---------------------------------------------------------------------------
# data_products.processing_level
# ---------------------------------------------------------------------------
class TestProcessingLevelColumn:
    def _insert(self, meta, scene_id, dataset_id, tier, band, level):
        job_id = meta.insert_processing_job(scene_id, "DOWNLOAD", parameters={})
        return meta.insert_data_product(
            scene_id=scene_id, job_id=job_id, dataset_id=dataset_id,
            product_tier=tier, source="SENTINEL1", product_type="CROPPED_TIFF",
            band_name=band, file_path=f"/tmp/{tier}_{band}_{level}.tif",
            file_name=f"{tier}_{band}_{level}.tif", file_size_mb=1.0,
            data_hash_sha256="a" * 64, processing_level=level,
        )

    def _level_of(self, db_client, product_id):
        with db_client.session() as sess:
            return sess.scalar(
                text("SELECT processing_level FROM data_products WHERE product_id = :p"),
                {"p": product_id},
            )

    def test_level_is_persisted(self, db_client, meta, sample_scene, sample_dataset):
        raw_id = self._insert(meta, sample_scene, sample_dataset, "BRONZE", "VV", RAW)
        proc_id = self._insert(meta, sample_scene, sample_dataset, "SILVER", "VV", PROCESSED)
        assert self._level_of(db_client, raw_id) == RAW
        assert self._level_of(db_client, proc_id) == PROCESSED

    def test_level_defaults_to_processed(self, meta, db_client, sample_scene, sample_dataset):
        """Perilaku pipeline sebelum migrasi 017: satu jalur, selalu penuh."""
        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD", parameters={})
        pid = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id, dataset_id=sample_dataset,
            product_tier="GOLD", source="SENTINEL1", product_type="COG",
            band_name="VH", file_path="/tmp/default.tif", file_name="default.tif",
            file_size_mb=1.0, data_hash_sha256="b" * 64,
        )
        assert self._level_of(db_client, pid) == PROCESSED

    def test_unknown_level_rejected_before_insert(self, meta, sample_scene, sample_dataset):
        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD", parameters={})
        with pytest.raises(ValueError):
            meta.insert_data_product(
                scene_id=sample_scene, job_id=job_id, dataset_id=sample_dataset,
                product_tier="BRONZE", source="SENTINEL1", product_type="CROPPED_TIFF",
                band_name="VV", file_path="/tmp/bad.tif", file_name="bad.tif",
                file_size_mb=1.0, data_hash_sha256="c" * 64,
                processing_level="BRONZE",
            )

    def test_check_constraint_guards_raw_sql(self, db_client, meta, sample_scene, sample_dataset):
        """Jaring pengaman untuk penulis yang tidak lewat MetadataManager."""
        product_id = self._insert(meta, sample_scene, sample_dataset, "BRONZE", "VV", RAW)
        with pytest.raises(IntegrityError):
            with db_client.session() as sess:
                sess.execute(
                    text("""
                        UPDATE data_products SET processing_level = 'SILVER'
                        WHERE product_id = :p
                    """),
                    {"p": product_id},
                )
