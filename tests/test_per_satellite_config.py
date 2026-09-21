"""
Integration tests: konfigurasi per-satelit sampai ke isi berkas HDF5 fusion.

Bedanya dengan tests/test_pipeline_branching.py — yang menguji tier dan
`data_products.processing_level` per sumber — modul ini menjalankan pipeline
sampai TAHAP FUSION dan PREVIEW, lalu membuka artefaknya:

    Test Case 1  sentinel1[RAW] + modis[PROCESSED] + gpm[RAW]
                 -> satu HDF5, group-nya persis sumber yang dikonfigurasi,
                    tiap sumber pada levelnya sendiri
    Test Case 2  sentinel1[RAW,PROCESSED] + modis[PROCESSED]
                 -> DUA HDF5 (satu RAW, satu PROCESSED), dua baris
                    fusion_products, dua baris data_products
    Test Case 3  modis[PROCESSED] saja
                 -> fusi mati, tidak ada HDF5 sama sekali

Ditambah: checksum, lineage, penandaan metadata, tier input yang dibaca
(BRONZE vs GOLD), dan alur "Pakai Config Sebelumnya" ujung-ke-ujung.

Karena fusion benar-benar membuka rasternya dengan rasterio, stub di sini
menulis GeoTIFF SUNGGUHAN (kecil, 24x32) — bukan byte placeholder seperti di
test_pipeline_branching. Yang di-stub hanya lapisan jaringan dan matematika
per-piksel; semua keputusan percabangan, penulisan berkas, dan registrasi
database berjalan apa adanya.

Run:
    pytest tests/test_per_satellite_config.py -v
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import h5py
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from sqlalchemy import text

from etl import folder_manager as fm
from etl import fusion_strategies as fs
from etl import module4_gold_export as m4
from etl import module5_orchestrator as m5
from etl import module7_modis_download as m7
from etl import module8_gpm_download as m8
from etl import module9_fusion as m9
from etl import module10_generate_preview as m10
from etl.database_client import (
    Dataset, DatasetJob, _validate_fusion_output_only,
)
from etl.module1_download import DownloadResult
from etl.processing_plan import plan_from_configs

ACQ = datetime(2024, 3, 5, 22, 50, tzinfo=timezone.utc)
S1_DATE = date(2024, 3, 5)
DATE_KEY = "20240305"
BBOX = (106.4, -6.7, 107.2, -5.9)
BBOX_WKT = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))"
PID = f"S1A_IW_GRDH_TEST_{DATE_KEY}"

# Raster uji dibuat kecil tapi BUKAN persegi: bug transpose baris/kolom lolos
# dari grid persegi, dan reprojeksi fusion adalah tempat bug seperti itu hidup.
RASTER_SHAPE = (24, 32)


def _write_tif(path, value: float = 1.0, dtype: str = "float32") -> str:
    """GeoTIFF nyata yang menutupi AOI uji. Isinya gradien, bukan konstanta,
    supaya reprojeksi yang salah tidak lolos hanya karena semua piksel sama."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    height, width = RASTER_SHAPE
    data = (
        np.arange(height * width, dtype="float32").reshape(RASTER_SHAPE) / 100.0 + value
    ).astype(dtype)
    with rasterio.open(
        path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype=dtype, crs="EPSG:4326",
        transform=from_origin(BBOX[0], BBOX[3], 0.025, 0.0333),
        nodata=None,
    ) as dst:
        dst.write(data, 1)
    return str(path)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def job_factory(db_client, sample_region, tmp_path, monkeypatch):
    """Dataset + dataset_job dengan konfigurasi sumber tertentu, semua tulisan
    berkas diarahkan ke tmp_path."""
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")

    def make(sources: dict, fusion_strategy=None, required_tiers=None,
             generate_preview=False, preview_options=None,
             date_start=S1_DATE, date_end=S1_DATE,
             fusion_output_only=False, s1_match_tolerance_days=None):
        from etl.database_client import derive_required_tiers, normalize_source_configs

        configs = normalize_source_configs(sources)
        tiers = required_tiers or derive_required_tiers(
            configs, with_fusion=bool(fusion_strategy)
        )
        with db_client.session() as sess:
            ds = Dataset(
                name=f"PERSAT_{datetime.now().timestamp()}",
                location_label="Test AOI",
                region_id=sample_region,
                bbox=f"SRID=4326;{BBOX_WKT}",
                bbox_wkt=BBOX_WKT,
                date_start=date_start,
                date_end=date_end,
                required_tiers=tiers,
                fusion_strategy=fusion_strategy,
                dataset_kind="STANDARD",
                status="QUEUED",
                generate_preview=generate_preview,
                preview_options=preview_options,
                fusion_output_only=_validate_fusion_output_only(
                    fusion_output_only, fusion_strategy
                ),
                **({"s1_match_tolerance_days": s1_match_tolerance_days}
                   if s1_match_tolerance_days is not None else {}),
            )
            sess.add(ds)
            sess.flush()
            dataset_id, dataset_name = ds.dataset_id, ds.name
            job = DatasetJob(
                dataset_id=dataset_id, job_type="CREATE", status="QUEUED",
                date_range_start=date_start, date_range_end=date_end,
            )
            sess.add(job)
            sess.flush()
            job_id = job.job_id

        for source, levels in configs.items():
            db_client.upsert_dataset_source_config(dataset_id, source, levels)
        return dataset_id, dataset_name, job_id

    return make


@pytest.fixture
def stub_rasters(monkeypatch):
    """Stub jaringan + matematika per-piksel, tapi tulis GeoTIFF sungguhan.

    create_fusion_stack SENGAJA tidak di-stub: isi HDF5-nya justru yang diuji.
    """

    def fake_discover(bbox_wkt, date_from, date_to, max_results=200):
        # `acquisition_datetime` ikut karena discover_scenes yang asli
        # membawanya (module1_download). Orchestrator memakainya untuk
        # menyusun himpunan tanggal S1 -- stub tanpa kunci itu akan menguji
        # jalur fallback, bukan jalur yang sebenarnya dipakai produksi.
        return [{"product_identifier": PID, "size_mb": 1.0, "cloud_cover": 0,
                 "acquisition_datetime": ACQ}]

    def fake_download(scene_meta, output_dir, keep_raw=True, progress_cb=None, reuse_root=None):
        out = Path(output_dir)
        (out / f"{PID}.SAFE.zip").parent.mkdir(parents=True, exist_ok=True)
        (out / f"{PID}.SAFE.zip").write_bytes(b"zip")
        return DownloadResult(
            product_identifier=PID,
            zip_path=str(out / f"{PID}.SAFE.zip"),
            vv_tif_path=_write_tif(out / f"{PID}_VV.tif", 1.0),
            vh_tif_path=_write_tif(out / f"{PID}_VH.tif", 2.0),
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
        return _write_tif(d / "cal_VV.tif", 1.1), _write_tif(d / "cal_VH.tif", 2.1)

    def fake_crop(vv, vh, out_dir, bbox):
        # Nama berkas mengikuti module2_crop asli (`*_{BAND}_crop.tif`) — itu
        # pola yang dicari module10 saat me-render preview level RAW.
        d = Path(out_dir)
        return (
            _write_tif(d / f"{PID}_VV_crop.tif", 1.2),
            _write_tif(d / f"{PID}_VH_crop.tif", 2.2),
        )

    def fake_lee(vv, vh, out_dir, window_size=7, looks=1):
        d = Path(out_dir)
        return (
            _write_tif(d / f"{PID}_VV_lee.tif", 1.3),
            _write_tif(d / f"{PID}_VH_lee.tif", 2.3),
        )

    def fake_gold(dataset_id, dataset_name, source, scene_key, silver_files):
        d = fm.ensure_scene_dir(dataset_id, dataset_name, "cog", source, scene_key)
        return {
            band: _write_tif(d / Path(path).name, 3.0)
            for band, path in silver_files.items()
        }

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

    def fake_modis_build(*, band, product, date, date_key, tiles, raw_dir, out_path,
                         aoi_bbox, plog, dataset_id, scene_label):
        _write_tif(out_path, 0.5 if band != "FLOOD" else 1.0)
        return {"band": band, "product": product, "path": str(out_path),
                "checksum_md5": "0" * 32, "source_tiles": {}, "skipped": False,
                "degraded": False, "failed_tiles": []}

    def fake_accumulate(end_date, num_days, raw_dir, *, plog=None, dataset_id=None,
                        scene_id="", window_name=""):
        return object(), object(), "EPSG:4326", {
            "2024-03-05": {"checksum_md5": "0" * 32, "run": "F"}
        }

    def fake_gpm_reproject(accum, src_transform, src_crs, aoi_bbox, output_path, tags=None):
        return Path(_write_tif(output_path, 7.0))

    monkeypatch.setattr(m5, "discover_scenes", fake_discover)
    monkeypatch.setattr(m5, "download_scene", fake_download)
    monkeypatch.setattr(m5, "calibrate_run", fake_calibrate)
    monkeypatch.setattr(m5, "crop_run", fake_crop)
    monkeypatch.setattr(m5, "lee_run", fake_lee)
    monkeypatch.setattr(m5, "export_scene_to_gold", fake_gold)
    monkeypatch.setattr(
        m5, "compute_band_metrics", lambda path, band, min_quality_score=60.0: _Metrics()
    )
    monkeypatch.setattr(m7, "_build_band_for_date", fake_modis_build)
    monkeypatch.setattr(m7, "_md5", lambda p, chunk=0: "0" * 32)
    monkeypatch.setattr(m8, "_accumulate_window", fake_accumulate)
    monkeypatch.setattr(m8, "_crop_to_aoi", fake_gpm_reproject)
    monkeypatch.setattr(m8, "_md5", lambda p, chunk=0: "0" * 32)
    monkeypatch.setattr(m4, "export_scene_to_gold", fake_gold)
    monkeypatch.setattr(m9.m4, "export_scene_to_gold", fake_gold)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def fusion_files(dataset_id, dataset_name) -> list[Path]:
    """Semua HDF5 fusi dataset ini, apa pun strateginya.

    Rekursif karena output sekarang dipecah per subfolder strategi
    (fusion/co-occurrence/, fusion/hybrid/, ...) supaya menjalankan ulang
    dataset dengan strategi lain tidak menimpa hasil sebelumnya."""
    d = fm.get_fusion_dir(dataset_id, dataset_name, DATE_KEY)
    return sorted(d.glob("**/*.h5")) if d.exists() else []


def fusion_dir(dataset_id, dataset_name, strategy) -> Path:
    """Folder output untuk satu strategi."""
    return fm.get_fusion_dir(dataset_id, dataset_name, DATE_KEY) / fs.SUBFOLDER[strategy]


def h5_layers(path: Path) -> set[str]:
    """Semua path dataset di dalam HDF5, mis. {"sentinel1/VV", "modis/FLOOD"}."""
    found: set[str] = set()

    def _visit(name, obj):
        if isinstance(obj, h5py.Dataset):
            found.add(name)

    with h5py.File(path, "r") as f:
        f.visititems(_visit)
    return found


def h5_groups(path: Path) -> set[str]:
    return {name.split("/")[0] for name in h5_layers(path)}


def h5_attrs(path: Path) -> dict:
    with h5py.File(path, "r") as f:
        return {k: f.attrs[k] for k in f.attrs}


def fusion_rows(db_client, dataset_id) -> list[dict]:
    """Baris fusion_products dataset ini di S1_DATE. Disaring per dataset,
    bukan per region: sejak migrasi 021 tiap dataset punya barisnya sendiri,
    dan dataset uji lain atas region + tanggal yang sama ada di DB yang sama."""
    with db_client.session() as sess:
        return [
            dict(r._mapping)
            for r in sess.execute(text("""
                SELECT fusion_id, feature_date, processing_level, fusion_strategy,
                       feature_stack_path, temporal_offset_modis, temporal_offset_gpm
                FROM   fusion_products
                WHERE  dataset_id = :ds AND feature_date = :d
                ORDER BY processing_level
            """), {"ds": dataset_id, "d": S1_DATE})
        ]


def products(db_client, dataset_id, tier=None, source=None) -> list[dict]:
    sql = """
        SELECT product_tier::text AS tier, processing_level, band_name, source,
               file_path, file_name, data_hash_sha256, is_latest, is_valid
        FROM   data_products WHERE dataset_id = :d
    """
    params = {"d": dataset_id}
    if tier:
        sql += " AND product_tier = :t"
        params["t"] = tier
    if source:
        sql += " AND source = :s"
        params["s"] = source
    sql += " ORDER BY product_tier, band_name"
    with db_client.session() as sess:
        return [dict(r._mapping) for r in sess.execute(text(sql), params)]


def lineage_into(db_client, target_product_id: int) -> list[tuple]:
    with db_client.session() as sess:
        return [
            tuple(r) for r in sess.execute(text("""
                SELECT transformation_type, parent_product_id, output_checksum
                FROM   data_lineage WHERE child_product_id = :t
            """), {"t": target_product_id})
        ]


def product_id_of(db_client, dataset_id, band_name, tier=None) -> int:
    """product_id satu band. `tier` wajib untuk band Sentinel-1: VV/VH ada di
    tier RAW **dan** BRONZE (dan GOLD), semuanya is_latest, jadi tanpa tier
    query ini mengembalikan baris yang kebetulan lebih dulu."""
    sql = """
        SELECT product_id FROM data_products
        WHERE dataset_id = :d AND band_name = :b AND is_latest = TRUE
    """
    params = {"d": dataset_id, "b": band_name}
    if tier:
        sql += " AND product_tier = :t"
        params["t"] = tier
    with db_client.session() as sess:
        return sess.scalar(text(sql), params)


# ---------------------------------------------------------------------------
# Test Case 1 — konfigurasi campuran, satu stack
# ---------------------------------------------------------------------------
class TestMixedLevelsSingleStack:
    """sentinel1[RAW] + modis[PROCESSED] + gpm[RAW].

    Tidak ada satu pun sumber yang diminta di KEDUA level, jadi tidak ada
    perbandingan untuk dibuat: satu stack saja (ProcessingPlan.output_levels).
    Di dalamnya tiap sumber ikut pada levelnya sendiri.
    """

    CONFIG = {
        "sentinel1": ["RAW"],
        "modis": ["PROCESSED"],
        "gpm": ["RAW"],
    }

    @pytest.fixture
    def ran(self, db_client, job_factory, stub_rasters):
        dataset_id, name, job_id = job_factory(
            self.CONFIG, fusion_strategy="CO_OCCURRENCE",
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "INDICES", "COG", "FUSED"],
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    def test_produces_exactly_one_stack(self, ran):
        dataset_id, name = ran
        assert len(fusion_files(dataset_id, name)) == 1

    def test_groups_are_only_configured_sources(self, ran):
        dataset_id, name = ran
        h5 = fusion_files(dataset_id, name)[0]
        assert h5_groups(h5) == {"sentinel1", "modis", "gpm"}

    def test_layers_follow_each_source_own_level(self, ran):
        """S1 ikut penuh (VV+VH di kedua level), MODIS PROCESSED menyumbang
        indeks turunannya, GPM RAW hanya curah hujan harian — bukan window
        akumulasi, yang di jalur RAW tidak pernah dihitung."""
        dataset_id, name = ran
        h5 = fusion_files(dataset_id, name)[0]
        assert h5_layers(h5) == {
            "sentinel1/VV", "sentinel1/VH",
            "modis/FLOOD", "modis/FLOOD_SOURCE",
            "modis/NDVI", "modis/NDVI_AGE_DAYS", "modis/NDWI", "modis/NDWI_AGE_DAYS",
            "gpm/rainfall_daily",
        }

    def test_gpm_raw_has_no_accumulation_windows(self, ran):
        dataset_id, name = ran
        layers = h5_layers(fusion_files(dataset_id, name)[0])
        assert "gpm/rainfall_24h" not in layers
        assert "gpm/rainfall_72h" not in layers
        assert "gpm/rainfall_7d" not in layers

    def test_h5_attrs_record_level_per_source(self, ran):
        dataset_id, name = ran
        attrs = h5_attrs(fusion_files(dataset_id, name)[0])
        levels = dict(zip(
            [s.decode() if isinstance(s, bytes) else str(s) for s in attrs["sources"]],
            [s.decode() if isinstance(s, bytes) else str(s) for s in attrs["source_levels"]],
        ))
        assert levels == {"SENTINEL1": "RAW", "MODIS": "PROCESSED", "GPM": "RAW"}
        assert attrs["fusion_strategy"] == "CO_OCCURRENCE"

    def test_metadata_json_records_source_tiers(self, ran):
        """Sidecar harus menyebut tier yang BENAR-BENAR dibaca tiap sumber —
        itulah bukti bahwa S1 RAW diambil dari BRONZE, bukan dari GOLD."""
        dataset_id, name = ran
        meta_path = (
            fusion_dir(dataset_id, name, "CO_OCCURRENCE")
            / m9.fusion_metadata_name("20240305", "PROCESSED", "CO_OCCURRENCE")
        )
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta["source_levels"] == {
            "SENTINEL1": "RAW", "MODIS": "PROCESSED", "GPM": "RAW",
        }
        assert meta["source_tiers"] == {
            "SENTINEL1": "ALIGNED", "MODIS": "COG", "GPM": "ALIGNED",
        }
        assert meta["fusion_strategy"] == "CO_OCCURRENCE"
        # layers di sidecar = lapisan yang ditulis, bukan daftar semua yang
        # mungkin: sidecar yang menjanjikan lapisan tak ada lebih buruk
        # daripada sidecar yang diam.
        assert set(meta["layers"]) == h5_layers(fusion_files(dataset_id, name)[0])

    def test_h5_layers_carry_their_own_provenance(self, ran):
        """Tanggal sumber, offset, dan satuan harus ada di HDF5 sendiri --
        di 24_try8 satu-satunya jejak itu ada di sidecar yang tertimpa."""
        import h5py

        dataset_id, name = ran
        with h5py.File(fusion_files(dataset_id, name)[0], "r") as f:
            assert f.attrs["feature_date"] == "2024-03-05"
            assert "linear" in f["sentinel1/VV"].attrs["units"]
            ndvi = f["modis/NDVI"].attrs
            assert ndvi["source_date"] == "2024-03-05"
            assert ndvi["day_offset"] == 0
            assert 0.0 <= ndvi["valid_fraction"] <= 1.0

    def test_layer_sources_point_at_the_right_tier_on_disk(self, ran):
        dataset_id, name = ran
        meta_path = (
            fusion_dir(dataset_id, name, "CO_OCCURRENCE")
            / m9.fusion_metadata_name("20240305", "PROCESSED", "CO_OCCURRENCE")
        )
        srcs = json.loads(meta_path.read_text(encoding="utf-8"))["layer_sources"]
        assert "sentinel-1/RAW" in srcs["sentinel1/VV"]["path"].replace("\\", "/")
        assert "modis/PROCESSED" in srcs["modis/NDVI"]["path"].replace("\\", "/")
        assert "gpm-imerg/RAW" in srcs["gpm/rainfall_daily"]["path"].replace("\\", "/")

    def test_s1_raw_artifact_exists_at_bronze(self, ran):
        dataset_id, name = ran
        bronze = fm.get_scene_dir(dataset_id, name, "aligned", "sentinel1", PID)
        assert bronze.is_dir()
        assert sorted(p.name for p in bronze.glob("*.tif")) == [
            f"{PID}_VH_crop.tif", f"{PID}_VV_crop.tif",
        ]

    def test_modis_processed_lands_in_the_processed_drawer(self, ran):
        """Artefak antara (SILVER) tidak lagi bertahan di disk: dia hidup di
        _work/ dan disapu di akhir job. Yang tersisa adalah laci finalnya."""
        dataset_id, name = ran
        d = fm.get_scene_dir(dataset_id, name, "cog", "modis", DATE_KEY)
        assert d.is_dir(), "modis/PROCESSED tidak ada"
        assert list(d.glob("*.tif"))

        scratch = fm.get_dataset_root(dataset_id, name) / fm.SCRATCH_DIRNAME
        assert not scratch.exists(), "_work/ seharusnya sudah disapu"

    def test_gpm_raw_stops_at_bronze(self, ran):
        dataset_id, name = ran
        assert fm.get_scene_dir(dataset_id, name, "aligned", "gpm", DATE_KEY).is_dir()
        gold = fm.get_scene_dir(dataset_id, name, "cog", "gpm", DATE_KEY)
        assert not gold.exists() or not list(gold.glob("*.tif"))

    def test_no_unconfigured_source_group(self, ran):
        """Regresi: group untuk sumber yang tidak dikonfigurasi tidak boleh
        muncul berisi NaN. Konsumen harus bisa membedakan "tidak diminta" dari
        "diminta tapi hilang", dan group NaN menghapus beda itu."""
        dataset_id, name = ran
        # Semua sumber dikonfigurasi di kasus ini, jadi yang diuji adalah
        # kebalikannya: tidak ada group asing.
        assert h5_groups(fusion_files(dataset_id, name)[0]) <= {
            "sentinel1", "modis", "gpm",
        }


class TestMixedLevelsDatabaseTagging:
    """Penandaan database untuk konfigurasi Test Case 1."""

    @pytest.fixture
    def ran(self, db_client, job_factory, stub_rasters):
        dataset_id, name, job_id = job_factory(
            {"sentinel1": ["RAW"], "modis": ["PROCESSED"], "gpm": ["RAW"]},
            fusion_strategy="CO_OCCURRENCE",
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "INDICES", "COG", "FUSED"],
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    def test_s1_bronze_tagged_raw(self, db_client, ran):
        dataset_id, _ = ran
        rows = products(db_client, dataset_id, tier="ALIGNED", source="SENTINEL1")
        assert rows and all(r["processing_level"] == "RAW" for r in rows)

    def test_s1_raw_never_reaches_gold(self, db_client, ran):
        dataset_id, _ = ran
        assert products(db_client, dataset_id, tier="COG", source="SENTINEL1") == []

    def test_modis_products_tagged_processed(self, db_client, ran):
        dataset_id, _ = ran
        rows = products(db_client, dataset_id, source="MODIS")
        assert rows and all(r["processing_level"] == "PROCESSED" for r in rows)
        assert {r["band_name"] for r in rows} == {"FLOOD", "NDVI", "NDWI"}

    def test_gpm_products_tagged_raw_single_window(self, db_client, ran):
        dataset_id, _ = ran
        rows = products(db_client, dataset_id, source="GPM")
        assert rows and all(r["processing_level"] == "RAW" for r in rows)
        assert {r["band_name"] for r in rows} == {"RAIN_24H"}
        assert {r["tier"] for r in rows} == {"ALIGNED"}

    def test_fusion_product_tagged_and_checksummed(self, db_client, ran):
        dataset_id, name = ran
        rows = products(db_client, dataset_id, tier="FUSED")
        assert len(rows) == 1
        row = rows[0]
        assert row["processing_level"] == "PROCESSED"
        assert row["band_name"] == "FUSION_PROCESSED"
        assert row["is_latest"] and row["is_valid"]
        # Checksum yang tercatat harus cocok dengan berkas di disk.
        from etl.lineage_tracker import LineageTracker
        h5 = fusion_files(dataset_id, name)[0]
        assert row["data_hash_sha256"] == LineageTracker.compute_sha256(h5)
        assert row["file_name"] == h5.name

    def test_metadata_json_checksum_matches_h5(self, ran):
        dataset_id, name = ran
        h5 = fusion_files(dataset_id, name)[0]
        meta = json.loads(
            h5.with_name(h5.stem + "_metadata.json").read_text(encoding="utf-8")
        )
        from etl.lineage_tracker import LineageTracker
        assert meta["checksum_sha256"] == LineageTracker.compute_sha256(h5)

    def test_fusion_lineage_edges_come_from_the_tier_actually_read(
        self, db_client, ran
    ):
        """S1 dikonfigurasi RAW, jadi induk lineage stack ini harus produk
        BRONZE — bukan GOLD, yang untuk dataset ini bahkan tidak ada."""
        dataset_id, _ = ran
        fusion_pid = product_id_of(db_client, dataset_id, "FUSION_PROCESSED")
        edges = lineage_into(db_client, fusion_pid)
        assert edges, "tidak ada baris lineage menuju produk FUSION"
        assert all(kind == "FUSION" for kind, _, _ in edges)

        bronze_ids = {
            product_id_of(db_client, dataset_id, band, tier="ALIGNED")
            for band in ("VV", "VH")
        }
        assert {src for _, src, _ in edges} == bronze_ids

    def test_fusion_products_row_records_strategy_and_level(
        self, db_client, ran, sample_region
    ):
        dataset_id, name = ran
        rows = fusion_rows(db_client, dataset_id)
        assert len(rows) == 1
        assert rows[0]["processing_level"] == "PROCESSED"
        assert rows[0]["fusion_strategy"] == "CO_OCCURRENCE"
        assert rows[0]["feature_stack_path"] == str(fusion_files(dataset_id, name)[0])
        # MODIS/GPM ditemukan di hari yang sama dengan S1.
        assert rows[0]["temporal_offset_modis"] == 0
        assert rows[0]["temporal_offset_gpm"] == 0


# ---------------------------------------------------------------------------
# Test Case 2 — satu sumber di dua level, dua stack
# ---------------------------------------------------------------------------
class TestBothLevelsProduceTwoStacks:
    """sentinel1[RAW,PROCESSED] + modis[PROCESSED].

    Inti ablation study: dua stack tanggal yang sama yang berbeda HANYA pada
    level Sentinel-1-nya, supaya performa model bisa dibandingkan langsung.
    """

    @pytest.fixture
    def ran(self, db_client, job_factory, stub_rasters):
        dataset_id, name, job_id = job_factory(
            {"sentinel1": ["RAW", "PROCESSED"], "modis": ["PROCESSED"]},
            fusion_strategy="HYBRID",
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "INDICES", "COG", "FUSED"],
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    def test_two_h5_files_written(self, ran):
        dataset_id, name = ran
        files = fusion_files(dataset_id, name)
        assert len(files) == 2
        assert sorted(f.name for f in files) == [
            m9.fusion_h5_name(DATE_KEY, "PROCESSED", "HYBRID"),
            m9.fusion_h5_name(DATE_KEY, "RAW", "HYBRID"),
        ]

    def test_each_stack_declares_its_own_level(self, ran):
        dataset_id, name = ran
        d = fusion_dir(dataset_id, name, "HYBRID")
        for level in ("RAW", "PROCESSED"):
            attrs = h5_attrs(d / m9.fusion_h5_name(DATE_KEY, level, "HYBRID"))
            assert attrs["processing_level"] == level

    def test_s1_level_differs_but_modis_is_shared(self, ran):
        """MODIS hanya punya PROCESSED, jadi dia ikut di KEDUA stack pada
        level itu — sumber tidak dihilangkan dari stack RAW hanya karena dia
        tidak punya varian RAW."""
        dataset_id, name = ran
        d = fusion_dir(dataset_id, name, "HYBRID")

        def levels(level):
            attrs = h5_attrs(d / m9.fusion_h5_name(DATE_KEY, level, "HYBRID"))
            return dict(zip(
                [str(s) for s in attrs["sources"]],
                [str(s) for s in attrs["source_levels"]],
            ))

        assert levels("RAW") == {"SENTINEL1": "RAW", "MODIS": "PROCESSED"}
        assert levels("PROCESSED") == {"SENTINEL1": "PROCESSED", "MODIS": "PROCESSED"}

    def test_both_stacks_have_the_same_layer_names(self, ran):
        """Perbandingan ablation cuma sah kalau bentuk kedua stack identik."""
        dataset_id, name = ran
        d = fusion_dir(dataset_id, name, "HYBRID")
        raw = h5_layers(d / m9.fusion_h5_name(DATE_KEY, "RAW", "HYBRID"))
        proc = h5_layers(d / m9.fusion_h5_name(DATE_KEY, "PROCESSED", "HYBRID"))
        assert raw == proc
        assert raw == {
            "sentinel1/VV", "sentinel1/VH",
            "modis/FLOOD", "modis/FLOOD_SOURCE",
            "modis/NDVI", "modis/NDVI_AGE_DAYS", "modis/NDWI", "modis/NDWI_AGE_DAYS",
        }

    def test_s1_layers_actually_differ_between_stacks(self, ran):
        """Bukti bahwa keduanya bukan salinan: stack RAW membaca hasil crop
        BRONZE, stack PROCESSED membaca COG GOLD, dan stub menulis nilai yang
        berbeda untuk keduanya."""
        dataset_id, name = ran
        d = fusion_dir(dataset_id, name, "HYBRID")
        with h5py.File(d / m9.fusion_h5_name(DATE_KEY, "RAW", "HYBRID"), "r") as f:
            raw_vv = f["sentinel1/VV"][:]
        with h5py.File(d / m9.fusion_h5_name(DATE_KEY, "PROCESSED", "HYBRID"), "r") as f:
            proc_vv = f["sentinel1/VV"][:]
        assert not np.allclose(raw_vv, proc_vv)

    def test_two_fusion_products_rows_one_per_level(
        self, db_client, ran, sample_region
    ):
        """Regresi kunci: kunci unik lama (feature_date, region_id) membuat
        baris kedua menimpa yang pertama. Migrasi 018 menambahkan
        processing_level ke kunci itu."""
        dataset_id, name = ran
        rows = fusion_rows(db_client, dataset_id)
        assert [r["processing_level"] for r in rows] == ["PROCESSED", "RAW"]
        assert len({r["fusion_id"] for r in rows}) == 2
        assert {r["feature_stack_path"] for r in rows} == {
            str(p) for p in fusion_files(dataset_id, name)
        }
        assert all(r["fusion_strategy"] == "HYBRID" for r in rows)

    def test_two_fusion_data_products_both_latest(self, db_client, ran):
        """band_name membawa level-nya, jadi dedup is_latest tidak membuat
        stack RAW menandai dirinya usang begitu stack PROCESSED terdaftar."""
        dataset_id, _ = ran
        rows = products(db_client, dataset_id, tier="FUSED")
        assert len(rows) == 2
        assert {r["band_name"] for r in rows} == {"FUSION_RAW", "FUSION_PROCESSED"}
        assert {r["processing_level"] for r in rows} == {"RAW", "PROCESSED"}
        assert all(r["is_latest"] and r["is_valid"] for r in rows)

    def test_checksums_of_the_two_stacks_differ(self, db_client, ran):
        dataset_id, _ = ran
        rows = products(db_client, dataset_id, tier="FUSED")
        hashes = {r["data_hash_sha256"] for r in rows}
        assert len(hashes) == 2, "dua stack berbeda harus punya checksum berbeda"
        assert all(len(h) == 64 for h in hashes)

    def test_s1_products_exist_at_both_levels(self, db_client, ran):
        dataset_id, _ = ran
        bronze = products(db_client, dataset_id, tier="ALIGNED", source="SENTINEL1")
        gold = products(db_client, dataset_id, tier="COG", source="SENTINEL1")
        assert {r["processing_level"] for r in bronze} == {"RAW"}
        assert {r["processing_level"] for r in gold} == {"PROCESSED"}

    def test_each_stack_lineage_points_at_its_own_input_tier(self, db_client, ran):
        """Stack RAW harus berinduk pada produk BRONZE, stack PROCESSED pada
        produk GOLD. Kalau keduanya berinduk ke tier yang sama, salah satunya
        membaca raster yang salah."""
        dataset_id, _ = ran
        raw_parents = {
            src for _, src, _ in lineage_into(
                db_client, product_id_of(db_client, dataset_id, "FUSION_RAW")
            )
        }
        proc_parents = {
            src for _, src, _ in lineage_into(
                db_client, product_id_of(db_client, dataset_id, "FUSION_PROCESSED")
            )
        }
        assert raw_parents and proc_parents
        assert raw_parents.isdisjoint(proc_parents)

        with db_client.session() as sess:
            def tiers(ids):
                return {
                    r[0] for r in sess.execute(text(
                        "SELECT product_tier::text FROM data_products "
                        "WHERE product_id = ANY(:ids)"
                    ), {"ids": list(ids)})
                }

            assert tiers(raw_parents) == {"ALIGNED"}
            assert tiers(proc_parents) == {"COG"}

    def test_separate_metadata_sidecar_per_level(self, ran):
        dataset_id, name = ran
        d = fusion_dir(dataset_id, name, "HYBRID")
        for level in ("RAW", "PROCESSED"):
            (sidecar,) = d.glob(f"*_{level.lower()}_metadata.json")
            meta = json.loads(sidecar.read_text(encoding="utf-8"))
            assert meta["processing_level"] == level
            assert meta["source_tiers"]["SENTINEL1"] == (
                "ALIGNED" if level == "RAW" else "COG"
            )


# ---------------------------------------------------------------------------
# Test Case 3 — sumber tunggal, fusi mati
# ---------------------------------------------------------------------------
class TestSingleSourceDisablesFusion:
    """modis[PROCESSED] saja: tidak ada yang bisa dipasangkan, jadi tidak ada
    HDF5 sama sekali (DOCS/ETL.md, "Fusion Stage")."""

    @pytest.fixture
    def ran(self, db_client, job_factory, stub_rasters, monkeypatch):
        monkeypatch.setattr(
            m5, "create_fusion_stack",
            lambda *a, **kw: pytest.fail("FUSION tidak boleh jalan untuk 1 sumber"),
        )
        dataset_id, name, job_id = job_factory(
            {"modis": ["PROCESSED"]},
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "COG"],
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    def test_no_h5_written(self, ran):
        dataset_id, name = ran
        assert fusion_files(dataset_id, name) == []

    def test_no_fusion_dir_at_all(self, ran):
        dataset_id, name = ran
        d = fm.get_fusion_dir(dataset_id, name, DATE_KEY)
        assert not d.exists() or not list(d.iterdir())

    def test_no_fusion_data_products(self, db_client, ran):
        dataset_id, _ = ran
        assert products(db_client, dataset_id, tier="FUSED") == []

    def test_plan_reports_fusion_ineligible(self):
        plan = plan_from_configs(1, {"modis": ["PROCESSED"]})
        assert plan.fusion_eligible("CO_OCCURRENCE") is False
        assert plan.fusion_eligible(None) is False

    def test_fusion_without_sentinel1_raises_rather_than_writing_garbage(
        self, db_client, job_factory, stub_rasters
    ):
        """Fusi di pipeline ini di-anchor ke grid dan tanggal scene S1. Kalau
        dipanggil untuk dataset tanpa S1, dia harus menolak dengan pesan yang
        menjelaskan sebabnya — bukan menulis stack tanpa grid referensi."""
        dataset_id, name, _ = job_factory(
            {"modis": ["PROCESSED"], "gpm": ["PROCESSED"]},
            fusion_strategy="FULL_COVERAGE",
        )
        with pytest.raises(RuntimeError, match="SENTINEL1"):
            m9.create_fusion_stack(
                dataset_id, name, S1_DATE, BBOX, scene_id=1, db=db_client,
            )


# ---------------------------------------------------------------------------
# Aturan level (unit) — kontrak yang dipakai fusion dan preview bersama
# ---------------------------------------------------------------------------
class TestOutputLevelRules:

    @pytest.mark.parametrize("config,expected", [
        ({"sentinel1": ["PROCESSED"], "modis": ["PROCESSED"]}, ("PROCESSED",)),
        ({"sentinel1": ["RAW"], "modis": ["RAW"]}, ("RAW",)),
        # Campuran tanpa sumber dua-level: satu stack. Dua stack di sini akan
        # berisi byte identik.
        ({"sentinel1": ["RAW"], "modis": ["PROCESSED"]}, ("PROCESSED",)),
        # Ada sumber dua-level: dua stack, itulah perbandingannya.
        ({"sentinel1": ["RAW", "PROCESSED"], "modis": ["PROCESSED"]},
         ("RAW", "PROCESSED")),
        ({"sentinel1": ["PROCESSED"], "gpm": ["RAW", "PROCESSED"]},
         ("RAW", "PROCESSED")),
    ])
    def test_output_levels(self, config, expected):
        assert plan_from_configs(1, config).output_levels() == expected

    def test_source_without_the_run_level_falls_back_to_its_highest(self):
        plan = plan_from_configs(1, {"sentinel1": ["RAW", "PROCESSED"], "modis": ["PROCESSED"]})
        assert plan.source_levels_for_run("RAW") == {
            "SENTINEL1": "RAW", "MODIS": "PROCESSED",
        }

    def test_tier_for_run_maps_level_to_tier(self):
        plan = plan_from_configs(1, {"sentinel1": ["RAW", "PROCESSED"]})
        s1 = plan.get("SENTINEL1")
        assert s1.tier_for_run("RAW") == "ALIGNED"
        assert s1.tier_for_run("PROCESSED") == "COG"

    @pytest.mark.parametrize("levels,expected", [
        ({"MODIS": "RAW"}, ["modis/FLOOD", "modis/FLOOD_SOURCE"]),
        ({"MODIS": "PROCESSED"}, [
            "modis/FLOOD", "modis/FLOOD_SOURCE", "modis/NDVI", "modis/NDVI_AGE_DAYS",
            "modis/NDWI", "modis/NDWI_AGE_DAYS",
        ]),
        ({"GPM": "RAW"}, ["gpm/rainfall_daily"]),
        ({"GPM": "PROCESSED"},
         ["gpm/rainfall_24h", "gpm/rainfall_72h", "gpm/rainfall_7d"]),
        ({"SENTINEL1": "RAW"}, ["sentinel1/VV", "sentinel1/VH"]),
        ({"SENTINEL1": "PROCESSED"}, ["sentinel1/VV", "sentinel1/VH"]),
    ])
    def test_fusion_layers_for(self, levels, expected):
        assert m9.fusion_layers_for(levels) == expected

    def test_unconfigured_source_contributes_no_group(self):
        layers = m9.fusion_layers_for({"SENTINEL1": "RAW"})
        assert not any(name.startswith(("modis/", "gpm/")) for name in layers)


# ---------------------------------------------------------------------------
# Preview: tier yang dibaca mengikuti level
# ---------------------------------------------------------------------------
class TestPreviewReadsCorrectTier:

    def test_tier_mapping(self):
        assert m10.tier_for_level("RAW") == "aligned"
        assert m10.tier_for_level("PROCESSED") == "cog"

    @pytest.fixture
    def ran(self, db_client, job_factory, stub_rasters):
        dataset_id, name, job_id = job_factory(
            {"sentinel1": ["RAW", "PROCESSED"], "modis": ["PROCESSED"]},
            fusion_strategy="HYBRID",
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "INDICES", "COG", "FUSED"],
            generate_preview=True,
            preview_options=["GRAYSCALE", "COLORED", "COMPOSITE"],
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    def test_one_preview_folder_per_level(self, ran):
        dataset_id, name = ran
        assert fm.list_preview_levels(dataset_id, name, DATE_KEY) == ["RAW", "PROCESSED"]

    def test_each_level_records_the_tier_it_read(self, ran):
        dataset_id, name = ran
        for level, tier in (("RAW", "ALIGNED"), ("PROCESSED", "COG")):
            meta = json.loads((
                fm.get_preview_level_dir(dataset_id, name, DATE_KEY, level)
                # Sidecar berprefiks tanggal: satu folder preview dipakai
                # bersama seluruh tanggal dataset.
                / fm.dated_filename(DATE_KEY, "preview_metadata.json")
            ).read_text(encoding="utf-8"))
            assert meta["processing_level"] == level
            assert meta["derived_from"] == tier

    def test_three_kind_dirs_per_level(self, ran):
        dataset_id, name = ran
        for level in ("RAW", "PROCESSED"):
            for kind in ("grayscale", "colored", "composite"):
                d = fm.get_preview_kind_dir(dataset_id, name, DATE_KEY, kind, level)
                assert d.is_dir(), f"{level}/{kind} tidak dibuat"

    def test_png_of_the_two_levels_do_not_overwrite_each_other(self, ran):
        """Nama berkasnya sama (`s1_vv.png`) di kedua level; kalau foldernya
        tidak dipisah, yang dirender belakangan menimpa yang lain."""
        dataset_id, name = ran
        raw = fm.get_preview_kind_dir(dataset_id, name, DATE_KEY, "grayscale", "RAW")
        proc = fm.get_preview_kind_dir(
            dataset_id, name, DATE_KEY, "grayscale", "PROCESSED"
        )
        assert (raw / f"{DATE_KEY}_s1_vv.png").exists()
        assert (proc / f"{DATE_KEY}_s1_vv.png").exists()
        assert (raw / f"{DATE_KEY}_s1_vv.png").read_bytes() != (proc / f"{DATE_KEY}_s1_vv.png").read_bytes()


class TestPreviewOptions:
    """datasets.preview_options memilih varian mana yang dirender."""

    def test_only_requested_kinds_are_written(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        gold = fm.ensure_scene_dir(9, "Opt Test", "cog", "sentinel1", PID)
        _write_tif(gold / f"{PID}_VV_lee.tif", 1.0)
        _write_tif(gold / f"{PID}_VH_lee.tif", 2.0)

        result = m10.generate_previews(
            9, "Opt Test", DATE_KEY, s1_scene_key=PID, options=["GRAYSCALE"],
        )
        assert result["counts"]["grayscale"] == 2
        assert result["counts"]["colored"] == 0
        assert result["counts"]["composite"] == 0
        assert fm.get_preview_kind_dir(9, "Opt Test", DATE_KEY, "grayscale").is_dir()
        assert not fm.get_preview_kind_dir(9, "Opt Test", DATE_KEY, "colored").exists()

    def test_none_means_all_three(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        gold = fm.ensure_scene_dir(10, "Opt Test", "cog", "sentinel1", PID)
        _write_tif(gold / f"{PID}_VV_lee.tif", 1.0)
        _write_tif(gold / f"{PID}_VH_lee.tif", 2.0)

        result = m10.generate_previews(10, "Opt Test", DATE_KEY, s1_scene_key=PID)
        assert result["counts"]["grayscale"] == 2
        assert result["counts"]["colored"] == 2
        assert result["counts"]["composite"] == 1

    def test_unknown_option_is_ignored_not_fatal(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
        gold = fm.ensure_scene_dir(11, "Opt Test", "cog", "sentinel1", PID)
        _write_tif(gold / f"{PID}_VV_lee.tif", 1.0)

        result = m10.generate_previews(
            11, "Opt Test", DATE_KEY, s1_scene_key=PID,
            options=["GRAYSCALE", "THUMBNAIL"],
        )
        assert result["counts"]["grayscale"] == 1


# ---------------------------------------------------------------------------
# Alur "Pakai Config Sebelumnya" ujung-ke-ujung
# ---------------------------------------------------------------------------
@pytest.fixture
def no_job_runner(monkeypatch):
    """POST /api/datasets menjalankan pipeline di thread latar; di sini yang
    diuji cuma penulisan konfigurasinya."""
    from etl.dataset_manager import DatasetManager

    monkeypatch.setattr(DatasetManager, "_spawn_job_runner", lambda self, job_id: None)


def source_configs_in_db(db_client, dataset_id) -> list[tuple]:
    with db_client.session() as sess:
        return [
            tuple(r) for r in sess.execute(text("""
                SELECT source_name, processing_levels
                FROM   dataset_source_config
                WHERE  dataset_id = :d
                ORDER BY source_name
            """), {"d": dataset_id})
        ]


class TestCloneLastConfigRoundTrip:
    """Buat dataset1 -> GET /last-config -> buat dataset2 dari config itu ->
    kedua dataset harus punya dataset_source_config yang identik."""

    CONFIG = {
        "sentinel1": {"processing": ["RAW", "PROCESSED"]},
        "modis": {"processing": ["PROCESSED"]},
        "gpm": {"processing": ["RAW"]},
    }

    def _create(self, api_client, region_id, name, **overrides):
        body = {
            "region_id": region_id,
            "date_start": "2024-01-01",
            "date_end": "2024-01-31",
            "name": name,
            "sources": self.CONFIG,
            "fusion_strategy": "CO_OCCURRENCE",
            "preview_options": ["GRAYSCALE", "COLORED"],
        }
        body.update(overrides)
        resp = api_client.post("/api/datasets", json=body)
        assert resp.status_code == 201, resp.text
        return resp.json()

    def test_config_survives_the_round_trip(
        self, api_client, db_client, sample_region, no_job_runner
    ):
        first = self._create(api_client, sample_region, "dataset1")

        cfg = api_client.get("/api/datasets/last-config").json()
        assert cfg["created_from_dataset_id"] == first["dataset_id"]
        assert cfg["sources"] == self.CONFIG

        # Dataset kedua dibuat dari config yang dikembalikan, persis seperti
        # yang dilakukan tombol "Pakai Config" di wisaya.
        second = self._create(
            api_client, cfg["region_id"], "dataset2",
            sources=cfg["sources"],
            fusion_strategy=cfg["fusion_strategy"],
            preview_options=cfg["preview_options"],
        )

        assert source_configs_in_db(db_client, first["dataset_id"]) == \
               source_configs_in_db(db_client, second["dataset_id"])

    def test_both_datasets_keep_strategy_and_preview_options(
        self, api_client, db_client, sample_region, no_job_runner
    ):
        first = self._create(api_client, sample_region, "dataset1")
        cfg = api_client.get("/api/datasets/last-config").json()
        second = self._create(
            api_client, cfg["region_id"], "dataset2",
            sources=cfg["sources"],
            fusion_strategy=cfg["fusion_strategy"],
            preview_options=cfg["preview_options"],
        )

        with db_client.session() as sess:
            rows = {
                r[0]: (r[1], r[2]) for r in sess.execute(text("""
                    SELECT dataset_id, fusion_strategy, preview_options
                    FROM   datasets WHERE dataset_id = ANY(:ids)
                """), {"ids": [first["dataset_id"], second["dataset_id"]]})
            }
        assert rows[first["dataset_id"]] == rows[second["dataset_id"]]
        assert rows[second["dataset_id"]][0] == "CO_OCCURRENCE"
        assert rows[second["dataset_id"]][1] == ["GRAYSCALE", "COLORED"]

    def test_clone_does_not_carry_name_but_does_carry_dates(
        self, api_client, sample_region, no_job_runner
    ):
        """Keputusan produk (DOCS/DECISIONS.md D13): user harus mengisi ulang
        nama supaya tidak tanpa sengaja menduplikasi dataset. Rentang tanggal
        justru diikutkan sebagai preset yang bisa diedit sebelum submit."""
        self._create(api_client, sample_region, "dataset1")
        cfg = api_client.get("/api/datasets/last-config").json()
        assert "name" not in cfg
        assert cfg["date_start"] == "2024-01-01"
        assert cfg["date_end"] == "2024-01-31"

    def test_plans_built_from_both_datasets_are_equal(
        self, api_client, db_client, sample_region, no_job_runner
    ):
        """Bukti terkuat bahwa klon berhasil: ProcessingPlan yang dibaca ETL
        dari kedua dataset harus menghasilkan keputusan yang sama."""
        from etl.processing_plan import load_processing_plan

        first = self._create(api_client, sample_region, "dataset1")
        cfg = api_client.get("/api/datasets/last-config").json()
        second = self._create(
            api_client, cfg["region_id"], "dataset2", sources=cfg["sources"],
            fusion_strategy=cfg["fusion_strategy"],
        )

        p1 = load_processing_plan(db_client, first["dataset_id"])
        p2 = load_processing_plan(db_client, second["dataset_id"])
        assert p1.summary() == p2.summary()
        assert p1.output_levels() == p2.output_levels()
        assert p1.required_tiers() == p2.required_tiers()


class TestFusionStrategyChangesOutput:
    """Bukti bahwa strategi fusi benar-benar bercabang.

    Sebelum etl/fusion_strategies.py ada, ketiga strategi menghasilkan berkas
    identik: `fusion_strategy` cuma ditulis sebagai atribut HDF5 dan tidak
    pernah jadi percabangan, sementara seluruh pipeline berjangkar pada scene
    S1. Kelas ini mengunci perbedaannya.

    Rentang 5-8 Maret dengan satu scene S1 (5 Maret): CO_OCCURRENCE merakit
    1 tanggal, FULL_COVERAGE merakit 4.
    """

    CONFIG = {"sentinel1": ["PROCESSED"], "modis": ["PROCESSED"]}
    RANGE = dict(date_start=date(2024, 3, 5), date_end=date(2024, 3, 8))
    DAYS = ("20240305", "20240306", "20240307", "20240308")

    def _run(self, db_client, job_factory, strategy):
        dataset_id, name, job_id = job_factory(
            self.CONFIG, fusion_strategy=strategy,
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "INDICES", "COG", "FUSED"],
            **self.RANGE
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    @staticmethod
    def _h5(dataset_id, name) -> list[Path]:
        return sorted(fm.get_dataset_root(dataset_id, name).glob("**/*.h5"))

    def test_co_occurrence_fuses_only_the_s1_date(
        self, db_client, job_factory, stub_rasters
    ):
        files = self._h5(*self._run(db_client, job_factory, "CO_OCCURRENCE"))
        assert len(files) == 1
        assert DATE_KEY in files[0].name

    def test_full_coverage_fuses_every_day_in_range(
        self, db_client, job_factory, stub_rasters
    ):
        """Hari tanpa scene S1 tetap jadi berkas — inti FULL_COVERAGE."""
        files = self._h5(*self._run(db_client, job_factory, "FULL_COVERAGE"))
        names = [f.name for f in files]
        assert len(files) == 4, names
        for day in self.DAYS:
            assert any(day in n for n in names), f"{day} tidak difusikan"

    def test_hybrid_fuses_like_cooccurrence(
        self, db_client, job_factory, stub_rasters
    ):
        """HYBRID berbagi sumbu RAKIT dengan CO_OCCURRENCE — yang berbeda
        hanya sumbu unduhnya, yang tidak terlihat dari jumlah berkas."""
        files = self._h5(*self._run(db_client, job_factory, "HYBRID"))
        assert len(files) == 1
        assert DATE_KEY in files[0].name

    def test_strategies_write_to_separate_subfolders(
        self, db_client, job_factory, stub_rasters
    ):
        """Output dua strategi tidak saling menimpa, dan subfolder-nya
        menyatakan strategi mana yang menghasilkannya."""
        co = self._h5(*self._run(db_client, job_factory, "CO_OCCURRENCE"))
        fc = self._h5(*self._run(db_client, job_factory, "FULL_COVERAGE"))
        assert {p.parent.name for p in co} == {"co-occurrence"}
        assert {p.parent.name for p in fc} == {"full-coverage"}

    def test_s1_less_day_keeps_a_nan_sentinel1_group(
        self, db_client, job_factory, stub_rasters
    ):
        """Group sentinel1/ tetap ada tapi berisi NaN pada hari tanpa S1.

        8 Maret, bukan 7: dengan toleransi default 2 hari, 7 Maret masih
        MEMINJAM scene 5 Maret. Hanya 8 Maret yang benar-benar di luar
        jangkauan dan karena itu tidak punya S1 sama sekali.

        Group-nya tidak dihilangkan: fusion_layers_for membedakan "sensor
        tidak diminta" (tidak ada group) dari "diminta tapi hilang hari itu"
        (group berisi NaN), dan S1 di sini jelas termasuk yang kedua.
        """
        dataset_id, name = self._run(db_client, job_factory, "FULL_COVERAGE")
        s1_less = [p for p in self._h5(dataset_id, name) if "20240308" in p.name]
        assert s1_less, "tanggal tanpa S1 tidak menghasilkan berkas"

        with h5py.File(s1_less[0], "r") as f:
            assert "sentinel1/VV" in f
            assert np.isnan(f["sentinel1/VV"][:]).all()

    def test_borrowed_s1_is_recorded_with_its_offset(
        self, db_client, job_factory, stub_rasters
    ):
        """Hari dalam toleransi MEMINJAM scene S1, bukan diisi NaN.

        Scene S1 ada di 5 Maret; toleransi default 2 hari. 6-7 Maret karena itu
        memakai scene yang sama dengan offset 1 dan 2, sementara 8 Maret di
        luar jangkauan dan tidak punya S1 sama sekali. `s1_offset_days`
        membedakan ketiganya -- tanpa kolom itu, fusi same-day dan fusi
        bertoleransi tidak bisa dipisahkan lagi setelah berkasnya ditulis.
        """
        dataset_id, name = self._run(db_client, job_factory, "FULL_COVERAGE")

        with db_client.session() as sess:
            rows = dict(sess.execute(text(
                "SELECT feature_date, s1_offset_days FROM fusion_products "
                "WHERE dataset_id = :ds AND feature_date BETWEEN :a AND :b"
            ), {"ds": dataset_id, "a": date(2024, 3, 5), "b": date(2024, 3, 8)}).all())

        assert rows[date(2024, 3, 5)] == 0      # same-day
        assert rows[date(2024, 3, 6)] == 1      # dipinjam, 1 hari
        assert rows[date(2024, 3, 7)] == 2      # dipinjam, batas toleransi
        assert rows[date(2024, 3, 8)] is None   # di luar toleransi

    def test_every_fused_day_registers_a_fusion_product_row(
        self, db_client, job_factory, stub_rasters
    ):
        """Berkas HDF5 yang ada di disk tapi tidak tercatat di DB adalah
        kegagalan diam: registrasi berjalan SETELAH berkasnya ditulis, jadi
        exception di sana tidak akan terlihat dari keberadaan berkas saja.

        Dicocokkan lewat feature_stack_path, bukan hanya jumlah baris: path
        membuktikan baris itu milik berkas INI, bukan berkas dataset lain.
        """
        dataset_id, name = self._run(db_client, job_factory, "FULL_COVERAGE")
        files = self._h5(dataset_id, name)

        with db_client.session() as sess:
            registered = {
                row[0] for row in sess.execute(text(
                    "SELECT feature_stack_path FROM fusion_products "
                    "WHERE feature_stack_path = ANY(:paths)"
                ), {"paths": [str(f) for f in files]}).all()
            }

        missing = {str(f) for f in files} - registered
        assert not missing, f"HDF5 tanpa baris fusion_products: {sorted(missing)}"

    # --- regresi run try1/try2/try3 (2026-09-16) ------------------------------
    def test_full_coverage_has_no_fusion_failures_or_dangling_jobs(
        self, db_client, job_factory, stub_rasters
    ):
        """Hari tanpa S1 dulu jatuh KeyError 'vv_product_id' SETELAH HDF5 dan
        baris fusion_products ditulis. Dua tes di atas lolos karena hanya
        memeriksa berkas + baris itu; kegagalannya cuma terlihat di
        processing_logs, di job FUSION yang tertinggal RUNNING, dan di status
        job yang tetap COMPLETED."""
        dataset_id, _ = self._run(db_client, job_factory, "FULL_COVERAGE")

        with db_client.session() as sess:
            failed_logs = sess.execute(text(
                "SELECT scene_id, message FROM processing_logs "
                "WHERE dataset_id = :ds AND status = 'FAILED'"
            ), {"ds": dataset_id}).all()
            dangling = sess.execute(text("""
                SELECT pj.job_id, ps.stage_name, pj.status::text
                FROM   processing_jobs pj
                JOIN   processing_stages ps ON ps.stage_id = pj.stage_id
                WHERE  pj.parameters_json->>'dataset_id' = :ds
                  AND  pj.status::text IN ('QUEUED', 'RUNNING')
            """), {"ds": str(dataset_id)}).all()
            status = sess.scalar(text(
                "SELECT status FROM datasets WHERE dataset_id = :ds"
            ), {"ds": dataset_id})

        assert not failed_logs, failed_logs
        assert not dangling, f"job tidak pernah ditutup: {dangling}"
        assert status == "COMPLETED"

    def test_full_coverage_every_stack_stays_latest(
        self, db_client, job_factory, stub_rasters
    ):
        """Hari yang meminjam scene S1 dulu mendaftarkan data_products-nya ke
        scene S1 itu, sehingga dedup is_latest membuat mereka saling menandai
        usang -- hanya satu stack yang tersisa di listing API."""
        dataset_id, name = self._run(db_client, job_factory, "FULL_COVERAGE")
        files = self._h5(dataset_id, name)
        latest = {
            r["file_path"]
            for r in products(db_client, dataset_id, tier="FUSED")
            if r["is_latest"]
        }
        assert latest == {str(f) for f in files}

    def test_full_coverage_days_share_one_grid(
        self, db_client, job_factory, stub_rasters
    ):
        """Hari tanpa S1 dulu memakai grid rumus AOI, berbeda dari grid raster
        S1 hari lain (try2: 578x473 vs 571x468), sehingga deret waktunya tidak
        bisa ditumpuk."""
        dataset_id, name = self._run(db_client, job_factory, "FULL_COVERAGE")
        shapes = set()
        for path in self._h5(dataset_id, name):
            with h5py.File(path, "r") as f:
                shapes.add(f["sentinel1/VV"].shape)
        assert len(shapes) == 1, shapes

    def test_two_datasets_on_same_aoi_and_date_keep_own_rows(
        self, db_client, job_factory, stub_rasters
    ):
        """try1/try2/try3 atas AOI dan tanggal yang sama dulu berbagi satu
        baris fusion_products (fusion_id=43), ditimpa dataset terakhir."""
        first, _ = self._run(db_client, job_factory, "CO_OCCURRENCE")
        second, _ = self._run(db_client, job_factory, "HYBRID")

        rows_first = fusion_rows(db_client, first)
        rows_second = fusion_rows(db_client, second)
        assert len(rows_first) == 1 and len(rows_second) == 1
        assert rows_first[0]["fusion_id"] != rows_second[0]["fusion_id"]
        assert rows_first[0]["fusion_strategy"] == "CO_OCCURRENCE"
        assert rows_second[0]["fusion_strategy"] == "HYBRID"

    def test_stage_that_raises_does_not_leave_job_running(
        self, db_client, job_factory, stub_rasters, monkeypatch
    ):
        """Tahap yang melempar setelah start_job (dataset 3: 16 job CROP
        tertinggal RUNNING sejak 2026-09-12) harus ditutup FAILED."""
        def boom(*args, **kwargs):
            raise MemoryError("calibrate kehabisan memori")

        monkeypatch.setattr(m5, "calibrate_run", boom)
        dataset_id, _ = self._run(db_client, job_factory, "CO_OCCURRENCE")

        with db_client.session() as sess:
            running = sess.execute(text("""
                SELECT pj.job_id, ps.stage_name
                FROM   processing_jobs pj
                JOIN   processing_stages ps ON ps.stage_id = pj.stage_id
                JOIN   satellite_scenes ss ON ss.scene_id = pj.scene_id
                WHERE  ss.product_identifier = :pid AND pj.status::text = 'RUNNING'
            """), {"pid": PID}).all()
            status = sess.scalar(text(
                "SELECT status FROM datasets WHERE dataset_id = :ds"
            ), {"ds": dataset_id})

        assert not running, f"job tertinggal RUNNING: {running}"
        assert status == "FAILED"

    def test_extra_aux_days_do_not_inflate_scene_counters(
        self, db_client, job_factory, stub_rasters
    ):
        """HYBRID mengunduh aux untuk tiap hari, tapi unit counter jalur S1
        adalah scene: try1 berakhir completed_scenes=16 untuk total_scenes=1.
        metadata.json juga dulu ditulis sebelum status akhir (DOWNLOADING)."""
        dataset_id, name = self._run(db_client, job_factory, "HYBRID")
        with db_client.session() as sess:
            row = sess.execute(text(
                "SELECT status, total_scenes, completed_scenes, failed_scenes "
                "FROM datasets WHERE dataset_id = :ds"
            ), {"ds": dataset_id}).one()
        assert (row.total_scenes, row.completed_scenes, row.failed_scenes) == (1, 1, 0)

        meta = fm.read_dataset_metadata(dataset_id, name)
        assert meta["status"] == row.status == "COMPLETED"
        assert meta["completed_scenes"] == 1


class TestFusionOutputOnly:
    """`fusion_output_only`: simpan HDF5 saja, buang artefak per-satelit."""

    CONFIG = {"sentinel1": ["PROCESSED"], "modis": ["PROCESSED"]}

    def _run(self, db_client, job_factory, **extra):
        dataset_id, name, job_id = job_factory(
            self.CONFIG, fusion_strategy="CO_OCCURRENCE",
            required_tiers=["RAW", "ALIGNED", "DESPECKLED", "INDICES", "COG", "FUSED"],
            **extra,
        )
        m5.run_dataset_job(db_client, job_id)
        return dataset_id, name

    def test_keeps_h5_and_drops_per_satellite_artifacts(
        self, db_client, job_factory, stub_rasters
    ):
        dataset_id, name = self._run(
            db_client, job_factory, fusion_output_only=True
        )
        root = fm.get_dataset_root(dataset_id, name)

        assert list(root.glob("**/fusion/**/*.h5")), "stack fusi ikut terhapus"
        for drawer in ("sentinel-1", "modis", "gpm-imerg", "_work"):
            leftovers = [p for p in root.glob(f"{drawer}/**/*") if p.is_file()]
            assert not leftovers, f"{drawer}/ masih menyisakan {leftovers[:3]}"

    def test_default_keeps_everything(
        self, db_client, job_factory, stub_rasters
    ):
        """Default FALSE harus berperilaku persis seperti sebelum fitur ini."""
        dataset_id, name = self._run(db_client, job_factory)
        root = fm.get_dataset_root(dataset_id, name)
        assert list(root.glob("sentinel-1/PROCESSED/*.tif")), "artefak per-satelit hilang"

    def test_rejected_without_a_fusion_strategy(self, db_client, job_factory):
        """Tanpa fusi tidak ada stack yang ditulis, jadi menghapus artefak
        per-satelit akan menyisakan dataset kosong. Ditolak, bukan diabaikan
        diam-diam -- user yang mencentangnya jelas mengharapkan sesuatu."""
        with pytest.raises(ValueError, match="fusion_output_only"):
            job_factory(
                {"sentinel1": ["PROCESSED"]},
                fusion_strategy=None,
                fusion_output_only=True,
            )
