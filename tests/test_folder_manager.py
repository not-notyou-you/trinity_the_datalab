# tests/test_folder_manager.py
"""
Tests untuk etl/folder_manager.py -- satu-satunya tempat layout on-disk
data/datasets/... dibangun -- dan untuk pemetaan path di
etl/migrate_data_structure.py.

Semuanya murni path/filesystem, tidak menyentuh database.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from etl import folder_manager as fm
from etl import migrate_data_structure as mig


DATASET_ID = 42
DATASET_NAME = "Banjir Jakarta 2026"
DATASET_DIR = "42_Banjir_Jakarta_2026"
SCENE_A = "S1A_IW_GRDH_1SDV_20260712T111407.SAFE"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    """Arahkan DATA_ROOT ke tmp_path supaya test tidak menyentuh data asli."""
    root = tmp_path / "data" / "datasets"
    monkeypatch.setattr(fm, "DATA_ROOT", root)
    return root


# ---------------------------------------------------------------------------
# Penyusunan path
# ---------------------------------------------------------------------------
class TestPathConstruction:
    def test_dataset_root_uses_id_and_slug(self, data_root):
        root = fm.get_dataset_root(DATASET_ID, DATASET_NAME)
        assert root == data_root / DATASET_DIR

    def test_slugify_replaces_unsafe_chars(self):
        assert fm.slugify("hakim d1") == "hakim_d1"
        assert fm.slugify("a/b:c") == "a_b_c"
        assert fm.slugify("///") == "dataset"

    def test_final_tiers_land_in_source_level_drawers(self, data_root):
        """Tier diterjemahkan jadi laci: BRONZE -> RAW/, GOLD -> PROCESSED/.
        Tidak ada folder tanggal maupun folder scene -- tanggal sudah ada di
        nama berkas."""
        assert fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "bronze", "modis", "20260712"
        ) == data_root / DATASET_DIR / "modis" / "RAW"

        pid = "S1D_IW_GRDH_1SDV_20260712T111407.SAFE"
        assert fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "gold", "sentinel1", pid
        ) == data_root / DATASET_DIR / "sentinel-1" / "PROCESSED"

        assert fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "gold", "gpm", "20260712"
        ) == data_root / DATASET_DIR / "gpm-imerg" / "PROCESSED"

    def test_intermediate_tiers_live_in_scratch(self, data_root):
        """RAW (ZIP SAFE) dan SILVER (Lee pre-COG) tidak punya laci: keduanya
        artefak antara yang dibuang setelah scene selesai. Source ikut ke path
        supaya MODIS dan GPM -- yang kunci scene-nya sama-sama tanggal -- tidak
        berbagi satu folder scratch."""
        assert fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "silver", "modis", "20260712"
        ) == data_root / DATASET_DIR / "_work" / "20260712" / "silver" / "modis"
        assert fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "silver", "gpm", "20260712"
        ) != fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "silver", "modis", "20260712"
        )

    def test_scene_date_key(self):
        assert fm.scene_date_key("20260712") == "20260712"
        assert fm.scene_date_key(
            "S1A_IW_GRDH_1SDV_20240115T111407_20240115T111432_052054_064A8B_1234.SAFE"
        ) == "20240115"
        assert fm.scene_date_key("S1A_IW_GRDH_TEST_20240305") == "20240305"

    def test_scene_key_without_date_rejected(self):
        """scene_date_key tetap menolak kunci tanpa tanggal.

        Sejak relayout, tanggal tidak lagi menentukan path tier final (dia ada
        di nama berkas), jadi get_scene_dir tidak lagi memanggilnya di sana.
        Tapi kontraknya sendiri masih berlaku untuk pemanggil yang memang
        butuh tanggalnya."""
        with pytest.raises(ValueError, match="tidak mengandung tanggal"):
            fm.scene_date_key("SCENE_A")
        with pytest.raises(ValueError):
            fm.scene_date_key("TEST_SCENE_1788773879.392203")

    def test_fusion_dir_has_no_source_level(self, data_root):
        """Lintas-source DAN lepas dari folder tanggal: satu folder memuat
        seluruh deret waktu, yang justru bentuk yang dicari konsumen."""
        p = fm.get_fusion_dir(DATASET_ID, DATASET_NAME, "20260712")
        assert p == data_root / DATASET_DIR / "fusion"

    def test_preview_dir_has_no_source_level(self, data_root):
        p = fm.get_preview_dir(DATASET_ID, DATASET_NAME, "20260712")
        assert p == data_root / DATASET_DIR / "preview"

    def test_one_source_is_downloadable_on_its_own(self, data_root):
        """Alasan relayout: "ambil Sentinel-1 PROCESSED saja" harus jadi satu
        folder, bukan berkas yang tersebar di satu folder per tanggal."""
        root = fm.get_dataset_root(DATASET_ID, DATASET_NAME)
        pid = "S1D_IW_GRDH_1SDV_20260712T111407.SAFE"

        s1_proc = fm.get_scene_dir(DATASET_ID, DATASET_NAME, "gold", "sentinel1", pid)
        assert s1_proc.parent.parent == root
        assert s1_proc.relative_to(root) == Path("sentinel-1") / "PROCESSED"

        # Tanggal lain mendarat di folder yang sama.
        pid2 = "S1D_IW_GRDH_1SDV_20260801T111407.SAFE"
        assert fm.get_scene_dir(
            DATASET_ID, DATASET_NAME, "gold", "sentinel1", pid2
        ) == s1_proc

    def test_preview_kind_dirs(self, data_root):
        """Level pemrosesan duduk di antara tanggal dan jenis render. Tanpa
        level di path, dataset RAW+PROCESSED menulis dua set PNG dengan nama
        berkas yang sama ke folder yang sama."""
        for kind in fm.PREVIEW_KINDS:
            p = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, "20260712", kind)
            assert p == (
                data_root / DATASET_DIR / "preview" / "PROCESSED" / kind
            )

    def test_preview_kind_dirs_per_level(self, data_root):
        for level in fm.PREVIEW_LEVELS:
            p = fm.get_preview_kind_dir(
                DATASET_ID, DATASET_NAME, "20260712", "colored", level
            )
            assert p == (
                data_root / DATASET_DIR / "preview" / level / "colored"
            )

    def test_preview_levels_are_separate_dirs(self, data_root):
        raw = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, "20260712", "colored", "RAW")
        processed = fm.get_preview_kind_dir(
            DATASET_ID, DATASET_NAME, "20260712", "colored", "PROCESSED"
        )
        assert raw != processed

    def test_composite_is_a_preview_kind(self, data_root):
        assert "composite" in fm.PREVIEW_KINDS

    def test_list_preview_levels_reads_disk(self, data_root):
        assert fm.list_preview_levels(DATASET_ID, DATASET_NAME, "20260712") == []
        fm.ensure_preview_kind_dir(
            DATASET_ID, DATASET_NAME, "20260712", "grayscale", "RAW"
        )
        assert fm.list_preview_levels(DATASET_ID, DATASET_NAME, "20260712") == ["RAW"]
        fm.ensure_preview_kind_dir(
            DATASET_ID, DATASET_NAME, "20260712", "grayscale", "PROCESSED"
        )
        assert fm.list_preview_levels(DATASET_ID, DATASET_NAME, "20260712") == [
            "RAW", "PROCESSED",
        ]

    def test_preview_kind_rejects_unknown_kind(self, data_root):
        with pytest.raises(ValueError):
            fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, "20260712", "thumbnail")

    def test_preview_kind_rejects_unknown_level(self, data_root):
        with pytest.raises(ValueError):
            fm.get_preview_kind_dir(
                DATASET_ID, DATASET_NAME, "20260712", "colored", "SILVER"
            )

    def test_preview_tier_rejects_source_level(self, data_root):
        """preview lintas-source persis seperti fusion, jadi get_source_dir
        harus menolaknya alih-alih diam-diam membuat preview/modis/."""
        with pytest.raises(ValueError):
            fm.get_source_dir(DATASET_ID, DATASET_NAME, "20260712", "preview", "modis")

    def test_granule_cache_is_flat_outside_dates(self, data_root):
        p = fm.get_granule_cache_dir(DATASET_ID, DATASET_NAME, "gpm")
        assert p == data_root / DATASET_DIR / "_granule_cache" / "gpm"

    def test_scratch_dir_outside_tiers(self, data_root):
        p = fm.get_scratch_dir(DATASET_ID, DATASET_NAME, "SCENE")
        assert p.parent.name == "_work"
        assert p.parent.parent == data_root / DATASET_DIR


# ---------------------------------------------------------------------------
# Validasi kombinasi tier/source
# ---------------------------------------------------------------------------
class TestValidation:
    def test_unknown_tier_rejected(self):
        with pytest.raises(ValueError, match="Tier tidak valid"):
            fm.get_tier_dir(DATASET_ID, DATASET_NAME, "20260712", "platinum")

    def test_unknown_source_rejected(self):
        with pytest.raises(ValueError, match="Source tidak valid"):
            fm.get_source_dir(DATASET_ID, DATASET_NAME, "20260712", "silver", "landsat")

    def test_fusion_tier_rejects_source(self):
        """Tier fusion gabungan semua source, jadi tidak boleh diberi satu."""
        with pytest.raises(ValueError, match="tidak punya level source"):
            fm.get_source_dir(DATASET_ID, DATASET_NAME, "20260712", "fusion", "modis")

    def test_bronze_accepts_every_source(self):
        """Sejak model per-satelit, bronze adalah tempat artefak level RAW
        SETIAP sumber berhenti — bukan cuma hasil crop Sentinel-1."""
        for source in ("sentinel1", "modis", "gpm"):
            assert fm.get_source_dir(DATASET_ID, DATASET_NAME, "20260712", "bronze", source) == (
                fm.get_dataset_root(DATASET_ID, DATASET_NAME) / "20260712" / "bronze" / source
            )

    def test_granule_cache_rejects_sentinel1(self):
        """Sentinel-1 disimpan per-scene, bukan sebagai cache granule flat."""
        with pytest.raises(ValueError, match="cache granule flat"):
            fm.get_granule_cache_dir(DATASET_ID, DATASET_NAME, "sentinel1")

    def test_db_source_mapping(self):
        assert fm.db_source("sentinel1") == "SENTINEL1"
        assert fm.db_source("MODIS") == "MODIS"

    def test_date_key_normalisation(self):
        assert fm.date_key("2026-07-12") == "20260712"
        assert fm.date_key("20260712") == "20260712"
        with pytest.raises(ValueError):
            fm.date_key("12 Juli 2026")


# ---------------------------------------------------------------------------
# Listing + ringkasan storage
# ---------------------------------------------------------------------------
def _touch(path: Path, size: int = 100) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


class TestListingAndStorage:
    @pytest.fixture
    def populated(self, data_root):
        # Layout pasca-relayout: sumber di depan, dua laci per sumber, tanggal
        # di nama berkas. Tidak ada folder tanggal dan tidak ada folder scene.
        base = data_root / DATASET_DIR
        _touch(base / "sentinel-1" / "PROCESSED" / f"{SCENE_A}_vv_lee.tif", 1000)
        _touch(base / "sentinel-1" / "PROCESSED" / f"{SCENE_A}_vh_lee.tif", 1000)
        _touch(base / "modis" / "PROCESSED" / "modis_20260712_ndvi.tif", 500)
        _touch(base / "gpm-imerg" / "PROCESSED" / "gpm_rain_24h_20260712.tif", 200)
        _touch(base / "_granule_cache" / "modis" / "MOD09GA.A2026193.h30v08.hdf", 700)
        _touch(base / "fusion" / "fusion_20260712.h5", 300)
        _touch(base / "metadata.json", 50)  # bukan tier, tidak boleh terhitung
        return base

    def test_list_scenes_per_source(self, populated):
        """Tanpa folder scene, kunci scene dibaca dari tanggal di nama berkas
        -- termasuk untuk Sentinel-1."""
        assert fm.list_scenes(DATASET_ID, DATASET_NAME, "gold", "sentinel1") == ["20260712"]
        assert fm.list_scenes(DATASET_ID, DATASET_NAME, "gold", "modis") == ["20260712"]
        assert fm.list_scenes(DATASET_ID, DATASET_NAME, "bronze", "gpm") == []

    def test_list_dates(self, populated):
        assert fm.list_dates(DATASET_ID, DATASET_NAME) == ["20260712"]

    def test_scene_files_filter_by_name_not_folder(self, populated):
        """Laci final memuat semua tanggal, jadi satu scene disaring lewat
        nama berkas alih-alih lewat folder sendiri."""
        s1 = fm.get_scene_files(DATASET_ID, DATASET_NAME, "gold", "sentinel1", SCENE_A)
        assert sorted(f.name for f in s1) == [
            f"{SCENE_A}_vh_lee.tif", f"{SCENE_A}_vv_lee.tif"
        ]
        modis = fm.get_scene_files(DATASET_ID, DATASET_NAME, "gold", "modis", "20260712")
        assert [f.name for f in modis] == ["modis_20260712_ndvi.tif"]

    def test_list_sources_only_returns_existing(self, populated):
        assert fm.list_sources(DATASET_ID, DATASET_NAME, "gold") == [
            "sentinel1", "modis", "gpm"
        ]
        assert fm.list_sources(DATASET_ID, DATASET_NAME, "bronze") == []
        # Tier fusion tidak punya level source sama sekali.
        assert fm.list_sources(DATASET_ID, DATASET_NAME, "fusion") == []

    def test_list_fusion_scenes(self, populated):
        assert fm.list_fusion_scenes(DATASET_ID, DATASET_NAME) == ["20260712"]
        # Pembungkus tipis di atas versi generiknya -- harus sama persis.
        assert fm.list_sourceless_scenes(DATASET_ID, DATASET_NAME, "fusion") == ["20260712"]

    def test_list_sourceless_scenes_rejects_tier_with_source(self, populated):
        with pytest.raises(ValueError):
            fm.list_sourceless_scenes(DATASET_ID, DATASET_NAME, "silver")

    def test_preview_scene_files_include_kind_subfolders(self, data_root):
        """Berkas preview duduk satu tingkat lebih dalam (grayscale/, colored/)
        daripada tier lain; listing-nya harus tetap menemukannya."""
        gray = fm.ensure_preview_kind_dir(DATASET_ID, DATASET_NAME, "20260712", "grayscale")
        color = fm.ensure_preview_kind_dir(DATASET_ID, DATASET_NAME, "20260712", "colored")
        # Prefiks tanggal wajib: folder preview tidak lagi bersarang di bawah
        # folder tanggal, jadi tanpa itu tanggal kedua menimpa yang pertama.
        (gray / "20260712_s1_vv.png").write_bytes(b"x" * 100)
        (color / "20260712_s1_vv.png").write_bytes(b"x" * 200)
        (fm.get_preview_dir(DATASET_ID, DATASET_NAME, "20260712")
         / "preview_metadata.json").write_bytes(b"{}")

        names = sorted(p.name for p in fm.get_preview_scene_files(DATASET_ID, DATASET_NAME, "20260712"))
        assert names == ["20260712_s1_vv.png", "20260712_s1_vv.png",
                         "preview_metadata.json"]
        assert fm.list_preview_scenes(DATASET_ID, DATASET_NAME) == ["20260712"]

        b = fm.storage_breakdown(DATASET_ID, DATASET_NAME)
        assert b["tiers"]["preview"]["size_bytes"] == 302
        assert b["tiers"]["preview"]["scene_count"] == 1
        # Tier lintas-source dilaporkan sebagai "source" semu bernama tier-nya,
        # supaya ukurannya tidak hilang dari agregat per-source.
        assert b["sources"]["preview"]["size_bytes"] == 302

    def test_granule_cache_is_not_listed_as_scene(self, populated):
        """File granule flat di _granule_cache/modis/ bukan scene, tapi tetap
        milik tier raw."""
        assert fm.list_scenes(DATASET_ID, DATASET_NAME, "raw", "modis") == []
        assert fm.list_sources(DATASET_ID, DATASET_NAME, "raw") == ["modis"]
        assert len(fm.get_source_files(DATASET_ID, DATASET_NAME, "raw", "modis")) == 1
        assert len(fm.list_loose_files(DATASET_ID, DATASET_NAME, "raw", "modis")) == 1

    def test_storage_breakdown_splits_by_tier_and_source(self, populated):
        b = fm.storage_breakdown(DATASET_ID, DATASET_NAME)

        # Laci PROCESSED dilaporkan di bawah tier GOLD -- tier tetap jadi
        # kosakata pelaporan walau bukan lagi segmen path (D14).
        assert b["tiers"]["cog"]["size_bytes"] == 2700
        assert b["tiers"]["cog"]["sources"]["sentinel1"]["size_bytes"] == 2000
        assert b["tiers"]["cog"]["sources"]["sentinel1"]["file_count"] == 2
        assert b["tiers"]["cog"]["sources"]["modis"]["size_bytes"] == 500
        assert b["tiers"]["cog"]["sources"]["gpm"]["size_bytes"] == 200
        # Tidak ada laci RAW yang terisi di fixture ini.
        assert b["tiers"]["aligned"]["size_bytes"] == 0

        # Tier fusion dilaporkan tanpa pecahan source.
        assert b["tiers"]["fused"]["size_bytes"] == 300
        assert b["tiers"]["fused"]["sources"] == {}
        assert b["tiers"]["fused"]["scene_count"] == 1

        # Agregat lintas tier.
        assert b["sources"]["modis"]["size_bytes"] == 1200  # 500 PROCESSED + 700 cache
        assert b["total_size_bytes"] == 3700

    def test_storage_breakdown_on_empty_dataset(self, data_root):
        b = fm.storage_breakdown(999, "kosong")
        assert b["total_size_bytes"] == 0
        assert b["sources"] == {}
        assert set(b["tiers"]) == set(fm.TIERS)


class TestDatasetMetadata:
    def test_write_then_read_roundtrip(self, data_root):
        fm.write_dataset_metadata(DATASET_ID, DATASET_NAME, {"dataset_id": DATASET_ID, "n": 3})
        assert fm.read_dataset_metadata(DATASET_ID, DATASET_NAME) == {
            "dataset_id": DATASET_ID, "n": 3
        }

    def test_read_missing_returns_none(self, data_root):
        assert fm.read_dataset_metadata(DATASET_ID, DATASET_NAME) is None

    def test_read_corrupt_returns_none(self, data_root):
        path = fm.get_dataset_metadata_path(DATASET_ID, DATASET_NAME)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{ not json")
        assert fm.read_dataset_metadata(DATASET_ID, DATASET_NAME) is None


# ---------------------------------------------------------------------------
# Pemetaan path migrasi
# ---------------------------------------------------------------------------
class TestMigrationTargets:
    """Layout asal yang didukung:
    L1  data/datasets/{id}/{YYYYMMDD}/{tier}/file
    L2  data/datasets/{id}_{slug}/{tier}/{scene}/file
    """

    def _row(self, **kw) -> dict:
        row = {
            "product_id": 1,
            "product_tier": "SILVER",
            "source": None,
            "product_type": "LEE_FILTERED",
            "file_name": "vv_lee.tif",
            "data_hash_sha256": "",
            "product_identifier": "S1D_SCENE_20260712T111407.SAFE",
        }
        row.update(kw)
        return row

    def test_intermediate_tier_is_skipped(self, data_root):
        """SILVER tidak punya laci di layout baru: get_scene_dir akan
        mengarahkannya ke _work/, yang dibuang setelah scene selesai, jadi
        memigrasikannya ke sana sama dengan menghapusnya lewat jalan memutar."""
        old = Path("data/datasets/42/20260712/silver/vv_lee.tif")
        assert mig._target_path(DATASET_ID, DATASET_NAME, self._row(), old) is None

    def test_sentinel1_gold_lands_in_processed_drawer(self, data_root):
        old = Path("data/datasets/42/20260712/gold/vv_lee.tif")
        target = mig._target_path(
            DATASET_ID, DATASET_NAME,
            self._row(product_tier="GOLD", product_type="S1_COG"), old
        )
        assert target == (
            data_root / DATASET_DIR / "sentinel-1" / "PROCESSED" / "vv_lee.tif"
        )

    def test_modis_uses_date_from_l1_folder(self, data_root):
        old = Path("data/datasets/42/20260712/bronze/modis_20260712_flood.tif")
        row = self._row(product_tier="BRONZE", product_type="MODIS_FLOOD",
                        file_name="modis_20260712_flood.tif")
        target = mig._target_path(DATASET_ID, DATASET_NAME, row, old)
        assert target == (
            data_root / DATASET_DIR / "modis" / "RAW" / "modis_20260712_flood.tif"
        )

    def test_modis_uses_date_from_l2_scene_folder(self, data_root):
        old = Path("data/datasets/42_Banjir_Jakarta_2026/bronze/20260712/modis_20260712_flood.tif")
        row = self._row(product_tier="BRONZE", product_type="MODIS_FLOOD",
                        file_name="modis_20260712_flood.tif")
        target = mig._target_path(DATASET_ID, DATASET_NAME, row, old)
        # Tanggal tidak lagi jadi segmen path -- dia tetap hidup di nama berkas.
        assert target.parent.name == "RAW"
        assert target.parent.parent.name == "modis"
        assert "20260712" in target.name

    def test_date_falls_back_to_filename(self, data_root):
        """Kalau tidak ada folder tanggal di path asal, tanggal diambil dari nama file."""
        old = Path("somewhere/else/gpm_rain_24h_20260712.tif")
        row = self._row(product_tier="BRONZE", product_type="GPM_RAINFALL",
                        file_name="gpm_rain_24h_20260712.tif")
        target = mig._target_path(DATASET_ID, DATASET_NAME, row, old)
        assert target.parent.parent.name == "gpm-imerg"
        assert "20260712" in target.name

    def test_fusion_h5_moves_to_fusion_tier_even_if_row_says_gold(self, data_root):
        """Instalasi yang belum menjalankan migrasi SQL 013 masih mencatat
        FUSION_H5 sebagai GOLD; file-nya tetap harus mendarat di fusion/."""
        old = Path("data/datasets/42_Banjir_Jakarta_2026/gold/20260712/fusion_20260712.h5")
        row = self._row(
            product_tier="GOLD", product_type="FUSION_H5", file_name="fusion_20260712.h5"
        )
        target = mig._target_path(DATASET_ID, DATASET_NAME, row, old)
        assert target == data_root / DATASET_DIR / "fusion" / "fusion_20260712.h5"

    def test_explicit_source_column_wins(self, data_root):
        """Kalau kolom source sudah terisi, itu yang dipakai -- bukan tebakan
        dari product_type."""
        old = Path("data/datasets/42/20260712/gold/thing.tif")
        row = self._row(product_tier="GOLD", source="GPM", product_type="SOMETHING_NEW",
                        file_name="thing.tif")
        target = mig._target_path(DATASET_ID, DATASET_NAME, row, old)
        assert target.parent.parent.name == "gpm-imerg"

    def test_unknown_date_reports_failure(self, data_root):
        old = Path("nowhere/flood.tif")
        row = self._row(product_type="MODIS_FLOOD", file_name="flood.tif")
        assert mig._target_path(DATASET_ID, DATASET_NAME, row, old) is None
