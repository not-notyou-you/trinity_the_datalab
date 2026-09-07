# tests/test_module10_preview.py
"""
Tests untuk etl/module10_generate_preview.py (tier PREVIEW).

Semuanya menulis GeoTIFF sintetis ke tmp_path lalu me-render dari sana —
tidak menyentuh database maupun data dataset asli.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from etl import folder_manager as fm
from etl import module10_generate_preview as m10


DATASET_ID = 7
DATASET_NAME = "Preview Test"
DATE_KEY = "20260712"
S1_SCENE = "S1D_IW_GRDH_1SDV_20260712T111407.SAFE"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "data" / "datasets"
    monkeypatch.setattr(fm, "DATA_ROOT", root)
    return root


def _write_tif(path, array, nodata=np.nan):
    path.parent.mkdir(parents=True, exist_ok=True)
    array = array.astype("float32")
    with rasterio.open(
        path, "w", driver="GTiff",
        height=array.shape[0], width=array.shape[1], count=1,
        dtype="float32", crs="EPSG:4326",
        transform=from_origin(106.7, -6.0, 0.001, 0.001),
        nodata=nodata,
    ) as dst:
        dst.write(array, 1)
    return path


def _s1_gold(data_root, band, array):
    """Tulis COG GOLD Sentinel-1 palsu dengan pola nama yang dicari modul:
    `*_{BAND}_lee.tif`."""
    d = fm.get_scene_dir(DATASET_ID, DATASET_NAME, "gold", "sentinel1", S1_SCENE)
    return _write_tif(d / f"S1D_calibrated_{band}_lee.tif", array)


def _linear_sigma0(seed, shape=(60, 80)):
    """Sigma0 linear yang menjulur ke kanan, seperti data GOLD sungguhan."""
    rng = np.random.default_rng(seed)
    return rng.lognormal(mean=-1.0, sigma=0.8, size=shape)


class TestRendering:
    def test_generates_both_kinds_and_sidecars(self, data_root):
        _s1_gold(data_root, "VV", _linear_sigma0(1))
        _s1_gold(data_root, "VH", _linear_sigma0(2))

        result = m10.generate_previews(
            DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE
        )

        gray = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "grayscale")
        color = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "colored")

        assert sorted(p.name for p in gray.glob("*.png")) == ["s1_vh.png", "s1_vv.png"]
        # colored/ punya satu berkas ekstra: komposit RGB VV/VH/(VV-VH).
        assert sorted(p.name for p in color.glob("*.png")) == [
            "s1_rgb_composite.png", "s1_vh.png", "s1_vv.png",
        ]
        assert (gray / "grayscale_info.json").exists()
        assert (color / "colored_info.json").exists()
        assert (fm.get_preview_dir(DATASET_ID, DATASET_NAME, DATE_KEY)
                / "preview_metadata.json").exists()

        assert result["counts"]["grayscale"] == 2
        assert result["counts"]["colored"] == 3

    def test_sentinel1_converted_to_db(self, data_root):
        """GOLD menyimpan sigma0 linear; tanpa konversi dB, stretch persentil
        menghasilkan citra gelap tak terbaca."""
        _s1_gold(data_root, "VV", _linear_sigma0(3))
        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE)

        gray = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "grayscale")
        info = json.loads((gray / "grayscale_info.json").read_text(encoding="utf-8"))
        entry = next(e for e in info["images"] if e["key"] == "s1_vv")

        assert entry["transform"] == "10*log10(sigma0)"
        assert entry["statistics"]["converted_to_db"] is True
        # Rentang dB masuk akal untuk backscatter, bukan lagi 0..ratusan.
        lo, hi = entry["value_range"]
        assert -40 <= lo < hi <= 20

    def test_already_db_input_is_not_converted_twice(self, data_root):
        """Instalasi dengan cog_convert_db=true sudah menulis dB; mengambil
        log kedua kali akan merusaknya."""
        rng = np.random.default_rng(4)
        _s1_gold(data_root, "VV", rng.normal(-12.0, 3.0, size=(60, 80)))
        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE)

        gray = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "grayscale")
        info = json.loads((gray / "grayscale_info.json").read_text(encoding="utf-8"))
        entry = next(e for e in info["images"] if e["key"] == "s1_vv")
        assert entry["transform"] == "none"

    def test_nodata_becomes_transparent(self, data_root):
        """NoData tidak boleh jadi hitam: hitam adalah nilai sah untuk
        backscatter rendah (air tenang)."""
        from PIL import Image

        arr = _linear_sigma0(5)
        arr[:10, :] = np.nan
        _s1_gold(data_root, "VV", arr)
        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE)

        gray = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "grayscale")
        img = Image.open(gray / "s1_vv.png")
        assert img.mode == "LA"
        alpha = np.array(img)[..., 1]
        assert alpha[0, 0] == 0       # baris NoData
        assert alpha[-1, -1] == 255   # baris berdata

        color = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "colored")
        cimg = Image.open(color / "s1_vv.png")
        assert cimg.mode == "RGBA"
        assert np.array(cimg)[0, 0, 3] == 0

    def test_downsamples_to_max_width(self, data_root):
        from PIL import Image

        _s1_gold(data_root, "VV", _linear_sigma0(6, shape=(400, 800)))
        m10.generate_previews(
            DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE, max_width=100
        )
        gray = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "grayscale")
        assert Image.open(gray / "s1_vv.png").size == (100, 50)

    def test_gpm_uses_fixed_zero_based_range(self, data_root):
        """Nol harus berarti nol untuk hujan, bukan minimum data — kalau tidak,
        area kering terbaca sebagai 'hujan sedikit'."""
        from etl import module8_gpm_download as m8

        d = fm.get_scene_dir(DATASET_ID, DATASET_NAME, "gold", "gpm", DATE_KEY)
        rain = np.full((30, 40), 5.0)
        rain[:5, :] = 40.0
        _write_tif(d / m8.band_filename("24h", DATE_KEY), rain)

        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY)

        color = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "colored")
        info = json.loads((color / "colored_info.json").read_text(encoding="utf-8"))
        entry = next(e for e in info["images"] if e["key"] == "gpm_rain_24h")
        assert entry["value_range"][0] == 0.0
        assert entry["range_method"] == "zero_based"

    def test_modis_range_is_pinned_across_dates(self, data_root):
        """NDVI dipatok -1..1 supaya warna bisa dibandingkan antar tanggal,
        bukan digeser mengikuti isi tiap berkas."""
        from etl import module7_modis_download as m7

        d = fm.get_scene_dir(DATASET_ID, DATASET_NAME, "gold", "modis", DATE_KEY)
        _write_tif(d / m7.band_filename("NDVI", DATE_KEY), np.full((20, 20), 0.4))

        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY)

        color = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "colored")
        info = json.loads((color / "colored_info.json").read_text(encoding="utf-8"))
        entry = next(e for e in info["images"] if e["key"] == "modis_ndvi")
        assert entry["value_range"] == [-1.0, 1.0]


class TestResilience:
    def test_missing_sources_are_skipped_not_fatal(self, data_root):
        """MODIS/GPM yang gagal diunduh tidak boleh menjatuhkan preview S1."""
        _s1_gold(data_root, "VV", _linear_sigma0(7))

        result = m10.generate_previews(
            DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE
        )

        assert result["counts"]["grayscale"] == 1
        skipped = {s["key"] for s in result["skipped"]}
        assert "s1_vh" in skipped and "modis_ndvi" in skipped
        assert result["sources_present"] == ["sentinel1"]

    def test_no_gold_at_all_still_writes_metadata(self, data_root):
        """Tanpa satu pun input, tetap tulis sidecar kosong daripada melempar:
        pemanggilnya (orchestrator) memakainya untuk melaporkan hasil."""
        result = m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY)

        assert result["counts"]["total_png"] == 0
        assert len(result["skipped"]) == len(m10.PREVIEW_SPECS)
        meta = fm.get_preview_dir(DATASET_ID, DATASET_NAME, DATE_KEY) / "preview_metadata.json"
        assert json.loads(meta.read_text(encoding="utf-8"))["counts"]["total_png"] == 0

    def test_all_nodata_band_is_skipped(self, data_root):
        _s1_gold(data_root, "VV", np.full((20, 20), np.nan))
        result = m10.generate_previews(
            DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE
        )
        reason = next(s["reason"] for s in result["skipped"] if s["key"] == "s1_vv")
        assert "NoData" in reason

    def test_rerun_overwrites_by_default(self, data_root):
        """Preview turunan murni: menulis ulang lebih aman daripada menyimpan
        PNG basi dari GOLD versi lama."""
        _s1_gold(data_root, "VV", _linear_sigma0(8))
        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE)

        gray = fm.get_preview_kind_dir(DATASET_ID, DATASET_NAME, DATE_KEY, "grayscale")
        (gray / "s1_vv.png").write_bytes(b"stale")

        m10.generate_previews(DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE)
        assert (gray / "s1_vv.png").read_bytes()[:4] == b"\x89PNG"

        result = m10.generate_previews(
            DATASET_ID, DATASET_NAME, DATE_KEY, s1_scene_key=S1_SCENE, overwrite=False
        )
        assert any(
            s["key"] == "s1_vv" and "overwrite=False" in s["reason"]
            for s in result["skipped"]
        )

    def test_explicit_gold_paths_win_over_glob(self, data_root):
        """Orchestrator mengoper hasil export_scene_to_gold; itu jawaban pasti
        untuk scene yang baru saja diproses."""
        path = _s1_gold(data_root, "VV", _linear_sigma0(9))
        inputs = m10.resolve_gold_inputs(
            DATASET_ID, DATASET_NAME, DATE_KEY,
            s1_gold_files={"VV": str(path)},
        )
        assert inputs["s1_vv"] == path

    def test_nonexistent_explicit_path_falls_back_to_glob(self, data_root):
        path = _s1_gold(data_root, "VV", _linear_sigma0(10))
        inputs = m10.resolve_gold_inputs(
            DATASET_ID, DATASET_NAME, DATE_KEY,
            s1_scene_key=S1_SCENE,
            s1_gold_files={"VV": str(path.parent / "tidak_ada.tif")},
        )
        assert inputs["s1_vv"] == path


class TestDateKeys:
    def test_accepts_date_object_and_string(self, data_root):
        from datetime import date

        _s1_gold(data_root, "VV", _linear_sigma0(11))
        r1 = m10.generate_previews(
            DATASET_ID, DATASET_NAME, date(2026, 7, 12), s1_scene_key=S1_SCENE
        )
        r2 = m10.generate_previews(
            DATASET_ID, DATASET_NAME, "2026-07-12", s1_scene_key=S1_SCENE
        )
        assert r1["acquisition_date"] == r2["acquisition_date"] == DATE_KEY
