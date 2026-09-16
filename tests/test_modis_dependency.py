"""Error environment (pyhdf hilang) harus gagal cepat, bukan download lalu fallback."""
import builtins
from datetime import datetime

import pytest

import etl.module7_modis_download as m7


def _block_pyhdf(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("pyhdf"):
            raise ImportError("No module named 'pyhdf'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)


def test_missing_pyhdf_fails_before_download(monkeypatch):
    _block_pyhdf(monkeypatch)
    called = []
    monkeypatch.setattr(m7, "_download_with_retry", lambda *a, **k: called.append(a))
    monkeypatch.setattr(m7, "_discover_tile_files_with_fallback", lambda *a, **k: called.append(a))

    with pytest.raises(RuntimeError, match="pyhdf tidak terpasang"):
        m7.download_modis_scene(1, "x", datetime(2025, 1, 1), datetime(2025, 1, 1))
    assert called == []


def test_import_error_during_extract_skips_fallback_product(monkeypatch, tmp_path):
    discovered = []

    def fake_discover(query_date, tiles, product):
        discovered.append(product)
        return [{"tile": "h28v09", "file_name": "a.hdf", "download_url": "u"}], product

    def boom(*a, **k):
        raise ImportError("No module named 'pyhdf'")

    monkeypatch.setattr(m7, "_discover_tile_files_with_fallback", fake_discover)
    monkeypatch.setattr(m7, "_download_with_retry", lambda *a, **k: "md5")
    monkeypatch.setattr(m7, "_normalized_index_tile", boom)

    with pytest.raises(ImportError):
        m7._build_band_for_date(
            band="NDVI", product=m7.MODIS_REFLECTANCE_PRODUCTS[0],
            date=datetime(2025, 1, 1), date_key="20250101", tiles=["h28v09"],
            raw_dir=tmp_path, out_path=tmp_path / "o.tif",
            aoi_bbox=m7.JABODETABEK_BBOX, plog=None, dataset_id=1, scene_label="s",
        )
    assert discovered == [m7.MODIS_REFLECTANCE_PRODUCTS[0]]
