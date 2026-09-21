# tests/test_mask_gallery.py
"""
Tests endpoint layer referensi di api/routes/datasets.py.

Bedanya dari galeri preview, dan itu yang diuji di sini: preview itu PER
TANGGAL, layer referensi PER DATASET. Satu berkas melayani seluruh stack
karena garis pantai tidak berubah antar tanggal — jadi payload-nya rata, tanpa
tingkat scene sama sekali.

Yang dijaga:

    - dataset tanpa masks/ menjawab 200 berisi daftar kosong, bukan 404;
      dataset yang belum sampai fusion memang belum punya, dan UI harus bisa
      membedakannya dari galat
    - statistik yang keluar diambil dari manifest apa adanya, bukan dihitung
      ulang, supaya angka di layar sama dengan yang tertanam di berkas
    - `filename` datang dari URL, jadi ".." dan berkas non-PNG harus ditolak

Murni filesystem — tidak menyentuh database.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from api.routes import datasets as api_datasets
from etl import folder_manager as fm
from etl import land_mask as lm

DATASET_ID = 77
DATASET_NAME = "Mask Test"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
    return tmp_path


@pytest.fixture
def masks_dir(data_root):
    root = fm.get_dataset_root(DATASET_ID, DATASET_NAME)
    d = lm.get_masks_dir(root)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_layer(masks_dir, stem: str, stats: dict, manifest_name: str) -> None:
    """Berkas layer minimal: TIF kecil, PNG, dan manifest-nya."""
    transform = from_origin(106.4, -5.9, 0.001, 0.001)
    data = np.arange(64, dtype="int16").reshape(8, 8)
    with rasterio.open(
        masks_dir / f"{stem}.tif", "w", driver="GTiff", height=8, width=8,
        count=1, dtype="int16", crs="EPSG:4326", transform=transform,
    ) as dst:
        dst.write(data, 1)
    (masks_dir / f"{stem}.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 32)
    (masks_dir / manifest_name).write_text(
        json.dumps({
            "layer": stem,
            "semantics": f"semantik {stem}",
            "source": {"name": "sumber uji"},
            "statistics": stats,
        }),
        encoding="utf-8",
    )


class TestListing:
    @pytest.mark.asyncio
    async def test_empty_dataset_returns_200_not_404(self, data_root, monkeypatch):
        """Dataset tanpa masks/ itu keadaan normal, bukan galat."""
        monkeypatch.setattr(
            api_datasets, "_mgr",
            lambda db: type("M", (), {"get_dataset": lambda s, i: {"name": DATASET_NAME}})(),
        )
        out = await api_datasets.list_dataset_masks(DATASET_ID, db=None)
        assert out["layer_count"] == 0
        assert out["layers"] == []

    @pytest.mark.asyncio
    async def test_statistics_come_from_the_manifest_verbatim(
        self, masks_dir, monkeypatch
    ):
        """Angkanya disalin dari manifest, tidak dihitung ulang di API."""
        _write_layer(
            masks_dir, "land_distance",
            {"pct_sea": 16.7804, "pct_land": 83.2196, "clamp_m": 32000},
            "manifest.json",
        )
        monkeypatch.setattr(
            api_datasets, "_mgr",
            lambda db: type("M", (), {"get_dataset": lambda s, i: {"name": DATASET_NAME}})(),
        )
        out = await api_datasets.list_dataset_masks(DATASET_ID, db=None)

        assert out["layer_count"] == 1
        layer = out["layers"][0]
        assert layer["key"] == "land_distance"
        assert layer["statistics"]["pct_sea"] == 16.7804
        assert layer["image_url"].endswith("/masks/land_distance.png")

    @pytest.mark.asyncio
    async def test_both_layers_are_listed_when_present(self, masks_dir, monkeypatch):
        _write_layer(masks_dir, "land_distance", {"pct_sea": 16.78},
                     "manifest.json")
        _write_layer(masks_dir, "water_occurrence", {"pct_occurrence_ge_90": 17.42},
                     "manifest_water_occurrence.json")
        monkeypatch.setattr(
            api_datasets, "_mgr",
            lambda db: type("M", (), {"get_dataset": lambda s, i: {"name": DATASET_NAME}})(),
        )
        out = await api_datasets.list_dataset_masks(DATASET_ID, db=None)

        assert {l["key"] for l in out["layers"]} == {
            "land_distance", "water_occurrence"
        }
        # Payload rata: tidak ada tingkat scene, karena layernya berlaku untuk
        # semua tanggal sekaligus.
        assert "scenes" not in out


class TestImageSafety:
    """`filename` datang dari URL, jadi diperlakukan sebagai masukan tak tepercaya."""

    @pytest.mark.asyncio
    async def test_non_png_is_rejected(self, masks_dir, monkeypatch):
        monkeypatch.setattr(
            api_datasets, "_mgr",
            lambda db: type("M", (), {"get_dataset": lambda s, i: {"name": DATASET_NAME}})(),
        )
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await api_datasets.get_mask_image(DATASET_ID, "land_distance.tif", db=None)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_path_component_is_rejected(self, masks_dir, monkeypatch):
        monkeypatch.setattr(
            api_datasets, "_mgr",
            lambda db: type("M", (), {"get_dataset": lambda s, i: {"name": DATASET_NAME}})(),
        )
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await api_datasets.get_mask_image(DATASET_ID, "../../secret.png", db=None)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_missing_file_is_404(self, masks_dir, monkeypatch):
        monkeypatch.setattr(
            api_datasets, "_mgr",
            lambda db: type("M", (), {"get_dataset": lambda s, i: {"name": DATASET_NAME}})(),
        )
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc:
            await api_datasets.get_mask_image(DATASET_ID, "tidak_ada.png", db=None)
        assert exc.value.status_code == 404
