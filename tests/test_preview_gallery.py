# tests/test_preview_gallery.py
"""
Tests galeri preview di api/routes/datasets.py.

Folder preview satu dataset dipakai bersama SELURUH tanggal (lihat
fm.get_preview_dir), jadi yang diuji di sini adalah satu hal: payload satu
tanggal hanya boleh memuat berkas milik tanggal itu. Sebelum sidecar diberi
prefiks tanggal, tiap tanggal menampilkan gambar tanggal yang kebetulan
dirender terakhir (dataset 22_try6).

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
from etl import module10_generate_preview as m10

DATASET_ID = 31
DATASET_NAME = "Gallery Test"
DAY_A = "20260712"
DAY_B = "20260724"
SCENE_A = f"S1A_IW_GRDH_1SDV_{DAY_A}T111407.SAFE"
SCENE_B = f"S1A_IW_GRDH_1SDV_{DAY_B}T111409.SAFE"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(fm, "DATA_ROOT", tmp_path / "datasets")
    return tmp_path


def _render_day(date_key: str, scene: str, seed: int) -> dict:
    """Tulis COG S1 palsu untuk satu tanggal lalu render preview-nya."""
    rng = np.random.default_rng(seed)
    array = rng.lognormal(mean=-1.0, sigma=0.8, size=(60, 80)).astype("float32")
    scene_dir = fm.get_scene_dir(DATASET_ID, DATASET_NAME, "cog", "sentinel1", scene)
    path = scene_dir / f"S1A_calibrated_VV_lee.tif"
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path, "w", driver="GTiff", height=array.shape[0], width=array.shape[1],
        count=1, dtype="float32", crs="EPSG:4326",
        transform=from_origin(106.7, -6.0, 0.001, 0.001), nodata=np.nan,
    ) as dst:
        dst.write(array, 1)
    return m10.generate_previews(
        DATASET_ID, DATASET_NAME, date_key, s1_scene_key=scene
    )


def _files_in_payload(payload: dict) -> list[str]:
    return sorted(
        image["file"]
        for level in payload["by_level"].values()
        for kind in level["kinds"].values()
        for image in kind["images"]
    )


class TestPerDateIsolation:
    def test_each_date_lists_only_its_own_images(self, data_root):
        _render_day(DAY_A, SCENE_A, seed=1)
        _render_day(DAY_B, SCENE_B, seed=2)

        assert fm.list_preview_scenes(DATASET_ID, DATASET_NAME) == [DAY_A, DAY_B]

        for day, scene in ((DAY_A, SCENE_A), (DAY_B, SCENE_B)):
            payload = api_datasets._preview_scene_payload(DATASET_ID, DATASET_NAME, day)
            files = _files_in_payload(payload)
            assert files, f"{day} tidak punya gambar sama sekali"
            assert all(f.startswith(day) for f in files), (day, files)
            assert payload["acquisition_date"] == day
            assert payload["s1_scene_key"] == scene

    def test_size_bytes_is_per_date_not_whole_dataset(self, data_root):
        _render_day(DAY_A, SCENE_A, seed=3)
        only_a = api_datasets._preview_scene_payload(
            DATASET_ID, DATASET_NAME, DAY_A
        )["size_bytes"]

        _render_day(DAY_B, SCENE_B, seed=4)
        after_b = api_datasets._preview_scene_payload(
            DATASET_ID, DATASET_NAME, DAY_A
        )["size_bytes"]

        assert after_b == only_a, "ukuran tanggal A ikut naik saat tanggal B dirender"

    def test_counts_match_files_on_disk(self, data_root):
        result_a = _render_day(DAY_A, SCENE_A, seed=5)
        _render_day(DAY_B, SCENE_B, seed=6)

        payload = api_datasets._preview_scene_payload(DATASET_ID, DATASET_NAME, DAY_A)
        kinds = payload["by_level"]["PROCESSED"]["kinds"]
        assert kinds["grayscale"]["count"] == result_a["counts"]["grayscale"]
        assert kinds["composite"]["count"] == result_a["counts"]["composite"]


class TestLegacySharedSidecar:
    """Dataset yang dirender sebelum sidecar berprefiks tanggal: satu sidecar
    bersama yang isinya tanggal terakhir. Itu tidak boleh bocor ke tanggal
    lain."""

    def _make_legacy(self) -> None:
        color = fm.ensure_preview_kind_dir(
            DATASET_ID, DATASET_NAME, DAY_A, "colored", "PROCESSED"
        )
        for day in (DAY_A, DAY_B):
            (color / f"{day}_s1_vv.png").write_bytes(b"\x89PNG" + b"x" * 40)
        # Sidecar bersama, menggambarkan DAY_A saja — persis kondisi try6.
        (color / "colored_info.json").write_text(json.dumps({
            "kind": "colored",
            "images": [{"key": "s1_vv", "file": f"{DAY_A}_s1_vv.png",
                        "label": "Sentinel-1 VV"}],
        }), encoding="utf-8")
        level_dir = fm.get_preview_level_dir(
            DATASET_ID, DATASET_NAME, DAY_A, "PROCESSED"
        )
        shared = json.dumps({
            "acquisition_date": DAY_A, "s1_scene_key": SCENE_A,
            "derived_from": "COG", "sources_present": ["sentinel1"], "skipped": [],
        })
        (level_dir / "preview_metadata.json").write_text(shared, encoding="utf-8")
        (fm.get_preview_dir(DATASET_ID, DATASET_NAME, DAY_A)
         / "preview_metadata.json").write_text(shared, encoding="utf-8")

    def test_other_date_does_not_inherit_shared_sidecar(self, data_root):
        self._make_legacy()

        payload_b = api_datasets._preview_scene_payload(DATASET_ID, DATASET_NAME, DAY_B)
        files_b = _files_in_payload(payload_b)
        assert files_b == [f"{DAY_B}_s1_vv.png"], files_b
        # Keterangan tanggal A tidak boleh dipinjamkan ke tanggal B.
        assert payload_b["s1_scene_key"] is None
        assert payload_b["acquisition_date"] == DAY_B

    def test_own_date_still_uses_shared_sidecar(self, data_root):
        self._make_legacy()

        payload_a = api_datasets._preview_scene_payload(DATASET_ID, DATASET_NAME, DAY_A)
        images = payload_a["by_level"]["PROCESSED"]["kinds"]["colored"]["images"]
        assert [i["file"] for i in images] == [f"{DAY_A}_s1_vv.png"]
        assert images[0]["label"] == "Sentinel-1 VV"
        assert payload_a["s1_scene_key"] == SCENE_A
