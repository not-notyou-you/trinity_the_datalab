# tests/test_dataset_sources_api.py
"""
API konfigurasi per-satelit: POST /api/datasets dengan objek `sources`,
dan GET /api/datasets/last-config.

Yang diuji di sini adalah kontrak HTTP-nya (DOCS/API.md bagian "Create
Dataset" dan "Get Last Configuration"). Aturan normalisasi sources-nya
sendiri diuji di tests/test_source_config.py.

Run:
    pytest tests/test_dataset_sources_api.py -v
"""

from __future__ import annotations

import pytest
from sqlalchemy import text


@pytest.fixture(autouse=True)
def no_job_runner(monkeypatch):
    """POST /api/datasets menjalankan pipeline di thread latar.

    Di tes, thread itu akan mencoba mengunduh scene sungguhan. Yang diuji di
    modul ini adalah lapisan HTTP + penulisan baris config, jadi runner-nya
    dimatikan -- job row-nya tetap dibuat dan tetap diperiksa.
    """
    from etl.dataset_manager import DatasetManager

    monkeypatch.setattr(DatasetManager, "_spawn_job_runner", lambda self, job_id: None)


@pytest.fixture
def no_existing_datasets(db_client):
    """Sembunyikan dataset yang sudah ada supaya last-config melihat tabel kosong.

    Soft-delete, bukan DELETE: baris `datasets` di database uji ini dipakai
    modul tes lain (produk, scene, storage) yang bisa berjalan sebelum atau
    sesudah modul ini, dan menghapusnya akan ikut menghapus produk mereka
    lewat ON DELETE CASCADE. get_last_dataset_config() sendiri memang
    melewati baris yang deleted_at-nya terisi, jadi soft-delete cukup untuk
    mensimulasikan "belum ada dataset sama sekali". Dikembalikan lagi di
    teardown supaya modul lain melihat kondisi semula.
    """
    with db_client.session() as sess:
        ids = [
            row[0] for row in sess.execute(
                text("SELECT dataset_id FROM datasets WHERE deleted_at IS NULL")
            )
        ]
        if ids:
            sess.execute(
                text("UPDATE datasets SET deleted_at = NOW() WHERE dataset_id = ANY(:ids)"),
                {"ids": ids},
            )
    yield
    with db_client.session() as sess:
        if ids:
            sess.execute(
                text("UPDATE datasets SET deleted_at = NULL WHERE dataset_id = ANY(:ids)"),
                {"ids": ids},
            )


def _payload(region_id, **overrides) -> dict:
    body = {
        "region_id": region_id,
        "date_start": "2024-01-01",
        "date_end": "2024-01-31",
        "name": "Ablation Study Jan 2024",
        "sources": {
            "sentinel1": {"processing": ["RAW", "PROCESSED"]},
            "modis": {"processing": ["PROCESSED"]},
        },
        "fusion_strategy": "HYBRID",
        "preview_options": ["COLORED", "COMPOSITE"],
    }
    body.update(overrides)
    return body


# ---------------------------------------------------------------------------
# POST /api/datasets
# ---------------------------------------------------------------------------

class TestCreateDatasetWithSources:

    def test_valid_sources_returns_201(self, api_client, sample_region, db_client):
        resp = api_client.post("/api/datasets", json=_payload(sample_region))
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["dataset_id"] > 0
        assert body["job_id"] > 0
        assert body["status"] == "QUEUED"
        # Urutan kanonik: S1 dulu (dia jangkar tanggal fusi).
        assert body["source_configs"] == [
            {"source": "sentinel1", "processing": ["RAW", "PROCESSED"]},
            {"source": "modis", "processing": ["PROCESSED"]},
        ]

        # Baris config benar-benar tertulis, bukan cuma dipantulkan di respons.
        configs = db_client.list_dataset_source_configs(body["dataset_id"])
        assert {c.source_name: list(c.processing_levels) for c in configs} == {
            "SENTINEL1": ["RAW", "PROCESSED"],
            "MODIS": ["PROCESSED"],
        }

    def test_required_tiers_derived_not_requested(self, api_client, sample_region):
        """`tiers` diturunkan internal: satu sumber RAW-only tidak boleh
        menghasilkan SILVER/GOLD, dan tanpa fusi tidak ada tier FUSION."""
        resp = api_client.post("/api/datasets", json=_payload(
            sample_region, sources={"gpm": {"processing": ["RAW"]}}, fusion_strategy=None,
        ))
        assert resp.status_code == 201, resp.text
        detail = api_client.get(f"/api/datasets/{resp.json()['dataset_id']}").json()
        assert detail["required_tiers"] == ["RAW", "ALIGNED"]
        assert detail["fusion_strategy"] is None

    def test_fusion_strategy_null_with_two_sources_rejected(self, api_client, sample_region):
        resp = api_client.post("/api/datasets", json=_payload(
            sample_region, fusion_strategy=None,
        ))
        assert resp.status_code == 422, resp.text
        assert "fusion_strategy required" in resp.text

    def test_fusion_strategy_set_with_one_source_rejected(self, api_client, sample_region):
        resp = api_client.post("/api/datasets", json=_payload(
            sample_region,
            sources={"sentinel1": {"processing": ["PROCESSED"]}},
            fusion_strategy="HYBRID",
        ))
        assert resp.status_code == 422, resp.text
        assert "must be null when only 1 source" in resp.text

    def test_empty_sources_rejected(self, api_client, sample_region):
        resp = api_client.post("/api/datasets", json=_payload(
            sample_region, sources={}, fusion_strategy=None,
        ))
        assert resp.status_code == 422, resp.text
        assert "minimal 1 sumber" in resp.text

    def test_missing_sources_rejected(self, api_client, sample_region):
        body = _payload(sample_region)
        del body["sources"]
        resp = api_client.post("/api/datasets", json=body)
        assert resp.status_code == 422

    def test_empty_processing_list_rejected(self, api_client, sample_region):
        resp = api_client.post("/api/datasets", json=_payload(
            sample_region, sources={"modis": {"processing": []}}, fusion_strategy=None,
        ))
        assert resp.status_code == 422, resp.text
        assert "sources.modis.processing must contain at least one value" in resp.text

    def test_unknown_source_rejected(self, api_client, sample_region):
        resp = api_client.post("/api/datasets", json=_payload(
            sample_region, sources={"landsat": {"processing": ["RAW"]}}, fusion_strategy=None,
        ))
        assert resp.status_code == 422, resp.text
        assert "source tidak dikenal" in resp.text


# ---------------------------------------------------------------------------
# Skema respons dataset
# ---------------------------------------------------------------------------

class TestDatasetResponseSchema:

    def test_detail_carries_source_configs(self, api_client, sample_region):
        created = api_client.post("/api/datasets", json=_payload(sample_region)).json()
        body = api_client.get(f"/api/datasets/{created['dataset_id']}").json()

        assert body["source_configs"] == [
            {"source": "sentinel1", "processing": ["RAW", "PROCESSED"]},
            {"source": "modis", "processing": ["PROCESSED"]},
        ]
        assert body["fusion_strategy"] == "HYBRID"
        assert body["preview_options"] == ["COLORED", "COMPOSITE"]
        assert body["last_config_source"] is None
        # Model lama sudah hilang dari respons.
        assert "selected_satellites" not in body
        assert "processing_level" not in body

    def test_list_carries_source_configs(self, api_client, sample_region):
        created = api_client.post("/api/datasets", json=_payload(sample_region)).json()
        items = api_client.get("/api/datasets?limit=50").json()["items"]

        item = next(it for it in items if it["dataset_id"] == created["dataset_id"])
        assert item["source_configs"] == created["source_configs"]
        assert item["fusion_strategy"] == "HYBRID"


# ---------------------------------------------------------------------------
# GET /api/datasets/last-config
# ---------------------------------------------------------------------------

class TestLastConfig:

    def test_404_when_no_dataset_yet(self, api_client, no_existing_datasets):
        resp = api_client.get("/api/datasets/last-config")
        assert resp.status_code == 404
        assert "No dataset found yet" in resp.text

    def test_returns_config_of_created_dataset(
        self, api_client, sample_region, no_existing_datasets
    ):
        created = api_client.post("/api/datasets", json=_payload(sample_region)).json()

        body = api_client.get("/api/datasets/last-config").json()
        assert body["region_id"] == sample_region
        assert body["region_name"] == "Jabodetabek"
        assert body["sources"] == {
            "sentinel1": {"processing": ["RAW", "PROCESSED"]},
            "modis": {"processing": ["PROCESSED"]},
        }
        assert body["fusion_strategy"] == "HYBRID"
        assert body["preview_options"] == ["COLORED", "COMPOSITE"]
        assert body["created_from_dataset_id"] == created["dataset_id"]
        assert body["created_at"]
        assert body["date_start"] == "2024-01-01"
        assert body["date_end"] == "2024-01-31"
        # Nama sengaja TIDAK ikut (DOCS/DECISIONS.md D13); tanggal IKUT sebagai
        # preset yang bisa diedit user.
        assert "name" not in body

    def test_returns_latest_of_two_datasets(
        self, api_client, sample_region, no_existing_datasets
    ):
        api_client.post("/api/datasets", json=_payload(sample_region, name="dataset1"))
        second = api_client.post("/api/datasets", json=_payload(
            sample_region,
            name="dataset2",
            sources={"gpm": {"processing": ["RAW", "PROCESSED"]}},
            fusion_strategy=None,
            preview_options=["GRAYSCALE"],
        )).json()

        body = api_client.get("/api/datasets/last-config").json()
        assert body["created_from_dataset_id"] == second["dataset_id"]
        assert body["sources"] == {"gpm": {"processing": ["RAW", "PROCESSED"]}}
        assert body["fusion_strategy"] is None
        assert body["preview_options"] == ["GRAYSCALE"]


class TestPerSourceCardStats:
    """`scenes_by_source` / `bytes_by_source` yang menyuapi kartu dataset.

    Keduanya dihitung dari data_products lewat SATU query agregat untuk
    seluruh halaman listing, bukan dengan menyusuri disk per kartu -- kartu
    dirender ulang tiap polling.
    """

    def test_absent_until_products_exist(self, api_client, sample_region):
        """Dataset baru belum punya produk, jadi kedua peta kosong -- bukan
        berisi nol untuk tiap source yang dikonfigurasi. Kartu membedakan
        "belum ada" dari "nol byte"."""
        api_client.post("/api/datasets", json=_payload(sample_region))
        item = api_client.get("/api/datasets").json()["items"][0]
        assert item["scenes_by_source"] == {}
        assert item["bytes_by_source"] == {}

    def test_counts_distinct_scenes_not_product_rows(
        self, api_client, db_client, meta, sample_region, sample_scene
    ):
        """Satu scene menghasilkan banyak baris produk (VV, VH, beberapa
        tier). Menghitung barisnya akan melaporkan angka berkali lipat dari
        jumlah scene yang sebenarnya."""
        ds_id = api_client.post(
            "/api/datasets", json=_payload(sample_region)
        ).json()["dataset_id"]

        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        for band in ("VV", "VH"):
            meta.insert_data_product(
                scene_id=sample_scene, job_id=job_id, dataset_id=ds_id,
                product_tier="COG", source="SENTINEL1", product_type="S1_COG",
                band_name=band, file_name=f"f_{band}.tif",
                file_path=f"/tmp/f_{band}.tif", file_size_mb=2.0,
                data_hash_sha256="",
            )

        item = next(
            i for i in api_client.get("/api/datasets").json()["items"]
            if i["dataset_id"] == ds_id
        )
        assert item["scenes_by_source"] == {"sentinel1": 1}   # 1 scene, 2 baris
        assert item["bytes_by_source"]["sentinel1"] == int(4.0 * 1024 * 1024)
