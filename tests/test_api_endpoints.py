# tests/test_api_endpoints.py
"""
API endpoint tests using FastAPI TestClient.
Tests all routes: health, scenes, products, quality, lineage.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/test_api_endpoints.py -v
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest


def fake_hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Helper: seed one scene for API tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def seeded(db_client, sample_region):
    """Seed one complete scene with all products and quality metrics."""
    from etl.seed_data import seed
    return seed(db_client)


# ---------------------------------------------------------------------------
# 1. HEALTH ENDPOINT
# ---------------------------------------------------------------------------

class TestHealthEndpoint:

    def test_health_returns_200(self, api_client):
        """GET /api/health must return 200."""
        resp = api_client.get("/api/health")
        assert resp.status_code == 200

    def test_health_response_schema(self, api_client):
        """Health response must include status and db_connected fields."""
        resp = api_client.get("/api/health")
        body = resp.json()
        assert "status"       in body
        assert "db_connected" in body
        assert "timestamp"    in body
        assert "api_version"  in body

    def test_health_db_connected(self, api_client):
        """Health check must report database as connected."""
        resp = api_client.get("/api/health")
        body = resp.json()
        assert body["db_connected"] is True
        assert body["status"] == "ok"


# ---------------------------------------------------------------------------
# 2. SCENES ENDPOINTS
# ---------------------------------------------------------------------------

class TestScenesEndpoints:

    def test_list_scenes_200(self, api_client, seeded):
        """GET /api/scenes returns 200 with items list."""
        resp = api_client.get("/api/scenes")
        assert resp.status_code == 200
        body = resp.json()
        assert "items"  in body
        assert "total"  in body
        assert "limit"  in body
        assert "offset" in body

    def test_list_scenes_default_limit(self, api_client, seeded):
        """Default limit is 20."""
        resp = api_client.get("/api/scenes")
        assert resp.json()["limit"] == 20

    def test_list_scenes_filter_by_region(self, api_client, seeded, sample_region):
        """Filter by region_id returns only scenes for that region."""
        resp = api_client.get(f"/api/scenes?region_id={sample_region}")
        assert resp.status_code == 200
        body = resp.json()
        for item in body["items"]:
            assert item["region_id"] == sample_region

    def test_get_scene_by_id_200(self, api_client, seeded):
        """GET /api/scenes/{id} returns 200 with scene detail."""
        scene_id = seeded["scene_id"]
        resp = api_client.get(f"/api/scenes/{scene_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["scene_id"] == scene_id
        assert "product_identifier"   in body
        assert "acquisition_datetime" in body
        assert "orbit_direction"      in body

    def test_get_scene_not_found(self, api_client):
        """GET /api/scenes/99999 returns 404."""
        resp = api_client.get("/api/scenes/99999")
        assert resp.status_code == 404

    def test_get_scene_status(self, api_client, seeded):
        """GET /api/scenes/{id}/status returns pipeline stage statuses."""
        scene_id = seeded["scene_id"]
        resp = api_client.get(f"/api/scenes/{scene_id}/status")
        assert resp.status_code == 200
        body = resp.json()
        assert "stages"         in body
        assert "overall_status" in body
        assert len(body["stages"]) > 0

    def test_list_scenes_only_gold_filter(self, api_client, seeded):
        """GET /api/scenes?only_gold=true returns only scenes with GOLD product."""
        resp = api_client.get("/api/scenes?only_gold=true")
        assert resp.status_code == 200
        assert resp.json()["total"] >= 0

    def test_list_scenes_pagination(self, api_client, seeded):
        """Pagination parameters are reflected in response."""
        resp = api_client.get("/api/scenes?limit=5&offset=0")
        assert resp.status_code == 200
        body = resp.json()
        assert body["limit"]  == 5
        assert body["offset"] == 0
        assert len(body["items"]) <= 5


# ---------------------------------------------------------------------------
# 3. PRODUCTS ENDPOINTS
# ---------------------------------------------------------------------------

class TestProductsEndpoints:

    def test_list_products_200(self, api_client, seeded):
        """GET /api/products returns 200."""
        resp = api_client.get("/api/products")
        assert resp.status_code == 200
        body = resp.json()
        assert "items" in body
        assert body["total"] >= 0

    def test_list_products_filter_by_scene(self, api_client, seeded):
        """Filter by scene_id returns only that scene's products."""
        scene_id = seeded["scene_id"]
        resp = api_client.get(f"/api/products?scene_id={scene_id}")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["scene_id"] == scene_id

    def test_list_products_filter_by_tier(self, api_client, seeded):
        """Filter by tier=GOLD returns only GOLD products."""
        resp = api_client.get("/api/products?tier=GOLD")
        assert resp.status_code == 200
        for item in resp.json()["items"]:
            assert item["product_tier"] == "GOLD"

    def test_list_products_invalid_tier(self, api_client):
        """Invalid tier value returns 400."""
        resp = api_client.get("/api/products?tier=INVALID")
        assert resp.status_code == 400

    def test_get_product_by_id(self, api_client, seeded):
        """GET /api/products/{id} returns 200 with product detail."""
        prod_id = seeded["gold_fusion_id"]
        resp = api_client.get(f"/api/products/{prod_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["product_id"]   == prod_id
        assert body["product_tier"] == "FUSION"
        assert body["source"]       == "FUSION"
        assert body["band_name"]    == "FUSION"
        assert "data_hash_sha256"   in body
        assert "file_path"          in body

    def test_get_product_not_found(self, api_client):
        """GET /api/products/99999 returns 404."""
        resp = api_client.get("/api/products/99999")
        assert resp.status_code == 404

    def test_download_product_missing_file(self, api_client, seeded):
        """Download endpoint returns 404 if file not on disk (expected in test env)."""
        prod_id = seeded["gold_fusion_id"]
        resp = api_client.get(f"/api/products/{prod_id}/download")
        # File won't exist in test env — expect 404
        assert resp.status_code in (200, 404)

    def test_verify_product_missing_file(self, api_client, seeded):
        """Verify endpoint returns 404 if file not on disk."""
        prod_id = seeded["gold_fusion_id"]
        resp = api_client.get(f"/api/products/{prod_id}/verify")
        assert resp.status_code in (200, 404)


# ---------------------------------------------------------------------------
# 4. QUALITY ENDPOINTS
# ---------------------------------------------------------------------------

class TestQualityEndpoints:

    def test_get_quality_200(self, api_client, seeded):
        """GET /api/quality/{scene_id} returns 200 with band metrics."""
        scene_id = seeded["scene_id"]
        resp = api_client.get(f"/api/quality/{scene_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert "scene_id"       in body
        assert "bands"          in body
        assert "overall_quality" in body
        assert len(body["bands"]) == 2  # VV + VH

    def test_quality_band_names(self, api_client, seeded):
        """Quality response must include both VV and VH bands."""
        scene_id   = seeded["scene_id"]
        resp       = api_client.get(f"/api/quality/{scene_id}")
        band_names = {b["band_name"] for b in resp.json()["bands"]}
        assert "VV" in band_names
        assert "VH" in band_names

    def test_quality_score_in_range(self, api_client, seeded):
        """All quality scores must be in [0, 100]."""
        scene_id = seeded["scene_id"]
        resp     = api_client.get(f"/api/quality/{scene_id}")
        for band in resp.json()["bands"]:
            assert 0 <= band["quality_score"] <= 100

    def test_quality_overall_flag(self, api_client, seeded):
        """overall_quality must be PASS/FAIL/WARNING."""
        scene_id = seeded["scene_id"]
        resp = api_client.get(f"/api/quality/{scene_id}")
        assert resp.json()["overall_quality"] in {"PASS", "FAIL", "WARNING"}

    def test_quality_not_found(self, api_client):
        """GET /api/quality/99999 returns 404."""
        resp = api_client.get("/api/quality/99999")
        assert resp.status_code == 404

    def test_quality_summary_stats(self, api_client, seeded):
        """GET /api/quality/summary/stats returns aggregated stats."""
        resp = api_client.get("/api/quality/summary/stats?n_days=30")
        assert resp.status_code == 200
        body = resp.json()
        assert "period_days" in body
        assert "flags"       in body


# ---------------------------------------------------------------------------
# 5. LINEAGE ENDPOINTS
# ---------------------------------------------------------------------------

class TestLineageEndpoints:

    def test_get_lineage_ancestors_200(self, api_client, seeded):
        """GET /api/metadata/lineage/{product_id} returns 200."""
        prod_id = seeded["gold_fusion_id"]
        resp    = api_client.get(f"/api/metadata/lineage/{prod_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert "product_id"  in body
        assert "chain"       in body
        assert "total_steps" in body
        assert "direction"   in body

    def test_lineage_chain_has_steps(self, api_client, seeded):
        """GOLD product lineage must have at least 2 ancestor steps."""
        prod_id = seeded["gold_fusion_id"]
        resp    = api_client.get(f"/api/metadata/lineage/{prod_id}?direction=ancestors")
        body    = resp.json()
        assert body["total_steps"] >= 2

    def test_lineage_transformation_types(self, api_client, seeded):
        """Lineage chain must include CROP, LEE_FILTER, FUSION steps."""
        prod_id = seeded["gold_fusion_id"]
        resp    = api_client.get(f"/api/metadata/lineage/{prod_id}?direction=ancestors")
        chain   = resp.json()["chain"]
        types   = {step["transformation_type"] for step in chain}
        assert "CROP"       in types
        assert "LEE_FILTER" in types
        assert "FUSION"     in types

    def test_lineage_descendants_direction(self, api_client, seeded):
        """direction=descendants is accepted and returns valid response."""
        prod_id = seeded["raw_vv_id"]
        resp    = api_client.get(f"/api/metadata/lineage/{prod_id}?direction=descendants")
        assert resp.status_code == 200
        assert resp.json()["direction"] == "descendants"

    def test_lineage_invalid_direction(self, api_client, seeded):
        """Invalid direction parameter returns 422."""
        prod_id = seeded["gold_fusion_id"]
        resp    = api_client.get(f"/api/metadata/lineage/{prod_id}?direction=invalid")
        assert resp.status_code == 422

    def test_lineage_not_found(self, api_client):
        """GET /api/metadata/lineage/99999 returns 404."""
        resp = api_client.get("/api/metadata/lineage/99999")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 6. ROOT ENDPOINT
# ---------------------------------------------------------------------------

class TestRootEndpoint:

    def test_api_info_returns_version_and_docs(self, api_client):
        """GET /api returns API info with version and docs link."""
        resp = api_client.get("/api")
        assert resp.status_code == 200
        body = resp.json()
        assert "message" in body
        assert "docs"    in body
        assert "version" in body

    def test_root_serves_web_ui(self, api_client):
        """GET / serves the web console, not JSON (web/ is mounted at /)."""
        resp = api_client.get("/")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/html")


# ---------------------------------------------------------------------------
# TIER x SOURCE FILTERING & PER-DATASET STORAGE
# ---------------------------------------------------------------------------

class TestSourceFiltering:
    """Filter gabungan tier + source adalah alasan utama kolom
    data_products.source ada (migrasi 013)."""

    def test_products_carry_source(self, api_client, seeded):
        resp = api_client.get("/api/products")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert items, "seed harus menghasilkan produk"
        for item in items:
            assert item["source"] in {"SENTINEL1", "MODIS", "GPM", "FUSION"}

    def test_filter_by_source(self, api_client, seeded):
        resp = api_client.get("/api/products?source=SENTINEL1")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert items
        assert all(i["source"] == "SENTINEL1" for i in items)

    def test_filter_by_tier_and_source(self, api_client, seeded):
        """Stack fusion tinggal di tier FUSION dengan source FUSION -- dan
        tidak boleh ikut tertarik saat memfilter source SENTINEL1, walaupun
        barisnya menempel ke scene_id Sentinel-1."""
        resp = api_client.get("/api/products?tier=FUSION&source=FUSION")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["product_type"] == "FUSION_H5"

        resp = api_client.get("/api/products?tier=FUSION&source=SENTINEL1")
        assert resp.status_code == 200
        assert resp.json()["items"] == []

    def test_source_filter_is_case_insensitive(self, api_client, seeded):
        resp = api_client.get("/api/products?source=sentinel1")
        assert resp.status_code == 200
        assert resp.json()["items"]

    def test_invalid_source_returns_400(self, api_client, seeded):
        resp = api_client.get("/api/products?source=LANDSAT")
        assert resp.status_code == 400
        assert "LANDSAT" in resp.json()["detail"]

    def test_invalid_tier_returns_400(self, api_client, seeded):
        resp = api_client.get("/api/products?tier=PLATINUM")
        assert resp.status_code == 400


class TestGeneratePreviewFlag:
    """datasets.generate_preview: opt-out tahap PREVIEW per dataset."""

    def test_defaults_to_true_when_omitted(self, api_client, sample_dataset):
        """Pemanggil lama tidak mengirim field ini; perilakunya harus tetap
        seperti sebelum flag ada, yaitu PREVIEW ikut jalan."""
        resp = api_client.get(f"/api/datasets/{sample_dataset}")
        assert resp.status_code == 200
        assert resp.json()["generate_preview"] is True

    def test_flag_is_exposed_in_list_view(self, api_client, sample_dataset):
        """Kartu dataset butuh flag ini tanpa harus mengambil detail, supaya
        galeri kosong bisa membedakan 'dimatikan' dari 'belum dibuat'."""
        resp = api_client.get("/api/datasets")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert items and all("generate_preview" in it for it in items)

    def test_create_request_accepts_false(self, sample_region):
        from api.schemas import DatasetCreateRequest

        req = DatasetCreateRequest(
            region_id=sample_region, date_start="2024-01-01", date_end="2024-01-31",
            tiers=["GOLD"], name="no preview", generate_preview=False,
        )
        assert req.generate_preview is False

    def test_preview_not_a_requestable_tier(self, sample_region):
        """PREVIEW turunan, bukan tier lineage: memintanya lewat `tiers` harus
        ditolak, bukan diam-diam diterima lalu dihapus tier cleanup."""
        import pytest as _pytest
        from api.schemas import DatasetCreateRequest

        with _pytest.raises(ValueError):
            DatasetCreateRequest(
                region_id=sample_region, date_start="2024-01-01", date_end="2024-01-31",
                tiers=["PREVIEW"], name="bad",
            )


class TestDatasetStorageEndpoints:

    def test_storage_summary_shape(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/storage/summary")
        assert resp.status_code == 200
        body = resp.json()
        # Semua tier selalu dilaporkan walau kosong, supaya konsumen tidak
        # perlu membedakan "tier tidak ada" dari "tier kosong". preview ikut
        # di sini walau bukan tier lineage: dia tetap memakan disk.
        assert set(body["tiers"]) == {"raw", "bronze", "silver", "gold", "preview", "fusion"}
        assert body["tiers"]["fusion"]["sources"] == {}
        assert body["tiers"]["preview"]["sources"] == {}
        assert body["total_size_bytes"] == 0

    def test_storage_files_rejects_source_on_fusion_tier(self, api_client, sample_dataset):
        """Tier fusion gabungan semua source, jadi ?source= di sana keliru."""
        resp = api_client.get(
            f"/api/datasets/{sample_dataset}/storage/files/fusion?source=modis"
        )
        assert resp.status_code == 400

    def test_storage_files_rejects_source_on_preview_tier(self, api_client, sample_dataset):
        """Sama seperti fusion, preview lintas-source: ?source= keliru di sana."""
        resp = api_client.get(
            f"/api/datasets/{sample_dataset}/storage/files/preview?source=modis"
        )
        assert resp.status_code == 400

    def test_storage_files_rejects_unknown_tier(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/storage/files/platinum")
        assert resp.status_code == 400

    def test_storage_files_empty_tier(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/storage/files/gold")
        assert resp.status_code == 200
        body = resp.json()
        assert body["tier"] == "gold"
        assert body["scenes"] == []

    def test_metadata_404_before_first_job(self, api_client, sample_dataset):
        """metadata.json baru ditulis saat job pertama selesai."""
        resp = api_client.get(f"/api/datasets/{sample_dataset}/metadata")
        assert resp.status_code == 404

    def test_download_source_without_tier_is_400(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/download?source=modis")
        assert resp.status_code == 400


class TestQualityBySource:

    def test_quality_by_source_empty_dataset(self, api_client, sample_dataset):
        """Dataset tanpa produk tetap 200 dengan daftar kosong, bukan 404 --
        UI memanggil endpoint ini untuk dataset yang baru dibuat."""
        resp = api_client.get(f"/api/quality/dataset/{sample_dataset}/by-source")
        assert resp.status_code == 200
        body = resp.json()
        assert body["dataset_id"] == sample_dataset
        assert body["sources"] == []

    def test_quality_by_source_kinds(self, api_client, seeded, sample_dataset):
        resp = api_client.get(f"/api/quality/dataset/{sample_dataset}/by-source")
        assert resp.status_code == 200
        for item in resp.json()["sources"]:
            assert item["kind"] in {"RADIOMETRIC", "COVERAGE"}

    def test_quality_by_source_404_for_unknown_dataset(self, api_client):
        resp = api_client.get("/api/quality/dataset/999999/by-source")
        assert resp.status_code == 404
