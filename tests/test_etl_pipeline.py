# tests/test_etl_pipeline.py
"""
Integration tests: end-to-end ETL pipeline flow (Module 1-6).
Tests metadata tracking, lineage recording, and checkpoint system.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/test_etl_pipeline.py -v
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest
from sqlalchemy import text


def fake_hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


# ---------------------------------------------------------------------------
# 1. END-TO-END PIPELINE FLOW
# ---------------------------------------------------------------------------

class TestEndToEndFlow:
    """Verify the complete Module 1–6 pipeline produces correct DB state."""

    def test_full_pipeline_seed(self, db_client, sample_region):
        """
        Run the seed_data script and verify all 11 tables are populated.
        This is the primary integration test for the entire pipeline.
        """
        from etl.seed_data import seed, verify_seed
        ids = seed(db_client)
        verify_seed(db_client, ids)

        assert ids["scene_id"]      > 0
        assert ids["gold_fusion_id"] > 0
        assert ids["vv_metric_id"] > 0
        assert ids["vh_metric_id"] > 0

    def test_pipeline_all_jobs_succeed(self, db_client, sample_region):
        """After full pipeline run, all 5 jobs must have status=SUCCESS."""
        from etl.seed_data import seed

        ids = seed(db_client)
        scene_id = ids["scene_id"]

        with db_client.session() as sess:
            failed = sess.scalar(text("""
                SELECT COUNT(*) FROM processing_jobs
                WHERE scene_id = :sid AND status = 'FAILED'
            """), {"sid": scene_id})
        assert failed == 0, f"{failed} failed jobs found — expected 0"

        with db_client.session() as sess:
            success = sess.scalar(text("""
                SELECT COUNT(*) FROM processing_jobs
                WHERE scene_id = :sid AND status = 'SUCCESS'
            """), {"sid": scene_id})
        assert success == 5, f"Expected 5 successful stages, got {success}"

    def test_pipeline_product_tiers(self, db_client, sample_region):
        """After pipeline, each band must have RAW, BRONZE, SILVER products,
        and GOLD must have the single fused H5 product (band_name=FUSION)."""
        from etl.seed_data import seed
        from etl.metadata_manager import MetadataManager

        ids  = seed(db_client)
        meta = MetadataManager(db_client)

        for band in ["VV", "VH"]:
            for tier in ["RAW", "BRONZE", "SILVER"]:
                products = meta.get_products_by_scene(
                    ids["scene_id"], tier=tier, latest_only=True
                )
                band_products = [p for p in products if p["band_name"] == band]
                assert len(band_products) >= 1, (
                    f"Missing {tier} product for band={band}"
                )

        # Stack fusion sekarang tinggal di tier FUSION sendiri; GOLD berisi
        # produk analysis-ready per-source (migrasi 013).
        fusion_products = meta.get_products_by_scene(
            ids["scene_id"], tier="FUSION", latest_only=True
        )
        assert len(fusion_products) == 1, "FUSION should have exactly 1 fused product, not per-band"
        assert fusion_products[0]["band_name"] == "FUSION"


# ---------------------------------------------------------------------------
# 2. METADATA TRACKING
# ---------------------------------------------------------------------------

class TestMetadataTracking:
    """Verify scene → job → product → quality linkage is complete."""

    def test_scene_to_jobs_link(self, db_client, meta, sample_scene):
        """Each pipeline stage creates exactly one job linked to the scene."""
        stages = ["DOWNLOAD", "CROP", "LEE_FILTER", "QUALITY_ANALYTICS", "FUSION"]
        for stage in stages:
            job_id = meta.insert_processing_job(sample_scene, stage)
            meta.start_job(job_id)
            meta.complete_job(job_id)

        status = meta.get_pipeline_status(sample_scene)
        assert len(status) == 5

        stage_names = {s["stage_name"] for s in status}
        assert stage_names == set(stages)

    def test_product_linked_to_job(self, db_client, meta, sample_scene):
        """Data products must be linked to the producing job."""
        job_id = meta.insert_processing_job(sample_scene, "FUSION")
        meta.start_job(job_id)

        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="FUSION", source="FUSION", product_type="FUSION_H5",
            band_name="FUSION", file_path="/tmp/test_fusion.h5", file_name="test_fusion.h5",
            file_size_mb=40.0, data_hash_sha256=fake_hash("TRACK_FUSION"), file_format="HDF5",
        )

        products = meta.get_products_by_scene(sample_scene, tier="FUSION")
        fusion = next((p for p in products if p["band_name"] == "FUSION"), None)
        assert fusion is not None
        assert fusion["job_id"] == job_id

    def test_quality_linked_to_product(self, db_client, meta, sample_scene):
        """Quality metrics must reference both scene and product."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        meta.start_job(job_id)
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED",
            band_name="VH", file_path="/tmp/vh.tif", file_name="vh.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("QUALITY_VH"),
        )
        metric_id = meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VH",
            total_pixels=500000, valid_pixels=490000, nodata_pixels=10000,
            quality_score=78.5, quality_flag="PASS",
        )

        metrics = meta.get_quality_by_scene(sample_scene)
        vh = next((m for m in metrics if m["band_name"] == "VH"), None)
        assert vh is not None
        assert vh["quality_score"] == 78.5

    def test_is_latest_flag_update(self, db_client, meta, sample_scene):
        """Inserting a new product for same scene/band/tier marks old one as not latest."""
        job_id = meta.insert_processing_job(sample_scene, "FUSION")

        # First product
        pid1 = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="FUSION", source="FUSION", product_type="FUSION_H5",
            band_name="FUSION", file_path="/tmp/f1.h5", file_name="f1.h5",
            file_size_mb=40.0, data_hash_sha256=fake_hash("LATEST_V1"), file_format="HDF5",
        )
        # Second product (same tier+band → should mark pid1 as not latest)
        pid2 = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="FUSION", source="FUSION", product_type="FUSION_H5",
            band_name="FUSION", file_path="/tmp/f2.h5", file_name="f2.h5",
            file_size_mb=39.0, data_hash_sha256=fake_hash("LATEST_V2"), file_format="HDF5",
        )

        from etl.database_client import DataProduct
        with db_client.session() as sess:
            p1 = sess.get(DataProduct, pid1)
            p2 = sess.get(DataProduct, pid2)
        assert p1.is_latest is False, "Old product should be marked not latest"
        assert p2.is_latest is True,  "New product should be latest"


# ---------------------------------------------------------------------------
# 3. DATA QUALITY TESTS
# ---------------------------------------------------------------------------

class TestDataQuality:
    """Verify quality score computation and flag assignment."""

    def test_quality_score_range(self, db_client, meta, sample_scene):
        """Quality score must always be in range [0, 100]."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED", band_name="VV",
            file_path="/tmp/score_test.tif", file_name="score_test.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("SCORE_RANGE"),
        )
        metric_id = meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VV",
            total_pixels=100000, valid_pixels=90000, nodata_pixels=10000,
            quality_score=72.3, quality_flag="PASS",
        )
        metrics = meta.get_quality_by_scene(sample_scene)
        for m in metrics:
            assert 0 <= m["quality_score"] <= 100, (
                f"Quality score {m['quality_score']} out of range [0, 100]"
            )

    def test_quality_flag_values(self, db_client, meta, sample_scene):
        """quality_flag must be one of: PASS, FAIL, WARNING, UNCHECKED."""
        valid_flags = {"PASS", "FAIL", "WARNING", "UNCHECKED"}
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED", band_name="VH",
            file_path="/tmp/flag_test.tif", file_name="flag_test.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("FLAG_TEST"),
        )
        meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VH",
            total_pixels=100000, valid_pixels=70000, nodata_pixels=30000,
            quality_score=45.0, quality_flag="FAIL",
        )
        metrics = meta.get_quality_by_scene(sample_scene)
        for m in metrics:
            assert m["quality_flag"] in valid_flags, (
                f"Invalid quality_flag: {m['quality_flag']}"
            )

    def test_nodata_percent_consistency(self, db_client, meta, sample_scene):
        """nodata_pixels must not exceed total_pixels."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED", band_name="VV",
            file_path="/tmp/nodata_test.tif", file_name="nodata_test.tif",
            file_size_mb=38.0, data_hash_sha256=fake_hash("NODATA_TEST"),
        )
        with pytest.raises(Exception):
            # nodata_pixels > total_pixels should violate business logic
            meta.insert_quality_metrics(
                scene_id=sample_scene, product_id=prod_id, band_name="VV",
                total_pixels=100,
                valid_pixels=-50,  # invalid
                nodata_pixels=200, # > total
                quality_score=150, # > 100: violates CHECK constraint
                quality_flag="FAIL",
            )


# ---------------------------------------------------------------------------
# 4. LINEAGE TRACKING TESTS
# ---------------------------------------------------------------------------

class TestLineageTracking:
    """Verify parent-child lineage recording and traversal."""

    def test_every_transform_type_resolves_to_a_seeded_stage(self, db_client):
        """Regression: GOLD_EXPORT was added to processing_stages by migration
        013 but never to _TRANSFORM_STAGE_MAP, so every SILVER->GOLD lineage
        write raised "Unknown transformation_type" and failed the GOLD stage
        after the COGs had already been written to disk."""
        from sqlalchemy import select

        from etl.database_client import ProcessingStage
        from etl.lineage_tracker import LineageTracker

        assert "GOLD_EXPORT" in LineageTracker._TRANSFORM_STAGE_MAP

        with db_client.session() as sess:
            seeded = set(sess.scalars(select(ProcessingStage.stage_name)).all())

        unresolved = {
            t: stage
            for t, stage in LineageTracker._TRANSFORM_STAGE_MAP.items()
            if stage not in seeded
        }
        assert not unresolved, f"transform types with no processing_stages row: {unresolved}"

    def test_silver_to_gold_export_lineage_is_writable(self, db_client, lineage, meta, sample_scene):
        """The exact write that failed the GOLD stage in production: a
        SILVER -> GOLD link tagged GOLD_EXPORT, for each of the three sources
        the GOLD tier now holds (Sentinel-1, MODIS, GPM)."""
        from sqlalchemy import select

        from etl.database_client import ProcessingStage

        lee_job  = meta.insert_processing_job(sample_scene, "LEE_FILTER")
        gold_job = meta.insert_processing_job(sample_scene, "GOLD_EXPORT")
        with db_client.session() as sess:
            gold_stage_id = sess.scalar(
                select(ProcessingStage.stage_id).where(
                    ProcessingStage.stage_name == "GOLD_EXPORT"
                )
            )

        for source, product_type, band in (
            ("SENTINEL1", "LEE_FILTERED", "VV"),
            ("MODIS",     "MODIS_SILVER", "NDVI"),
            ("GPM",       "GPM_SILVER",   "PRECIP"),
        ):
            silver_id = meta.insert_data_product(
                scene_id=sample_scene, job_id=lee_job,
                product_tier="SILVER", source=source, product_type=product_type, band_name=band,
                file_path=f"/tmp/silver_{source}.tif", file_name=f"silver_{source}.tif",
                file_size_mb=48.0, data_hash_sha256=fake_hash(f"GE_SILVER_{source}"),
            )
            gold_id = meta.insert_data_product(
                scene_id=sample_scene, job_id=gold_job,
                product_tier="GOLD", source=source, product_type="COG", band_name=band,
                file_path=f"/tmp/gold_{source}.tif", file_name=f"gold_{source}.tif",
                file_size_mb=32.0, data_hash_sha256=fake_hash(f"GE_GOLD_{source}"),
            )

            lineage_id = lineage.record_transformation(
                silver_id, gold_id, "GOLD_EXPORT", gold_job, {"source": source.lower()},
            )
            assert lineage_id is not None

            chain = lineage.get_lineage_chain(gold_id, direction="ancestors")
            assert [step["transformation_type"] for step in chain] == ["GOLD_EXPORT"]
            assert chain[0]["stage_id"] == gold_stage_id

    def test_lineage_chain_recorded(self, db_client, lineage, meta, sample_scene):
        """Full pipeline lineage (RAW→BRONZE→SILVER→GOLD) must be queryable."""
        dl_job     = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        crop_job   = meta.insert_processing_job(sample_scene, "CROP")
        lee_job    = meta.insert_processing_job(sample_scene, "LEE_FILTER")
        fusion_job = meta.insert_processing_job(sample_scene, "FUSION")

        raw_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=dl_job,
            product_tier="RAW", source="SENTINEL1",    product_type="ORIGINAL_TIFF", band_name="VV",
            file_path="/tmp/raw.tif", file_name="raw.tif",
            file_size_mb=400.0, data_hash_sha256=fake_hash("LIN_RAW"),
        )
        bronze_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=crop_job,
            product_tier="BRONZE", source="SENTINEL1", product_type="CROPPED_TIFF", band_name="VV",
            file_path="/tmp/bronze.tif", file_name="bronze.tif",
            file_size_mb=48.0, data_hash_sha256=fake_hash("LIN_BRONZE"),
        )
        silver_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=lee_job,
            product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED", band_name="VV",
            file_path="/tmp/silver.tif", file_name="silver.tif",
            file_size_mb=45.0, data_hash_sha256=fake_hash("LIN_SILVER"),
        )
        gold_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=fusion_job,
            product_tier="FUSION", source="FUSION",   product_type="FUSION_H5", band_name="FUSION",
            file_path="/tmp/gold.h5", file_name="gold.h5",
            file_size_mb=41.0, data_hash_sha256=fake_hash("LIN_GOLD"), file_format="HDF5",
        )

        lineage.record_transformation(raw_id,    bronze_id, "CROP",       crop_job)
        lineage.record_transformation(bronze_id, silver_id, "LEE_FILTER", lee_job)
        lineage.record_transformation(silver_id, gold_id,   "FUSION",     fusion_job)

        # Trace ancestors of GOLD → should find 3 steps
        chain = lineage.get_lineage_chain(gold_id, direction="ancestors")
        assert len(chain) == 3, f"Expected 3 lineage steps, got {len(chain)}"

        transform_types = {step["transformation_type"] for step in chain}
        assert "CROP"       in transform_types
        assert "LEE_FILTER" in transform_types
        assert "FUSION"     in transform_types

    def test_descendants_chain(self, db_client, lineage, meta, sample_scene):
        """Descendants traversal from RAW should reach GOLD."""
        dl_job  = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        cr_job  = meta.insert_processing_job(sample_scene, "CROP")

        raw_id    = meta.insert_data_product(
            scene_id=sample_scene, job_id=dl_job,
            product_tier="RAW", source="SENTINEL1", product_type="ORIGINAL_TIFF", band_name="VH",
            file_path="/tmp/raw_desc.tif", file_name="raw_desc.tif",
            file_size_mb=400.0, data_hash_sha256=fake_hash("DESC_RAW"),
        )
        bronze_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=cr_job,
            product_tier="BRONZE", source="SENTINEL1", product_type="CROPPED_TIFF", band_name="VH",
            file_path="/tmp/brnz_desc.tif", file_name="brnz_desc.tif",
            file_size_mb=48.0, data_hash_sha256=fake_hash("DESC_BRONZE"),
        )
        lineage.record_transformation(raw_id, bronze_id, "CROP", cr_job)

        chain = lineage.get_lineage_chain(raw_id, direction="descendants")
        assert len(chain) >= 1
        assert any(s["child_product_id"] == bronze_id for s in chain)


# ---------------------------------------------------------------------------
# 5. CHECKPOINT / RESUME TESTS
# ---------------------------------------------------------------------------

class TestCheckpointSystem:
    """Verify PostgreSQL-backed checkpoint and pipeline resume logic.

    Catatan arsitektur: kelas ``PipelineOrchestrator`` (dengan
    ``get_completed_stages`` / ``is_stage_complete``) dibuang di commit fb04dad
    saat orchestrator ditulis ulang jadi ``run_dataset_job``. Checkpoint per-scene
    sekarang dibaca lewat ``MetadataManager.get_pipeline_status``, sedangkan titik
    resume per dataset-job disimpan di tabel ``scene_job_state``. Tes di bawah
    menguji perilaku yang sama terhadap dua mekanisme yang benar-benar dipakai.
    """

    @staticmethod
    def _completed_stages(meta, scene_id: int) -> list[str]:
        """Stage yang SUCCESS untuk sebuah scene — pengganti get_completed_stages."""
        return [
            row["stage_name"]
            for row in meta.get_pipeline_status(scene_id)
            if row["status"] == "SUCCESS"
        ]

    def test_completed_stages_query(self, meta, sample_scene):
        """Stage yang sukses muncul di pipeline status, yang belum jalan tidak."""
        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        meta.start_job(job_id)
        meta.complete_job(job_id)

        completed = self._completed_stages(meta, sample_scene)
        assert "DOWNLOAD" in completed
        assert "CROP"     not in completed

    def test_is_stage_complete_true(self, meta, sample_scene):
        """Stage yang sudah sukses terdeteksi selesai; yang belum, tidak."""
        job_id = meta.insert_processing_job(sample_scene, "CROP")
        meta.start_job(job_id)
        meta.complete_job(job_id)

        completed = self._completed_stages(meta, sample_scene)
        assert "CROP" in completed
        assert "LEE_FILTER" not in completed

    def test_failed_job_not_in_completed(self, meta, sample_scene):
        """Job FAILED tidak boleh dihitung sebagai stage yang selesai."""
        from etl.database_client import JobStatusEnum

        job_id = meta.insert_processing_job(sample_scene, "LEE_FILTER")
        meta.start_job(job_id)
        meta.complete_job(job_id, status=JobStatusEnum.FAILED,
                          error_code="TIMEOUT", error_message="Lee filter timed out")

        completed = self._completed_stages(meta, sample_scene)
        assert "LEE_FILTER" not in completed
        # Job-nya tetap tercatat, hanya statusnya bukan SUCCESS.
        statuses = {r["stage_name"]: r["status"] for r in meta.get_pipeline_status(sample_scene)}
        assert statuses["LEE_FILTER"] == "FAILED"

    def test_scene_job_state_checkpoint_roundtrip(self, db_client, sample_dataset):
        """Titik resume per dataset-job tersimpan dan terbaca kembali."""
        from etl.database_client import DatasetJob
        from etl.dataset_manager import DatasetManager

        with db_client.session() as sess:
            job = DatasetJob(dataset_id=sample_dataset, job_type="CREATE", status="QUEUED")
            sess.add(job)
            sess.flush()
            job_id = job.job_id

        dsmgr = DatasetManager(db_client)
        pid = "TEST_SCENE_CHECKPOINT"
        dsmgr.upsert_scene_job_state(job_id, pid, current_stage="CROP", stage_status="COMPLETED")
        state = dsmgr.get_scene_job_state(job_id, pid)
        assert state is not None
        assert state["current_stage"] == "CROP"
        assert state["stage_status"] == "COMPLETED"

        # Upsert kedua harus memperbarui baris yang sama, bukan menambah baris baru.
        dsmgr.upsert_scene_job_state(job_id, pid, current_stage="FUSION", stage_status="RUNNING")
        state = dsmgr.get_scene_job_state(job_id, pid)
        assert state["current_stage"] == "FUSION"
        assert state["stage_status"] == "RUNNING"
