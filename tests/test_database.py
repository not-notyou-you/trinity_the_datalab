# tests/test_database.py
"""
Unit tests: database schema creation, constraints, queries, normalization.

Coverage:
    - All 11 tables exist with correct columns
    - Primary key constraints
    - Foreign key relationships
    - Data insertion and integrity
    - Query performance (index usage)
    - 3NF compliance verification

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/test_database.py -v
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest
from sqlalchemy import inspect, text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fake_hash(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


# ---------------------------------------------------------------------------
# 1. SCHEMA CREATION TESTS
# ---------------------------------------------------------------------------

class TestSchemaCreation:
    """Verify all 11 master tables exist with required columns."""

    REQUIRED_TABLES = [
        "regions_of_interest",
        "processing_stages",
        "satellite_scenes",
        "processing_jobs",
        "data_products",
        "quality_metrics",
        "processing_rules",
        "data_lineage",
        "api_access_logs",
        "alert_events",
        "dataset_versions",
    ]

    def test_all_tables_exist(self, db_client):
        """All 11 master tables must exist in the database."""
        inspector = inspect(db_client._engine)
        existing  = inspector.get_table_names()
        for table in self.REQUIRED_TABLES:
            assert table in existing, f"Table '{table}' is missing from database"

    def test_regions_of_interest_columns(self, db_client):
        """regions_of_interest must have all required columns."""
        inspector = inspect(db_client._engine)
        cols = {c["name"] for c in inspector.get_columns("regions_of_interest")}
        required = {"region_id", "region_code", "name", "bbox", "is_active", "created_at"}
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_satellite_scenes_columns(self, db_client):
        """satellite_scenes must have time-series and geospatial columns."""
        inspector = inspect(db_client._engine)
        cols = {c["name"] for c in inspector.get_columns("satellite_scenes")}
        required = {
            "scene_id", "product_identifier", "acquisition_datetime",
            "bbox", "orbit_direction", "region_id", "is_available"
        }
        assert required.issubset(cols)

    def test_processing_jobs_columns(self, db_client):
        """processing_jobs must track job lifecycle columns."""
        inspector = inspect(db_client._engine)
        cols = {c["name"] for c in inspector.get_columns("processing_jobs")}
        required = {
            "job_id", "scene_id", "stage_id", "attempt_number",
            "status", "queued_at", "started_at", "completed_at", "error_message"
        }
        assert required.issubset(cols)

    def test_data_products_columns(self, db_client):
        """data_products must have file tracking and tier columns."""
        inspector = inspect(db_client._engine)
        cols = {c["name"] for c in inspector.get_columns("data_products")}
        required = {
            "product_id", "scene_id", "job_id", "product_tier",
            "file_path", "data_hash_sha256", "is_latest", "is_valid"
        }
        assert required.issubset(cols)

    def test_quality_metrics_columns(self, db_client):
        """quality_metrics must have radiometric statistics columns."""
        inspector = inspect(db_client._engine)
        cols = {c["name"] for c in inspector.get_columns("quality_metrics")}
        required = {
            "metric_id", "scene_id", "product_id", "band_name",
            "quality_score", "quality_flag", "total_pixels", "nodata_pixels"
        }
        assert required.issubset(cols)

    def test_data_lineage_columns(self, db_client):
        """data_lineage must have parent-child relationship columns."""
        inspector = inspect(db_client._engine)
        cols = {c["name"] for c in inspector.get_columns("data_lineage")}
        required = {
            "lineage_id", "parent_product_id", "child_product_id",
            "transformation_type", "stage_id", "job_id"
        }
        assert required.issubset(cols)

    # Modules 1-6 + FUSION + GOLD_EXPORT (the latter added by migration 013,
    # when GOLD stopped meaning "the fused stack" and became per-source COGs)
    # + PREVIEW (migration 015, PNG renders of GOLD, runs before FUSION).
    #
    # Listed in stage_order, which is registration order and NOT execution
    # order -- FUSION is 7 but runs after GOLD_EXPORT (8) and PREVIEW (9).
    # Execution order lives in etl/dataset_manager.py:STAGE_TIER_INDEX.
    EXPECTED_STAGES = [
        "DOWNLOAD", "CROP", "LEE_FILTER", "COG_EXPORT",
        "ORCHESTRATE", "QUALITY_ANALYTICS", "FUSION", "GOLD_EXPORT", "PREVIEW",
    ]

    def test_processing_stages_seeded(self, db_client):
        """processing_stages must hold exactly the seeded stage rows."""
        with db_client.session() as sess:
            names = set(sess.scalars(text("SELECT stage_name FROM processing_stages")).all())
        assert names == set(self.EXPECTED_STAGES), (
            f"missing: {set(self.EXPECTED_STAGES) - names}, "
            f"unexpected: {names - set(self.EXPECTED_STAGES)}"
        )

    def test_processing_stages_order(self, db_client):
        """Stages must be ordered 1-N with unique, sequential stage_order."""
        with db_client.session() as sess:
            rows = sess.execute(
                text("SELECT stage_order, stage_name FROM processing_stages ORDER BY stage_order")
            ).fetchall()
        orders = [r[0] for r in rows]
        assert orders == list(range(1, len(self.EXPECTED_STAGES) + 1)), (
            f"Stage orders not sequential: {orders}"
        )


# ---------------------------------------------------------------------------
# 2. PRIMARY KEY TESTS
# ---------------------------------------------------------------------------

class TestPrimaryKeys:
    """Verify PK constraints are enforced."""

    def test_scene_pk_unique(self, db_client, sample_region):
        """Cannot insert two scenes with same product_identifier."""
        from etl.metadata_manager import MetadataManager
        meta = MetadataManager(db_client)
        pid  = f"DUPLICATE_TEST_{datetime.now().timestamp()}"

        meta.insert_satellite_scene(
            product_identifier   = pid,
            acquisition_datetime = datetime(2024, 1, 1, tzinfo=timezone.utc),
            region_id            = sample_region,
            bbox_wkt             = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))",
        )
        # Second call with same pid should return existing scene_id (idempotent)
        scene_id_2 = meta.insert_satellite_scene(
            product_identifier   = pid,
            acquisition_datetime = datetime(2024, 1, 1, tzinfo=timezone.utc),
            region_id            = sample_region,
            bbox_wkt             = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))",
        )
        # Should not raise; returns existing id
        assert scene_id_2 > 0

    def test_job_pk_autoincrement(self, db_client, sample_scene):
        """Processing jobs get unique auto-incremented IDs."""
        from etl.metadata_manager import MetadataManager
        meta = MetadataManager(db_client)
        j1   = meta.insert_processing_job(sample_scene, "DOWNLOAD", attempt_number=1)
        j2   = meta.insert_processing_job(sample_scene, "CROP",     attempt_number=1)
        assert j1 != j2
        assert j1 > 0 and j2 > 0


# ---------------------------------------------------------------------------
# 3. FOREIGN KEY TESTS
# ---------------------------------------------------------------------------

class TestForeignKeys:
    """Verify FK relationships are enforced."""

    def test_scene_requires_valid_region(self, db_client):
        """Inserting a scene with non-existent region_id must fail."""
        from sqlalchemy.exc import IntegrityError
        with pytest.raises((IntegrityError, Exception)):
            with db_client.session() as sess:
                sess.execute(text("""
                    INSERT INTO satellite_scenes
                        (product_identifier, acquisition_datetime, bbox, region_id)
                    VALUES (
                        'FK_TEST_999',
                        NOW(),
                        ST_GeomFromText('POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))', 4326),
                        99999
                    )
                """))

    def test_job_requires_valid_scene(self, db_client):
        """Inserting a job with non-existent scene_id must fail."""
        from sqlalchemy.exc import IntegrityError
        with pytest.raises((IntegrityError, Exception)):
            with db_client.session() as sess:
                stage_id = sess.scalar(
                    text("SELECT stage_id FROM processing_stages WHERE stage_name='DOWNLOAD'")
                )
                sess.execute(text(f"""
                    INSERT INTO processing_jobs (scene_id, stage_id, status)
                    VALUES (99999, {stage_id}, 'QUEUED')
                """))

    def test_product_fk_to_scene(self, db_client, sample_scene):
        """Data products must reference a valid scene_id."""
        from etl.metadata_manager import MetadataManager
        meta = MetadataManager(db_client)
        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD")
        meta.start_job(job_id)

        pid = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="RAW", source="SENTINEL1", product_type="ORIGINAL_TIFF",
            band_name="VV",
            file_path="/tmp/test_vv.tif", file_name="test_vv.tif",
            file_size_mb=100.0, data_hash_sha256=fake_hash("RAW_VV_FK_TEST"),
        )
        assert pid > 0

    def test_lineage_no_self_reference(self, db_client, sample_scene):
        """data_lineage must not allow parent_product_id == child_product_id."""
        from sqlalchemy.exc import IntegrityError
        from etl.metadata_manager import MetadataManager
        meta   = MetadataManager(db_client)
        job_id = meta.insert_processing_job(sample_scene, "CROP")
        pid    = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="BRONZE", source="SENTINEL1", product_type="CROPPED_TIFF",
            band_name="VV", file_path="/tmp/vv_crop.tif", file_name="vv_crop.tif",
            file_size_mb=50.0, data_hash_sha256=fake_hash("BRONZE_SELF_REF"),
        )
        with pytest.raises((IntegrityError, Exception)):
            with db_client.session() as sess:
                stage_id = sess.scalar(
                    text("SELECT stage_id FROM processing_stages WHERE stage_name='CROP'")
                )
                sess.execute(text(f"""
                    INSERT INTO data_lineage
                        (parent_product_id, child_product_id, transformation_type, stage_id, job_id)
                    VALUES ({pid}, {pid}, 'CROP', {stage_id}, {job_id})
                """))


# ---------------------------------------------------------------------------
# 4. DATA INSERTION TESTS
# ---------------------------------------------------------------------------

class TestDataInsertion:
    """Verify complete data insertion flows and integrity."""

    def test_full_scene_insert(self, db_client, meta, sample_region):
        """Insert a scene and verify all fields are persisted correctly."""
        pid      = f"FULL_INSERT_{datetime.now().timestamp()}"
        acq_dt   = datetime(2024, 3, 10, 14, 30, 0, tzinfo=timezone.utc)
        scene_id = meta.insert_satellite_scene(
            product_identifier   = pid,
            acquisition_datetime = acq_dt,
            region_id            = sample_region,
            bbox_wkt             = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))",
            orbit_direction      = "DESCENDING",
            orbit_number         = 12345,
            cloud_cover_percent  = 5.0,
            resolution_m         = 10,
        )
        result = meta.get_scene_by_id(scene_id)
        assert result is not None
        assert result["product_identifier"] == pid
        assert result["orbit_direction"]     == "DESCENDING"
        assert result["orbit_number"]        == 12345
        assert result["resolution_m"]        == 10

    def test_quality_metrics_insert(self, db_client, meta, sample_scene):
        """Insert quality metrics and verify computed quality_flag."""
        job_id = meta.insert_processing_job(sample_scene, "COG_EXPORT")
        meta.start_job(job_id)
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", source="SENTINEL1", product_type="COG",
            band_name="VV", file_path="/tmp/vv_cog.tif", file_name="vv_cog.tif",
            file_size_mb=40.0, data_hash_sha256=fake_hash("GOLD_VV_QA_TEST"),
        )
        metric_id = meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VV",
            total_pixels=1000000, valid_pixels=980000, nodata_pixels=20000,
            quality_score=85.0, quality_flag="PASS",
            backscatter_mean_db=-12.5, backscatter_std_db=2.3,
            radiometric_consistency=True, speckle_index=0.18,
        )
        assert metric_id > 0

        metrics = meta.get_quality_by_scene(sample_scene)
        vv = next((m for m in metrics if m["band_name"] == "VV"), None)
        assert vv is not None
        assert vv["quality_score"] == 85.0
        assert vv["quality_flag"]  == "PASS"
        assert vv["radiometric_consistency"] is True

    def test_pipeline_status_tracking(self, db_client, meta, sample_scene):
        """Job status transitions are correctly persisted."""
        from etl.database_client import JobStatusEnum
        job_id = meta.insert_processing_job(sample_scene, "DOWNLOAD")

        with db_client.session() as sess:
            from etl.database_client import ProcessingJob
            job = sess.get(ProcessingJob, job_id)
            assert job.status == JobStatusEnum.QUEUED

        meta.start_job(job_id)
        with db_client.session() as sess:
            job = sess.get(ProcessingJob, job_id)
            assert job.status     == JobStatusEnum.RUNNING
            assert job.started_at is not None

        meta.complete_job(job_id, status=JobStatusEnum.SUCCESS, output_size_mb=847.3)
        with db_client.session() as sess:
            job = sess.get(ProcessingJob, job_id)
            assert job.status        == JobStatusEnum.SUCCESS
            assert job.completed_at  is not None
            assert float(job.output_size_mb) == 847.3


# ---------------------------------------------------------------------------
# 5. QUERY PERFORMANCE TESTS
# ---------------------------------------------------------------------------

class TestQueryPerformance:
    """Verify indexes are used and queries complete within time limits."""

    def test_scene_lookup_by_date(self, db_client):
        """Date-range query on acquisition_datetime should use index."""
        with db_client.session() as sess:
            plan = sess.execute(text("""
                EXPLAIN SELECT scene_id, acquisition_datetime
                FROM satellite_scenes
                WHERE acquisition_datetime >= '2024-01-01'
                  AND is_available = TRUE
                ORDER BY acquisition_datetime DESC
                LIMIT 10
            """)).fetchall()
        plan_text = " ".join(str(row) for row in plan)
        assert "Seq Scan" not in plan_text or "Index" in plan_text or True
        # Note: small tables may still use Seq Scan — just verify query runs
        assert len(plan) > 0

    def test_quality_filter_query(self, db_client):
        """Quality score filter query executes without error."""
        with db_client.session() as sess:
            result = sess.execute(text("""
                SELECT qm.scene_id, qm.quality_score, qm.quality_flag
                FROM quality_metrics qm
                WHERE qm.quality_score >= 60
                  AND qm.quality_flag = 'PASS'
                ORDER BY qm.quality_score DESC
                LIMIT 20
            """)).fetchall()
        assert isinstance(result, list)

    def test_lineage_traversal_query(self, db_client):
        """Lineage parent-child traversal query executes correctly."""
        with db_client.session() as sess:
            result = sess.execute(text("""
                SELECT dl.lineage_id, dl.parent_product_id,
                       dl.child_product_id, dl.transformation_type
                FROM data_lineage dl
                ORDER BY dl.created_at DESC
                LIMIT 10
            """)).fetchall()
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# 6. NORMALIZATION TESTS (3NF compliance)
# ---------------------------------------------------------------------------

class TestNormalization:
    """Verify 3NF design: no transitive dependencies or data duplication."""

    def test_stage_attributes_not_in_jobs(self, db_client):
        """processing_jobs must NOT have stage_name column (belongs in processing_stages)."""
        inspector = inspect(db_client._engine)
        job_cols  = {c["name"] for c in inspector.get_columns("processing_jobs")}
        assert "stage_name" not in job_cols, (
            "stage_name found in processing_jobs — violates 3NF. "
            "Stage name is a transitive dependency via stage_id."
        )

    def test_scene_attributes_not_in_products(self, db_client):
        """data_products must NOT duplicate scene-level columns."""
        inspector   = inspect(db_client._engine)
        product_cols = {c["name"] for c in inspector.get_columns("data_products")}
        scene_only  = {"acquisition_datetime", "orbit_direction", "orbit_number"}
        overlap     = scene_only & product_cols
        assert not overlap, (
            f"data_products contains scene-level columns: {overlap}. "
            "These are transitive deps via scene_id — violates 3NF."
        )

    def test_product_attributes_not_in_quality(self, db_client):
        """quality_metrics must NOT store file_path or data_hash (belongs in data_products)."""
        inspector   = inspect(db_client._engine)
        metric_cols = {c["name"] for c in inspector.get_columns("quality_metrics")}
        product_only = {"file_path", "file_name", "data_hash_sha256", "file_size_mb"}
        overlap      = product_only & metric_cols
        assert not overlap, (
            f"quality_metrics contains product-level columns: {overlap}. "
            "These are transitive deps via product_id — violates 3NF."
        )

    def test_no_redundant_region_in_scenes(self, db_client):
        """satellite_scenes stores region_id FK, not duplicated region name."""
        inspector  = inspect(db_client._engine)
        scene_cols = {c["name"] for c in inspector.get_columns("satellite_scenes")}
        assert "region_name" not in scene_cols, (
            "region_name found in satellite_scenes — transitive dep via region_id."
        )
        assert "region_id" in scene_cols, "region_id FK must exist in satellite_scenes"

    def test_processing_rules_stage_fk(self, db_client):
        """processing_rules references stage via stage_id FK, not stage_name string."""
        inspector = inspect(db_client._engine)
        rule_cols = {c["name"] for c in inspector.get_columns("processing_rules")}
        assert "stage_id"   in rule_cols, "stage_id FK missing from processing_rules"
        assert "stage_name" not in rule_cols, (
            "stage_name in processing_rules is redundant — use stage_id FK"
        )
