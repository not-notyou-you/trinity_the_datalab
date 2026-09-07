# tests/test_quality_metrics.py
"""
Quality metrics unit tests: score computation, flag logic, and SHA-256 integrity.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/test_quality_metrics.py -v
"""

from __future__ import annotations

import hashlib
import os
import tempfile

import pytest


# ---------------------------------------------------------------------------
# 1. QUALITY SCORE COMPUTATION
# ---------------------------------------------------------------------------

class TestQualityScoreComputation:
    """Test the composite quality score formula from module6_analytics."""

    def test_perfect_quality_score(self):
        """0% nodata, low speckle, radiometric OK → score close to 100."""
        from etl.module6_analytics import compute_quality_score
        score = compute_quality_score(
            nodata_percent=0.0,
            speckle_index=0.0,
            radiometric_ok=True,
        )
        assert score == 100.0

    def test_zero_quality_score(self):
        """100% nodata, high speckle, radiometric fail → score = 0."""
        from etl.module6_analytics import compute_quality_score
        score = compute_quality_score(
            nodata_percent=100.0,
            speckle_index=5.0,
            radiometric_ok=False,
        )
        assert score == 0.0

    def test_typical_good_quality(self):
        """Typical good scene: 2% nodata, speckle=0.2, radiometric OK → ~83+."""
        from etl.module6_analytics import compute_quality_score
        score = compute_quality_score(
            nodata_percent=2.0,
            speckle_index=0.2,
            radiometric_ok=True,
        )
        assert score >= 60.0, f"Expected score ≥ 60, got {score}"

    def test_quality_score_always_in_range(self):
        """Quality score must always be clamped to [0, 100]."""
        from etl.module6_analytics import compute_quality_score
        test_cases = [
            (0.0,   0.0,  True),
            (50.0,  0.5,  True),
            (100.0, 10.0, False),
            (0.0,   1.0,  False),
            (30.0,  0.3,  True),
        ]
        for nodata, speckle, radio_ok in test_cases:
            score = compute_quality_score(nodata, speckle, radio_ok)
            assert 0.0 <= score <= 100.0, (
                f"Score {score} out of range for "
                f"nodata={nodata} speckle={speckle} radio={radio_ok}"
            )

    def test_radiometric_fail_reduces_score(self):
        """Radiometric failure must reduce score by exactly 20 points."""
        from etl.module6_analytics import compute_quality_score
        score_ok   = compute_quality_score(5.0, 0.1, radiometric_ok=True)
        score_fail = compute_quality_score(5.0, 0.1, radiometric_ok=False)
        assert abs(score_ok - score_fail - 20.0) < 0.01, (
            f"Radiometric component should be exactly 20. "
            f"Got diff={score_ok - score_fail}"
        )

    def test_nodata_penalty_scaling(self):
        """Higher nodata% → lower score (monotonic relationship)."""
        from etl.module6_analytics import compute_quality_score
        scores = [
            compute_quality_score(nd, 0.2, True)
            for nd in [0, 10, 20, 30, 50, 80, 100]
        ]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], (
                f"Score not monotonically decreasing: {scores}"
            )


# ---------------------------------------------------------------------------
# 2. QUALITY FLAG LOGIC
# ---------------------------------------------------------------------------

class TestQualityFlagLogic:
    """Test quality flag assignment based on score threshold."""

    @pytest.mark.parametrize("score,expected_flag", [
        (85.0, "PASS"),
        (60.0, "PASS"),   # exactly at threshold
        (59.9, "FAIL"),
        (0.0,  "FAIL"),
        (100.0, "PASS"),
    ])
    def test_flag_from_score_threshold(self, db_client, meta, sample_scene, score, expected_flag):
        """Scores >= 60 → PASS, below 60 → FAIL."""
        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", source="SENTINEL1", product_type="COG",
            band_name="VV",
            file_path=f"/tmp/flag_score_{score}.tif",
            file_name=f"flag_{score}.tif",
            file_size_mb=38.0,
            data_hash_sha256=hashlib.sha256(f"FLAG_{score}".encode()).hexdigest(),
        )
        metric_id = meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VV",
            total_pixels=1000000, valid_pixels=900000, nodata_pixels=100000,
            quality_score=score,
            quality_flag=expected_flag,
        )
        assert metric_id > 0

        metrics = meta.get_quality_by_scene(sample_scene)
        inserted = next((m for m in metrics if m["quality_score"] == score), None)
        if inserted:
            assert inserted["quality_flag"] == expected_flag


# ---------------------------------------------------------------------------
# 3. SHA-256 INTEGRITY
# ---------------------------------------------------------------------------

class TestSHA256Integrity:
    """Test SHA-256 hash computation and file integrity verification."""

    def test_sha256_deterministic(self):
        """Same content → same hash (deterministic)."""
        from etl.lineage_tracker import LineageTracker
        content = b"sentinel1_test_content_12345"
        h1 = LineageTracker.compute_sha256_from_bytes(content)
        h2 = LineageTracker.compute_sha256_from_bytes(content)
        assert h1 == h2

    def test_sha256_different_content(self):
        """Different content → different hash."""
        from etl.lineage_tracker import LineageTracker
        h1 = LineageTracker.compute_sha256_from_bytes(b"content_a")
        h2 = LineageTracker.compute_sha256_from_bytes(b"content_b")
        assert h1 != h2

    def test_sha256_hex_length(self):
        """SHA-256 hex digest must be exactly 64 characters."""
        from etl.lineage_tracker import LineageTracker
        h = LineageTracker.compute_sha256_from_bytes(b"test")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_sha256_file_computation(self, tmp_path):
        """Compute SHA-256 of a real temp file."""
        from etl.lineage_tracker import LineageTracker
        content   = b"This is test GeoTIFF content for Sentinel-1 unit test"
        test_file = tmp_path / "test.tif"
        test_file.write_bytes(content)

        computed  = LineageTracker.compute_sha256(str(test_file))
        expected  = hashlib.sha256(content).hexdigest()
        assert computed == expected

    def test_sha256_file_not_found(self):
        """Hashing a non-existent file raises FileNotFoundError."""
        from etl.lineage_tracker import LineageTracker
        with pytest.raises(FileNotFoundError):
            LineageTracker.compute_sha256("/nonexistent/path/file.tif")

    def test_integrity_verification_pass(self, db_client, meta, lineage, sample_scene, tmp_path):
        """verify_integrity returns integrity_ok=True when hash matches."""
        content = b"Mock COG content for integrity test"
        tif     = tmp_path / "integrity_test.tif"
        tif.write_bytes(content)

        correct_hash = hashlib.sha256(content).hexdigest()
        job_id  = meta.insert_processing_job(sample_scene, "COG_EXPORT")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", source="SENTINEL1", product_type="COG",
            band_name="VV", file_path=str(tif), file_name=tif.name,
            file_size_mb=0.001, data_hash_sha256=correct_hash,
        )

        result = lineage.verify_integrity(prod_id, str(tif))
        assert result["integrity_ok"] is True
        assert result["stored_hash"]  == result["computed_hash"]

    def test_integrity_verification_fail(self, db_client, meta, lineage, sample_scene, tmp_path):
        """verify_integrity returns integrity_ok=False when file is corrupted."""
        original  = b"Original content"
        corrupted = b"Corrupted content XYZ"
        tif       = tmp_path / "corrupted_test.tif"
        tif.write_bytes(corrupted)

        original_hash = hashlib.sha256(original).hexdigest()   # hash of original, not corrupted
        job_id  = meta.insert_processing_job(sample_scene, "COG_EXPORT")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", source="SENTINEL1", product_type="COG",
            band_name="VH", file_path=str(tif), file_name=tif.name,
            file_size_mb=0.001, data_hash_sha256=original_hash,
        )

        result = lineage.verify_integrity(prod_id, str(tif))
        assert result["integrity_ok"] is False
        assert result["stored_hash"] != result["computed_hash"]


# ---------------------------------------------------------------------------
# 4. ALERT AUTO-TRIGGER
# ---------------------------------------------------------------------------

class TestAlertAutoTrigger:
    """Verify FAIL quality score triggers an alert event automatically."""

    def test_quality_fail_creates_alert(self, db_client, meta, sample_scene):
        """quality_flag=FAIL must auto-create a quality_warning alert."""
        from sqlalchemy import text

        # Count alerts before
        with db_client.session() as sess:
            before = sess.scalar(text(
                "SELECT COUNT(*) FROM alert_events WHERE scene_id = :sid AND event_type = 'QUALITY_WARNING'",
            ), {"sid": sample_scene})

        job_id  = meta.insert_processing_job(sample_scene, "QUALITY_ANALYTICS")
        prod_id = meta.insert_data_product(
            scene_id=sample_scene, job_id=job_id,
            product_tier="GOLD", source="SENTINEL1", product_type="COG", band_name="VV",
            file_path="/tmp/fail_alert.tif", file_name="fail_alert.tif",
            file_size_mb=38.0,
            data_hash_sha256=hashlib.sha256(b"FAIL_ALERT").hexdigest(),
        )
        meta.insert_quality_metrics(
            scene_id=sample_scene, product_id=prod_id, band_name="VV",
            total_pixels=1000000, valid_pixels=500000, nodata_pixels=500000,
            quality_score=35.0, quality_flag="FAIL",
        )

        with db_client.session() as sess:
            after = sess.scalar(text(
                "SELECT COUNT(*) FROM alert_events WHERE scene_id = :sid AND event_type = 'QUALITY_WARNING'",
            ), {"sid": sample_scene})

        assert after > before, "Expected a QUALITY_WARNING alert to be auto-created on FAIL"
