# tests/conftest.py
"""
pytest fixtures and test database setup/teardown.
Used by all test modules in the tests/ directory.

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/ -v --cov=etl --cov=api
"""

from __future__ import annotations

import os
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from dotenv import load_dotenv

load_dotenv()


def _resolve_test_db_url() -> str:
    """URL database uji.

    Urutan: TEST_DATABASE_URL kalau di-set; kalau tidak, turunkan dari
    DATABASE_URL di .env dengan menambah akhiran `_test` pada nama database.
    Sebelumnya nilai default-nya dihardcode dengan password 'postgres', sehingga
    di mesin dengan kredensial lain seluruh test error saat setup.
    """
    explicit = os.getenv("TEST_DATABASE_URL")
    if explicit:
        return explicit

    main_url = os.getenv("DATABASE_URL")
    if not main_url:
        return "postgresql+psycopg2://postgres:postgres@localhost:5432/sentinel1_flood_test"

    base, _, dbname = main_url.rpartition("/")
    dbname, sep, query = dbname.partition("?")
    return f"{base}/{dbname}_test{sep}{query}"


TEST_DB_URL = _resolve_test_db_url()


def _guard_not_production(url: str) -> None:
    """Cegah test menghapus database produksi.

    Fixture db_client menjalankan Base.metadata.drop_all() saat teardown, jadi
    kalau URL uji tidak sengaja menunjuk database utama, seluruh tabel produksi
    ikut terhapus. Lebih baik gagal keras di awal.
    """
    main_url = os.getenv("DATABASE_URL")
    if main_url and url.rstrip("/") == main_url.rstrip("/"):
        raise RuntimeError(
            "Test database URL sama dengan DATABASE_URL produksi. "
            "Test melakukan drop_all() saat teardown — batalkan. "
            "Set TEST_DATABASE_URL ke database terpisah."
        )
    if not url.rpartition("/")[2].partition("?")[0].endswith("_test"):
        raise RuntimeError(
            f"Nama database uji harus berakhiran '_test', dapat: {url.rpartition('/')[2]}"
        )


@pytest.fixture(scope="session")
def db_client():
    """
    Session-scoped DatabaseClient connected to the test database.
    Creates all tables on setup, drops them on teardown.
    """
    from sqlalchemy import text
    from etl.database_client import DatabaseClient, Base

    _guard_not_production(TEST_DB_URL)
    client = DatabaseClient(TEST_DB_URL, pool_size=2, max_overflow=2)

    # Tabel memakai kolom PostGIS, jadi ekstensinya harus ada sebelum create_all.
    # Di database uji yang baru dibuat ekstensi ini belum tentu terpasang.
    try:
        with client.session() as sess:
            sess.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    except Exception as exc:  # pragma: no cover - hanya jalur pesan error
        pytest.exit(
            f"Tidak bisa menyiapkan database uji {TEST_DB_URL!r}: {exc}\n"
            "Buat dulu databasenya, mis.:\n"
            '  psql -U postgres -c "CREATE DATABASE sentinel1_flood_test"',
            returncode=1,
        )

    client.create_tables()
    # create_tables() only runs SQLAlchemy DDL (Base.metadata.create_all) — it
    # doesn't run schema.sql's raw seed INSERTs, so processing_stages (needed
    # by MetadataManager.insert_processing_job's FK lookup) starts empty on a
    # fresh test DB. Seed the same stage rows schema.sql seeds in production.
    # Keep this list in sync with database/schema.sql — drift here is what let
    # the missing GOLD_EXPORT lineage mapping reach production untested.
    with client.session() as sess:
        sess.execute(text("""
            INSERT INTO processing_stages (stage_name, stage_code, stage_order, description, timeout_minutes, retry_count, retry_delay_sec, is_mandatory, is_active)
            VALUES
                ('DOWNLOAD',          'DL',  1, 'Sentinel-1 scene discovery and download from Copernicus Hub', 120, 3, 60, TRUE, TRUE),
                ('CROP',              'CR',  2, 'Spatial subsetting to Region of Interest bounding box',       30,  2, 30, TRUE, TRUE),
                ('LEE_FILTER',        'LF',  3, 'SAR speckle reduction using Lee adaptive filter',            45,  2, 30, TRUE, TRUE),
                ('COG_EXPORT',        'CE',  4, 'Cloud-Optimized GeoTIFF normalization and export',           30,  2, 30, TRUE, TRUE),
                ('ORCHESTRATE',       'OR',  5, 'Pipeline orchestration, checkpointing, and retry management', 10,  1, 10, TRUE, TRUE),
                ('QUALITY_ANALYTICS', 'QA',  6, 'Quality metrics computation and visualization',              30,  2, 30, TRUE, TRUE),
                ('FUSION',            'FS',  7, 'Multi-modal HDF5 feature stack fusion (Sentinel-1 + MODIS + GPM) for GOLD tier', 60, 2, 30, TRUE, TRUE),
                ('GOLD_EXPORT',       'GE',  8, 'Per-source Cloud-Optimized GeoTIFF export untuk tier GOLD',  45,  2, 30, TRUE, TRUE),
                ('PREVIEW',           'PV',  9, 'Render PNG preview (grayscale + colored) dari tier GOLD, sebelum FUSION', 15, 1, 15, FALSE, TRUE)
            ON CONFLICT (stage_name) DO NOTHING
        """))
    yield client
    # Teardown: drop all tables after full test session
    Base.metadata.drop_all(client._engine)
    client.dispose()


@pytest.fixture(scope="function")
def db_session(db_client):
    """
    Function-scoped database session. Rolls back after each test
    so tests are fully isolated.
    """
    with db_client._SessionFactory() as session:
        yield session
        session.rollback()


@pytest.fixture(scope="function")
def meta(db_client):
    """MetadataManager instance for tests."""
    from etl.metadata_manager import MetadataManager
    return MetadataManager(db_client)


@pytest.fixture(scope="function")
def lineage(db_client):
    """LineageTracker instance for tests."""
    from etl.lineage_tracker import LineageTracker
    return LineageTracker(db_client)


@pytest.fixture(scope="function")
def plog(db_client):
    """PipelineLogManager instance for tests."""
    from etl.pipeline_logger import PipelineLogManager
    return PipelineLogManager(db_client)


@pytest.fixture(scope="session")
def sample_region(db_client):
    """
    Insert a Jabodetabek ROI once per session and return its region_id.
    """
    from sqlalchemy import text
    with db_client.session() as sess:
        existing = sess.scalar(
            text("SELECT region_id FROM regions_of_interest WHERE region_code = 'JABODTK'")
        )
        if existing:
            return existing

        sess.execute(text("""
            INSERT INTO regions_of_interest
                (region_code, name, description, bbox, area_km2, admin_level, country_code, is_active)
            VALUES (
                'JABODTK', 'Jabodetabek', 'Test AOI',
                ST_GeomFromText('POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))', 4326),
                6392.0, 2, 'ID', TRUE
            )
        """))
        return sess.scalar(
            text("SELECT region_id FROM regions_of_interest WHERE region_code = 'JABODTK'")
        )


@pytest.fixture(scope="function")
def sample_scene(meta, sample_region) -> int:
    """Insert a single test scene and return its scene_id."""
    return meta.insert_satellite_scene(
        product_identifier   = f"TEST_SCENE_{datetime.now().timestamp()}",
        acquisition_datetime = datetime(2024, 1, 15, 22, 50, 0, tzinfo=timezone.utc),
        region_id            = sample_region,
        bbox_wkt             = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))",
        orbit_direction      = "ASCENDING",
        orbit_number         = 52186,
        cloud_cover_percent  = 12.5,
        resolution_m         = 10,
    )


@pytest.fixture(scope="function")
def sample_dataset(db_client, sample_region) -> int:
    """
    Insert a minimal Dataset row directly (bypassing DatasetManager.create_dataset,
    which spawns a background job runner thread) and return its dataset_id.
    """
    from etl.database_client import Dataset

    with db_client.session() as sess:
        ds = Dataset(
            name=f"TEST_DATASET_{datetime.now().timestamp()}",
            location_label="Test AOI",
            region_id=sample_region,
            bbox="SRID=4326;POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))",
            bbox_wkt="POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))",
            date_start=datetime(2024, 1, 1, tzinfo=timezone.utc).date(),
            date_end=datetime(2024, 1, 31, tzinfo=timezone.utc).date(),
            required_tiers=["RAW", "GOLD"],
            dataset_kind="STANDARD",
            status="DRAFT",
        )
        sess.add(ds)
        sess.flush()
        return ds.dataset_id


@pytest.fixture(scope="function")
def api_client(db_client):
    """
    FastAPI TestClient with DB dependency override.
    Allows testing API endpoints without running a real server.
    """
    from fastapi.testclient import TestClient
    from api.main import app, get_db

    app.dependency_overrides[get_db] = lambda: db_client
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()
