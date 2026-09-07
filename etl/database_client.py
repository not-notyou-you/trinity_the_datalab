# etl/database_client.py
from __future__ import annotations
import logging
import os
import time
from contextlib import contextmanager
from enum import Enum as PyEnum
from typing import Generator
from geoalchemy2 import Geometry
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Computed,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB, UUID
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)


class OrbitDirectionEnum(str, PyEnum):
    ASCENDING = "ASCENDING"
    DESCENDING = "DESCENDING"


class JobStatusEnum(str, PyEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ProductTierEnum(str, PyEnum):
    RAW = "RAW"
    BRONZE = "BRONZE"
    SILVER = "SILVER"
    GOLD = "GOLD"
    FUSION = "FUSION"


class ProductSourceEnum(str, PyEnum):
    """Sensor asal sebuah data_product. Dicerminkan oleh level {source}
    di path on-disk (etl/folder_manager.py) — kecuali FUSION, yang lintas
    source dan tinggal di tier fusion/ tanpa folder source sendiri.
    Disimpan sebagai VARCHAR + CHECK constraint, bukan ENUM Postgres:
    menambah source baru nanti cukup ALTER CONSTRAINT, tidak perlu
    ALTER TYPE yang tidak bisa jalan di dalam transaksi."""
    SENTINEL1 = "SENTINEL1"
    MODIS = "MODIS"
    GPM = "GPM"
    FUSION = "FUSION"


class StorageLocationEnum(str, PyEnum):
    LOCAL = "LOCAL"
    S3 = "S3"
    GCS = "GCS"
    AZURE_BLOB = "AZURE_BLOB"


class RuleTypeEnum(str, PyEnum):
    THRESHOLD = "THRESHOLD"
    TRANSFORMATION = "TRANSFORMATION"
    VALIDATION = "VALIDATION"
    FILTER = "FILTER"


class AlertSeverityEnum(str, PyEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertEventTypeEnum(str, PyEnum):
    DATA_ARRIVAL = "DATA_ARRIVAL"
    QUALITY_WARNING = "QUALITY_WARNING"
    PIPELINE_ERROR = "PIPELINE_ERROR"
    THRESHOLD_BREACH = "THRESHOLD_BREACH"
    SYSTEM_ALERT = "SYSTEM_ALERT"


class HttpMethodEnum(str, PyEnum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"


class DatasetKindEnum(str, PyEnum):
    STANDARD = "STANDARD"
    LIVE = "LIVE"


class DatasetStatusEnum(str, PyEnum):
    DRAFT = "DRAFT"
    QUEUED = "QUEUED"
    PREPARING = "PREPARING"
    DOWNLOADING = "DOWNLOADING"
    PROCESSING = "PROCESSING"
    PAUSED = "PAUSED"
    CLEANUP = "CLEANUP"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DELETING = "DELETING"
    DELETED = "DELETED"


class DatasetJobTypeEnum(str, PyEnum):
    CREATE = "CREATE"
    BACKFILL = "BACKFILL"
    LIVE_INGEST = "LIVE_INGEST"


class DatasetJobStatusEnum(str, PyEnum):
    QUEUED = "QUEUED"
    PREPARING = "PREPARING"
    DOWNLOADING = "DOWNLOADING"
    PROCESSING = "PROCESSING"
    PAUSED = "PAUSED"
    CLEANUP = "CLEANUP"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class SceneJobStageStatusEnum(str, PyEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class CleanupOperationTypeEnum(str, PyEnum):
    TIER_CLEANUP = "TIER_CLEANUP"
    FULL_DELETE = "FULL_DELETE"


class CleanupOperationStatusEnum(str, PyEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class LiveSourceNameEnum(str, PyEnum):
    SENTINEL1 = "SENTINEL1"
    MODIS = "MODIS"
    GPM = "GPM"


class Base(DeclarativeBase):
    pass


class RegionOfInterest(Base):
    __tablename__ = "regions_of_interest"
    region_id = Column(Integer, primary_key=True, autoincrement=True)
    region_code = Column(String(20), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    bbox = Column(Geometry("POLYGON", srid=4326), nullable=False)
    centroid = Column(Geometry("POINT", srid=4326))
    area_km2 = Column(Numeric(12, 4))
    admin_level = Column(SmallInteger, nullable=False, default=2)
    country_code = Column(String(2), nullable=False, default="ID")
    is_active = Column(Boolean, nullable=False, default=True)
    # SEEDER = bawaan sistem, USER = ditambah lewat UI, GEOCODE = auto dari Nominatim.
    # server_default wajib: tabel ini juga dibuat lewat Base.metadata.create_all()
    # (tes, instalasi baru), dan INSERT SQL mentah yang tidak menyebut kolom ini
    # akan kena NOT NULL kalau DDL-nya tidak ikut membawa DEFAULT.
    source = Column(String(20), nullable=False, default="USER", server_default=text("'USER'"))
    # Soft-delete: baris tidak pernah dihapus fisik karena scenes/datasets
    # mereferensikan region_id dengan ON DELETE RESTRICT.
    deleted_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    scenes = relationship("SatelliteScene", back_populates="region")
    datasets = relationship("Dataset", back_populates="region")

    def __repr__(self) -> str:
        return f"<RegionOfInterest id={self.region_id} code={self.region_code}>"


class ProcessingStage(Base):
    __tablename__ = "processing_stages"
    __table_args__ = (
        UniqueConstraint("stage_order", name="uq_stage_order"),
    )
    stage_id = Column(Integer, primary_key=True, autoincrement=True)
    stage_name = Column(String(50), nullable=False, unique=True)
    stage_code = Column(String(20), nullable=False, unique=True)
    stage_order = Column(SmallInteger, nullable=False)
    description = Column(Text)
    timeout_minutes = Column(SmallInteger, nullable=False, default=60)
    retry_count = Column(SmallInteger, nullable=False, default=3)
    retry_delay_sec = Column(SmallInteger, nullable=False, default=30)
    is_mandatory = Column(Boolean, nullable=False, default=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    jobs = relationship("ProcessingJob", back_populates="stage")
    rules = relationship("ProcessingRule", back_populates="stage")

    def __repr__(self) -> str:
        return f"<ProcessingStage id={self.stage_id} name={self.stage_name}>"


class SatelliteScene(Base):
    __tablename__ = "satellite_scenes"
    scene_id = Column(Integer, primary_key=True, autoincrement=True)
    scene_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                         server_default=text("uuid_generate_v4()"))
    product_identifier = Column(String(200), nullable=False, unique=True)
    platform = Column(String(20), nullable=False, default="SENTINEL-1")
    instrument_mode = Column(String(10), nullable=False, default="IW")
    polarization_vv = Column(Boolean, nullable=False, default=True)
    polarization_vh = Column(Boolean, nullable=False, default=True)
    acquisition_datetime = Column(DateTime(timezone=True), nullable=False, index=True)
    orbit_number = Column(Integer)
    orbit_direction = Column(
        Enum(OrbitDirectionEnum, name="orbit_direction_enum"),
        nullable=False, default=OrbitDirectionEnum.ASCENDING
    )
    relative_orbit = Column(SmallInteger)
    bbox = Column(Geometry("POLYGON", srid=4326), nullable=False)
    cloud_cover_percent = Column(Numeric(5, 2))
    incidence_angle_near = Column(Numeric(6, 3))
    incidence_angle_far = Column(Numeric(6, 3))
    resolution_m = Column(SmallInteger, nullable=False, default=10)
    region_id = Column(Integer, ForeignKey("regions_of_interest.region_id",
                                            ondelete="RESTRICT"), nullable=False)
    raw_file_path = Column(Text)
    raw_file_size_mb = Column(Numeric(12, 3))
    download_url = Column(Text)
    checksum_md5 = Column(String(32))
    is_available = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    region = relationship("RegionOfInterest", back_populates="scenes")
    jobs = relationship("ProcessingJob", back_populates="scene", cascade="all, delete-orphan")
    products = relationship("DataProduct", back_populates="scene", cascade="all, delete-orphan")
    quality_metrics = relationship("QualityMetric", back_populates="scene", cascade="all, delete-orphan")
    alert_events = relationship("AlertEvent", back_populates="scene")
    scene_job_states = relationship("SceneJobState", back_populates="scene")

    def __repr__(self) -> str:
        return f"<SatelliteScene id={self.scene_id} pid={self.product_identifier}>"


class ProcessingJob(Base):
    __tablename__ = "processing_jobs"
    __table_args__ = (
        UniqueConstraint("scene_id", "stage_id", "attempt_number",
                          name="uq_job_scene_stage_attempt"),
    )
    job_id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                       server_default=text("uuid_generate_v4()"))
    scene_id = Column(Integer, ForeignKey("satellite_scenes.scene_id",
                                           ondelete="CASCADE"), nullable=False)
    stage_id = Column(Integer, ForeignKey("processing_stages.stage_id",
                                           ondelete="RESTRICT"), nullable=False)
    attempt_number = Column(SmallInteger, nullable=False, default=1)
    status = Column(
        Enum(JobStatusEnum, name="job_status_enum"),
        nullable=False, default=JobStatusEnum.QUEUED
    )
    queued_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    worker_hostname = Column(String(100))
    cpu_usage_percent = Column(Numeric(5, 2))
    memory_usage_mb = Column(Numeric(10, 2))
    input_size_mb = Column(Numeric(12, 3))
    output_size_mb = Column(Numeric(12, 3))
    error_code = Column(String(50))
    error_message = Column(Text)
    log_file_path = Column(Text)
    parameters_json = Column(JSONB, nullable=False, default={})
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    scene = relationship("SatelliteScene", back_populates="jobs")
    stage = relationship("ProcessingStage", back_populates="jobs")
    products = relationship("DataProduct", back_populates="job")
    lineages = relationship("DataLineage", back_populates="job",
                             foreign_keys="DataLineage.job_id")

    def __repr__(self) -> str:
        return f"<ProcessingJob id={self.job_id} scene={self.scene_id} stage={self.stage_id} status={self.status}>"


class DataProduct(Base):
    __tablename__ = "data_products"
    product_id = Column(BigInteger, primary_key=True, autoincrement=True)
    product_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                           server_default=text("uuid_generate_v4()"))
    scene_id = Column(Integer, ForeignKey("satellite_scenes.scene_id",
                                           ondelete="CASCADE"), nullable=False)
    job_id = Column(BigInteger, ForeignKey("processing_jobs.job_id",
                                            ondelete="RESTRICT"), nullable=False)
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id", ondelete="CASCADE"))
    product_tier = Column(
        Enum(ProductTierEnum, name="product_tier_enum"),
        nullable=False
    )
    source = Column(String(20), nullable=False, default=ProductSourceEnum.SENTINEL1.value)
    product_type = Column(String(50), nullable=False)
    band_name = Column(String(10), nullable=False)
    file_name = Column(String(255), nullable=False)
    file_path = Column(Text, nullable=False)
    file_size_mb = Column(Numeric(12, 3), nullable=False)
    file_format = Column(String(20), nullable=False, default="TIFF")
    data_hash_sha256 = Column(String(64), nullable=False)
    crs = Column(String(50), nullable=False, default="EPSG:4326")
    pixel_size_m = Column(Numeric(8, 3))
    nodata_value = Column(Numeric)
    rows = Column(Integer)
    cols = Column(Integer)
    band_count = Column(SmallInteger, nullable=False, default=1)
    storage_location = Column(
        Enum(StorageLocationEnum, name="storage_location_enum"),
        nullable=False, default=StorageLocationEnum.LOCAL
    )
    is_valid = Column(Boolean, nullable=False, default=True)
    is_latest = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    scene = relationship("SatelliteScene", back_populates="products")
    job = relationship("ProcessingJob", back_populates="products")
    dataset = relationship("Dataset", back_populates="products")
    quality_metrics = relationship("QualityMetric", back_populates="product", cascade="all, delete-orphan")
    versions = relationship("DatasetVersion", back_populates="product")
    lineages_as_parent = relationship("DataLineage", back_populates="parent_product",
                                       foreign_keys="DataLineage.parent_product_id")
    lineages_as_child = relationship("DataLineage", back_populates="child_product",
                                      foreign_keys="DataLineage.child_product_id")

    def __repr__(self) -> str:
        return f"<DataProduct id={self.product_id} tier={self.product_tier} band={self.band_name}>"


class QualityMetric(Base):
    __tablename__ = "quality_metrics"
    __table_args__ = (
        UniqueConstraint("scene_id", "product_id", "band_name",
                          name="uq_quality_scene_product_band"),
        CheckConstraint("quality_score BETWEEN 0 AND 100", name="chk_quality_score_range"),
    )
    metric_id = Column(BigInteger, primary_key=True, autoincrement=True)
    scene_id = Column(Integer, ForeignKey("satellite_scenes.scene_id",
                                           ondelete="CASCADE"), nullable=False)
    product_id = Column(BigInteger, ForeignKey("data_products.product_id",
                                                ondelete="CASCADE"), nullable=False)
    band_name = Column(String(10), nullable=False)
    assessed_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    total_pixels = Column(BigInteger, nullable=False)
    valid_pixels = Column(BigInteger, nullable=False)
    nodata_pixels = Column(BigInteger, nullable=False, default=0)
    backscatter_mean_db = Column(Numeric(8, 4))
    backscatter_std_db = Column(Numeric(8, 4))
    backscatter_min_db = Column(Numeric(8, 4))
    backscatter_max_db = Column(Numeric(8, 4))
    cloud_threshold_percent = Column(Numeric(5, 2), nullable=False, default=20.0)
    radiometric_consistency = Column(Boolean)
    speckle_index = Column(Numeric(8, 4))
    quality_score = Column(Numeric(5, 2), nullable=False)
    quality_flag = Column(String(20), nullable=False, default="UNCHECKED")
    notes = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    scene = relationship("SatelliteScene", back_populates="quality_metrics")
    product = relationship("DataProduct", back_populates="quality_metrics")

    def __repr__(self) -> str:
        return f"<QualityMetric id={self.metric_id} scene={self.scene_id} band={self.band_name} score={self.quality_score}>"


class ProcessingRule(Base):
    __tablename__ = "processing_rules"
    rule_id = Column(Integer, primary_key=True, autoincrement=True)
    stage_id = Column(Integer, ForeignKey("processing_stages.stage_id",
                                           ondelete="CASCADE"), nullable=False)
    rule_name = Column(String(100), nullable=False)
    rule_code = Column(String(30), nullable=False, unique=True)
    rule_type = Column(Enum(RuleTypeEnum, name="rule_type_enum"), nullable=False)
    description = Column(Text)
    threshold_value = Column(Numeric(12, 4))
    threshold_unit = Column(String(20))
    operator = Column(String(10))
    action_on_fail = Column(String(50), nullable=False, default="WARN")
    is_mandatory = Column(Boolean, nullable=False, default=True)
    is_active = Column(Boolean, nullable=False, default=True)
    version = Column(String(20), nullable=False, default="1.0.0")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    stage = relationship("ProcessingStage", back_populates="rules")

    def __repr__(self) -> str:
        return f"<ProcessingRule id={self.rule_id} code={self.rule_code}>"


class DataLineage(Base):
    __tablename__ = "data_lineage"
    __table_args__ = (
        UniqueConstraint("parent_product_id", "child_product_id", name="uq_lineage_parent_child"),
        CheckConstraint("parent_product_id <> child_product_id", name="chk_lineage_no_self_ref"),
    )
    lineage_id = Column(BigInteger, primary_key=True, autoincrement=True)
    parent_product_id = Column(BigInteger, ForeignKey("data_products.product_id",
                                                        ondelete="CASCADE"), nullable=False)
    child_product_id = Column(BigInteger, ForeignKey("data_products.product_id",
                                                       ondelete="CASCADE"), nullable=False)
    transformation_type = Column(String(50), nullable=False)
    stage_id = Column(Integer, ForeignKey("processing_stages.stage_id",
                                           ondelete="RESTRICT"), nullable=False)
    job_id = Column(BigInteger, ForeignKey("processing_jobs.job_id",
                                            ondelete="RESTRICT"), nullable=False)
    transformation_params = Column(JSONB, nullable=False, default={})
    input_checksum = Column(String(64))
    output_checksum = Column(String(64))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    parent_product = relationship("DataProduct", back_populates="lineages_as_parent",
                                   foreign_keys=[parent_product_id])
    child_product = relationship("DataProduct", back_populates="lineages_as_child",
                                  foreign_keys=[child_product_id])
    stage = relationship("ProcessingStage")
    job = relationship("ProcessingJob", back_populates="lineages",
                        foreign_keys=[job_id])

    def __repr__(self) -> str:
        return f"<DataLineage id={self.lineage_id} {self.parent_product_id}->{self.child_product_id}>"


class ApiAccessLog(Base):
    __tablename__ = "api_access_logs"
    log_id = Column(BigInteger, primary_key=True, autoincrement=True)
    log_uuid = Column(UUID(as_uuid=True), nullable=False,
                       server_default=text("uuid_generate_v4()"))
    endpoint = Column(String(200), nullable=False)
    http_method = Column(Enum(HttpMethodEnum, name="http_method_enum"),
                          nullable=False, default=HttpMethodEnum.GET)
    user_ip = Column(INET, nullable=False)
    user_agent = Column(Text)
    request_timestamp = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    scene_id_queried = Column(Integer, ForeignKey("satellite_scenes.scene_id",
                                                    ondelete="SET NULL"))
    product_id_queried = Column(BigInteger, ForeignKey("data_products.product_id",
                                                         ondelete="SET NULL"))
    query_params = Column(JSONB, default={})
    response_status = Column(SmallInteger, nullable=False)
    response_time_ms = Column(Integer, nullable=False)
    response_size_kb = Column(Numeric(12, 3))
    error_detail = Column(Text)
    api_key_id = Column(String(50))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    def __repr__(self) -> str:
        return f"<ApiAccessLog id={self.log_id} endpoint={self.endpoint} status={self.response_status}>"


class AlertEvent(Base):
    __tablename__ = "alert_events"
    alert_id = Column(BigInteger, primary_key=True, autoincrement=True)
    alert_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                         server_default=text("uuid_generate_v4()"))
    event_type = Column(Enum(AlertEventTypeEnum, name="alert_event_type_enum"), nullable=False)
    severity = Column(Enum(AlertSeverityEnum, name="alert_severity_enum"),
                       nullable=False, default=AlertSeverityEnum.INFO)
    scene_id = Column(Integer, ForeignKey("satellite_scenes.scene_id",
                                           ondelete="SET NULL"))
    job_id = Column(BigInteger, ForeignKey("processing_jobs.job_id",
                                            ondelete="SET NULL"))
    product_id = Column(BigInteger, ForeignKey("data_products.product_id",
                                                ondelete="SET NULL"))
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    metadata_json = Column(JSONB, default={})
    is_resolved = Column(Boolean, nullable=False, default=False)
    resolved_at = Column(DateTime(timezone=True))
    resolved_by = Column(String(100))
    resolution_note = Column(Text)
    triggered_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    scene = relationship("SatelliteScene", back_populates="alert_events")

    def __repr__(self) -> str:
        return f"<AlertEvent id={self.alert_id} type={self.event_type} severity={self.severity}>"


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"
    __table_args__ = (
        UniqueConstraint("product_id", "version_number", name="uq_version_product_semver"),
    )
    version_id = Column(Integer, primary_key=True, autoincrement=True)
    version_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                           server_default=text("uuid_generate_v4()"))
    product_id = Column(BigInteger, ForeignKey("data_products.product_id",
                                                ondelete="RESTRICT"), nullable=False)
    version_number = Column(String(20), nullable=False)
    release_date = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    release_notes = Column(Text)
    change_log = Column(Text)
    is_production = Column(Boolean, nullable=False, default=False)
    is_deprecated = Column(Boolean, nullable=False, default=False)
    deprecated_at = Column(DateTime(timezone=True))
    deprecated_reason = Column(Text)
    released_by = Column(String(100))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    product = relationship("DataProduct", back_populates="versions")

    def __repr__(self) -> str:
        return f"<DatasetVersion id={self.version_id} product={self.product_id} v={self.version_number}>"


class Dataset(Base):
    __tablename__ = "datasets"
    dataset_id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                           server_default=text("uuid_generate_v4()"))
    name = Column(String(255), nullable=False)
    description = Column(Text)
    location_label = Column(String(255))
    region_id = Column(Integer, ForeignKey("regions_of_interest.region_id", ondelete="SET NULL"))
    bbox = Column(Geometry("POLYGON", srid=4326), nullable=False)
    bbox_wkt = Column(Text, nullable=False)
    date_start = Column(Date, nullable=False)
    date_end = Column(Date, nullable=False)
    required_tiers = Column(ARRAY(String), nullable=False)
    quality_settings = Column(JSONB, nullable=False, default={})
    dataset_kind = Column(String(10), nullable=False, default="STANDARD")
    status = Column(String(20), nullable=False, default="DRAFT")
    total_scenes = Column(Integer, nullable=False, default=0)
    completed_scenes = Column(Integer, nullable=False, default=0)
    failed_scenes = Column(Integer, nullable=False, default=0)
    total_size_bytes = Column(BigInteger, nullable=False, default=0)
    is_deletable = Column(Boolean, nullable=False, default=True)
    # Jalankan tahap PREVIEW (render PNG dari GOLD) untuk dataset ini.
    # Kolom sendiri, bukan key di quality_settings: ini pilihan user yang bisa
    # di-query, sementara quality_settings isinya ambang mutu data. Lihat
    # migrasi 016 -- WAJIB dijalankan, kolom ini dipetakan tanpa syarat.
    generate_preview = Column(Boolean, nullable=False, default=True)
    live_enabled = Column(Boolean, nullable=False, default=False)
    live_last_checked_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    deleted_at = Column(DateTime(timezone=True))

    region = relationship("RegionOfInterest", back_populates="datasets")
    jobs = relationship("DatasetJob", back_populates="dataset", cascade="all, delete-orphan")
    products = relationship("DataProduct", back_populates="dataset")

    def __repr__(self) -> str:
        return f"<Dataset id={self.dataset_id} name={self.name} kind={self.dataset_kind} status={self.status}>"


class DatasetJob(Base):
    __tablename__ = "dataset_jobs"
    job_id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                       server_default=text("uuid_generate_v4()"))
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id", ondelete="CASCADE"), nullable=False)
    job_type = Column(String(20), nullable=False, default="CREATE")
    status = Column(String(20), nullable=False, default="QUEUED")
    paused_at = Column(DateTime(timezone=True))
    paused_by = Column(String(20))
    pause_reason = Column(Text)
    resumed_at = Column(DateTime(timezone=True))
    resume_count = Column(SmallInteger, nullable=False, default=0)
    date_range_start = Column(Date)
    date_range_end = Column(Date)
    total_scenes = Column(Integer, nullable=False, default=0)
    downloaded_count = Column(Integer, nullable=False, default=0)
    processed_count = Column(Integer, nullable=False, default=0)
    failed_count = Column(Integer, nullable=False, default=0)
    cleaned_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))

    dataset = relationship("Dataset", back_populates="jobs")
    scene_states = relationship("SceneJobState", back_populates="job", cascade="all, delete-orphan")
    cleanup_operations = relationship("CleanupOperation", back_populates="job")

    def __repr__(self) -> str:
        return f"<DatasetJob id={self.job_id} dataset={self.dataset_id} type={self.job_type} status={self.status}>"


class SceneJobState(Base):
    __tablename__ = "scene_job_state"
    __table_args__ = (
        UniqueConstraint("job_id", "product_identifier", name="uq_job_product"),
    )
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(BigInteger, ForeignKey("dataset_jobs.job_id", ondelete="CASCADE"), nullable=False)
    product_identifier = Column(String(200), nullable=False)
    scene_id = Column(Integer, ForeignKey("satellite_scenes.scene_id", ondelete="SET NULL"))
    current_stage = Column(String(30))
    stage_status = Column(String(20), nullable=False, default="PENDING")
    produced_files = Column(JSONB, nullable=False, default={})
    attempt_number = Column(SmallInteger, nullable=False, default=1)
    max_retries = Column(SmallInteger, nullable=False, default=3)
    last_error = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))

    job = relationship("DatasetJob", back_populates="scene_states")
    scene = relationship("SatelliteScene", back_populates="scene_job_states")

    def __repr__(self) -> str:
        return f"<SceneJobState id={self.id} job={self.job_id} pid={self.product_identifier} stage={self.current_stage}>"


class CleanupOperation(Base):
    __tablename__ = "cleanup_operations"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    # Not a FK: this row must outlive its dataset so deletion-progress
    # remains readable after the dataset row is deleted (migration 006).
    dataset_id = Column(Integer, nullable=False)
    job_id = Column(BigInteger, ForeignKey("dataset_jobs.job_id", ondelete="SET NULL"))
    operation_type = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING")
    total_files = Column(Integer, nullable=False, default=0)
    deleted_count = Column(Integer, nullable=False, default=0)
    freed_bytes = Column(BigInteger, nullable=False, default=0)
    error_log = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))

    job = relationship("DatasetJob", back_populates="cleanup_operations")

    def __repr__(self) -> str:
        return f"<CleanupOperation id={self.id} dataset={self.dataset_id} type={self.operation_type} status={self.status}>"


class ProcessingLog(Base):
    __tablename__ = "processing_logs"
    log_id = Column(BigInteger, primary_key=True, autoincrement=True)
    log_uuid = Column(UUID(as_uuid=True), nullable=False, unique=True,
                       server_default=text("uuid_generate_v4()"))
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id", ondelete="CASCADE"), nullable=False)
    scene_id = Column(String(255), nullable=False)
    module = Column(String(50), nullable=False)
    stage = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False)
    message = Column(Text, nullable=False)
    details = Column(JSONB, nullable=False, default={})
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    def __repr__(self) -> str:
        return (f"<ProcessingLog id={self.log_id} dataset={self.dataset_id} scene={self.scene_id} "
                f"stage={self.stage} status={self.status}>")


class LiveDatasetSource(Base):
    __tablename__ = "live_dataset_sources"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(20), nullable=False, unique=True)
    enabled = Column(Boolean, nullable=False, default=True)
    last_check = Column(DateTime(timezone=True))
    last_ingest = Column(DateTime(timezone=True))
    next_check = Column(DateTime(timezone=True))
    source_config = Column(JSONB, nullable=False, default={})
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    def __repr__(self) -> str:
        return f"<LiveDatasetSource id={self.id} source={self.source_name} enabled={self.enabled}>"


class NasaScene(Base):
    __tablename__ = "nasa_scenes"
    __table_args__ = (
        UniqueConstraint("source", "tile_id", "product_short_name", "acquisition_date",
                          name="uq_nasa_scene"),
    )
    nasa_scene_id = Column(BigInteger, primary_key=True, autoincrement=True)
    source = Column(String(20), nullable=False)
    tile_id = Column(String(10), nullable=False)
    product_short_name = Column(String(50), nullable=False)
    acquisition_date = Column(Date, nullable=False)
    region_id = Column(Integer, ForeignKey("regions_of_interest.region_id", ondelete="RESTRICT"), nullable=False)
    raw_file_path = Column(Text)
    download_url = Column(Text)
    is_available = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    region = relationship("RegionOfInterest")

    def __repr__(self) -> str:
        return f"<NasaScene id={self.nasa_scene_id} source={self.source} tile={self.tile_id} date={self.acquisition_date}>"


class DatabaseClient:
    _RETRY_ATTEMPTS = 3
    _RETRY_BACKOFF = 2.0

    def __init__(self, database_url: str, pool_size: int = 5,
                 max_overflow: int = 10, echo: bool = False) -> None:
        self._database_url = database_url
        self._engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=echo,
        )
        self._SessionFactory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
        self._register_listeners()
        logger.info("DatabaseClient initialized. Pool size: %d + %d overflow", pool_size, max_overflow)

    @classmethod
    def from_env(cls) -> "DatabaseClient":
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        name = os.getenv("DB_NAME", "sentinel1_flood")
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "")
        pool = int(os.getenv("DB_POOL_SIZE", "5"))
        overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))
        echo = os.getenv("DB_ECHO", "false").lower() == "true"
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
        return cls(url, pool_size=pool, max_overflow=overflow, echo=echo)

    def _register_listeners(self) -> None:
        @event.listens_for(self._engine, "connect")
        def _on_connect(dbapi_conn, _connection_record):
            logger.debug("New DB connection opened")

        @event.listens_for(self._engine, "checkout")
        def _on_checkout(dbapi_conn, _record, _proxy):
            logger.debug("DB connection checked out from pool")

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        sess: Session = self._SessionFactory()
        try:
            yield sess
            sess.commit()
        except SQLAlchemyError as exc:
            sess.rollback()
            logger.error("Session rolled back due to error: %s", exc)
            raise
        finally:
            sess.close()

    @contextmanager
    def session_with_retry(self) -> Generator[Session, None, None]:
        attempt = 0
        delay = self._RETRY_BACKOFF
        while True:
            attempt += 1
            try:
                with self.session() as sess:
                    yield sess
                return
            except OperationalError as exc:
                if attempt >= self._RETRY_ATTEMPTS:
                    logger.error("All %d retry attempts exhausted. Last error: %s",
                                 self._RETRY_ATTEMPTS, exc)
                    raise
                logger.warning("Transient DB error (attempt %d/%d). Retrying in %.1fs: %s",
                               attempt, self._RETRY_ATTEMPTS, delay, exc)
                time.sleep(delay)
                delay *= 2

    def check_health(self) -> dict:
        pool = self._engine.pool
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return {
                "connected": True,
                "pool_size": pool.size(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
            }
        except Exception as exc:
            logger.error("Health check failed: %s", exc)
            return {"connected": False, "error": str(exc)}

    def create_tables(self) -> None:
        Base.metadata.create_all(self._engine)
        logger.info("All ORM tables created (or already exist)")

    def dispose(self) -> None:
        self._engine.dispose()
        logger.info("DatabaseClient disposed. All connections closed.")


class FusionProduct(Base):
    """Multi-sensor (S1 + MODIS + GPM) feature stack registry for ML training."""

    __tablename__ = "fusion_products"
    __table_args__ = (
        UniqueConstraint("feature_date", "region_id", name="uq_fusion_date_region"),
    )

    fusion_id          = Column(BigInteger, primary_key=True, autoincrement=True)
    feature_date       = Column(Date, nullable=False)
    region_id          = Column(Integer, ForeignKey("regions_of_interest.region_id",
                                                     ondelete="RESTRICT"), nullable=False)
    s1_scene_id        = Column(Integer, ForeignKey("satellite_scenes.scene_id",
                                                     ondelete="SET NULL"))
    modis_scene_id     = Column(BigInteger, ForeignKey("nasa_scenes.nasa_scene_id",
                                                        ondelete="SET NULL"))
    gpm_scene_id       = Column(BigInteger, ForeignKey("nasa_scenes.nasa_scene_id",
                                                        ondelete="SET NULL"))
    days_since_s1      = Column(Integer, nullable=False)
    feature_stack_path = Column(Text, nullable=False)
    created_at         = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    def __repr__(self) -> str:
        return f"<FusionProduct id={self.fusion_id} date={self.feature_date} path={self.feature_stack_path}>"