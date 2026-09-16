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
    null,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, INET, JSONB, UUID
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker
from sqlalchemy.pool import QueuePool

from etl import tier_names as tn

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
    """Nilai `data_products.product_tier`.

    Kosakata D14 (dinamai menurut kontrak yang dipenuhi artefaknya) plus
    kosakata lama yang DIPERTAHANKAN supaya baris pra-migrasi masih bisa
    dibaca kembali jadi objek. Nama lama tidak pernah ditulis lagi oleh kode
    baru; lihat etl/tier_names.py untuk pemetaan dan peringkatnya.

    Keduanya tidak bisa jadi alias Python (alias menuntut nilai yang sama),
    jadi ini anggota biasa yang ditandai warisan lewat komentar.
    """
    RAW = "RAW"
    ALIGNED = "ALIGNED"
    DESPECKLED = "DESPECKLED"
    INDICES = "INDICES"
    ACCUMULATED = "ACCUMULATED"
    COG = "COG"
    FUSED = "FUSED"
    # -- warisan pra-D14, dibaca tapi tidak pernah ditulis --
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


class ProcessingLevelEnum(str, PyEnum):
    """Level pemrosesan yang diminta user untuk SATU sumber.

    Artinya berbeda per satelit (DOCS/DESIGN.md, tabel "What RAW vs
    PROCESSED means per satellite"): untuk S1 RAW = kalibrasi + crop tanpa
    Lee filter, untuk GPM RAW = curah hujan harian tanpa akumulasi. Yang
    sama di semua sumber: RAW berhenti di BRONZE, PROCESSED lanjut ke
    SILVER/GOLD. Berbeda dari ProductTierEnum, yang menyatakan posisi
    artefak di lineage, bukan konfigurasi yang diminta."""
    RAW = "RAW"
    PROCESSED = "PROCESSED"


class FusionStrategyEnum(str, PyEnum):
    CO_OCCURRENCE = "CO_OCCURRENCE"
    FULL_COVERAGE = "FULL_COVERAGE"
    HYBRID = "HYBRID"


class PreviewOptionEnum(str, PyEnum):
    GRAYSCALE = "GRAYSCALE"
    COLORED = "COLORED"
    COMPOSITE = "COMPOSITE"


class DatasetSourceNameEnum(str, PyEnum):
    """Sumber yang bisa dikonfigurasi per dataset.

    Terpisah dari ProductSourceEnum (yang punya FUSION -- hasil, bukan
    sumber yang bisa dipilih) dan dari LiveSourceNameEnum (sakelar ingest
    live global, tabel lain). Nilainya sengaja dijaga sama dengan CHECK
    chk_source_config_source_name di migrasi 017."""
    SENTINEL1 = "SENTINEL1"
    MODIS = "MODIS"
    GPM = "GPM"


# Urutan kanonik sumber. Dipakai untuk mengurutkan hasil query dan payload
# API supaya stabil (S1 dulu: dia yang menjadi jangkar tanggal fusi).
SOURCE_NAME_ORDER: tuple[str, ...] = (
    DatasetSourceNameEnum.SENTINEL1.value,
    DatasetSourceNameEnum.MODIS.value,
    DatasetSourceNameEnum.GPM.value,
)

# API memakai key huruf kecil ("sentinel1"), database memakai huruf besar
# ("SENTINEL1") -- lihat DOCS/API.md bagian "Create Dataset". Pemetaan ada di
# satu tempat supaya tidak ada .upper()/.lower() yang tersebar.
API_KEY_TO_SOURCE_NAME: dict[str, str] = {name.lower(): name for name in SOURCE_NAME_ORDER}
SOURCE_NAME_TO_API_KEY: dict[str, str] = {name: name.lower() for name in SOURCE_NAME_ORDER}


def normalize_source_configs(sources: dict) -> dict[str, list[str]]:
    """Ubah objek `sources` gaya API menjadi {SOURCE_NAME: [levels]}.

    Menerima dua bentuk supaya pemanggil internal (ETL, tes) tidak perlu
    membungkus levels dalam dict:
        {"sentinel1": {"processing": ["RAW", "PROCESSED"]}}   <- payload API
        {"SENTINEL1": ["RAW", "PROCESSED"]}                   <- bentuk ringkas

    Validasi di sini bersifat fail-fast: ValueError dilempar SEBELUM ada
    satu pun baris ditulis, jadi payload yang salah tidak pernah membuka
    transaksi. CHECK constraint di database tetap ada sebagai jaring
    pengaman untuk penulis lain (SQL mentah, ORM langsung).

    Raises:
        ValueError: sources kosong, nama sumber tidak dikenal, level tidak
            dikenal, atau daftar level kosong.
    """
    if not isinstance(sources, dict) or not sources:
        raise ValueError("sources wajib berisi minimal 1 sumber")

    valid_levels = {level.value for level in ProcessingLevelEnum}
    normalized: dict[str, list[str]] = {}

    for raw_key, raw_value in sources.items():
        key = str(raw_key).strip()
        source_name = API_KEY_TO_SOURCE_NAME.get(key.lower())
        if source_name is None:
            raise ValueError(
                f"source tidak dikenal: {raw_key!r} "
                f"(pilihan: {', '.join(sorted(API_KEY_TO_SOURCE_NAME))})"
            )
        if source_name in normalized:
            raise ValueError(f"source ganda dalam payload: {source_name}")

        if isinstance(raw_value, dict):
            levels = raw_value.get("processing")
        else:
            levels = raw_value
        if isinstance(levels, str):
            levels = [levels]
        if not levels:
            raise ValueError(
                f"sources.{SOURCE_NAME_TO_API_KEY[source_name]}.processing "
                "must contain at least one value"
            )

        seen: list[str] = []
        for raw_level in levels:
            level = str(raw_level).strip().upper()
            if level not in valid_levels:
                raise ValueError(
                    f"processing level tidak dikenal untuk "
                    f"{SOURCE_NAME_TO_API_KEY[source_name]}: {raw_level!r} "
                    f"(pilihan: {', '.join(sorted(valid_levels))})"
                )
            if level not in seen:      # duplikat dibuang, bukan ditolak
                seen.append(level)
        # Urutan level dinormalkan supaya dua payload yang setara menghasilkan
        # baris yang identik -- perbandingan array di SQL peka urutan.
        normalized[source_name] = [
            lv.value for lv in ProcessingLevelEnum if lv.value in seen
        ]

    # Diurutkan, bukan difilter: nama di luar SOURCE_NAME_ORDER tidak boleh
    # hilang diam-diam dari hasil. Nama seperti itu tidak bisa lolos dari loop
    # di atas hari ini, tapi kalau daftar sumber dan urutannya sempat
    # berbeda, membuang baris tanpa suara akan membuat dataset dibuat dengan
    # sumber yang lebih sedikit dari yang diminta user.
    return dict(sorted(
        normalized.items(),
        key=lambda item: SOURCE_NAME_ORDER.index(item[0])
        if item[0] in SOURCE_NAME_ORDER else len(SOURCE_NAME_ORDER),
    ))


def source_configs_to_api(configs: "list[DatasetSourceConfig]") -> dict[str, dict]:
    """Bentuk objek `sources` gaya API dari baris dataset_source_config."""
    ordered = sorted(
        configs,
        key=lambda c: SOURCE_NAME_ORDER.index(c.source_name)
        if c.source_name in SOURCE_NAME_ORDER else len(SOURCE_NAME_ORDER),
    )
    return {
        SOURCE_NAME_TO_API_KEY.get(c.source_name, c.source_name.lower()):
            {"processing": list(c.processing_levels or [])}
        for c in ordered
    }


# Tier yang dihasilkan tiap processing level. RAW dan ALIGNED muncul di
# keduanya: download selalu menghasilkan artefak RAW, dan crop/kalibrasi
# selalu menghasilkan ALIGNED. Yang membedakan adalah lanjutan ke tier rank 2
# (per-source) lalu COG. Peringkatnya dipegang etl/tier_names.rank().
def _tiers_for(level: str, source: str) -> tuple[str, ...]:
    """Tier yang dihasilkan satu source pada satu level.

    Source ikut jadi argumen karena rank 2 bercabang (D14): PROCESSED
    menghasilkan DESPECKLED untuk S1, INDICES untuk MODIS, ACCUMULATED untuk
    GPM. Versi lama fungsi ini membuang kunci source dan karena itu secara
    harfiah tidak bisa memancarkan ketiganya.
    """
    if level == ProcessingLevelEnum.RAW.value:
        return (tn.RAW, tn.ALIGNED)
    if level == ProcessingLevelEnum.PROCESSED.value:
        return (tn.RAW, tn.ALIGNED, tn.RANK2_BY_SOURCE[source.upper()], tn.COG)
    return ()


def derive_required_tiers(
    configs: dict[str, list[str]], with_fusion: bool = False
) -> list[str]:
    """Turunkan `datasets.required_tiers` dari konfigurasi per-sumber.

    `required_tiers` bukan lagi input user (DOCS/PROTOTYPE_CHANGELOG.md:
    "`tiers` is no longer user-facing -- derived internally"), tapi kolomnya
    NOT NULL dan masih dipakai orchestrator untuk memutuskan tahap mana yang
    dilewati. Fungsi ini yang menjembatani keduanya.

    FUSED hanya ikut kalau ada strategi fusi DAN ada sumber yang PROCESSED:
    fusi menyusun stack dari artefak COG, jadi dataset yang semua sumbernya
    RAW-only tidak punya bahan untuk difusikan.
    """
    tiers: set[str] = set()
    for source, levels in configs.items():
        for level in levels:
            tiers.update(_tiers_for(level, source))
    if with_fusion and tn.COG in tiers:
        tiers.add(tn.FUSED)
    return sorted(tiers, key=tn.sort_key)


def _validate_s1_tolerance(value) -> int | None:
    """Validasi `datasets.s1_match_tolerance_days`. None = pakai default kolom.

    Batas atas 14 hari melebihi satu siklus revisit penuh Sentinel-1A (~12
    hari): di luar itu "scene terdekat" sudah bisa berasal dari lintasan yang
    kondisinya sama sekali berbeda, dan menyebut hasilnya fusi akan
    menyesatkan. Dicek di sini supaya payload yang salah tidak perlu menyentuh
    database; CHECK constraint tetap ada sebagai jaring pengaman.
    """
    if value is None:
        return None
    try:
        days = int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"s1_match_tolerance_days harus bilangan bulat, bukan {value!r}"
        ) from None
    if not 0 <= days <= 14:
        raise ValueError(
            f"s1_match_tolerance_days di luar jangkauan: {days} (0-14)"
        )
    return days


def _validate_fusion_output_only(value, fusion_strategy: "str | None") -> bool:
    """`fusion_output_only` hanya masuk akal kalau ada fusi.

    Tanpa strategi fusi tidak ada stack HDF5 yang ditulis, jadi menghapus
    artefak per-satelit akan menyisakan dataset kosong. Ditolak, bukan
    diam-diam diabaikan: user yang mencentangnya jelas mengharapkan sesuatu.
    """
    enabled = bool(value)
    if enabled and fusion_strategy is None:
        raise ValueError(
            "fusion_output_only butuh fusion_strategy: tanpa fusi, menghapus "
            "artefak per-satelit tidak menyisakan output apa pun"
        )
    return enabled


def _validate_fusion_strategy(value, source_count: int) -> "str | None":
    """Validasi fusion_strategy terhadap jumlah sumber yang dikonfigurasi.

    Aturannya (DOCS/API.md, bagian Validation) tidak bisa jadi CHECK
    constraint: jumlah sumber ada di tabel lain, dan CHECK tidak boleh
    membaca tabel lain. Jadi di sinilah aturan itu ditegakkan.

    Returns:
        Strategi dalam huruf besar, atau None untuk dataset satu sumber.

    Raises:
        ValueError: strategi tidak dikenal, hilang padahal sumbernya >1, atau
            terisi padahal sumbernya cuma 1.
    """
    strategy = None if value is None else str(value).strip().upper()
    if strategy == "":
        strategy = None

    if source_count > 1:
        if strategy is None:
            raise ValueError("fusion_strategy required when multiple sources configured")
    elif strategy is not None:
        raise ValueError("fusion_strategy must be null when only 1 source configured")

    valid = {s.value for s in FusionStrategyEnum}
    if strategy is not None and strategy not in valid:
        raise ValueError(
            f"fusion_strategy tidak dikenal: {value!r} (pilihan: {', '.join(sorted(valid))})"
        )
    return strategy


def _validate_preview_options(value) -> list[str]:
    """Normalkan preview_options. None -> semua varian, [] -> tidak ada.

    None dan [] sengaja DIBEDAKAN: None berarti "user tidak menyebutkan"
    (pakai default kolom, yaitu ketiga varian), [] berarti "user sengaja
    tidak mau preview apa pun". Kalau keduanya disamakan, mematikan preview
    jadi mustahil lewat jalur ini.
    """
    if value is None:
        return [opt.value for opt in PreviewOptionEnum]
    if isinstance(value, str):
        value = [value]

    valid = {opt.value for opt in PreviewOptionEnum}
    seen: list[str] = []
    for raw in value:
        option = str(raw).strip().upper()
        if option not in valid:
            raise ValueError(
                f"preview option tidak dikenal: {raw!r} (pilihan: {', '.join(sorted(valid))})"
            )
        if option not in seen:
            seen.append(option)
    return [opt.value for opt in PreviewOptionEnum if opt.value in seen]


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
    __table_args__ = (
        # Dicerminkan dari chk_dprods_processing_level (migrasi 017) supaya
        # database uji hasil create_all menegakkan aturan yang sama dengan
        # produksi.
        CheckConstraint(
            "processing_level IS NULL OR processing_level IN ('RAW', 'PROCESSED')",
            name="chk_dprods_processing_level",
        ),
        Index("idx_dprods_dataset_level", "dataset_id", "processing_level"),
    )
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
    # Berbeda dari product_tier: tier adalah posisi artefak di lineage,
    # processing_level adalah level yang diminta user untuk sumbernya di
    # dataset_source_config (migrasi 017). NULL hanya untuk baris warisan.
    processing_level = Column(
        String(20), nullable=True,
        server_default=text("'PROCESSED'"),
        default=ProcessingLevelEnum.PROCESSED.value,
    )
    product_type = Column(String(50), nullable=False)
    # VARCHAR(20), bukan (10) seperti quality_metrics: produk tier FUSION
    # menamai band-nya per level ("FUSION_PROCESSED", 16 karakter) supaya dua
    # stack tanggal yang sama tidak saling menandai usang lewat dedup
    # is_latest, yang berjalan atas (scene_id, band_name, tier, dataset_id).
    # Lihat migrasi 018.
    band_name = Column(String(20), nullable=False)
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
    # CATATAN MODEL LAMA: `selected_satellites` dan `processing_level` tidak
    # pernah ada di tabel ini (lihat migrasi 004), jadi tidak ada yang perlu
    # dihapus dari pemetaan ini. Konfigurasi per-satelit tinggal di
    # DatasetSourceConfig -- satu baris per (dataset, sumber), karena tiap
    # pasangan punya atributnya sendiri (processing_levels). Migrasi 017 tetap
    # men-DROP kedua kolom itu secara defensif untuk database yang pernah
    # ditambal manual.
    __table_args__ = (
        CheckConstraint(
            "fusion_strategy IS NULL OR fusion_strategy IN "
            "('CO_OCCURRENCE', 'FULL_COVERAGE', 'HYBRID')",
            name="chk_datasets_fusion_strategy",
        ),
    )
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
    # Diturunkan dari source_configs, bukan diisi user (DOCS/DESIGN.md).
    required_tiers = Column(ARRAY(String), nullable=False)
    # NULL = dataset satu sumber, tidak ada yang perlu difusikan. Aturan
    # "harus NULL kalau sumbernya cuma 1" tidak bisa jadi CHECK (jumlah sumber
    # ada di tabel lain), jadi ditegakkan di create_dataset_with_sources().
    fusion_strategy = Column(String(20), server_default=text("'FULL_COVERAGE'"))
    # Varian PNG yang dirender tahap PREVIEW. Array kosong = tidak ada varian;
    # sakelar on/off tahapnya tetap `generate_preview` di bawah.
    preview_options = Column(
        ARRAY(Text),
        server_default=text("ARRAY['GRAYSCALE', 'COLORED', 'COMPOSITE']::TEXT[]"),
    )
    # Migrasi 019 -- kontrol strategi fusi.
    # Hapus artefak per-satelit setelah stack fusi tanggal itu ditulis. BUKAN
    # "lewati pemrosesan": fusi tetap butuh bahannya.
    fusion_output_only = Column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
    # Hanya dipakai FULL_COVERAGE. CO_OCCURRENCE dan HYBRID berjangkar pada
    # scene S1, jadi tidak pernah perlu meminjam dari hari lain.
    s1_match_tolerance_days = Column(
        SmallInteger, nullable=False, server_default=text("2")
    )
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
    # delete-orphan: baris konfigurasi tidak punya arti tanpa datasetnya.
    # ON DELETE CASCADE di database menangani DELETE lewat SQL mentah; cascade
    # ORM ini menangani jalur session (sess.delete(dataset), atau mencabut satu
    # config dari koleksi). Keduanya perlu -- yang satu tidak menggantikan yang
    # lain. lazy="selectin": pemanggil hampir selalu butuh configs bersama
    # datasetnya (kartu dataset, detail, ETL), dan lazy default akan meledak
    # jadi N+1 query di list_datasets.
    source_configs = relationship(
        "DatasetSourceConfig",
        back_populates="dataset",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="DatasetSourceConfig.config_id",
    )

    def __repr__(self) -> str:
        return f"<Dataset id={self.dataset_id} name={self.name} kind={self.dataset_kind} status={self.status}>"


class DatasetSourceConfig(Base):
    """Konfigurasi pemrosesan per-satelit untuk sebuah dataset.

    Satu baris per (dataset, sumber). ETL membaca tabel ini untuk menentukan
    sumber mana yang dijalankan dan sampai level apa (DOCS/ETL.md). Constraint
    di __table_args__ sengaja dicerminkan dari migrasi 017 supaya database uji
    yang dibuat lewat Base.metadata.create_all() menegakkan aturan yang sama
    dengan produksi -- tanpa itu, tes tidak akan pernah melihat kegagalan CHECK
    yang di produksi menjaga integritas.
    """

    __tablename__ = "dataset_source_config"
    __table_args__ = (
        UniqueConstraint("dataset_id", "source_name",
                          name="uq_source_config_dataset_source"),
        # array_length() atas array kosong mengembalikan NULL, bukan 0 --
        # karena itu IS NOT NULL, bukan cuma > 0.
        CheckConstraint(
            "array_length(processing_levels, 1) IS NOT NULL "
            "AND array_length(processing_levels, 1) > 0",
            name="chk_source_config_levels_not_empty",
        ),
        CheckConstraint(
            "processing_levels <@ ARRAY['RAW', 'PROCESSED']::TEXT[]",
            name="chk_source_config_levels_valid",
        ),
        CheckConstraint(
            "source_name IN ('SENTINEL1', 'MODIS', 'GPM')",
            name="chk_source_config_source_name",
        ),
        Index("idx_source_config_dataset", "dataset_id"),
    )

    config_id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(Integer, ForeignKey("datasets.dataset_id", ondelete="CASCADE"),
                         nullable=False)
    source_name = Column(String(20), nullable=False)
    # TEXT[], bukan JSON: nilainya terbatas dan di-CHECK di database.
    processing_levels = Column(
        ARRAY(Text), nullable=False,
        server_default=text("ARRAY['PROCESSED']::TEXT[]"),
    )
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    dataset = relationship("Dataset", back_populates="source_configs")

    @property
    def api_key(self) -> str:
        """Nama sumber dalam ejaan API ("sentinel1"), lihat DOCS/API.md."""
        return SOURCE_NAME_TO_API_KEY.get(self.source_name, self.source_name.lower())

    def has_level(self, level: str) -> bool:
        """True kalau sumber ini diminta diproses sampai `level`."""
        return str(level).upper() in set(self.processing_levels or [])

    def to_dict(self) -> dict:
        return {
            "config_id": self.config_id,
            "dataset_id": self.dataset_id,
            "source": self.api_key,
            "source_name": self.source_name,
            "processing": list(self.processing_levels or []),
        }

    def __repr__(self) -> str:
        return (f"<DatasetSourceConfig id={self.config_id} dataset={self.dataset_id} "
                f"source={self.source_name} levels={list(self.processing_levels or [])}>")


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

    # -----------------------------------------------------------------------
    # Konfigurasi per-satelit (dataset_source_config)
    # -----------------------------------------------------------------------
    # Objek yang dikembalikan di bawah sudah lepas dari session (detached).
    # Itu aman karena sessionmaker di kelas ini memakai expire_on_commit=False:
    # atribut yang sudah dimuat tetap terbaca setelah session ditutup. Relasi
    # yang belum dimuat TIDAK bisa di-lazy-load setelahnya, jadi apa pun yang
    # perlu dibaca pemanggil harus dimuat di dalam blok session (lihat
    # lazy="selectin" pada Dataset.source_configs).

    def get_dataset_source_config(
        self, dataset_id: int, source_name: str
    ) -> "DatasetSourceConfig | None":
        """Ambil satu baris konfigurasi untuk (dataset, sumber).

        `source_name` menerima ejaan API maupun database ("sentinel1" atau
        "SENTINEL1"). Nama yang tidak dikenal mengembalikan None, bukan
        melempar: pemanggilnya adalah ETL yang bertanya "apakah sumber ini
        dikonfigurasi?" dan untuk nama asing jawabannya memang "tidak".
        """
        canonical = API_KEY_TO_SOURCE_NAME.get(str(source_name).strip().lower())
        if canonical is None:
            logger.debug("get_dataset_source_config: source tidak dikenal %r", source_name)
            return None
        with self.session() as sess:
            return sess.scalar(
                select(DatasetSourceConfig).where(
                    DatasetSourceConfig.dataset_id == dataset_id,
                    DatasetSourceConfig.source_name == canonical,
                )
            )

    def list_dataset_source_configs(self, dataset_id: int) -> "list[DatasetSourceConfig]":
        """Semua konfigurasi sumber sebuah dataset, urut S1 -> MODIS -> GPM."""
        with self.session() as sess:
            rows = list(sess.scalars(
                select(DatasetSourceConfig).where(
                    DatasetSourceConfig.dataset_id == dataset_id
                )
            ))
        return sorted(
            rows,
            key=lambda c: SOURCE_NAME_ORDER.index(c.source_name)
            if c.source_name in SOURCE_NAME_ORDER else len(SOURCE_NAME_ORDER),
        )

    def upsert_dataset_source_config(
        self, dataset_id: int, source_name: str, processing_levels
    ) -> "DatasetSourceConfig":
        """Buat atau perbarui konfigurasi satu sumber.

        Dipakai jalur edit: mengubah level satu satelit tanpa menyentuh yang
        lain. Validasinya memakai normalize_source_configs() yang sama dengan
        jalur create, jadi tidak ada aturan yang cuma berlaku di satu jalur.

        Raises:
            ValueError: nama sumber atau level tidak dikenal, atau daftar
                level kosong.
        """
        normalized = normalize_source_configs({source_name: processing_levels})
        canonical, levels = next(iter(normalized.items()))
        with self.session() as sess:
            row = sess.scalar(
                select(DatasetSourceConfig).where(
                    DatasetSourceConfig.dataset_id == dataset_id,
                    DatasetSourceConfig.source_name == canonical,
                )
            )
            if row is None:
                row = DatasetSourceConfig(
                    dataset_id=dataset_id,
                    source_name=canonical,
                    processing_levels=levels,
                )
                sess.add(row)
            else:
                row.processing_levels = levels
                row.updated_at = func.now()
            sess.flush()
        logger.info("[SOURCE_CONFIG] dataset_id=%s %s -> %s", dataset_id, canonical, levels)
        return row

    def create_dataset_with_sources(
        self, dataset_dict: dict, sources_dict: dict
    ) -> "Dataset":
        """Buat dataset beserta konfigurasi per-satelitnya dalam SATU transaksi.

        Dataset tanpa baris dataset_source_config adalah dataset yang tidak
        bisa diproses ETL -- tidak ada satu pun sumber yang dinyatakan. Karena
        itu keduanya harus jadi atau tidak sama sekali: satu session, satu
        commit. Kalau langkah mana pun gagal, session() melakukan rollback dan
        tidak ada baris `datasets` yatim yang tertinggal.

        Args:
            dataset_dict: kolom tabel `datasets`. `required_tiers`,
                `fusion_strategy`, dan `preview_options` opsional --
                required_tiers diturunkan dari sources kalau tidak diisi.
            sources_dict: objek `sources` gaya API, mis.
                {"sentinel1": {"processing": ["RAW", "PROCESSED"]},
                 "modis": {"processing": ["PROCESSED"]}}

        Returns:
            Dataset yang sudah tersimpan, dengan `source_configs` terisi.

        Raises:
            ValueError: sources kosong/tidak valid, kolom dataset tidak
                dikenal, fusion_strategy tidak sesuai jumlah sumber, atau
                preview_options tidak dikenal.
        """
        # Semua validasi selesai SEBELUM session dibuka: payload yang salah
        # tidak perlu menyentuh database sama sekali. CHECK constraint di
        # database tetap ada sebagai jaring pengaman untuk penulis lain.
        configs = normalize_source_configs(sources_dict)

        payload = dict(dataset_dict or {})
        unknown = set(payload) - set(Dataset.__table__.columns.keys())
        if unknown:
            raise ValueError(f"kolom dataset tidak dikenal: {sorted(unknown)}")

        fusion_strategy = _validate_fusion_strategy(
            payload.get("fusion_strategy"), source_count=len(configs)
        )
        # null(), bukan None: kolomnya punya server_default 'FULL_COVERAGE',
        # dan SQLAlchemy tidak bisa membedakan "diisi None" dari "tidak diisi"
        # -- keduanya membuat kolom dihilangkan dari INSERT sehingga server
        # default yang terpakai. Dataset satu sumber akan berakhir punya
        # strategi fusi yang tidak pernah diminta. null() memaksa NULL eksplisit.
        payload["fusion_strategy"] = fusion_strategy if fusion_strategy is not None else null()
        payload["preview_options"] = _validate_preview_options(
            payload.get("preview_options")
        )
        payload["fusion_output_only"] = _validate_fusion_output_only(
            payload.get("fusion_output_only"), fusion_strategy
        )
        tolerance = _validate_s1_tolerance(payload.get("s1_match_tolerance_days"))
        if tolerance is None:
            payload.pop("s1_match_tolerance_days", None)
        else:
            payload["s1_match_tolerance_days"] = tolerance
        if not payload.get("required_tiers"):
            payload["required_tiers"] = derive_required_tiers(
                configs, with_fusion=fusion_strategy is not None
            )

        with self.session() as sess:
            dataset = Dataset(**payload)
            # Baris config ditempel lewat relasi, bukan INSERT terpisah:
            # SQLAlchemy yang mengisi dataset_id-nya setelah flush, jadi tidak
            # ada jendela di mana dataset sudah ada tapi configs belum.
            dataset.source_configs = [
                DatasetSourceConfig(source_name=name, processing_levels=levels)
                for name, levels in configs.items()
            ]
            sess.add(dataset)
            sess.flush()
            # refresh: tarik nilai yang diisi server (dataset_uuid, created_at,
            # dan fusion_strategy yang tadi dikirim sebagai null()) supaya objek
            # yang dikembalikan -- yang lepas dari session dan tidak bisa
            # lazy-load lagi -- membawa isi baris yang sebenarnya, bukan
            # placeholder SQL.
            sess.refresh(dataset)
            dataset_id = dataset.dataset_id
        logger.info(
            "[DATASET] created dataset_id=%s sources=%s fusion=%s tiers=%s",
            dataset_id, configs, fusion_strategy, payload["required_tiers"],
        )
        return dataset

    def get_last_dataset_config(self) -> dict:
        """Konfigurasi dataset terakhir yang dibuat, untuk tombol "Pakai Config
        Sebelumnya" (DOCS/DECISIONS.md D13, DOCS/API.md GET
        /api/datasets/last-config).

        Sengaja TIDAK mengembalikan `name` -- itu keputusan produk: user harus
        sadar mengisi ulang nama supaya tidak tanpa sengaja menduplikasi
        dataset. Rentang tanggal DIIKUTSERTAKAN sebagai preset (bisa diedit
        user di wizard), bukan dikunci.

        Dataset yang sudah di-soft-delete dilewati: config yang dikembalikan
        harus mencerminkan sesuatu yang masih dianggap ada oleh user.

        Returns:
            dict berisi region_id, region_name, sources, fusion_strategy,
            preview_options, date_start, date_end, created_from_dataset_id,
            created_at. Dict KOSONG kalau belum ada dataset sama sekali --
            route API menerjemahkannya jadi 404.
        """
        with self.session() as sess:
            dataset = sess.scalar(
                select(Dataset)
                .where(Dataset.deleted_at.is_(None))
                # dataset_id sebagai pemecah seri: dua dataset bisa dibuat pada
                # timestamp yang sama, dan "terakhir" harus deterministik.
                .order_by(Dataset.created_at.desc(), Dataset.dataset_id.desc())
                .limit(1)
            )
            if dataset is None:
                return {}
            region_name = None
            if dataset.region_id is not None:
                region_name = sess.scalar(
                    select(RegionOfInterest.name).where(
                        RegionOfInterest.region_id == dataset.region_id
                    )
                )
            return {
                "region_id": dataset.region_id,
                "region_name": region_name,
                "sources": source_configs_to_api(list(dataset.source_configs)),
                "fusion_strategy": dataset.fusion_strategy,
                "fusion_output_only": dataset.fusion_output_only,
                "s1_match_tolerance_days": dataset.s1_match_tolerance_days,
                "preview_options": list(dataset.preview_options or []),
                "date_start": dataset.date_start,
                "date_end": dataset.date_end,
                "created_from_dataset_id": dataset.dataset_id,
                "created_at": dataset.created_at,
            }

    def dispose(self) -> None:
        self._engine.dispose()
        logger.info("DatabaseClient disposed. All connections closed.")


class FusionProduct(Base):
    """Multi-sensor (S1 + MODIS + GPM) feature stack registry for ML training."""

    __tablename__ = "fusion_products"
    __table_args__ = (
        # processing_level ikut kunci unik: dataset yang meminta sebuah sumber
        # RAW **dan** PROCESSED menghasilkan DUA stack untuk tanggal yang sama
        # (DOCS/ETL.md, "Which input tier does fusion use?"). Dengan kunci lama
        # (feature_date, region_id) stack kedua akan menimpa yang pertama dan
        # ablation study-nya kehilangan salah satu sisi perbandingan.
        #
        # dataset_id ikut kunci sejak migrasi 021: tanpa itu dua dataset atas
        # AOI dan tanggal yang sama berbagi satu baris, dan dataset yang selesai
        # terakhir menimpa feature_stack_path dataset lain.
        UniqueConstraint(
            "dataset_id", "feature_date", "processing_level",
            name="uq_fusion_dataset_date_level",
        ),
    )

    fusion_id          = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id         = Column(Integer, ForeignKey("datasets.dataset_id",
                                                     ondelete="CASCADE"))
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
    # Kolom metadata fusi (migrasi 017). Sebelumnya ada di database tapi tidak
    # di model ini, jadi ORM tidak bisa membaca/menulisnya sama sekali.
    fusion_strategy    = Column(String(20))
    # RAW | PROCESSED -- level input yang dipakai stack ini, bukan tier-nya.
    processing_level   = Column(String(20), server_default=text("'PROCESSED'"))
    temporal_offset_modis = Column(Integer)
    temporal_offset_gpm   = Column(Integer)
    # Migrasi 019: jarak hari S1 yang benar-benar terpakai. 0 = same-day,
    # NULL = hari itu tanpa S1 (group sentinel1/ berisi NaN).
    s1_offset_days     = Column(SmallInteger)
    created_at         = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

    def __repr__(self) -> str:
        return (
            f"<FusionProduct id={self.fusion_id} date={self.feature_date} "
            f"level={self.processing_level} path={self.feature_stack_path}>"
        )