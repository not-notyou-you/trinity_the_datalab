# api/schemas.py
from __future__ import annotations
from datetime import date, datetime
from typing import Any
from uuid import UUID
from pydantic import BaseModel, Field, field_validator, model_validator


class OkResponse(BaseModel):
    ok: bool = True
    message: str = "Success"


class SceneListItem(BaseModel):
    scene_id: int
    scene_uuid: str
    product_identifier: str
    platform: str
    instrument_mode: str
    polarization_vv: bool
    polarization_vh: bool
    acquisition_datetime: datetime
    orbit_direction: str
    orbit_number: int | None
    relative_orbit: int | None
    cloud_cover_percent: float | None
    resolution_m: int
    region_id: int
    is_available: bool
    created_at: datetime
    model_config = {"from_attributes": True}


class SceneDetail(SceneListItem):
    raw_file_path: str | None
    raw_file_size_mb: float | None
    download_url: str | None
    checksum_md5: str | None
    incidence_angle_near: float | None
    incidence_angle_far: float | None
    updated_at: datetime


class SceneListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[SceneListItem]


class SceneQueryParams(BaseModel):
    region_id: int | None = None
    orbit_direction: str | None = Field(None, pattern="^(ASCENDING|DESCENDING)$")
    date_from: datetime | None = None
    date_to: datetime | None = None
    min_quality: float | None = Field(None, ge=0, le=100)
    only_gold: bool = False
    limit: int = Field(20, ge=1, le=200)
    offset: int = Field(0, ge=0)


class ProductItem(BaseModel):
    product_id: int
    product_uuid: str
    scene_id: int
    job_id: int
    dataset_id: int | None = None
    product_tier: str
    source: str
    product_type: str
    band_name: str
    file_name: str
    file_path: str
    file_size_mb: float
    file_format: str
    data_hash_sha256: str
    crs: str
    pixel_size_m: float | None
    rows: int | None
    cols: int | None
    band_count: int
    storage_location: str
    is_valid: bool
    is_latest: bool
    created_at: datetime
    model_config = {"from_attributes": True}


class ProductListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[ProductItem]


class IntegrityCheckResponse(BaseModel):
    product_id: int
    file_path: str
    stored_hash: str
    computed_hash: str
    integrity_ok: bool
    file_size_mb: float


class QualityMetricItem(BaseModel):
    metric_id: int
    scene_id: int
    product_id: int
    band_name: str
    assessed_at: datetime
    total_pixels: int
    valid_pixels: int
    nodata_pixels: int
    nodata_percent: float | None
    backscatter_mean_db: float | None
    backscatter_std_db: float | None
    backscatter_min_db: float | None
    backscatter_max_db: float | None
    cloud_threshold_percent: float
    radiometric_consistency: bool | None
    speckle_index: float | None
    quality_score: float
    quality_flag: str
    notes: str | None
    created_at: datetime
    model_config = {"from_attributes": True}


class QualityResponse(BaseModel):
    scene_id: int
    bands: list[QualityMetricItem]
    overall_quality: str


class LineageStep(BaseModel):
    lineage_id: int
    parent_product_id: int
    child_product_id: int
    transformation_type: str
    stage_id: int
    job_id: int
    transformation_params: dict[str, Any]
    input_checksum: str | None
    output_checksum: str | None
    created_at: datetime
    # Sejak layout tier-source, satu dataset punya rantai paralel per sensor;
    # ketiga field ini yang membuat langkah rantai bisa dibaca per source
    # tanpa menarik tiap product satu per satu.
    source: str | None = None
    parent_tier: str | None = None
    child_tier: str | None = None


class LineageResponse(BaseModel):
    product_id: int
    direction: str
    chain: list[LineageStep]
    total_steps: int


class JobStatusItem(BaseModel):
    job_id: int
    stage_name: str
    stage_order: int
    attempt_number: int
    status: str
    queued_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None


class PipelineStatusResponse(BaseModel):
    scene_id: int
    stages: list[JobStatusItem]
    overall_status: str


class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    pool_size: int | None
    checked_out: int | None
    api_version: str = "1.0.0"
    timestamp: datetime


class DatasetQualitySettings(BaseModel):
    min_cloud_cover: float | None = None
    min_quality_score: float | None = None
    resolution_m: int | None = None


class DatasetCreateRequest(BaseModel):
    # Jalur utama: UI mengirim region_id dari tabel lokasi. `location` tetap
    # diterima untuk pemanggil lama (dan CLI) — di-resolve lewat nama/geocoding.
    region_id: int | None = None
    location: str | None = None
    date_start: date
    date_end: date
    tiers: list[str] = Field(default_factory=lambda: ["GOLD"])
    name: str
    description: str | None = None
    quality_settings: DatasetQualitySettings | None = None
    # Default True: preview murah dan berguna untuk riset, jadi opt-out, bukan
    # opt-in. Pemanggil lama yang tidak mengirim field ini tetap dapat perilaku
    # lamanya (PREVIEW jalan).
    generate_preview: bool = True

    @field_validator("tiers")
    @classmethod
    def _validate_tiers(cls, v: list[str]) -> list[str]:
        from etl.dataset_manager import TIER_ORDER

        allowed = set(TIER_ORDER)
        normalized = [t.upper() for t in v]
        invalid = set(normalized) - allowed
        if invalid:
            raise ValueError(f"Invalid tiers: {invalid}. Valid: {allowed}")
        if not normalized:
            raise ValueError("tiers must not be empty")
        return normalized

    @model_validator(mode="after")
    def _validate_date_range(self) -> "DatasetCreateRequest":
        if self.date_end < self.date_start:
            raise ValueError("date_end must be >= date_start")
        return self

    @model_validator(mode="after")
    def _require_location(self) -> "DatasetCreateRequest":
        if self.region_id is None and not (self.location or "").strip():
            raise ValueError("Isi region_id atau location")
        return self


class DatasetCreateResponse(BaseModel):
    dataset_id: int
    job_id: int
    status: str


class DatasetItem(BaseModel):
    dataset_id: int
    dataset_uuid: str
    name: str
    description: str | None
    location_label: str | None
    date_start: date
    date_end: date
    required_tiers: list[str]
    dataset_kind: str
    status: str
    total_scenes: int
    completed_scenes: int
    failed_scenes: int
    total_size_bytes: int
    is_deletable: bool
    generate_preview: bool
    live_enabled: bool
    created_at: datetime
    updated_at: datetime
    model_config = {"from_attributes": True}


class DatasetDetail(DatasetItem):
    bbox_wkt: str
    region_id: int | None
    quality_settings: dict[str, Any]
    live_last_checked_at: datetime | None
    deleted_at: datetime | None


class DatasetListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DatasetItem]


class SceneJobStateItem(BaseModel):
    id: int
    job_id: int
    product_identifier: str
    scene_id: int | None
    current_stage: str | None
    stage_status: str
    attempt_number: int
    max_retries: int
    last_error: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    model_config = {"from_attributes": True}


class DatasetJobItem(BaseModel):
    job_id: int
    job_uuid: str
    dataset_id: int
    job_type: str
    status: str
    paused_at: datetime | None
    paused_by: str | None
    pause_reason: str | None
    resumed_at: datetime | None
    resume_count: int
    total_scenes: int
    downloaded_count: int
    processed_count: int
    failed_count: int
    cleaned_count: int
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    model_config = {"from_attributes": True}


class DatasetProgressResponse(BaseModel):
    dataset_id: int
    job_id: int | None
    status: str
    total_scenes: int
    downloaded_count: int
    processed_count: int
    failed_count: int
    cleaned_count: int
    progress_percent: int
    paused: bool
    pause_reason: str | None
    scenes: list[SceneJobStateItem]


class DatasetPauseRequest(BaseModel):
    reason: str = "user_requested"


class DatasetPauseResponse(BaseModel):
    status: str
    reason: str | None


class DatasetResumeResponse(BaseModel):
    status: str
    resume_count: int


class DatasetDeleteResponse(BaseModel):
    status: str
    dataset_id: int


class DatasetCancelRequest(BaseModel):
    cascade_delete: bool = True


class DatasetCancelResponse(BaseModel):
    status: str
    deleted_files: int
    retained_tier: str


class DatasetLogEntry(BaseModel):
    log_id: int
    timestamp: datetime
    module: str
    dataset_id: int
    scene_id: str
    stage: str
    status: str
    message: str
    details: dict


class DatasetLogsResponse(BaseModel):
    total: int
    limit: int
    logs: list[DatasetLogEntry]


class CleanupOperationItem(BaseModel):
    id: int
    dataset_id: int
    job_id: int | None
    operation_type: str
    status: str
    total_files: int
    deleted_count: int
    freed_bytes: int
    error_log: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    model_config = {"from_attributes": True}


class DeletionProgressResponse(BaseModel):
    status: str
    total_files: int
    deleted_count: int
    freed_bytes: int
    progress_percent: int


class LiveSourceItem(BaseModel):
    source_name: str
    enabled: bool
    last_check: datetime | None
    last_ingest: datetime | None
    next_check: datetime | None
    model_config = {"from_attributes": True}


class LiveStatusResponse(BaseModel):
    dataset_id: int
    enabled: bool
    status: str
    required_tiers: list[str]
    bbox_wkt: str
    total_size_bytes: int
    last_checked_at: datetime | None
    sources: list[LiveSourceItem]


class LiveToggleRequest(BaseModel):
    enabled: bool


class LiveToggleResponse(BaseModel):
    enabled: bool


class LiveClearResponse(BaseModel):
    status: str
    freed_bytes: int
    deleted_count: int


class LiveBackfillRequest(BaseModel):
    date_start: date
    date_end: date

    @model_validator(mode="after")
    def _validate_date_range(self) -> "LiveBackfillRequest":
        if self.date_end < self.date_start:
            raise ValueError("date_end must be >= date_start")
        return self


class LiveBackfillResponse(BaseModel):
    status: str
    job_id: int
    date_range: str


class LiveSceneItem(BaseModel):
    product_id: int
    scene_date: datetime
    tier: str
    size_mb: float


class SourceStorageItem(BaseModel):
    """Pemakaian disk satu source di dalam satu tier."""
    size_bytes: int
    size_mb: float
    file_count: int
    scene_count: int | None = None


class TierStorageItem(BaseModel):
    """Pemakaian disk satu tier, dipecah per source.

    `sources` kosong untuk tier fusion: isinya gabungan semua source, jadi
    tidak ada pecahan per-source yang bermakna di sana.
    """
    size_bytes: int
    size_mb: float
    file_count: int
    scene_count: int
    sources: dict[str, SourceStorageItem] = Field(default_factory=dict)


class DatasetStorageSummary(BaseModel):
    dataset_id: int
    tiers: dict[str, TierStorageItem]
    sources: dict[str, SourceStorageItem]
    total_size_bytes: int
    total_size_mb: float


class DatasetFileItem(BaseModel):
    name: str
    path: str
    size_mb: float


class DatasetSceneFiles(BaseModel):
    scene: str
    source: str | None = None
    files: list[DatasetFileItem]


class DatasetTierFilesResponse(BaseModel):
    dataset_id: int
    tier: str
    source: str | None = None
    scenes: list[DatasetSceneFiles]


class SourceQualityItem(BaseModel):
    """Kualitas satu source untuk satu dataset.

    Cuma SENTINEL1 yang punya metrik radiometrik sungguhan (quality_metrics,
    dihitung module6_analytics atas band VV/VH). Untuk MODIS/GPM yang
    dilaporkan adalah *coverage*: berapa band/hari yang benar-benar mendarat
    di disk dibanding yang diharapkan — bukan skor radiometrik, dan sengaja
    dibedakan namanya supaya tidak dibaca sebagai hal yang sama.
    """
    source: str
    kind: str                       # "RADIOMETRIC" | "COVERAGE"
    product_count: int
    scene_count: int
    quality_score: float | None = None
    quality_flag: str | None = None
    bands: dict[str, float] = Field(default_factory=dict)


class DatasetQualityBySourceResponse(BaseModel):
    dataset_id: int
    sources: list[SourceQualityItem]


class RegionItem(BaseModel):
    region_id: int
    region_code: str
    name: str
    description: str | None
    bbox: list[float]          # [min_lon, min_lat, max_lon, max_lat]
    area_km2: float | None
    source: str = "SEEDER"     # SEEDER | USER | GEOCODE
    created_at: datetime | None = None
    # Lokasi bawaan sistem tidak boleh dihapus dari UI; front-end memakai flag ini
    # untuk menyembunyikan tombol hapus, tapi API tetap yang menegakkan aturannya.
    deletable: bool = True


class RegionListResponse(BaseModel):
    items: list[RegionItem]
    total: int = 0


class RegionCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float
    description: str | None = None
    region_code: str | None = Field(default=None, max_length=20)

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Nama lokasi tidak boleh kosong")
        return v

    @model_validator(mode="after")
    def _validate_bbox(self) -> "RegionCreateRequest":
        # Aturan bbox dipinjam dari etl.geo_utils supaya API, UI, dan pipeline
        # memakai definisi "bbox sah" yang sama persis.
        from etl.geo_utils import validate_bbox

        self.min_lon, self.min_lat, self.max_lon, self.max_lat = validate_bbox(
            self.min_lon, self.min_lat, self.max_lon, self.max_lat
        )
        return self


class RegionUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    description: str | None = None

    @model_validator(mode="after")
    def _at_least_one(self) -> "RegionUpdateRequest":
        if self.name is None and self.description is None:
            raise ValueError("Tidak ada perubahan: isi name atau description")
        if self.name is not None:
            self.name = self.name.strip()
            if not self.name:
                raise ValueError("Nama lokasi tidak boleh kosong")
        return self


class GeocodeItem(BaseModel):
    name: str
    display_name: str
    bbox: list[float]
    type: str | None = None


class GeocodeSearchResponse(BaseModel):
    items: list[GeocodeItem]