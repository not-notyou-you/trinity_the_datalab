# DATA DICTIONARY
## Sentinel-1 Flood Detection Data Pipeline

**Author:** Julius Marselinus (BRONTO) — NIM 00000111989  
**Program:** Sistem Informasi — Universitas Multimedia Nusantara  
**Database:** PostgreSQL 14+ with PostGIS and TimescaleDB

---

## Table Index

| # | Table | Purpose | TimescaleDB |
|---|-------|---------|-------------|
| 1 | `regions_of_interest` | AOI master data | — |
| 2 | `processing_stages` | Pipeline stage config | — |
| 3 | `satellite_scenes` | Scene registry | ✅ hypertable |
| 4 | `processing_jobs` | Job execution log | — |
| 5 | `data_products` | Output artifact registry | — |
| 6 | `quality_metrics` | Radiometric QA results | — |
| 7 | `processing_rules` | Configurable QA rules | — |
| 8 | `data_lineage` | Transformation DAG | — |
| 9 | `api_access_logs` | API audit trail | ✅ hypertable |
| 10 | `alert_events` | Monitoring events | ✅ hypertable |
| 11 | `dataset_versions` | Semantic versioning | — |

---

## 1. `regions_of_interest`

Master table of geographic Areas of Interest for Sentinel-1 coverage.

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `region_id` | SERIAL | PK | Surrogate primary key |
| `region_code` | VARCHAR(20) | UNIQUE, NOT NULL | Short identifier: `JKT`, `JABODTK` |
| `name` | VARCHAR(100) | NOT NULL | Full region name |
| `description` | TEXT | NULLABLE | Narrative description |
| `bbox` | GEOMETRY(POLYGON,4326) | NOT NULL | WGS84 bounding polygon |
| `centroid` | GEOMETRY(POINT,4326) | AUTO | Auto-computed via trigger |
| `area_km2` | NUMERIC(12,4) | NULLABLE | Surface area in km² |
| `admin_level` | SMALLINT | NOT NULL, DEFAULT 2 | 1=national, 2=province, 3=city |
| `country_code` | CHAR(2) | NOT NULL, DEFAULT 'ID' | ISO 3166-1 alpha-2 |
| `is_active` | BOOLEAN | NOT NULL, DEFAULT TRUE | Soft-delete flag |
| `created_at` | TIMESTAMPTZ | NOT NULL | Record creation time |
| `updated_at` | TIMESTAMPTZ | NOT NULL, AUTO | Last modification time |

---

## 2. `processing_stages`

Ordered ETL pipeline stage definitions. Controls execution sequence and retry policy.

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `stage_id` | SERIAL | PK | Surrogate primary key |
| `stage_name` | VARCHAR(50) | UNIQUE, NOT NULL | `DOWNLOAD`, `CROP`, `LEE_FILTER`, etc. |
| `stage_code` | VARCHAR(20) | UNIQUE, NOT NULL | Short code: `DL`, `CR`, `LF`, `CE`, `OR`, `QA` |
| `stage_order` | SMALLINT | UNIQUE, NOT NULL | Execution sequence (1–6) |
| `description` | TEXT | NULLABLE | Stage purpose |
| `timeout_minutes` | SMALLINT | NOT NULL, DEFAULT 60 | Max allowed execution time |
| `retry_count` | SMALLINT | NOT NULL, DEFAULT 3 | Max retry attempts on failure |
| `retry_delay_sec` | SMALLINT | NOT NULL, DEFAULT 30 | Wait between retries (seconds) |
| `is_mandatory` | BOOLEAN | NOT NULL, DEFAULT TRUE | Whether stage can be skipped |
| `is_active` | BOOLEAN | NOT NULL, DEFAULT TRUE | Enable/disable stage |
| `created_at` | TIMESTAMPTZ | NOT NULL | Record creation time |
| `updated_at` | TIMESTAMPTZ | NOT NULL, AUTO | Last modification time |

**Seeded values:**

| stage_order | stage_name | stage_code | timeout_min |
|-------------|------------|------------|-------------|
| 1 | DOWNLOAD | DL | 120 |
| 2 | CROP | CR | 30 |
| 3 | LEE_FILTER | LF | 45 |
| 4 | COG_EXPORT | CE | 30 |
| 5 | ORCHESTRATE | OR | 10 |
| 6 | QUALITY_ANALYTICS | QA | 30 |

---

## 3. `satellite_scenes`

Core registry of all Sentinel-1 SAR scenes. Root entity — all other tables reference this. **TimescaleDB hypertable** partitioned by `acquisition_datetime` (1-month chunks).

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `scene_id` | SERIAL | PK | Surrogate primary key |
| `scene_uuid` | UUID | UNIQUE, NOT NULL, AUTO | External-safe identifier |
| `product_identifier` | VARCHAR(200) | UNIQUE, NOT NULL | ESA Copernicus Hub product ID |
| `platform` | VARCHAR(20) | NOT NULL, DEFAULT 'SENTINEL-1' | Satellite platform |
| `instrument_mode` | VARCHAR(10) | NOT NULL, DEFAULT 'IW' | IW/EW/SM/WV acquisition mode |
| `polarization_vv` | BOOLEAN | NOT NULL, DEFAULT TRUE | VV band availability |
| `polarization_vh` | BOOLEAN | NOT NULL, DEFAULT TRUE | VH band availability |
| `acquisition_datetime` | TIMESTAMPTZ | NOT NULL | UTC SAR acquisition time (hypertable axis) |
| `orbit_number` | INTEGER | NULLABLE | Absolute orbit number |
| `orbit_direction` | ENUM | NOT NULL | `ASCENDING` or `DESCENDING` |
| `relative_orbit` | SMALLINT | NULLABLE | Relative orbit (1–175) |
| `bbox` | GEOMETRY(POLYGON,4326) | NOT NULL | Scene footprint polygon |
| `cloud_cover_percent` | NUMERIC(5,2) | CHECK 0–100 | Optical cloud coverage estimate |
| `incidence_angle_near` | NUMERIC(6,3) | NULLABLE | Near-range incidence angle (°) |
| `incidence_angle_far` | NUMERIC(6,3) | NULLABLE | Far-range incidence angle (°) |
| `resolution_m` | SMALLINT | NOT NULL, DEFAULT 10 | Ground resolution (meters) |
| `region_id` | INTEGER | FK→ROI, NOT NULL | Associated AOI |
| `raw_file_path` | TEXT | NULLABLE | Local path to downloaded archive |
| `raw_file_size_mb` | NUMERIC(12,3) | NULLABLE | Raw download size (MB) |
| `download_url` | TEXT | NULLABLE | Copernicus Hub URL |
| `checksum_md5` | VARCHAR(32) | NULLABLE | MD5 integrity check |
| `is_available` | BOOLEAN | NOT NULL, DEFAULT TRUE | Data availability flag |
| `created_at` | TIMESTAMPTZ | NOT NULL | Record creation time |
| `updated_at` | TIMESTAMPTZ | NOT NULL, AUTO | Last modification time |

---

## 4. `processing_jobs`

ETL job execution log. One row per scene × stage × attempt. Enables retry tracking and performance analysis.

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `job_id` | BIGSERIAL | PK | Surrogate primary key |
| `job_uuid` | UUID | UNIQUE, NOT NULL, AUTO | External-safe job reference |
| `scene_id` | INTEGER | FK→scenes, NOT NULL | Scene being processed |
| `stage_id` | INTEGER | FK→stages, NOT NULL | Pipeline stage |
| `attempt_number` | SMALLINT | NOT NULL, DEFAULT 1 | Retry counter |
| `status` | ENUM | NOT NULL, DEFAULT 'QUEUED' | `QUEUED`/`RUNNING`/`SUCCESS`/`FAILED`/`CANCELLED` |
| `queued_at` | TIMESTAMPTZ | NOT NULL | When job entered queue |
| `started_at` | TIMESTAMPTZ | NULLABLE | Execution start time |
| `completed_at` | TIMESTAMPTZ | NULLABLE | Execution end time |
| `duration_seconds` | NUMERIC(10,3) | GENERATED | Auto: `completed_at - started_at` |
| `worker_hostname` | VARCHAR(100) | NULLABLE | Executing machine hostname |
| `cpu_usage_percent` | NUMERIC(5,2) | NULLABLE | CPU utilization |
| `memory_usage_mb` | NUMERIC(10,2) | NULLABLE | RAM usage |
| `input_size_mb` | NUMERIC(12,3) | NULLABLE | Input file size |
| `output_size_mb` | NUMERIC(12,3) | NULLABLE | Output file size |
| `error_code` | VARCHAR(50) | NULLABLE | Structured error code |
| `error_message` | TEXT | NULLABLE | Full error traceback |
| `log_file_path` | TEXT | NULLABLE | Path to execution log |
| `parameters_json` | JSONB | NOT NULL, DEFAULT '{}' | ETL parameters used |
| `created_at` | TIMESTAMPTZ | NOT NULL | Record creation time |
| `updated_at` | TIMESTAMPTZ | NOT NULL, AUTO | Last modification time |

---

## 5. `data_products`

Output artifact registry. Tracks every file produced by each ETL stage.

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `product_id` | BIGSERIAL | PK | Surrogate primary key |
| `product_uuid` | UUID | UNIQUE, NOT NULL, AUTO | External-safe reference |
| `scene_id` | INTEGER | FK→scenes, NOT NULL | Parent scene |
| `job_id` | BIGINT | FK→jobs, NOT NULL | Producing ETL job |
| `product_tier` | ENUM | NOT NULL | `RAW`/`BRONZE`/`SILVER`/`GOLD` |
| `product_type` | VARCHAR(50) | NOT NULL | `ORIGINAL_TIFF`, `CROPPED_TIFF`, `LEE_FILTERED`, `COG` |
| `band_name` | VARCHAR(10) | NOT NULL | `VV`, `VH`, or `VV_VH` |
| `file_name` | VARCHAR(255) | NOT NULL | Filename only |
| `file_path` | TEXT | NOT NULL | Full storage path |
| `file_size_mb` | NUMERIC(12,3) | NOT NULL | File size (MB) |
| `file_format` | VARCHAR(20) | NOT NULL, DEFAULT 'TIFF' | `TIFF`, `COG`, `NetCDF` |
| `data_hash_sha256` | VARCHAR(64) | NOT NULL | SHA-256 content hash |
| `crs` | VARCHAR(50) | NOT NULL, DEFAULT 'EPSG:4326' | Coordinate reference system |
| `pixel_size_m` | NUMERIC(8,3) | NULLABLE | Ground sampling distance (m) |
| `nodata_value` | NUMERIC | NULLABLE | NoData sentinel value |
| `rows` | INTEGER | NULLABLE | Raster row count |
| `cols` | INTEGER | NULLABLE | Raster column count |
| `band_count` | SMALLINT | NOT NULL, DEFAULT 1 | Number of bands |
| `storage_location` | ENUM | NOT NULL, DEFAULT 'LOCAL' | `LOCAL`/`S3`/`GCS`/`AZURE_BLOB` |
| `is_valid` | BOOLEAN | NOT NULL, DEFAULT TRUE | Integrity check passed |
| `is_latest` | BOOLEAN | NOT NULL, DEFAULT TRUE | Latest version flag |
| `created_at` | TIMESTAMPTZ | NOT NULL | Record creation time |
| `updated_at` | TIMESTAMPTZ | NOT NULL, AUTO | Last modification time |

**Product tier progression:**

```
RAW (download) → BRONZE (crop) → SILVER (Lee filter) → GOLD (COG export)
```

---

## 6. `quality_metrics`

Per-scene, per-band radiometric validation results.

| Column | Type | Constraint | Purpose |
|--------|------|------------|---------|
| `metric_id` | BIGSERIAL | PK | Surrogate primary key |
| `scene_id` | INTEGER | FK→scenes, NOT NULL | Parent scene |
| `product_id` | BIGINT | FK→products, NOT NULL | Assessed product |
| `band_name` | VARCHAR(10) | NOT NULL | `VV` or `VH` |
| `assessed_at` | TIMESTAMPTZ | NOT NULL | Assessment timestamp |
| `total_pixels` | BIGINT | NOT NULL | Total pixel count |
| `valid_pixels` | BIGINT | NOT NULL | Non-nodata pixel count |
| `nodata_pixels` | BIGINT | NOT NULL | NoData pixel count |
| `nodata_percent` | NUMERIC(5,2) | GENERATED | `nodata_pixels/total×100` |
| `backscatter_mean_db` | NUMERIC(8,4) | NULLABLE | Mean backscatter (dB) |
| `backscatter_std_db` | NUMERIC(8,4) | NULLABLE | Std dev backscatter (dB) |
| `backscatter_min_db` | NUMERIC(8,4) | NULLABLE | Min backscatter (dB) |
| `backscatter_max_db` | NUMERIC(8,4) | NULLABLE | Max backscatter (dB) |
| `cloud_threshold_percent` | NUMERIC(5,2) | NOT NULL | Cloud mask threshold used |
| `radiometric_consistency` | BOOLEAN | NULLABLE | Radiometric check result |
| `speckle_index` | NUMERIC(8,4) | NULLABLE | Coefficient of variation |
| `quality_score` | NUMERIC(5,2) | NOT NULL, CHECK 0–100 | Composite score (0–100) |
| `quality_flag` | VARCHAR(20) | NOT NULL | `PASS`/`FAIL`/`WARNING`/`UNCHECKED` |
| `notes` | TEXT | NULLABLE | Analyst annotation |
| `created_at` | TIMESTAMPTZ | NOT NULL | Record creation time |

**Quality score formula:**
```
score = nodata_component (50 pts)
      + speckle_component (30 pts)
      + radiometric_component (20 pts)
```

---

## 7–11. Remaining Tables

| Table | Key Fields | Notes |
|-------|-----------|-------|
| `processing_rules` | `rule_id`, `stage_id`, `rule_code`, `threshold_value` | Configurable QA thresholds per stage. `action_on_fail`: WARN/FAIL/SKIP |
| `data_lineage` | `lineage_id`, `parent_product_id`, `child_product_id`, `transformation_type` | DAG of transformations. UNIQUE(parent, child). Self-reference forbidden |
| `api_access_logs` | `log_id`, `endpoint`, `user_ip`, `response_status`, `response_time_ms` | High-volume TimescaleDB hypertable (7-day chunks). Full audit trail |
| `alert_events` | `alert_id`, `event_type`, `severity`, `is_resolved` | TimescaleDB hypertable. Auto-triggered on quality FAIL. Severity: INFO/WARNING/CRITICAL |
| `dataset_versions` | `version_id`, `product_id`, `version_number`, `is_production` | SemVer tracking per GOLD product. Supports deprecation and rollback |

---

## ENUM Types

| Type Name | Values |
|-----------|--------|
| `orbit_direction_enum` | `ASCENDING`, `DESCENDING` |
| `job_status_enum` | `QUEUED`, `RUNNING`, `SUCCESS`, `FAILED`, `CANCELLED` |
| `product_tier_enum` | `RAW`, `BRONZE`, `SILVER`, `GOLD` |
| `storage_location_enum` | `LOCAL`, `S3`, `GCS`, `AZURE_BLOB` |
| `rule_type_enum` | `THRESHOLD`, `TRANSFORMATION`, `VALIDATION`, `FILTER` |
| `alert_severity_enum` | `INFO`, `WARNING`, `CRITICAL` |
| `alert_event_type_enum` | `DATA_ARRIVAL`, `QUALITY_WARNING`, `PIPELINE_ERROR`, `THRESHOLD_BREACH`, `SYSTEM_ALERT` |
| `http_method_enum` | `GET`, `POST`, `PUT`, `PATCH`, `DELETE` |
