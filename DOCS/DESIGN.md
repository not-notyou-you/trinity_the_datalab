# Database Design

## ER Relationships (Text Diagram)

```
regions_of_interest (1) ──── (N) datasets
datasets (1) ──── (N) dataset_source_config    # per-satellite processing config
datasets (1) ──── (N) dataset_jobs
dataset_jobs (1) ──── (N) scene_job_state
dataset_jobs (1) ──── (N) processing_logs

satellite_scenes (1) ──── (N) processing_jobs
satellite_scenes (1) ──── (N) data_products
satellite_scenes (1) ──── (N) quality_metrics

data_products (M) ── data_lineage ── (N) data_products   # provenance graph
fusion_products (1) ──── (1) data_products[FUSION]
                    ──── (0..1) data_products[S1 GOLD]
                    ──── (0..1) data_products[MODIS GOLD]
                    ──── (0..1) data_products[GPM GOLD]
```

## Tables

### datasets — Core configuration table (DataLab-specific fields marked ★)

| Column | Type | Notes |
|---|---|---|
| dataset_id | SERIAL PK | |
| dataset_name | VARCHAR(255) | |
| region_id | INT FK → regions_of_interest | |
| bbox | GEOMETRY(Polygon, 4326) | AOI as WGS84 polygon |
| date_start | DATE | |
| date_end | DATE | |
| ★ fusion_strategy | VARCHAR(20) | `CO_OCCURRENCE`, `FULL_COVERAGE`, `HYBRID`, or NULL (single source) |
| ★ preview_options | TEXT[] | `{'GRAYSCALE','COLORED','COMPOSITE'}` or empty |
| required_tiers | TEXT[] | Derived internally from source configs |
| dataset_kind | VARCHAR(20) | `STANDARD` or `LIVE` |
| status | VARCHAR(20) | `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `PAUSED`, `CANCELLED` |
| created_at | TIMESTAMPTZ | DEFAULT NOW() |
| completed_at | TIMESTAMPTZ | NULL until done |

Note: `selected_satellites` and `processing_level` are NOT on this table — they live in `dataset_source_config` because each satellite has its own processing definition.

**Constraint**: `fusion_strategy` must be NULL when only 1 source is configured.

### ★ dataset_source_config — Per-satellite processing configuration

| Column | Type | Notes |
|---|---|---|
| config_id | SERIAL PK | |
| dataset_id | INT FK → datasets ON DELETE CASCADE | |
| source_name | VARCHAR(20) NOT NULL | `SENTINEL1`, `MODIS`, `GPM` |
| processing_levels | TEXT[] NOT NULL | `{'RAW'}`, `{'PROCESSED'}`, or `{'RAW','PROCESSED'}` |
| UNIQUE | (dataset_id, source_name) | One config row per source per dataset |

**What RAW vs PROCESSED means per satellite:**

| Source | RAW | PROCESSED |
|---|---|---|
| SENTINEL1 | Calibrate + reproject + crop (no Lee filter, no QA) → BRONZE | + Lee filter 7×7 + QA analytics + COG export → SILVER → GOLD |
| MODIS | Flood map extract + reproject + crop (no derived indices) → BRONZE | + NDVI + NDWI computation → SILVER → GOLD |
| GPM | Daily rainfall extract + crop (single day only) → BRONZE | + Accumulation windows 24h/72h/7d → SILVER → GOLD |

**Constraint**: `processing_levels` must contain at least one value.

### dataset_sources — Per-satellite enable flags for live ingestion

| Column | Type | Notes |
|---|---|---|
| source_id | SERIAL PK | |
| dataset_id | INT FK → datasets | |
| source_name | VARCHAR(20) | `SENTINEL1`, `MODIS`, `GPM` |
| enabled | BOOLEAN | DEFAULT TRUE |
| last_checked_at | TIMESTAMPTZ | |
| last_ingest_at | TIMESTAMPTZ | |

### satellite_scenes — Sentinel-1 scene metadata

| Column | Type | Notes |
|---|---|---|
| scene_id | SERIAL PK | |
| dataset_id | INT FK → datasets | |
| product_identifier | VARCHAR(255) UNIQUE | Full SAFE name |
| acquisition_datetime | TIMESTAMPTZ | |
| orbit_direction | VARCHAR(20) | `ASCENDING` or `DESCENDING` |
| geometry | GEOMETRY(Polygon, 4326) | Scene footprint |
| source | VARCHAR(20) | `SENTINEL1` |
| created_at | TIMESTAMPTZ | |

### nasa_scenes — MODIS/GPM scene metadata

| Column | Type | Notes |
|---|---|---|
| nasa_scene_id | SERIAL PK | |
| dataset_id | INT FK → datasets | |
| source | VARCHAR(20) | `MODIS` or `GPM` |
| product_id | VARCHAR(255) | Granule identifier |
| acquisition_date | DATE | |
| tile_id | VARCHAR(20) | MODIS tile (e.g. h30v08), NULL for GPM |

### data_products — Registry of every output file

| Column | Type | Notes |
|---|---|---|
| product_id | SERIAL PK | |
| scene_id | INT FK → satellite_scenes (nullable for NASA) | |
| nasa_scene_id | INT FK → nasa_scenes (nullable for S1) | |
| dataset_id | INT FK → datasets | |
| product_tier | VARCHAR(20) | `RAW`, `BRONZE`, `SILVER`, `GOLD`, `PREVIEW`, `FUSION` |
| source | VARCHAR(20) | `SENTINEL1`, `MODIS`, `GPM`, `FUSION` |
| ★ processing_level | VARCHAR(20) | `RAW` or `PROCESSED` — which level produced this artifact |
| band_name | VARCHAR(20) | `VV`, `NDVI`, `RAIN_24H`, … and for tier FUSION: `FUSION_RAW` / `FUSION_PROCESSED` |
| file_path | TEXT | Relative to DATA_DIR |
| size_bytes | BIGINT | |
| format | VARCHAR(20) | `GEOTIFF`, `COG`, `HDF5`, `PNG`, `JSON` |
| checksum | VARCHAR(64) | SHA-256 |
| is_valid | BOOLEAN | DEFAULT TRUE, set FALSE on cleanup |
| created_at | TIMESTAMPTZ | |

### data_lineage — Provenance graph (parent → child)

| Column | Type | Notes |
|---|---|---|
| lineage_id | SERIAL PK | |
| source_product_id | INT FK → data_products | |
| target_product_id | INT FK → data_products | |
| transformation_type | VARCHAR(50) | e.g. `CALIBRATE`, `LEE_FILTER`, `COMPUTE_NDVI`, `ACCUMULATE_RAIN`, `FUSE` |
| checksum_source | VARCHAR(64) | SHA-256 of input at transform time |
| checksum_target | VARCHAR(64) | SHA-256 of output |

### fusion_products — HDF5 fusion-specific metadata

| Column | Type | Notes |
|---|---|---|
| fusion_product_id | SERIAL PK | |
| dataset_id | INT FK → datasets | |
| fusion_date | DATE | |
| ★ fusion_strategy | VARCHAR(20) | Strategy used for this specific fusion |
| ★ processing_level | VARCHAR(20) | `RAW` or `PROCESSED` — which tier inputs were used |
| s1_product_id | INT FK → data_products (nullable) | |
| modis_product_id | INT FK → data_products (nullable) | |
| gpm_product_id | INT FK → data_products (nullable) | |
| fusion_file_path | TEXT | |
| ★ temporal_offset_modis | INT | Days offset from S1 date |
| ★ temporal_offset_gpm | INT | Days offset from S1 date |

**Constraint**: `UNIQUE (fusion_date, region_id, processing_level)`. The level is
part of the key because a dataset that requests a source at both RAW and
PROCESSED produces two stacks for the same date; without it, the second
overwrites the first (migration 018).

### quality_metrics — Per-band quality scores (Sentinel-1 only)

| Column | Type | Notes |
|---|---|---|
| metric_id | SERIAL PK | |
| scene_id | INT FK → satellite_scenes | |
| band_name | VARCHAR(20) | `VV`, `VH` |
| nodata_percent | FLOAT | 0.0–1.0 |
| backscatter_mean_db | FLOAT | |
| speckle_index | FLOAT | std / |mean| |
| quality_flag | VARCHAR(10) | `PASS`, `WARNING`, `FAIL` |
| quality_score | FLOAT | 0–100 composite |

### Other Tables (unchanged from standard design)

- **processing_stages**: Stage definitions (id, name, order, retry_count)
- **processing_jobs**: Per-scene per-stage execution (job_id, scene_id, stage_id, status, duration, cpu/memory peaks)
- **dataset_jobs**: Per-dataset job aggregation (job_id, dataset_id, kind, status)
- **scene_job_state**: Per-scene progress within a dataset job
- **processing_logs**: Structured event log (stage, status, duration, error details)
- **alert_events**: Quality failures (hypertable if TimescaleDB available)
- **regions_of_interest**: Named AOIs with PostGIS geometry
- **processing_rules**: Configurable QA thresholds per stage
- **cleanup_operations**: Tier deletion progress tracking
- **api_access_logs**: API audit trail (hypertable if TimescaleDB available)

## Key Indexes

```sql
CREATE INDEX idx_source_config_dataset ON dataset_source_config(dataset_id);
CREATE INDEX idx_data_products_dataset_tier ON data_products(dataset_id, product_tier, source);
CREATE INDEX idx_data_products_level ON data_products(dataset_id, processing_level);
CREATE INDEX idx_scenes_dataset_date ON satellite_scenes(dataset_id, acquisition_datetime);
CREATE INDEX idx_fusion_dataset_date ON fusion_products(dataset_id, fusion_date);
CREATE INDEX idx_fusion_region_date_level ON fusion_products(region_id, fusion_date, processing_level);
CREATE INDEX idx_lineage_target ON data_lineage(target_product_id);
CREATE INDEX idx_lineage_source ON data_lineage(source_product_id);
```

## Table Count

17 tables total — exceeds the 12-master-table academic minimum.
