# THE TRINITY: Comprehensive Documentation

> **Satellite-driven flood monitoring data pipeline for Jabodetabek (Jakarta–Bogor–Depok–Tangerang–Bekasi), Indonesia.**
> This document describes the system **as implemented in the current codebase** (git branch `main`). Note: `README.md` and `DOCS/README.md` in this repository describe an **older architecture** (a `module4_cog_export.py` COG-export stage and a flat `processed/{tier}/` storage layout). That architecture has been replaced. The current pipeline runs RAW -> BRONZE -> SILVER -> GOLD -> FUSION: GOLD holds analysis-ready Cloud-Optimized GeoTIFFs per band per sensor, and a final FUSION stage produces a single HDF5 multi-modal stack. Storage is organized per-dataset, then per tier, then per source. This document and `STORAGE_STRUCTURE.md` reflect the current state of the code — treat the two README files as outdated where they conflict.

---

## Table of Contents

1. [Quickstart](#1-quickstart)
2. [What Is THE TRINITY?](#2-what-is-the-trinity)
3. [Architecture Overview](#3-architecture-overview)
4. [Data Sources](#4-data-sources)
5. [Pipeline Explained](#5-pipeline-explained)
6. [How to Use (User Guide)](#6-how-to-use-user-guide)
7. [API Reference](#7-api-reference)
8. [For Deep Learning Engineers](#8-for-deep-learning-engineers)
9. [Technical Deep Dive](#9-technical-deep-dive)
10. [Troubleshooting & FAQ](#10-troubleshooting--faq)
11. [Glossary](#11-glossary)

---

## 1. Quickstart

### System Requirements

- **OS**: Windows, macOS, or Linux
- **Python**: 3.10+
- **Database**: PostgreSQL 14+ with PostGIS and TimescaleDB extensions
- **RAM**: 8 GB minimum, 16 GB recommended (Lee filtering and HDF5 fusion are memory-intensive)
- **Disk**: 20–50 GB free for active datasets (a single Sentinel-1 scene's RAW tier alone is ~1.6–2 GB)
- **Network**: stable connection — this pipeline performs genuine, live downloads from ESA and NASA data archives (not simulated data)

### Installation

**Step 1 — Install PostgreSQL with extensions**

```bash
# After installing PostgreSQL, enable the required extensions inside your target database:
CREATE EXTENSION postgis;
CREATE EXTENSION timescaledb;
CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION pgcrypto;
```

**Step 2 — Apply the schema**

```bash
psql -U postgres -d sentinel_db -f database/schema.sql
# Then apply each migration file in database/migrations/ in numeric order (001 → 011)
for f in database/migrations/*.sql; do psql -U postgres -d sentinel_db -f "$f"; done
```

There is no Alembic-driven migration runner in active use here — despite `alembic` being listed in `requirements.txt`, migrations in this project are plain, hand-authored SQL files applied manually and sequentially.

**Step 3 — Set up Python environment**

```bash
python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate
pip install -r requirements.txt
pip install Pillow   # required by the preview/thumbnail endpoint but currently missing from requirements.txt
```

**Step 4 — Configure environment variables**

Copy `.env.example` to `.env` and fill in:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sentinel_db
DB_USER=postgres
DB_PASSWORD=your_password

# Not present in .env.example but REQUIRED by the code — add these manually:
COPERNICUS_USER=your_esa_cdse_email
COPERNICUS_PASSWORD=your_esa_cdse_password
NASA_EARTHDATA_TOKEN=your_nasa_earthdata_bearer_token
```

Get CDSE credentials at `dataspace.copernicus.eu`; get a NASA Earthdata bearer token at `urs.earthdata.nasa.gov` (used for both LAADS DAAC/MODIS and GES DISC/GPM).

**Step 5 — Start the application**

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

**Step 6 — Open the dashboard**

Visit `http://localhost:8000/` — FastAPI serves the single-page web app (`web/index.html`) directly at the root.

**Step 7 — Create your first dataset**

Go to the "Buat Dataset" (Create Dataset) tab, pick a preset region (e.g. Jabodetabek), set a short date range (a few days, for a fast first run), leave the default GOLD tier checked, and click Create. Processing starts immediately in the background.

---

## 2. What Is THE TRINITY?

### 2.1 The Problem

Jakarta and the surrounding Jabodetabek metropolitan area are chronically vulnerable to flooding — from monsoon rainfall, tidal (rob) flooding along the coast, and inadequate urban drainage. Traditional flood monitoring relies on sparse ground gauges and manual reporting, which is slow to update and has poor spatial coverage across a metro area of ~6,400 km². Satellite remote sensing offers continuous, wide-area, and — critically for a tropical monsoon climate with heavy cloud cover — all-weather observation, because radar (SAR) satellites see through clouds where optical satellites cannot.

THE TRINITY exists to turn raw satellite archives into analysis-ready, machine-learning-ready data, automatically, on a recurring basis, without requiring the end user to understand satellite data formats, radiometric calibration, or geospatial reprojection.

### 2.2 What This App Does (Plain-Language Explanation)

```
1. Automatically discovers and downloads satellite imagery from three independent
   space agencies: ESA (Sentinel-1 radar), NASA (MODIS optical flood product),
   and NASA/JAXA (GPM rainfall).
2. Processes the radar data through a 7-stage pipeline: calibration, cropping to
   your area of interest, speckle-noise filtering, quality scoring, and finally
   fusion with the optical and rainfall data.
3. Produces a single "fusion stack" — an HDF5 file combining SAR backscatter,
   flood-extent maps, and rainfall accumulation, all reprojected onto the same
   pixel grid — ready to feed into a machine learning model.
4. A deep learning engineer loads these HDF5 files directly into PyTorch or
   TensorFlow with a few lines of code (see Section 8).
5. Everything is orchestrated automatically: pause/resume, retries, quality
   filtering, storage cleanup, and — optionally — a daily "Live" ingestion job
   that checks for new satellite passes every night and processes them without
   any manual intervention.
```

### 2.3 Key Features

- **Genuine multi-source satellite fusion.** This is not a simulated or mocked pipeline — it performs live, authenticated downloads from the Copernicus Data Space Ecosystem (Sentinel-1), NASA's LAADS DAAC (MODIS flood product), and NASA's GES DISC (GPM IMERG rainfall).
- **Automated 7-stage pipeline**: Download → Calibrate → Crop → Lee-filter → Quality Analytics → (auxiliary MODIS/GPM ingestion) → Fusion.
- **Configurable tiered storage** (RAW/BRONZE/SILVER/GOLD) with automatic cleanup of intermediate tiers you don't need, saving disk space.
- **Quality scoring** on every processed scene (0–100), with automatic PASS/WARNING/FAIL flags and alerting.
- **Full data lineage tracking** — every output file's parent inputs are recorded with SHA-256 checksums, so provenance and integrity are independently verifiable.
- **Pause/resume/cancel** on any long-running dataset job, with graceful mid-pipeline interruption.
- **A "Live" mode**: one always-on dataset that checks daily (2 AM Asia/Jakarta time) for new Sentinel-1/MODIS/GPM data and ingests it automatically.
- **Structured, queryable processing logs** for every stage of every scene, surfaced through both the API and the web dashboard.
- **A web dashboard** (in Indonesian) for non-technical operators to create, monitor, and manage datasets without touching code.
- **A REST API** for programmatic integration.

### 2.4 Who Uses This?

- **Data operators**: create and monitor datasets through the web dashboard ("Konsol Data Banjir" — Flood Data Console), pause/resume jobs, inspect logs, manage disk usage.
- **Deep learning engineers**: consume the GOLD-tier HDF5 fusion stacks to train flood-detection or flood-prediction models.
- **Researchers**: use quality metrics and lineage records to validate data provenance and reproducibility.
- **DevOps/backend engineers**: deploy, extend, and maintain the FastAPI service, PostgreSQL/PostGIS/TimescaleDB database, and the live scheduler.

---

## 3. Architecture Overview

### 3.1 High-Level Data Flow

```
┌───────────────────────────────────────────────────────────────────┐
│                    SATELLITE DATA SOURCES (live)                    │
│  ┌───────────────┬────────────────────┬───────────────────────┐   │
│  │  Sentinel-1    │  MODIS              │  GPM IMERG             │   │
│  │  (ESA/CDSE)    │  (NASA/LAADS DAAC)  │  (NASA-JAXA/GES DISC)  │   │
│  │  SAR radar     │  Flood extent       │  Rainfall accumulation │   │
│  └───────┬────────┴──────────┬──────────┴───────────┬───────────┘   │
└──────────┼───────────────────┼──────────────────────┼───────────────┘
           │                   │                       │
┌──────────▼───────────────────▼───────────────────────▼───────────────┐
│                  ETL PIPELINE — module5_orchestrator.py               │
│  DOWNLOAD → CALIBRATE → CROP → LEE_FILTER → QUALITY_ANALYTICS →      │
│  (MODIS/GPM auxiliary ingestion) → FUSION                             │
└──────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  DATA STORAGE                                                            │
│  • PostgreSQL + PostGIS + TimescaleDB — metadata, quality, lineage      │
│  • Local disk — data/datasets/{dataset_id}_{slug}/{tier}/{scene}/      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  API — FastAPI (api/main.py)                                            │
│  /api/datasets  /api/live  /api/scenes  /api/products  /api/quality    │
│  /api/metadata  /api/preview  /api/storage  /api/pipeline  /api/regions │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  WEB DASHBOARD (web/index.html) — 3 tabs: Buat Dataset / Dataset Saya / Live │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Web framework | FastAPI 0.115, served with `uvicorn[standard]` |
| Database | PostgreSQL 14+, PostGIS (spatial), TimescaleDB (hypertables for time-series tables) |
| ORM | SQLAlchemy 2.0 declarative models (`etl/database_client.py`) |
| Geospatial processing | `rasterio`, `shapely`, `pyproj` |
| Multi-dimensional data | `h5py` (HDF5 fusion output), `xarray`, `dask[array]` |
| Numerical | `numpy`, `scipy` |
| Plotting | `matplotlib`, `seaborn` (quality-metric charts) |
| Scheduling | `APScheduler` (daily live-ingestion cron job) |
| HTTP | `requests` (downloads), `httpx` (testing) |
| Resilience | `tenacity` (retry logic), `psutil` (resource sampling in pipeline logger) |
| Frontend | Vanilla HTML/CSS/JS, Leaflet.js (map), no build step |
| Testing | `pytest`, `pytest-asyncio`, `pytest-cov` |

### 3.3 Database Schema

The schema is defined in `database/schema.sql` (11 base tables) and extended by 11 sequential migration files in `database/migrations/`. Key tables:

| Table | Purpose |
|---|---|
| `regions_of_interest` | Named areas of interest with PostGIS polygon geometry (e.g. Jabodetabek and its sub-regions) |
| `processing_stages` | The ordered list of pipeline stages (DOWNLOAD, CROP, LEE_FILTER, COG_EXPORT [legacy], ORCHESTRATE, QUALITY_ANALYTICS, FUSION) |
| `satellite_scenes` | Sentinel-1 scene metadata — a **TimescaleDB hypertable** partitioned by `acquisition_datetime` (1-month chunks) |
| `nasa_scenes` | MODIS/GPM scene/tile metadata (added in migration 003) |
| `processing_jobs` | Per-scene, per-stage execution records (status, duration, CPU/memory usage, retry attempt number) |
| `data_products` | Registry of every output file (RAW/BRONZE/SILVER/GOLD/FUSION tier, `source` sensor, file path, checksum, format) |
| `quality_metrics` | Per-band quality scores (nodata %, backscatter statistics, speckle index, PASS/FAIL/WARNING flag) |
| `data_lineage` | Parent→child provenance graph between `data_products`, with transformation type and checksums |
| `processing_rules` | Configurable quality thresholds per stage |
| `datasets` | User-created datasets — bbox, date range, requested tiers, quality settings, STANDARD vs LIVE kind, status |
| `dataset_jobs` | Execution jobs for a dataset (CREATE / BACKFILL / LIVE_INGEST), with pause/resume state |
| `scene_job_state` | Per-scene progress within a dataset job |
| `cleanup_operations` | Tracks tier-cleanup and full-delete operations, with resumable progress |
| `live_dataset_sources` | Per-source (SENTINEL1/MODIS/GPM) enable flags and last-checked timestamps for the Live scheduler |
| `fusion_products` | One row per fused HDF5 output, linking the contributing S1/MODIS/GPM scenes |
| `processing_logs` | Append-only structured event log for every pipeline stage (added in migration 011) |
| `alert_events`, `api_access_logs` | TimescaleDB hypertables for alerting and API audit trail |

Three tables use TimescaleDB hypertables for efficient time-range queries: `satellite_scenes` (1-month chunks), `api_access_logs` (7-day chunks), and `alert_events` (1-month chunks).

### 3.4 Data Tiers & On-Disk Layout

The authoritative on-disk layout (see `STORAGE_STRUCTURE.md`) is
tier-first, then **source**, then one subfolder per scene, under a dataset folder
named `{dataset_id}_{slug(dataset name)}`:

```
data/datasets/{dataset_id}_{slug}/          ← e.g. 7_hakim_d1
├── metadata.json                          ← dataset-level summary, written by folder_manager
├── raw/
│   ├── sentinel1/{product_identifier}.SAFE/
│   │   ├── ..._....SAFE.zip               ← original archive (kept — required by calibration)
│   │   └── ..._VV.tif, ..._VH.tif         ← bands extracted from the SAFE zip
│   ├── modis/                             ← raw MODIS granule cache (flat, spans dates)
│   └── gpm/                               ← raw GPM granule cache (flat, spans dates)
├── bronze/
│   └── sentinel1/{product_identifier}.SAFE/
│       └── ..._VV_crop.tif, ..._VH_crop.tif   ← calibrated + cropped to the dataset's bbox
├── silver/
│   ├── sentinel1/{product_identifier}.SAFE/
│   │   ├── ..._VV_lee.tif, ..._VH_lee.tif     ← speckle-filtered (Lee filter)
│   │   └── metadata_qa.json                   ← quality metrics for this scene
│   ├── modis/{YYYYMMDD}/
│   │   └── modis_{date}_{flood,ndvi,ndwi}.tif
│   └── gpm/{YYYYMMDD}/
│       └── gpm_rain_{24h,72h,7d}_{date}.tif
├── gold/                                   ← analysis-ready COGs, one file per band per source
│   ├── sentinel1/{product_identifier}.SAFE/
│   ├── modis/{YYYYMMDD}/
│   └── gpm/{YYYYMMDD}/
└── fusion/                                 ← cross-source: has NO source level
    └── {YYYYMMDD}/
        ├── fusion_{date}.h5                    ← THE final ML-ready product
        └── fusion_metadata.json                ← layers, bbox, shape, contributing source scenes
```

Every tier except `fusion` carries a `{source}` level. The fusion tier is
*the combination* of all three sources, so giving it one source folder would
misrepresent it — `folder_manager` rejects a source argument there rather than
silently writing to the wrong place.

Scene keys differ per source: Sentinel-1 uses the scene's `product_identifier`
(unique down to the second), while MODIS/GPM and the fusion output use the
acquisition date `YYYYMMDD`. This matters — two Sentinel-1 slices from the same
orbit pass can land on the same UTC day over the same AOI, so a date alone is
not a unique key for S1.

`raw/modis/` and `raw/gpm/` are deliberately flat rather than per-date: one
daily GPM granule is reused by the 72h/7d accumulation windows of subsequent
dates, so it cannot belong to a single date folder. `bronze/` only has
`sentinel1/` — MODIS/GPM have no separate crop stage; they go straight from raw
granules to daily products in `silver/`.

**Tier meanings changed in migration 013.** GOLD used to mean "the fusion HDF5
stack" (migration 010, when `module4_cog_export.py` was deleted). It now means
per-source analysis-ready COGs, and the fusion stack moved to its own FUSION
tier. `data_products.product_tier` gained a `FUSION` value and the table gained
a `source` column (`SENTINEL1` | `MODIS` | `GPM` | `FUSION`) so a query can
filter by tier *and* sensor via one index.

**Why these tiers?**
- **RAW**: kept for reproducibility, and is actually *required* to exist for calibration to run (the SAFE zip contains the calibration LUT).
- **BRONZE**: crop-validation checkpoint (Sentinel-1 only).
- **SILVER**: inspect filter quality; also holds the daily MODIS and GPM products.
- **GOLD**: per-source, per-band Cloud-Optimized GeoTIFFs — internally tiled with overviews, so a consumer can read a window over HTTP range requests without pulling the whole raster. This is the tier to point a DL engineer at for single-sensor experiments.
- **FUSION**: the multi-modal HDF5 stack — the production, ML-ready deliverable.
- Tiers you don't request via `required_tiers` when creating a dataset are automatically deleted after being produced (one file at a time, not a bulk directory wipe), and the corresponding `data_products` rows are marked `is_valid=False` rather than deleted — so the audit trail is preserved even after files are cleaned up.

**Fusion HDF5 contents** — datasets are grouped by source, not flat:

```
/sentinel1/VV, /sentinel1/VH
/modis/FLOOD, /modis/NDVI, /modis/NDWI
/gpm/rainfall_24h, /gpm/rainfall_72h, /gpm/rainfall_7d
```

All layers are reprojected onto that scene's Sentinel-1 grid. `/modis/FLOOD` is
uint8 with 255 = nodata (flood class is categorical; NaN doesn't fit in uint8);
the rest are float32 with NaN as nodata. A layer whose input was unavailable is
still written, filled entirely with nodata — the file shape is therefore
constant and consumers never have to test for a dataset's existence.

MODIS NDVI/NDWI are computed from MOD09GA surface reflectance:
`NDVI = (b02_NIR − b01_red) / (b02 + b01)` and
`NDWI = (b04_green − b02_NIR) / (b04 + b02)` — the McFeeters open-water
formulation, not Gao's vegetation-moisture NDWI, because this is a flood
pipeline. The index is computed on the native sinusoidal grid and only then
reprojected; resampling each band first and dividing afterwards would mix
neighbouring reflectances differently in numerator and denominator, shifting
index values along every feature edge.

### 3.5 Per-Scene Data Flow

```
SCENE ARRIVES
    ↓
[1] DOWNLOAD     — discover + download Sentinel-1 SAFE zip from CDSE, extract VV/VH bands (RAW)
    ↓
[2] CALIBRATE    — apply sigma-nought radiometric calibration LUT, reproject via GCPs (scratch _work/)
    ↓
[3] CROP         — clip calibrated bands to the dataset's bbox (BRONZE)
    ↓
[4] LEE_FILTER   — adaptive speckle-noise reduction (SILVER)
    ↓
[5] QUALITY_ANALYTICS — compute nodata%, backscatter stats, speckle index, composite quality score (writes metadata_qa.json)
    ↓
[6] GOLD_EXPORT  — rewrite the Sentinel-1 SILVER bands as analysis-ready COGs (GOLD)
    ↓
[7] Auxiliary ingestion — fetch MODIS flood/NDVI/NDWI + GPM rainfall for the same
                          date into SILVER, then export those to GOLD too
    ↓
[8] FUSION       — reproject every GOLD layer onto the S1 grid, write fusion_{date}.h5 (FUSION)
    ↓
TIER CLEANUP     — delete any tier not in the dataset's requested tiers
```

### 3.6 Execution Model

- Each dataset job runs a **three-thread pipeline**: a download worker, a processing worker (bounded by a semaphore — `PIPELINE_MAX_CONCURRENT_SCENES`, default 2 concurrent scenes), and a cleanup worker, connected by queues.
- Every stage checks shared `threading.Event` objects for **pause** and **cancel**, so an operator can pause a multi-hour job mid-scene and resume it later without losing progress.
- Failures are retried per stage (configurable retry count/delay from `processing_rules` and `config/config.json`), and every stage transition (STARTED / COMPLETED / FAILED) is written to `processing_logs` with duration, peak memory, and peak CPU usage sampled via `psutil`.
- A separate `LiveScheduler` (APScheduler `BackgroundScheduler`, cron trigger at 02:00 Asia/Jakarta time) runs once daily and checks each enabled source in `live_dataset_sources` for new data, ingesting it into a single dedicated LIVE dataset.

---

## 4. Data Sources

All three satellite data sources in THE TRINITY are **live, authenticated, real data feeds** — nothing here is simulated or mocked in the production pipeline (the only synthetic data is `etl/seed_data.py`, a standalone dev/test fixture generator, entirely separate from the live pipeline).

### 4.1 Sentinel-1 SAR (ESA / Copernicus Data Space Ecosystem)

**What it is**: A Synthetic Aperture Radar (SAR) satellite constellation operated by the European Space Agency, accessed via the **Copernicus Data Space Ecosystem (CDSE)** at `dataspace.copernicus.eu`.

**Why SAR for flood detection**:
- Penetrates cloud cover — critical for a tropical monsoon climate where optical satellites are frequently blocked.
- Works day and night (active sensor, doesn't depend on sunlight).
- Standing water produces a strong, distinctive radar signature (smooth water surfaces reflect radar away from the sensor — appearing dark), giving high contrast between flooded and dry land.
- ~10 m spatial resolution.

**What is downloaded**: GRD (Ground Range Detected) products in Interferometric Wide (IW) swath mode, dual polarization (VV + VH), discovered via CDSE's OData API filtered by Collection=SENTINEL-1, spatial intersection with the dataset's bbox, and date range. Authentication uses OAuth2 password-grant against `identity.dataspace.copernicus.eu`, with credentials from the `COPERNICUS_USER`/`COPERNICUS_PASSWORD` environment variables. Downloads stream with HTTP Range-based resume support and MD5 verification.

**Revisit frequency**: roughly 6 days for a given location (can miss very fast-onset flash floods between passes).

**Quality considerations**: inherent speckle noise (addressed by the Lee filter stage), and swath-edge NoData regions (commonly 10–20% of a cropped tile) — this is a normal artifact, not a data quality failure.

### 4.2 MODIS Flood Product (NASA / LAADS DAAC)

**What it is**: The MCDWD_L3_F2_NRT (Near Real-Time) flood product derived from the MODIS instruments aboard NASA's Terra and Aqua satellites, retrieved from the **LAADS DAAC** archive.

**Why it's used**: MODIS provides an independently-derived, optically-based flood extent map that complements the SAR-based detection — useful as a cross-check / auxiliary channel in the fusion stack, and offers near-daily revisit.

**What is downloaded**: HDF4 tiles (`h30v08`/`h31v08`, covering the Jabodetabek area) via Bearer-token authentication (`NASA_EARTHDATA_TOKEN`), with the "Flood 1-day 250m" subdataset extracted via rasterio's HDF4 EOS driver, reprojected to EPSG:4326, mosaicked, and cropped to the dataset's area of interest.

**Limitation**: cloud contamination remains an issue since MODIS is optical; the pipeline treats MODIS ingestion failures as non-fatal — the fusion stack can still be produced with a missing MODIS layer.

### 4.3 GPM IMERG Rainfall (NASA/JAXA / GES DISC)

**What it is**: Rainfall accumulation data from the Global Precipitation Measurement (GPM) mission's IMERG Final Run product (`GPM_3IMERGDF v07`), retrieved from NASA's **GES DISC** archive.

**Why it's used**: Rainfall is a direct precursor to flooding; accumulated rainfall over multiple time windows (24-hour and 72-hour) is a strong predictive feature for flood risk.

**What is downloaded**: Daily NetCDF4 granules, summed to build 24-hour and 72-hour accumulation windows, authenticated the same way as MODIS (`NASA_EARTHDATA_TOKEN`), then reprojected onto the Sentinel-1 pixel grid (~10 m resolution) and cropped to the AOI.

**Limitation**: native GPM resolution (~10 km) is far coarser than Sentinel-1's 10 m, so the reprojected rainfall layer represents a smoothed, regional rainfall signal rather than fine-grained local variation. Like MODIS, missing GPM data degrades but does not fail the fusion stack.

### 4.4 Geocoding (OpenStreetMap Nominatim)

When a user types a free-text location name rather than picking a preset region, the system geocodes it via a live call to OpenStreetMap's Nominatim service and auto-creates a new `regions_of_interest` row for future reuse.

### 4.5 Data Coverage Summary

| Source | Coverage | Resolution | Typical Latency | Cost |
|---|---|---|---|---|
| Sentinel-1 (CDSE) | Global | ~10 m | Sub-day to a few days after acquisition | Free |
| MODIS flood (LAADS DAAC) | Global (tiled) | 250 m native | 1–2 days | Free |
| GPM IMERG (GES DISC) | 60°N–60°S | ~10 km native | Days (Final Run product has a processing delay) | Free |

### 4.6 Jabodetabek Context

The system's built-in region presets (`config/config_locations.json`, seeded via migration 005) cover the Jabodetabek metropolitan area and its administrative sub-regions: Jabodetabek (`JABODTK`, primary), DKI Jakarta (`JKT`), a smaller test tile (`JKT_TEST`), Bogor (`BGR`, described as an upstream flood-source area), plus city/regency-level sub-regions (Kota/Kab Bogor, Depok, Tangerang, Bekasi) added for the location picker in the dashboard. Indonesia's wet season (roughly November–March) is when flood events concentrate; the dry season (June–September) yields clearer optical (MODIS) imagery due to fewer clouds.

---

## 5. Pipeline Explained

This section walks through each stage of the pipeline as implemented in `etl/module5_orchestrator.py` and the modules it calls.

### 5.1 Stage 1: DOWNLOAD (`etl/module1_download.py`)

- `discover_scenes(bbox_wkt, date_from, date_to, ...)` queries CDSE's OData API for Sentinel-1 GRD scenes intersecting the requested bbox and date range, filtered to IW mode.
- `download_scene(scene_meta, output_dir, keep_raw, progress_cb)` streams the SAFE zip with resumable `.part` files (HTTP Range headers), retries up to 3 times, and verifies an MD5 checksum.
- VV and VH GeoTIFF bands are extracted from the SAFE zip's `measurement/` folder.
- Output: RAW-tier SAFE zip + extracted band GeoTIFFs, and a `satellite_scenes` row.

### 5.2 Stage 2: CALIBRATE (`etl/module1b_calibrate.py`)

- Parses the SAFE zip's `annotation/calibration/calibration-*.xml` to extract the sigma-nought calibration lookup table.
- Applies the calibration via `RegularGridInterpolator`, converting raw digital numbers into geophysically meaningful backscatter (sigma0, in dB).
- Reprojects using the raw GeoTIFF's embedded Ground Control Points (GCPs) to EPSG:4326 via `rasterio.warp.calculate_default_transform`/`reproject`.
- This runs into a temporary working directory that is deleted once cropping completes; its output feeds directly into Stage 3.

### 5.3 Stage 3: CROP (`etl/module2_crop.py`)

- `crop_to_bbox()` uses `rasterio.mask.mask` with a Shapely bounding-box polygon to clip the calibrated VV/VH bands to the dataset's requested area of interest.
- Output: BRONZE-tier cropped GeoTIFFs.

### 5.4 Stage 4: LEE_FILTER (`etl/module3_lee_filter.py`)

- `lee_filter(img, window_size=7, looks=1)` implements the classic adaptive Lee speckle filter using `scipy.ndimage.uniform_filter` to compute local mean/variance, adaptively blending the original pixel with its local neighborhood mean depending on local variance (an edge-preserving smoothing approach, unlike a plain Gaussian blur).
- Default window size is 7×7 (configurable in `config/config.json`).
- Output: SILVER-tier filtered GeoTIFFs.

### 5.5 Stage 5: QUALITY_ANALYTICS (`etl/module6_analytics.py`)

- `compute_band_metrics(file_path, band_name, nodata_value, min_quality_score)` reads the filtered raster and computes:
  - `nodata_percent` — proportion of NoData pixels.
  - `backscatter_mean/std/min/max_db`.
  - `speckle_index` = std / |mean| of backscatter.
  - `radiometric_ok` — a boolean, true if mean backscatter falls within a plausible range (roughly -35 to +5 dB).
- Composite quality score formula (`compute_quality_score`):

  ```
  score = 50 × (1 − nodata_percent)
        + 30 × max(0, 1 − speckle_index)
        + 20 × (1 if radiometric_ok else 0)
  ```

  This yields a 0–100 score. Scenes at or above the configured `min_quality_score` (default 60) are flagged PASS; below it, FAIL. A row is written to `quality_metrics`, and — if the flag is FAIL — an `alert_events` row is automatically inserted (`MetadataManager.insert_quality_metrics`).
- Results are also written to a per-scene `metadata_qa.json` in the SILVER folder.

**Quality score interpretation**:

| Score | Meaning |
|---|---|
| 80–100 | Excellent — low speckle, minimal NoData, plausible radiometry |
| 60–79 | Good — acceptable for most uses |
| 40–59 | Marginal — inspect before use |
| 0–39 | Poor — likely excessive NoData or corrupted radiometry |

### 5.6 Auxiliary Ingestion: MODIS + GPM

`module9_fusion.py`'s `ensure_aux_inputs_for_date()` calls `module7_modis_download.download_modis_scene()` and `module8_gpm_download.download_gpm_scene()` for the Sentinel-1 scene's acquisition date, registering the results as SILVER-tier `data_products` (deduplicated by file path, so re-running is idempotent). Failures here are logged but non-fatal — the fusion stage proceeds with whatever auxiliary data is actually available, filling missing layers with NaN/nodata.

### 5.7 Stage 6: FUSION (`etl/module9_fusion.py`)

This is the stage that produces the GOLD deliverable:

1. Locates the SILVER-tier S1 VV/VH products for the target date.
2. Locates the nearest MODIS and GPM (24h and 72h) auxiliary files within a 24-hour alignment window (checking ±1 day offsets if an exact-date file isn't available).
3. Reprojects each auxiliary layer onto the Sentinel-1 reference grid — nearest-neighbor resampling for the categorical MODIS flood mask, bilinear resampling for the continuous GPM rainfall values.
4. Writes an HDF5 file (`fusion_{date}.h5`) with five gzip-compressed, chunked datasets:
   - `s1_vv`, `s1_vh` — SAR backscatter (float, dB)
   - `modis_flood` — flood classification (uint8, nodata=255)
   - `gpm_rainfall_24h`, `gpm_rainfall_72h` — rainfall accumulation (float, mm)
5. Writes a companion `fusion_metadata.json` describing the layers, bbox, shape, contributing source scene IDs, and `days_since_s1` (temporal offset for each auxiliary layer relative to the S1 acquisition).
6. Records a `fusion_products` row and a `data_products` row (tier=GOLD, type=FUSION_H5).
7. Records lineage edges from the contributing SILVER products to the new GOLD product.

### 5.8 Tier Cleanup

After the FUSION stage, the orchestrator compares the dataset's `required_tiers` against what was actually produced and deletes files for any tier the user didn't ask for — one file at a time (not a recursive directory wipe), removing the now-empty directory only if nothing remains. The corresponding `data_products` rows are marked `is_valid=False` rather than deleted outright, preserving the audit trail even after the underlying file is gone.

### 5.9 Error Recovery & Resumability

- Each `(scene, stage, attempt_number)` combination is tracked in `processing_jobs`, so partial pipeline runs resume without re-doing completed stages.
- Retries use the stage's configured `retry_count`/`retry_delay_sec` (from `processing_rules`/`config/config.json`).
- Pause and cancel are cooperative: every stage checks a shared `threading.Event` before proceeding, so an operator-triggered pause takes effect between stages, not mid-write.

---

## 6. How to Use (User Guide)

### 6.1 Creating Your First Dataset

1. Open the **"Buat Dataset"** (Create Dataset) tab — this is the default tab on load.
2. **Choose a location**: click one of the preset region cards (populated from `GET /api/regions`) — e.g. "Jabodetabek" — or type a free-text location, which is geocoded automatically via OpenStreetMap. The map preview (Leaflet) shows your selection.
3. **Set a date range.** For a first run, use a short window (a few days to a week) — Sentinel-1 downloads are large (1.6–2 GB each) and processing several scenes takes real time.
4. **Choose data tiers** to keep: RAW, BRONZE, SILVER, GOLD (checkboxes, GOLD checked by default; a "Semua" (all) toggle selects everything). If you only need ML-ready output, keep GOLD only — the pipeline still runs every stage internally, but deletes the intermediate files afterward, saving disk space.
5. **Advanced settings** ("Pengaturan lanjutan"): maximum acceptable cloud cover, minimum quality score threshold, target resolution.
6. Name the dataset, add an optional description, and click **Create**. This calls `POST /api/datasets`, which immediately spawns a background job runner thread — no separate "start" step is needed.

### 6.2 Monitoring Progress

Go to the **"Dataset Saya"** (My Datasets) tab. Each dataset card shows:
- **Status**: DRAFT → QUEUED → PREPARING → DOWNLOADING → PROCESSING → (PAUSED if paused) → CLEANUP → COMPLETED, or FAILED/CANCELLED.
- **Progress**: fetched from `GET /api/datasets/{id}/status`, computed as `(downloaded + processed + cleaned) / (total_scenes × 3) × 100`.
- **Recent logs**: the last few structured log entries (`GET /api/datasets/{id}/logs?limit=5`), showing stage, status, and message.

Action buttons available per dataset: Pause, Resume, Retry (retries the most recently failed job), Cancel (stops the job; by default cascades a delete of RAW/BRONZE/SILVER but keeps GOLD), Delete (removes everything, asynchronously, with progress tracked), and Download.

### 6.3 Viewing Logs

Each dataset's log stream (backed by the `processing_logs` table, queryable by stage/status/scene) shows structured entries like:

```
[DOWNLOAD]  scene S1x_IW_GRDH...  STARTED
[DOWNLOAD]  scene S1x_IW_GRDH...  COMPLETED  duration=612s
[CROP]      scene S1x_IW_GRDH...  COMPLETED  duration=48s  memory_peak_mb=812
[LEE_FILTER] scene S1x_IW_GRDH...  COMPLETED duration=203s
[QUALITY_ANALYTICS] scene S1x_IW_GRDH... COMPLETED  message="Quality score: 78 PASS"
[FUSION]    scene S1x_IW_GRDH...  COMPLETED  message="fusion_20260724.h5 written"
```

Failed stages include `error_type`, `error_message`, and a truncated traceback.

### 6.4 Downloading Results

Once a dataset reaches COMPLETED status, use the Download action, which calls `GET /api/datasets/{id}/download` — this zips the entire dataset's on-disk directory tree on the fly and streams it back (the temp zip is deleted server-side after the response completes). For programmatic access to individual files, use `GET /api/products/{product_id}/download`.

### 6.5 Live Ingestion

The **"Live"** tab manages a single, dedicated always-on dataset (`dataset_kind = LIVE`; the schema enforces there is only ever one). Toggle it on with `POST /api/live/toggle`. Once enabled, the `LiveScheduler` runs a daily check at 02:00 Asia/Jakarta time and ingests any new Sentinel-1/MODIS/GPM data (per-source, individually enable/disable-able) since the last check. Use "Backfill" to retroactively ingest a past date range into the live dataset, and "Clear" to empty it and reset it to DRAFT without deleting the dataset row itself.

### 6.6 Storage Management

Per-dataset storage usage is available via `GET /api/datasets/{id}/storage/summary` (file counts and sizes per tier, across all acquisition dates) and `GET /api/datasets/{id}/storage/files/{tier}` (per-date listing). Note: the older global `/api/storage/*` endpoints exist but read from a legacy `processed/{tier}/` directory structure that the current per-dataset layout no longer uses — treat those as deprecated in favor of the `/api/datasets/{id}/storage/*` endpoints.

### 6.7 Troubleshooting Tips for Operators

- A `.part` file left in a `raw/` folder means a download was interrupted — it will resume automatically on retry.
- A quality score in the 40–60 range with high NoData% is often just a normal SAR swath-edge artifact, not a data problem — check `metadata_qa.json` before assuming a failure.
- If MODIS/GPM layers are missing from a fusion stack, check the logs for that scene's auxiliary ingestion — a missing NASA Earthdata token or a temporary LAADS/GES DISC outage degrades but does not fail the pipeline.

---

## 7. API Reference

**Base URL**: `http://localhost:8000/api/` · **Auth**: none in this build (add a proper auth layer before exposing publicly) · **CORS**: currently wide open (`allow_origins=["*"]`) · Format: JSON request/response bodies, Pydantic-validated.

### 7.1 Health

- `GET /api/health` → `{status, db_connected, pool_size, checked_out, timestamp}`

### 7.2 Datasets (`/api/datasets`)

- `POST /api/datasets` — body: `{location, date_start, date_end, tiers: ["RAW","GOLD",...], name, description?, quality_settings?: {min_cloud_cover, min_quality_score, resolution_m}}` → `{dataset_id, job_id, status}`. Validates `date_end >= date_start` and that `tiers` is a subset of `{RAW,BRONZE,SILVER,GOLD}`.
- `GET /api/datasets?limit=&offset=` → list of STANDARD-kind datasets (LIVE dataset excluded).
- `GET /api/datasets/{id}` → full dataset detail.
- `GET /api/datasets/{id}/status` → progress percentage + per-scene state.
- `POST /api/datasets/{id}/pause`
- `POST /api/datasets/{id}/resume`
- `POST /api/datasets/{id}/cancel` — body `{cascade_delete: bool}` (default true — deletes RAW/BRONZE/SILVER, keeps GOLD).
- `GET /api/datasets/{id}/logs?stage=&status=&scene_id=&limit=&order=` → structured log entries.
- `DELETE /api/datasets/{id}?force=` → begins async full deletion.
- `GET /api/datasets/{id}/deletion-progress` → deletion progress (files deleted, bytes freed).
- `GET /api/datasets/{id}/download` → streams a zip of the whole dataset directory tree.
- `GET /api/datasets/{id}/storage/summary` → per-tier file count and size.
- `GET /api/datasets/{id}/storage/files/{tier}?acquisition_date=` → file listing for one tier/date.

### 7.3 Live (`/api/live`)

- `GET /api/live` → `{dataset_id, enabled, status, required_tiers, bbox_wkt, total_size_bytes, last_checked_at, sources: [{source_name, enabled, last_check, last_ingest, next_check}]}`
- `POST /api/live/toggle` — body `{enabled: bool}`
- `POST /api/live/clear` — empties the live dataset and resets it to DRAFT
- `POST /api/live/backfill` — body `{date_start, date_end}`, triggers a BACKFILL job
- `GET /api/live/scenes?limit=` → recent products ingested into the live dataset

### 7.4 Scenes (`/api/scenes`)

- `GET /api/scenes?region_id=&orbit_direction=&date_from=&date_to=&only_gold=&limit=&offset=` → list of Sentinel-1 scenes
- `GET /api/scenes/{scene_id}` → scene detail (404 if not found)
- `GET /api/scenes/{scene_id}/status` → per-stage job status + overall status (NOT_STARTED/IN_PROGRESS/COMPLETE/FAILED)

### 7.5 Products (`/api/products`)

- `GET /api/products?scene_id=&tier=&band_name=&latest_only=&valid_only=&limit=&offset=`
- `GET /api/products/{id}` → product detail
- `GET /api/products/{id}/download` → streams the actual file (`application/x-hdf5` for HDF5, `image/tiff` for GeoTIFF)
- `GET /api/products/{id}/verify` → recomputes SHA-256 and compares against the stored hash

### 7.6 Quality (`/api/quality`)

- `GET /api/quality/{scene_id}` → per-band metrics + overall PASS/FAIL/WARNING
- `GET /api/quality/summary/stats?region_id=&n_days=` → aggregated counts/averages by quality flag

### 7.7 Lineage (`/api/metadata`)

- `GET /api/metadata/lineage/{product_id}?direction=ancestors|descendants` → the provenance chain

### 7.8 Preview (`/api/preview`)

- `GET /api/preview/latest?limit=&band=VV|VH` → recent scenes that have a SILVER-tier product available for thumbnailing (GOLD is HDF5 and can't be thumbnailed directly, so SILVER is the newest raster tier usable for a quick-look image)
- `GET /api/preview/{product_id}?width=&band=` → a contrast-stretched PNG thumbnail (2nd/98th percentile stretch)

### 7.9 Pipeline (`/api/pipeline`)

- `GET /api/pipeline/status/current` → status of the most recently created processing job
- `POST /api/pipeline/trigger?dataset_id=` → retries the last failed job for a dataset

### 7.10 Regions (`/api/regions`)

- `GET /api/regions` → active regions of interest, with bbox as `[minx, miny, maxx, maxy]`

### 7.11 Common Error Codes

| Code | Meaning |
|---|---|
| 200 / 201 | Success / Created |
| 400 | Invalid request parameters (e.g. bad date range, invalid tier name) |
| 404 | Resource not found |
| 409 | Conflict (e.g. dataset name collision) |
| 500 | Internal server/database error |

---

## 8. For Deep Learning Engineers

### 8.1 The HDF5 Fusion Stack Format

Each processed scene's GOLD deliverable is `data/datasets/{dataset_id}_{slug}/gold/{date}/fusion_{date}.h5`, containing:

```python
import h5py

with h5py.File("fusion_20260724.h5", "r") as f:
    s1_vv = f["s1_vv"][:]                       # float32, SAR backscatter, dB
    s1_vh = f["s1_vh"][:]                       # float32
    modis_flood = f["modis_flood"][:]           # uint8, 255 = nodata
    gpm_24h = f["gpm_rainfall_24h"][:]           # float32, mm
    gpm_72h = f["gpm_rainfall_72h"][:]           # float32, mm
```

A companion `fusion_metadata.json` in the same folder documents the layer names, the reprojected grid's bounding box and shape, the contributing source scene identifiers, and `days_since_s1` (how far in time each auxiliary layer's source acquisition was from the Sentinel-1 pass it was fused with — useful for filtering out stale auxiliary data if your model is sensitive to temporal misalignment).

### 8.2 Loading into PyTorch

```python
import h5py
import numpy as np
import torch

def load_fusion_stack(filepath):
    with h5py.File(filepath, "r") as f:
        layers = [
            f["s1_vv"][:],
            f["s1_vh"][:],
            f["modis_flood"][:].astype(np.float32),
            f["gpm_rainfall_24h"][:],
            f["gpm_rainfall_72h"][:],
        ]
    stack = np.stack(layers, axis=0)   # shape: (5, H, W)
    return torch.from_numpy(stack).float()

class FusionDataset(torch.utils.data.Dataset):
    def __init__(self, h5_paths):
        self.h5_paths = h5_paths

    def __len__(self):
        return len(self.h5_paths)

    def __getitem__(self, idx):
        return load_fusion_stack(self.h5_paths[idx])

loader = torch.utils.data.DataLoader(FusionDataset(paths), batch_size=8)
```

### 8.3 Normalization Guidance

| Layer | Typical range | Suggested normalization |
|---|---|---|
| `s1_vv`, `s1_vh` | roughly -35 to +5 dB | `(x + 35) / 40` → clip to [0, 1] |
| `modis_flood` | categorical, 255 = nodata | mask out 255 before use; treat as class labels, not a continuous value |
| `gpm_rainfall_24h`, `_72h` | 0 to a few hundred mm | `clip(x / rainfall_max, 0, 1)` with `rainfall_max` chosen from your dataset's actual distribution |

Always inspect the actual value distribution in your dataset before finalizing normalization constants — don't hard-code the ranges above blindly.

### 8.4 Discovering GOLD Files Programmatically

Rather than walking the filesystem directly, query the API:

```python
import requests

products = requests.get(
    "http://localhost:8000/api/products",
    params={"tier": "GOLD", "valid_only": "true", "limit": 200},
).json()["items"]

for p in products:
    print(p["file_path"], p["scene_id"])
```

Each product's SHA-256 integrity can be verified via `GET /api/products/{id}/verify` before trusting it for training.

### 8.5 Building a Time Series

To assemble a sequence of fusion stacks for a temporal model (ConvLSTM, video transformer, etc.), sort a dataset's GOLD products by acquisition date and stack them along a new time axis:

```python
def load_time_series(h5_paths_sorted_by_date):
    frames = [load_fusion_stack(p) for p in h5_paths_sorted_by_date]
    return torch.stack(frames, dim=0)   # shape: (T, C, H, W)
```

### 8.6 Data Quality Guidance for Training

- Prefer scenes where the corresponding `quality_metrics` row has `quality_flag = "PASS"` (check via `GET /api/quality/{scene_id}`) — filter your training set by this before assembling batches.
- A high `modis_flood` nodata fraction or missing GPM layers (all-NaN) usually means that auxiliary source was unavailable for that date — check `fusion_metadata.json`'s `days_since_s1` field, and consider excluding fusion stacks where the auxiliary alignment window was large.
- Sentinel-1 swath-edge NoData (visible as NaN bands at the array edges) is expected and not a defect — don't discard a scene purely for this unless it covers a large fraction of your area of interest.

---

## 9. Technical Deep Dive

### 9.1 Project Structure

```
sentinel-metadata/
├── api/
│   ├── main.py                     FastAPI app, lifespan, router registration, static file mount
│   ├── schemas.py                  Pydantic v2 request/response models
│   └── routes/
│       ├── health.py  scenes.py  products.py  quality.py  lineage.py
│       ├── preview.py  storage.py  pipeline.py  datasets.py  live.py  regions.py
├── etl/
│   ├── module1_download.py         Sentinel-1 discovery + download (CDSE)
│   ├── module1b_calibrate.py       Sigma0 calibration + GCP reprojection
│   ├── module2_crop.py             Bbox cropping
│   ├── module3_lee_filter.py       Adaptive speckle filter
│   ├── module5_orchestrator.py     Pipeline driver: threads, queues, pause/cancel, stage sequencing
│   ├── module6_analytics.py        Quality score computation
│   ├── module7_modis_download.py   MODIS flood product ingestion (LAADS DAAC)
│   ├── module8_gpm_download.py     GPM IMERG rainfall ingestion (GES DISC)
│   ├── module9_fusion.py           Multi-modal HDF5 fusion stack builder (GOLD tier)
│   ├── pipeline_logger.py          Structured logging into processing_logs, resource sampling
│   ├── database_client.py          SQLAlchemy ORM models + connection pooling
│   ├── dataset_manager.py          Business logic behind /api/datasets and /api/live
│   ├── lineage_tracker.py          SHA-256 checksums + data_lineage graph traversal
│   ├── live_scheduler.py           APScheduler daily cron for Live ingestion
│   ├── metadata_manager.py         Low-level CRUD over jobs/scenes/products/quality/alerts
│   ├── folder_manager.py           Single source of truth for on-disk paths
│   ├── deletion_manager.py         Resumable, manifest-based dataset deletion
│   ├── location_resolver.py        Region lookup + Nominatim geocoding fallback
│   ├── migrate_data_structure.py   One-time migration tool (old flat layout → per-date layout)
│   ├── constants.py                Shared dedup keys (e.g. MODIS/GPM product short names)
│   └── seed_data.py                Synthetic dev/test fixture (NOT used by the live pipeline)
├── database/
│   ├── schema.sql                  Base 11-table DDL
│   └── migrations/                 001 through 011, sequential SQL migrations
├── web/
│   └── index.html                  Single-page dashboard (3 tabs), no build step
├── config/
│   ├── config.json                 Pipeline runtime settings (Lee filter window, retries, thresholds)
│   └── config_locations.json       AOI presets
├── tests/                          pytest suite (schema, ETL, API, quality metrics, pipeline logger)
├── requirements.txt
└── .env.example
```

### 9.2 Key Dependencies (from `requirements.txt`)

```
SQLAlchemy==2.0.36        GeoAlchemy2==0.15.2       alembic==1.14.0 (listed but not actively used for migrations)
fastapi==0.115.5          uvicorn[standard]==0.32.1  pydantic==2.10.3
rasterio==1.4.3           shapely==2.0.6             pyproj==3.7.0
h5py==3.12.1              numpy==2.1.3               scipy==1.14.1
xarray==2024.11.0         dask[array]==2024.11.2     pandas==2.2.3
matplotlib==3.9.3         seaborn==0.13.2
apscheduler>=3.10.0       tenacity==9.0.0            psutil==6.1.0
requests==2.32.3          httpx==0.28.1              python-dotenv==1.0.1
pytest==8.3.4             pytest-asyncio==0.24.0     pytest-cov==6.0.0
```

**Known gap**: `Pillow` is imported by `api/routes/preview.py` for thumbnail generation but is not listed in `requirements.txt` — install it explicitly (`pip install Pillow`).

### 9.3 Configuration Reference

**`.env`** required variables (note: `.env.example` in this repo is itself incomplete — it does not list the CDSE/NASA credentials that the code actually requires; add them manually):

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sentinel_db
DB_USER=postgres
DB_PASSWORD=...
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=5

# Required by module1_download.py but missing from .env.example:
COPERNICUS_USER=...
COPERNICUS_PASSWORD=...
# Required by module7_modis_download.py and module8_gpm_download.py:
NASA_EARTHDATA_TOKEN=...
```

`.env.example` also includes commented-out placeholders for S3/GCS/Azure Blob credentials; note that although the schema's `storage_location` enum includes S3/GCS/AZURE_BLOB values, **no cloud-upload code exists anywhere in the reviewed codebase** — storage is local-disk-only in the current implementation.

**`config/config.json`** highlights:
- `pipeline.lee_window_size = 7`, `lee_looks = 1`
- `pipeline.retry_count = 3`, `retry_delay_sec = 30`
- `pipeline.check_interval_hours = 12`, `max_scenes_per_run`
- `pipeline.keep_raw` defaults true and is effectively mandatory — calibration reads the SAFE zip directly
- `quality.min_quality_score = 60`, `max_nodata_percent = 30`, `cloud_threshold_percent = 20`

### 9.4 Database Connection Pooling

`DatabaseClient` (in `etl/database_client.py`) builds a SQLAlchemy engine with `QueuePool`, `pool_pre_ping=True`, and `pool_recycle=3600` (recycled hourly to avoid stale-connection errors). `session_with_retry()` retries transient `OperationalError`s up to 3 times with exponential backoff (2s/4s/8s). Pool stats are exposed via `GET /api/health`.

### 9.5 Concurrency & Threading Model

`module5_orchestrator.run_dataset_job()` spins up three daemon threads per dataset job:
1. **Download worker** — pulls scenes off the discovery queue, downloads them, pushes to the processing queue.
2. **Pipeline worker** — bounded by a semaphore (`PIPELINE_MAX_CONCURRENT_SCENES`, default 2) to limit simultaneous CPU/memory-heavy processing (particularly the Lee filter and fusion reprojection steps).
3. **Cleanup worker** — deletes unwanted tier files once a scene's processing completes.

Pause/cancel are implemented as shared `threading.Event`s per job (module-level dicts in `dataset_manager.py`, keyed by job ID), checked cooperatively between stages rather than forcibly interrupting an in-progress stage.

### 9.6 Error Handling Strategy

Each `(scene, stage, attempt)` triple is a row in `processing_jobs`. On failure, the orchestrator retries up to the stage's configured `retry_count` with a delay, then marks the job FAILED and moves on to the next scene (a single scene's failure doesn't halt the whole dataset job). `pipeline_logger.py`'s `PipelineLogger.stage()` async context manager wraps each stage, automatically emitting STARTED/COMPLETED/FAILED events (including `duration_seconds`, `memory_peak_mb`, `cpu_peak_percent`, and on failure `error_type`/`error_message`/a truncated traceback) into `processing_logs`.

### 9.7 Deployment Checklist

- Add authentication (none exists currently — CORS is wide open and there's no auth middleware).
- Fill in `COPERNICUS_USER`/`COPERNICUS_PASSWORD`/`NASA_EARTHDATA_TOKEN` — the app degrades badly without these (Sentinel-1/MODIS/GPM ingestion will fail).
- Set up PostgreSQL backups (the pipeline's only persistence for metadata; local disk holds the actual raster/HDF5 files, so back up both).
- Monitor disk usage — a single dataset with several scenes and all four tiers kept can consume many gigabytes; use the storage summary endpoints and cleanup routinely.
- Add `Pillow` to your deployment's installed packages (missing from `requirements.txt`).
- If you intend to rely on migrations for schema evolution, note that this project does not currently use Alembic's revision machinery despite it being a dependency — apply new `database/migrations/*.sql` files by hand, in order, and update `schema_migrations`.

---

## 10. Troubleshooting & FAQ

| Symptom | Likely Cause | Fix |
|---|---|---|
| "No scenes found" for a dataset | AOI too small, or no Sentinel-1 pass in the date range | Widen the bbox, extend the date range |
| Download stuck at a `.part` file | Interrupted transfer | Safe to leave — it resumes automatically on retry |
| CROP stage fails | Disk space exhausted | Check available disk, delete unneeded datasets/tiers |
| Quality score 30–50 | Normal SAR swath-edge NoData, or genuinely noisy scene | Inspect `metadata_qa.json`; lower `min_quality_score` if the artifact is expected, don't assume it's a bug |
| MODIS or GPM layer missing from `fusion_*.h5` | LAADS/GES DISC outage, missing/expired `NASA_EARTHDATA_TOKEN`, or no auxiliary data within the alignment window | Check `processing_logs` for that scene's auxiliary-ingestion entries; verify the token is valid |
| Database connection error | PostgreSQL not running, or `.env` credentials wrong | Start PostgreSQL; verify `.env` |
| Thumbnail endpoint (`/api/preview/...`) errors | `Pillow` not installed | `pip install Pillow` |
| Old `/api/storage/*` endpoints show nothing | They read a legacy global folder structure no longer used by the current per-dataset layout | Use `/api/datasets/{id}/storage/summary` instead |
| A deleted dataset's folder still has a stray `.deletion_manifest.json` | A known edge case in the deletion cleanup path | Harmless leftover; safe to remove manually if needed |

**Q: Is any of this data simulated?**
A: No — Sentinel-1, MODIS, and GPM downloads are all live, authenticated calls to their respective agency archives. The only synthetic data generator in the codebase is `etl/seed_data.py`, a standalone dev/test fixture script, entirely separate from the production pipeline.

**Q: Can I get real-time flood alerts from this?**
A: The pipeline is near-real-time at best (Sentinel-1 revisit ~6 days, MODIS 1–2 days, GPM IMERG Final Run has its own processing delay). It is not designed for minute-scale real-time alerting.

**Q: Why did GOLD tier change from per-band GeoTIFFs to a single HDF5 file?**
A: The pipeline was refactored twice. First a FUSION stage was added that combines Sentinel-1, MODIS, and GPM into one aligned multi-modal stack, replacing the older per-band Cloud-Optimized GeoTIFF export (`module4_cog_export.py`, deleted at that point) — GOLD then meant the HDF5 stack. Later that proved to conflate two different things, so the tiers were split: GOLD went back to meaning per-band, per-sensor analysis-ready COGs (now written by `module4_gold_export.py`), and the fusion stack moved to its own FUSION tier. A DL engineer wanting one sensor reads GOLD; one wanting the aligned multi-channel array reads FUSION — without either having to filter the other out.

**Q: Can I extend this to another region outside Jabodetabek?**
A: Yes — add a new bbox to `regions_of_interest` (via the location resolver's geocoding, or a new migration/config entry) and create a dataset against it; the pipeline itself is region-agnostic.

---

## 11. Glossary

| Term | Definition |
|---|---|
| SAR | Synthetic Aperture Radar — an active microwave imaging instrument (used by Sentinel-1) |
| GRD | Ground Range Detected — a Sentinel-1 product type, geometrically corrected to ground range |
| Sigma0 (σ⁰) | Radar backscatter coefficient, the calibrated, geophysically meaningful quantity derived from raw SAR digital numbers |
| Speckle | Inherent granular noise in SAR imagery, addressed here by the Lee adaptive filter |
| GCP | Ground Control Point — used to geometrically reproject imagery |
| NDVI/NDWI | Optical vegetation/water indices (referenced conceptually; not directly computed by this pipeline — MODIS input here is the flood product, not raw reflectance bands) |
| COG | Cloud-Optimized GeoTIFF — a tiled, pyramided GeoTIFF format for efficient partial reads (used historically; the current GOLD tier is HDF5, not COG) |
| HDF5 | Hierarchical Data Format v5 — the container format for the current fusion stack output |
| Tier (RAW/BRONZE/SILVER/GOLD) | This pipeline's data-maturity staging convention, from original download through fully processed/fused output |
| CDSE | Copernicus Data Space Ecosystem — ESA's Sentinel-1 data access platform |
| LAADS DAAC | NASA's Level-1 and Atmosphere Archive & Distribution System — source of the MODIS flood product used here |
| GES DISC | NASA's Goddard Earth Sciences Data and Information Services Center — source of GPM IMERG rainfall data |
| IMERG | Integrated Multi-satellitE Retrievals for GPM — the rainfall product used |
| Lineage | The recorded parent→child provenance chain between data products, with checksums, used to trace and verify how any output file was derived |
