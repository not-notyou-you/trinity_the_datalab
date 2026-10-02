# Architecture

Tech stack, deployment, database schema, and on-disk layout. (Merged from the former `INFRASTRUCTURE.md` + `DESIGN.md` — see DOCS/README.md for the current doc set.)

## Tech Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Language | Python | 3.10+ | Core implementation |
| Web Framework | FastAPI | 0.115+ | REST API |
| ASGI Server | uvicorn[standard] | 0.32+ | HTTP server |
| ORM | SQLAlchemy | 2.0+ | Database abstraction |
| Validation | pydantic | 2.10+ | Request/response schemas |
| Database | PostgreSQL | 14+ | Relational storage |
| Extensions | PostGIS 3.0+ | — | Spatial queries, geometry |
| | TimescaleDB | — | Time-series hypertables (optional, degrades gracefully) |
| Geospatial | rasterio 1.4+, shapely 2.0+, pyproj 3.7+, GeoAlchemy2 0.15+ | — | Raster I/O, geometry, CRS |
| Data | numpy, scipy, h5py | — | Arrays, interpolation, HDF5 |
| HDF4 | pyhdf 0.11+ | — | Reading MODIS HDF4 granules |
| HTTP | requests, httpx | — | Downloads, async testing |
| Scheduling | APScheduler 3.10+ | — | Live Monitoring cron (01:00/07:00/13:00/19:00 Asia/Jakarta) |
| Resilience | (built-in) | — | Shared retry/backoff and `Retry-After` handling in `etl/download_guard.py` |
| Visualization | matplotlib, Pillow | — | Preview PNG generation, report charts |
| PDF report | reportlab 5.0 | — | Dataset report assembly (`etl/report_generator.py`) — chosen over weasyprint because it is pure Python (no GTK/Pango on Windows). Uses the DejaVu fonts bundled with matplotlib for ✓ ⚠ ° ± glyphs |
| PDF test reader | pypdf | — | Used only by `tests/test_report_generation.py` to read generated PDFs back |
| DB driver / migrations | psycopg2-binary | — | Postgres driver; migrations are hand-written `database/migrations/*.sql` applied by `database/run_migration.py` |
| System | psutil, python-dotenv | — | CPU/memory telemetry (`cpu_usage_percent`), `.env` loading |
| Frontend | HTML5, CSS3, vanilla JS, Leaflet.js | — | Dashboard (no build step) |
| Testing | pytest, pytest-asyncio, pytest-cov | — | Test suite |

## Database Setup

**Default DB name is `sentinel1_flood`**, not `trinity_datalab` — that's what `.env.example`, `etl/config.py`, `etl/database_client.py`'s `from_env()` default, and `docker-compose.yml` all actually use. The project's public name ("Trinity: The DataLab") and its database name are historical leftovers from before the rename and were never reconciled; rename it in your own `.env` if you want them to match.

```bash
# Create database
psql -U postgres -c "CREATE DATABASE sentinel1_flood;"

# Enable extensions
psql -U postgres -d sentinel1_flood <<EOF
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;  -- skip if unavailable
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
EOF

# Apply schema + migrations
psql -U postgres -d sentinel1_flood -f database/schema.sql
for f in database/migrations/*.sql; do
  psql -U postgres -d sentinel1_flood -f "$f"
done
```

**TimescaleDB note**: On PostgreSQL 18 (Windows), TimescaleDB is unavailable. Schema uses `IF EXISTS` guards — affected tables fall back to plain PostgreSQL tables. PostgreSQL 17 has an official TimescaleDB Windows installer if hypertables are needed during development.

## Environment Variables (.env)

The real template is `.env.example` — copy it (`cp .env.example .env`), don't hand-write one from this list. Reproduced here with the vars actually read by the code (`etl/config.py`, `etl/database_client.py`, `etl/pipeline_logger.py`, `etl/module5_orchestrator.py`, `etl/download_guard.py`, `api/main.py`, `etl/__init__.py`):

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sentinel1_flood
DB_USER=postgres
DB_PASSWORD=<strong-password>
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_ECHO=false            # true logs every SQL statement — dev only

# API
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=false
AUTO_RESUME_JOBS=true    # resume interrupted jobs on startup

# ESA Copernicus (register at dataspace.copernicus.eu) — NOT in .env.example,
# add these yourself; module1_download.py reads them at call time.
COPERNICUS_USER=<email>
COPERNICUS_PASSWORD=<password>
# OAuth2 token endpoint: identity.dataspace.copernicus.eu

# NASA Earthdata (get token at urs.earthdata.nasa.gov) — also not in
# .env.example; used for both MODIS (LAADS DAAC) and GPM (GES DISC)
NASA_EARTHDATA_TOKEN=<bearer-token>

# Pipeline / job scheduling
MAX_ACTIVE_JOBS=2                  # etl/dataset_manager.py — dataset jobs running at once; the rest wait FIFO (status QUEUED)
PIPELINE_MAX_CONCURRENT_SCENES=2   # module5_orchestrator.py — scene pipelines per job
S1_PARALLEL_DOWNLOADS=3            # module5_orchestrator.py — S1 download workers per job, clamped 1–4 (Live jobs always use 1)
LOGS_DIR=logs_pipeline             # NOT "LOG_DIR" — plural, and this is the real default
GDAL_CACHEMAX=<mb>
GDAL_NUM_THREADS=<n>

# Download resilience (etl/download_guard.py) — defaults shown
DOWNLOAD_MIN_KBPS=75               # StallGuard: abort an attempt slower than this...
DOWNLOAD_STALL_WINDOW_S=90         # ...averaged over this window
CDSE_MAX_CONNECTIONS=3             # global connection slots per provider, shared by ALL jobs + Live
LAADS_MAX_CONNECTIONS=4
GESDISC_MAX_CONNECTIONS=4
DOWNLOAD_MAX_RETRY_AFTER_S=300     # cap on an honoured Retry-After (429/503)
CONNECTION_RETRY_BASE_S=2.5        # backoff base for dropped connections
DOWNLOAD_BREAKER_WINDOW_S=30       # circuit breaker: N connection failures within the window...
DOWNLOAD_BREAKER_THRESHOLD=2       # ...pause that provider...
DOWNLOAD_BREAKER_COOLDOWN_S=90     # ...for this long
PROGRESS_STALL_AFTER_S=1800        # UI "no progress since ..." warning threshold

# Live Monitoring
LIVE_RESULT_SHOW_S=900             # how long the "last cycle result" note stays on an area card
```

**`DATA_DIR` has no effect.** `etl/folder_manager.py` hardcodes `DATA_ROOT = Path("data") / "datasets"` — there is no env override for it, despite an older draft of this doc suggesting one. If you need data on a different volume, symlink `data/` rather than trying to set an env var.

`DATABASE_URL` / `TEST_DATABASE_URL` are also read directly in a few places (`etl/reference_layers.py`, `tests/conftest.py`) as an alternative to the discrete `DB_*` vars.

## Data Source Authentication

| Source | Auth Method | Endpoint |
|---|---|---|
| Sentinel-1 (CDSE) | OAuth2 password grant | `identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token` |
| MODIS (LANCE NRT, then LAADS DAAC archive) | Bearer token | `nrt3.modaps.eosdis.nasa.gov` first, `ladsweb.modaps.eosdis.nasa.gov` as fallback |
| GPM IMERG (GES DISC) | Bearer token (same NASA token) | `disc.gsfc.nasa.gov` |

All data access is through authorized APIs — not scraping. The CDSE token is cached per process and shared by every download thread (refreshed shortly before expiry or when the server rejects it), rather than one login per scene. A NASA 401/403 fails fast and raises a "NASA_EARTHDATA_TOKEN is invalid or expired" alert in the UI instead of being retried.

## Docker (Optional)

The real `docker-compose.yml` (repo root):

```yaml
services:
  db:
    image: timescale/timescaledb-ha:pg14-latest
    container_name: sentinel1_db
    environment:
      POSTGRES_DB: sentinel1_flood
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports: ["5432:5432"]
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./database/schema.sql:/docker-entrypoint-initdb.d/01_schema.sql
      - ./database/seed_data.sql:/docker-entrypoint-initdb.d/02_seed.sql
      - ./database/indexes.sql:/docker-entrypoint-initdb.d/03_indexes.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d sentinel1_flood"]
      interval: 10s
      timeout: 5s
      retries: 5
  api:
    build: .
    container_name: sentinel1_api
    depends_on:
      db: { condition: service_healthy }
    environment:
      DB_HOST: db
      DB_PORT: 5432
      DB_NAME: sentinel1_flood
      DB_USER: postgres
      DB_PASSWORD: postgres
      API_HOST: 0.0.0.0
      API_PORT: 8000
    ports: ["8000:8000"]
    volumes: [".:/app", "./processed:/app/processed"]
    command: uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
volumes:
  pgdata:
```
Note `database/migrations/*.sql` are **not** mounted into `docker-entrypoint-initdb.d/` — only `schema.sql`, `seed_data.sql`, `indexes.sql` are. A fresh container needs the migrations applied by hand afterwards, same as the "Database Setup" loop above.

---

## Disk Layout

Relaid out in DOCS/DECISIONS.md D15 to be **source-first**, with tier as a
database/lineage concept rather than a path segment (full rationale and
per-item notes in DOCS/PIPELINE.md "On-disk layout"):

```
data/
├── datasets/{dataset_id}_{slug}/
│   ├── metadata.json
│   ├── sentinel-1/{RAW,PROCESSED}/      # e.g. S1A_..._20240305T111407_VV_crop.tif
│   ├── modis/{RAW,PROCESSED}/           # e.g. modis_20240305_ndvi.tif
│   ├── gpm-imerg/{RAW,PROCESSED}/       # e.g. gpm_rain_24h_20240305.tif
│   ├── fusion/{co-occurrence,full-coverage,hybrid}/
│   ├── preview/{RAW,PROCESSED}/{grayscale,colored,composite}/
│   ├── masks/                           # land_distance.tif, water_occurrence.tif — dataset-wide, not per-date
│   ├── reports/                         # report_{YYYYMMDDTHHMMSSZ}.pdf + .json (etl/report_generator.py), latest reused as cache
│   ├── live/{YYYYMMDD}/{key}.png        # LIVE_AREA datasets only: the 8 Live Monitoring previews per scene
│   ├── _granule_cache/{modis,gpm}/      # raw NASA granules, shared across dates (accounted as tier RAW)
│   └── _work/                           # scratch: SAFE zip, pre-COG Lee output — swept at end of every job,
│                                        # except a FAILED scene's folder, so its .part can resume next run
├── merged/                              # cross-dataset merge output (etl/dataset_merge.py) — sibling of datasets/, not inside it
│   └── merged_{YYYYMMDD}.h5
└── _job_locks/                          # etl/job_lock.py — one lock file per running job_id (and per Live Area
                                         # cycle, key live-area-{id}), self-releasing

logs/{dataset_id}_{slug}.txt     # One run log per dataset
```

Only the tiers actually configured for a dataset get a drawer; a source never
requested gets no folder at all. Dates live in filenames, not folders — every
writer already embeds the date, so listing dates means reading filenames back,
not walking a `{YYYYMMDD}/` tree. Datasets created **before** this relayout
keep their old `{YYYYMMDD}/{tier}/{source}/` tree; `folder_manager.is_legacy_layout()`
detects them and the UI shows a legacy-layout notice instead of a tree built
from vocabulary that no longer applies to their files.

Storage per Sentinel-1 scene: ~2.4 GB (all tiers) or ~0.25 GB (COG+FUSED only).

---

## Database Design

### ER Relationships (Text Diagram)

```
regions_of_interest (1) ──── (N) datasets
datasets (1) ──── (N) dataset_source_config    # per-satellite processing config
datasets (1) ──── (N) dataset_jobs
dataset_jobs (1) ──── (N) scene_job_state
dataset_jobs (1) ──── (N) processing_logs

satellite_scenes (1) ──── (N) processing_jobs
satellite_scenes (1) ──── (N) data_products
satellite_scenes (1) ──── (N) quality_metrics

data_products (M) ── data_lineage ── (N) data_products   # provenance graph (parent_product_id / child_product_id)
fusion_products (0..1) ──── (1) satellite_scenes[s1_scene_id]
                       ──── (0..1) nasa_scenes[modis_scene_id]
                       ──── (0..1) nasa_scenes[gpm_scene_id]
                       ──── (1) regions_of_interest[region_id]
                       ──── (0..1) datasets[dataset_id]

reference_land_polygons                                   # unrelated to any dataset — pure reference geometry
                                                            # (not FK'd from datasets; joined spatially at mask-build time)

live_areas (0..1) ──── (1) datasets[dataset_id]  (dataset_kind='LIVE_AREA', FK ON DELETE SET NULL)
live_areas ··· live_scenes ··· live_events       # area_id / dataset_id are plain columns, NOT FKs:
                                                 # these logs must outlive the scenes and areas they describe
```

**`fusion_products` does not point at `data_products` at all** — its `s1_scene_id`/`modis_scene_id`/`gpm_scene_id` columns are FKs to `satellite_scenes`/`nasa_scenes` (the *inputs*), not to the FUSED-tier `data_products` row it produces. The FUSED `data_products` row for a given date is found by matching `dataset_id` + tier + the fusion filename, not by a foreign key — there is no direct link column between the two tables.

**Outside this graph**: reference layers (`masks/*.tif`, one pair per dataset) and cross-dataset merge output (`data/merged/*.h5`, spanning several datasets) are filesystem artifacts with no rows in `data_products` or `data_lineage` — see "Not a table" below.

### Tables

#### datasets — Core configuration table (DataLab-specific fields marked ★)

| Column | Type | Notes |
|---|---|---|
| dataset_id | SERIAL PK | |
| dataset_uuid | UUID | `uuid_generate_v4()`, unique |
| name | VARCHAR(255) | |
| description | TEXT | Optional |
| location_label | VARCHAR(255) | Free-text label shown in UI |
| region_id | INT FK → regions_of_interest ON DELETE SET NULL | |
| bbox | GEOMETRY(Polygon, 4326) | AOI as WGS84 polygon |
| bbox_wkt | TEXT NOT NULL | Same AOI as WKT — one bbox column, not one per row, is why AOI splits (D16 in DECISIONS.md) create N sibling datasets rather than N rows on one dataset |
| date_start / date_end | DATE | |
| required_tiers | TEXT[] | Derived internally from source configs, not user input |
| ★ fusion_strategy | VARCHAR(20) | `CO_OCCURRENCE`, `FULL_COVERAGE`, `HYBRID`, or NULL (single source) — CHECK constraint enforces the allowed values |
| ★ preview_options | TEXT[] | `{'GRAYSCALE','COLORED','COMPOSITE'}` or empty (empty means "no previews", not "unspecified" — migration 018) |
| ★ fusion_output_only | BOOLEAN DEFAULT FALSE | Keep only FUSED output; per-source artifacts are deleted **after** each date's stack is written (still built, not skipped) |
| ★ s1_match_tolerance_days | SMALLINT DEFAULT 2 | FULL_COVERAGE only — how far a borrowed S1 scene may be from the target date |
| ★ fusion_grid | JSONB | `{transform, width, height, crs, source_product_id, pinned_at}`, pinned once on first fusion and read thereafter so the grid can't drift when raster availability changes (migration 024, DECISIONS.md D16/D19) |
| quality_settings | JSONB DEFAULT `{}` | `min_cloud_cover`, `min_quality_score`, `resolution_m`, `orbit_direction` (`ASCENDING`/`DESCENDING`/absent = both — read by the orchestrator after S1 discovery) |
| dataset_kind | VARCHAR(10) DEFAULT `STANDARD` | `STANDARD`, `LIVE` (legacy single live dataset, D23) or `LIVE_AREA` (one per Live Area, migration 025) — CHECK `chk_dataset_kind` |
| status | VARCHAR(20) DEFAULT `DRAFT` | `DRAFT`, `QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`, `PAUSED`, `CANCELLED` |
| total_scenes / completed_scenes / failed_scenes | INT DEFAULT 0 | |
| total_size_bytes | BIGINT DEFAULT 0 | |
| is_deletable | BOOLEAN DEFAULT TRUE | |
| generate_preview | BOOLEAN DEFAULT TRUE | Runs the PREVIEW stage; separate from `quality_settings` because it's a user choice, not a QA threshold |
| live_enabled | BOOLEAN DEFAULT FALSE | |
| live_last_checked_at | TIMESTAMPTZ | |
| created_at / updated_at | TIMESTAMPTZ | DEFAULT NOW() |
| deleted_at | TIMESTAMPTZ | Soft-delete marker |

Note: `selected_satellites` and `processing_level` are NOT on this table — they live in `dataset_source_config` because each satellite has its own processing definition.

**Constraint**: `fusion_strategy` must be NULL when only 1 source is configured (enforced in `create_dataset_with_sources()`, not a CHECK — the source count lives in a different table).

**AOI splits share a fusion_grid, not a table row.** When one AOI is too large and gets split into adjacent bbox strips (DECISIONS.md D16 — the JAWA dataset became 4 sibling datasets), each sibling is a full row in this table with its own `bbox_wkt`. Pinning them to the *same* `fusion_grid` value is what makes their pixel grids line up well enough to be stitched later by `etl/dataset_merge.py` (DECISIONS.md D19) — nothing in the schema enforces this automatically; it has to be arranged before the first fusion run on any of the siblings.

#### ★ dataset_source_config — Per-satellite processing configuration

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
| SENTINEL1 | Calibrate + reproject + crop (no Lee filter, no QA) → ALIGNED | + Lee filter 7×7 + QA analytics + COG export → DESPECKLED → COG |
| MODIS | Flood map extract + reproject + crop (no derived indices) → ALIGNED | + NDVI + NDWI computation → INDICES → COG |
| GPM | Daily rainfall extract + crop (single day only) → ALIGNED | + Accumulation windows 24h/72h/7d → ACCUMULATED → COG |

(Tier names per DOCS/DECISIONS.md D14 — `ALIGNED`/`COG` are shared across sources; the rank-2 name branches per source because Lee filtering, index computation, and rain accumulation are not the same kind of operation.)

**Constraint**: `processing_levels` must contain at least one value.

#### satellite_scenes — Sentinel-1 scene metadata

| Column | Type | Notes |
|---|---|---|
| scene_id | SERIAL PK | |
| scene_uuid | UUID | unique, `uuid_generate_v4()` |
| product_identifier | VARCHAR(200) UNIQUE | Full SAFE name |
| platform | VARCHAR(20) DEFAULT `SENTINEL-1` | Not `source` — there is no `source` column on this table (it's implicitly S1-only) |
| instrument_mode | VARCHAR(10) DEFAULT `IW` | |
| polarization_vv / polarization_vh | BOOLEAN | |
| acquisition_datetime | TIMESTAMPTZ NOT NULL | |
| orbit_number | INT | |
| orbit_direction | `orbit_direction_enum` | Real Postgres ENUM, not VARCHAR |
| relative_orbit | SMALLINT | |
| bbox | GEOMETRY(Polygon, 4326) NOT NULL | Column is `bbox`, not `geometry` |
| cloud_cover_percent | NUMERIC(5,2) | |
| incidence_angle_near / incidence_angle_far | NUMERIC(6,3) | |
| resolution_m | SMALLINT DEFAULT 10 | |
| region_id | INT NOT NULL FK → regions_of_interest ON DELETE RESTRICT | **There is no `dataset_id` column on this table at all** — a scene links to a dataset only indirectly, through `data_products.dataset_id` + `data_products.scene_id` |
| raw_file_path | TEXT | |
| raw_file_size_mb | NUMERIC(12,3) | |
| download_url | TEXT | |
| checksum_md5 | VARCHAR(32) | |
| is_available | BOOLEAN DEFAULT TRUE | |
| created_at / updated_at | TIMESTAMPTZ | |

#### nasa_scenes — MODIS/GPM scene metadata

| Column | Type | Notes |
|---|---|---|
| nasa_scene_id | BIGSERIAL PK | |
| source | VARCHAR(20) NOT NULL | `MODIS` or `GPM` |
| tile_id | VARCHAR(10) NOT NULL | MODIS tile (e.g. h30v08); GPM rows still get a value here, it is **not nullable** |
| product_short_name | VARCHAR(50) NOT NULL | Granule product name — not called `product_id` |
| acquisition_date | DATE NOT NULL | |
| region_id | INT NOT NULL FK → regions_of_interest ON DELETE RESTRICT | Same pattern as `satellite_scenes` — **no `dataset_id` column here either** |
| raw_file_path | TEXT | |
| download_url | TEXT | |
| is_available | BOOLEAN DEFAULT TRUE | |
| created_at | TIMESTAMPTZ | |
| UNIQUE | (source, tile_id, product_short_name, acquisition_date) | |

#### live_areas — One row per Live Area (migration 025)

| Column | Type | Notes |
|---|---|---|
| area_id | SERIAL PK | |
| dataset_id | INT FK → datasets ON DELETE SET NULL | The area's own `LIVE_AREA` dataset, processed by the normal pipeline |
| name | VARCHAR(255) NOT NULL | |
| region_id | INT FK → regions_of_interest ON DELETE SET NULL | The saved location the area was created from |
| location_label / bbox_wkt | VARCHAR(255) / TEXT NOT NULL | Copied at creation |
| retention | SMALLINT DEFAULT 6 | Scenes kept, CHECK 1–12 |
| enabled | BOOLEAN DEFAULT TRUE | Scheduler skips disabled areas |
| status | VARCHAR(20) DEFAULT `BACKFILLING` | `BACKFILLING`, `ACTIVE`, `RUNNING`, `WAITING` (queued behind another area's cycle, migration 026), `ERROR`, `DELETED` |
| status_message | TEXT | Shown on the card |
| last_checked_at | TIMESTAMPTZ | Also the FIFO key for the cycle queue (oldest first) |
| forecast / forecast_updated_at | JSONB / TIMESTAMPTZ | Stored forecast so opening the card never recomputes it |
| created_at / updated_at / deleted_at | TIMESTAMPTZ | Soft-delete; at most `MAX_AREAS=5` non-deleted areas |

#### live_scenes — Per-scene log of a Live Area (never deleted)

| Column | Type | Notes |
|---|---|---|
| live_scene_id | BIGSERIAL PK | |
| area_id / dataset_id | INT (not FKs) | |
| scene_date | DATE NOT NULL | The Sentinel-1 acquisition date — UNIQUE `(area_id, scene_date)` |
| s1_product_ids | TEXT[] | All S1 frames of that date |
| status | VARCHAR(20) | `PROCESSING`, `READY`, `PARTIAL` (a MODIS/GPM source failed — still shown), `FAILED`, `DELETED` |
| source_status | JSONB | `{sentinel1|modis|gpm: {status: OK|FAILED|UNAVAILABLE, detail, matched_date, nearest}}` |
| metrics / interpretations / area_status / previews | JSONB | Small summary numbers, the condition sentences, the one-line area status, and the PNG manifest |
| deleted_at / delete_reason / deleted_files / freed_bytes | | Filled by retention or area deletion; the row itself stays for audit |
| created_at / updated_at | TIMESTAMPTZ | |

#### live_events — Step log of Live Area cycles

`event_id` BIGSERIAL PK, `area_id`, `scene_date`, `step` (e.g. `CYCLE`, discovery, ingest, retention), `status`, `message`, `details` JSONB, `created_at`. Indexed on `(area_id, created_at DESC)`; merged with the area dataset's `processing_logs` by `GET /api/live/areas/{id}/activity`.

#### live_dataset_sources — Per-satellite enable flags for the legacy LIVE dataset

**Legacy.** Belongs to the single `dataset_kind='LIVE'` dataset (D23), which is no longer scheduled — Live Areas replaced it. Kept for the old `/api/live` endpoints and existing data.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL PK | |
| source_name | VARCHAR(20) UNIQUE | `SENTINEL1`, `MODIS`, `GPM` — one row per source, not per dataset: there is exactly one LIVE dataset |
| enabled | BOOLEAN | DEFAULT TRUE |
| last_check / last_ingest / next_check | TIMESTAMPTZ | |
| source_config | JSONB DEFAULT `{}` | |
| created_at / updated_at | TIMESTAMPTZ | |

#### data_products — Registry of every output file

| Column | Type | Notes |
|---|---|---|
| product_id | BIGSERIAL PK | |
| product_uuid | UUID | unique |
| scene_id | INT NOT NULL FK → satellite_scenes ON DELETE CASCADE | **Always required**, even for MODIS/GPM products — those are registered against a placeholder `satellite_scenes` row (`product_identifier` like `NASA_AUX_{SOURCE}_{dataset_id}_{YYYYMMDD}`), not a separate `nasa_scene_id` column. There is no `nasa_scene_id` column on this table. |
| job_id | BIGINT NOT NULL FK → processing_jobs ON DELETE RESTRICT | |
| dataset_id | INT FK → datasets ON DELETE CASCADE | |
| product_tier | `product_tier_enum` | Current (D14, migration 020): `RAW`, `ALIGNED`, `DESPECKLED`, `INDICES`, `ACCUMULATED`, `COG`, `FUSED`. Legacy values `BRONZE`, `SILVER`, `GOLD`, `FUSION` are still valid enum members and still present on rows written before the rename — they are never rewritten, only read via `etl/tier_names.py` (`rank()`/`equivalent_tiers()`), which understands both vocabularies. `PREVIEW` sits outside the tier/lineage rank entirely. |
| source | VARCHAR(20) DEFAULT `SENTINEL1` | `SENTINEL1`, `MODIS`, `GPM`, `FUSION` |
| ★ processing_level | VARCHAR(20) NULL, default `PROCESSED` | `RAW` or `PROCESSED` — which level produced this artifact; NULL only on legacy rows |
| product_type | VARCHAR(50) NOT NULL | e.g. `CROPPED_TIFF` |
| band_name | VARCHAR(20) NOT NULL | Widened from VARCHAR(10) in migration 018 specifically so FUSION rows can be tagged `FUSION_RAW`/`FUSION_PROCESSED` (16 chars) — see DOCS/PIPELINE.md §3.1 |
| file_name | VARCHAR(255) NOT NULL | |
| file_path | TEXT NOT NULL | |
| file_size_mb | NUMERIC(12,3) NOT NULL | **Megabytes**, not bytes — column is `file_size_mb`, not `size_bytes` |
| file_format | VARCHAR(20) DEFAULT `TIFF` | Column is `file_format`, not `format` |
| data_hash_sha256 | VARCHAR(64) NOT NULL | SHA-256; column is `data_hash_sha256`, not `checksum` |
| crs | VARCHAR(50) DEFAULT `EPSG:4326` | |
| pixel_size_m | NUMERIC(8,3) | |
| nodata_value | NUMERIC | |
| rows / cols | INT | |
| band_count | SMALLINT DEFAULT 1 | |
| storage_location | `storage_location_enum` | |
| is_valid | BOOLEAN DEFAULT TRUE | set FALSE on tier cleanup |
| is_latest | BOOLEAN DEFAULT TRUE | Drives dedup: unique-ish on `(scene_id, band_name, product_tier, dataset_id)` — see DOCS/PIPELINE.md §3.1/§3.2b for the traps this has caused |
| created_at / updated_at | TIMESTAMPTZ | |

#### data_lineage — Provenance graph (parent → child)

| Column | Type | Notes |
|---|---|---|
| lineage_id | BIGSERIAL PK | |
| parent_product_id | BIGINT NOT NULL FK → data_products ON DELETE CASCADE | Not `source_product_id` |
| child_product_id | BIGINT NOT NULL FK → data_products ON DELETE CASCADE | Not `target_product_id` |
| transformation_type | VARCHAR(50) NOT NULL | e.g. `CALIBRATE`, `LEE_FILTER`, `COMPUTE_NDVI`, `ACCUMULATE_RAIN`, `FUSE` |
| stage_id | INT NOT NULL FK → processing_stages ON DELETE RESTRICT | |
| job_id | BIGINT NOT NULL FK → processing_jobs ON DELETE RESTRICT | |
| transformation_params | JSONB DEFAULT `{}` | |
| input_checksum | VARCHAR(64) | Not `checksum_source` |
| output_checksum | VARCHAR(64) | Not `checksum_target` |
| created_at | TIMESTAMPTZ | |
| CHECK | `parent_product_id <> child_product_id` | |
| UNIQUE | (parent_product_id, child_product_id) | |

#### fusion_products — HDF5 fusion-specific metadata

| Column | Type | Notes |
|---|---|---|
| fusion_id | BIGSERIAL PK | Not `fusion_product_id` |
| dataset_id | INT FK → datasets ON DELETE CASCADE | |
| feature_date | DATE NOT NULL | Not `fusion_date` |
| region_id | INT NOT NULL FK → regions_of_interest ON DELETE RESTRICT | |
| s1_scene_id | INT FK → satellite_scenes ON DELETE SET NULL | Points at the **scene**, not a `data_products` row |
| modis_scene_id | BIGINT FK → nasa_scenes.nasa_scene_id ON DELETE SET NULL | Likewise — not `modis_product_id`, not a `data_products` FK |
| gpm_scene_id | BIGINT FK → nasa_scenes.nasa_scene_id ON DELETE SET NULL | Likewise |
| days_since_s1 | INT NOT NULL | |
| feature_stack_path | TEXT NOT NULL | Not `fusion_file_path` |
| ★ fusion_strategy | VARCHAR(20) | Strategy used for this specific fusion (migration 017) |
| ★ processing_level | VARCHAR(20) DEFAULT `PROCESSED` | `RAW` or `PROCESSED` — which tier inputs were used |
| ★ temporal_offset_modis / temporal_offset_gpm | INT | Days offset from S1 date |
| ★ s1_offset_days | SMALLINT | Migration 019 — actual gap used when an S1 scene was borrowed; `0` = same-day, `NULL` = no S1 that day |
| created_at | TIMESTAMPTZ | |
| UNIQUE | (dataset_id, feature_date, processing_level) | `uq_fusion_dataset_date_level`, migration 021 |

#### quality_metrics — Per-band quality scores (Sentinel-1 only)

| Column | Type | Notes |
|---|---|---|
| metric_id | BIGSERIAL PK | |
| scene_id | INT NOT NULL FK → satellite_scenes ON DELETE CASCADE | |
| product_id | BIGINT NOT NULL FK → data_products ON DELETE CASCADE | Present, not mentioned in earlier drafts of this doc |
| band_name | VARCHAR(10) | `VV`, `VH` — stays at 10 chars (migration 018 only widened `data_products.band_name`) |
| assessed_at | TIMESTAMPTZ | |
| total_pixels / valid_pixels | BIGINT NOT NULL | |
| nodata_pixels | BIGINT DEFAULT 0 | |
| nodata_percent | NUMERIC(5,2), GENERATED | **Percentage 0–100**, not a 0.0–1.0 fraction |
| backscatter_mean_db / std_db / min_db / max_db | NUMERIC(8,4) | |
| cloud_threshold_percent | NUMERIC(5,2) DEFAULT 20.0 | |
| radiometric_consistency | BOOLEAN | |
| speckle_index | NUMERIC(8,4) | std / \|mean\| |
| quality_score | NUMERIC(5,2) NOT NULL | 0–100, CHECK constrained |
| quality_flag | VARCHAR(20) DEFAULT `UNCHECKED` | Four values: `PASS`, `FAIL`, `WARNING`, `UNCHECKED` — not three, and not VARCHAR(10) |
| notes | TEXT | |
| created_at | TIMESTAMPTZ | |
| UNIQUE | (scene_id, product_id, band_name) | |

#### Other Tables (unchanged from standard design)

- **processing_stages**: Stage definitions (id, name, order, retry_count)
- **processing_jobs**: Per-scene per-stage execution (job_id, scene_id, stage_id, status, duration, cpu/memory peaks)
- **dataset_jobs**: Per-dataset job aggregation (job_id, dataset_id, kind, status)
- **scene_job_state**: Per-scene progress within a dataset job
- **processing_logs**: Structured event log (stage, status, duration, error details, `details.aux_complete` used by restart resume — see DOCS/PIPELINE.md "Concurrency & Durability Safeguards")
- **alert_events**: Quality failures (hypertable if TimescaleDB available)
- **regions_of_interest**: Named AOIs with PostGIS geometry (`source` = `SEEDER` for built-in read-only regions, `USER` for ones created via `POST /api/regions`; soft-deletable)
- **reference_land_polygons**: OSM land polygons (ODbL), clipped to Indonesia, GiST-indexed — reference data for `etl/land_mask.py`'s coastline-distance layer (migration 023). Pure reference: no tier, no lineage, not tied to any one dataset, never mutates RAW or FUSED data.
- **processing_rules**: Configurable QA thresholds per stage
- **cleanup_operations**: Tier deletion progress tracking
- **api_access_logs**: API audit trail (hypertable if TimescaleDB available)
- **dataset_versions**: Semantic-version metadata on top of a `data_products` row (`version_number`, `is_production`, `is_deprecated`, release notes) — not exposed by any current API route; exists for future product-versioning use

**Not a table**: the reference layers themselves (`masks/land_distance.tif`, `masks/water_occurrence.tif`) and cross-dataset merge output (`data/merged/merged_{date}.h5`) are filesystem artifacts with no `data_products`/`data_lineage` rows — see DOCS/PIPELINE.md "Reference Layers" and "Cross-Dataset Merge". Reference layers are dataset-scoped (one pair of rasters per dataset, all dates share them, rebuilt automatically when `fusion_grid` changes under them — DECISIONS.md D20). Merge output is *cross*-dataset and intentionally outside `data/datasets/` so the storage scanner doesn't double-count it against any one source dataset.

### Tier Definitions

Canonical names per DOCS/DECISIONS.md D14 — named for the **contract** the artifact satisfies, not a medallion quality tier (GOLD/COG is a re-wrap of SILVER/DESPECKLED pixels, not higher quality; rank 2 branches by source because Lee filtering, index computation, and rain accumulation are different kinds of operations). Legacy medallion names (`BRONZE`/`SILVER`/`GOLD`/`FUSION`) are still accepted everywhere data is read (`etl/tier_names.py`), since rows written before the rename keep their original value.

| Tier (rank) | Legacy equivalent | Purpose | When Created | Sentinel-1 | MODIS | GPM |
|---|---|---|---|---|---|---|
| RAW (0) | RAW | Original download, unprocessed | DOWNLOAD stage | N/A | N/A | N/A |
| ALIGNED (1) | BRONZE | EPSG:4326 + cropped to AOI, minimum usable | CROP stage (S1 RAW) | Calibrated + cropped, no filter | Flood map extracted, no indices | Rainfall extracted, no accum |
| DESPECKLED / INDICES / ACCUMULATED (2, per source) | SILVER | Source-specific value add | LEE_FILTER (S1) or COMPUTE_* (MODIS/GPM) | Lee-filtered, QA-scored | NDVI/NDWI computed | 24h/72h/7d accum computed |
| COG (3) | GOLD | Cloud-Optimized GeoTIFF, production-ready | GOLD_EXPORT stage | COG with overviews | COG with overviews | COG with overviews |
| PREVIEW (outside rank) | PREVIEW | PNG visualizations for human inspection | PREVIEW stage | Grayscale/Colored/Composite per processing level | Grayscale/Colored per processing level | Typically not created (coarse res) |
| FUSED (4) | FUSION | HDF5 multi-source stacks, ML-ready | FUSION stage | Multi-source per processing level | Multi-source per processing level | Multi-source per processing level |

**Mapping to folder structure**: `data/datasets/{id}_{slug}/{source}/{RAW|PROCESSED}/`
(full tree and rationale in DOCS/PIPELINE.md "On-disk layout"). Tier is **not** a
path segment (D15) — it remains the value of `product_tier`, the vocabulary of
`data_lineage`, and a key of `storage_breakdown`, mapped onto just two drawers
per source:
- `{source}/RAW/` → tier ALIGNED (calibrated/cropped, no per-source value-add)
- `{source}/PROCESSED/` → tier COG (analysis-ready, COG-wrapped)
- `fusion/{co-occurrence,full-coverage,hybrid}/` → HDF5 stacks, one folder per strategy
- `masks/` → reference layers (not a `product_tier` value at all — see "Not a table" above)
- `_work/` → tiers RAW (SAFE zip) and DESPECKLED/INDICES/ACCUMULATED (pre-COG intermediate): swept at job end, never retained

RAW and rank-2 (DESPECKLED/INDICES/ACCUMULATED) therefore have **no drawer of
their own**: they exist as `product_tier` values and lineage edges, not as
retained files.

**Granule cache quirk**: raw MODIS/GPM granules live in `_granule_cache/`, outside any per-date grouping, because one daily GPM granule feeds the 72h/7d windows of later dates — it cannot be owned by a single date. They are still *accounted* as tier `RAW` in `storage_breakdown`.

### Key Indexes

```sql
CREATE INDEX idx_source_config_dataset ON dataset_source_config(dataset_id);
CREATE INDEX idx_data_products_dataset_tier ON data_products(dataset_id, product_tier);
CREATE INDEX idx_dprods_dataset_tier_source ON data_products(dataset_id, product_tier, source) WHERE is_latest = TRUE;
CREATE INDEX idx_dprods_dataset_level ON data_products(dataset_id, processing_level);
CREATE INDEX idx_scenes_acq_dt ON satellite_scenes(acquisition_datetime);
CREATE INDEX idx_scenes_region_id ON satellite_scenes(region_id);
CREATE INDEX idx_fusion_dataset_date ON fusion_products(dataset_id, feature_date);
CREATE INDEX idx_lineage_child_id ON data_lineage(child_product_id);
CREATE INDEX idx_lineage_parent_id ON data_lineage(parent_product_id);
```
(`satellite_scenes` has no `dataset_id` column, so there is no `idx_scenes_dataset_date` — date/region indexes are separate.)

### Table Count

24 domain tables (`schema_migrations` not counted) across `database/schema.sql` and `database/migrations/*.sql` (001–026) — exceeds the 12-master-table academic minimum. Full list: `datasets`, `dataset_source_config`, `dataset_jobs`, `scene_job_state`, `satellite_scenes`, `nasa_scenes`, `data_products`, `dataset_versions`, `data_lineage`, `fusion_products`, `quality_metrics`, `regions_of_interest`, `reference_land_polygons`, `live_dataset_sources`, `live_areas`, `live_scenes`, `live_events`, `processing_stages`, `processing_jobs`, `processing_logs`, `processing_rules`, `alert_events`, `cleanup_operations`, `api_access_logs`.

Reports are not stored in the database either: `reports/*.pdf|json` are files in the dataset folder, regenerated when `datasets.updated_at` is newer than the latest file.
