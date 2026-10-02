# Trinity: The DataLab

**Parametric, user-configurable multi-modal satellite data lake for flood detection research — plus automatic per-area Live Monitoring and PDF dataset reports.**

Users select which satellites to ingest, which processing level to apply, which fusion strategy to use, and which previews to generate — all at dataset creation time. No other platform (GEE, openEO, MPC, Sen1Floods11, Kuro Siwo) offers this parametric configurability.

## What It Does

1. Ingests satellite data from three authorized APIs: Sentinel-1 SAR (ESA/CDSE), MODIS (NASA LAADS DAAC / LANCE NRT), GPM IMERG rainfall (NASA/JAXA GES DISC).
2. Processes each source through its own pipeline (calibration, filtering, quality analytics, indices, rain accumulation).
3. Fuses selected sources into a single ML-ready HDF5 file per date, on one pinned grid per dataset.
4. Tracks full data lineage with SHA-256 checksums at every stage.
5. Builds dataset-wide reference layers (distance to coastline, permanent water occurrence) next to the fusion stacks.
6. Stitches fusion stacks of adjacent split-AOI datasets into one array per date (cross-dataset merge).
7. Generates an 11-section PDF report (+ JSON export) per dataset, on demand.
8. **Live Monitoring**: up to 5 user-defined "Live Areas" are checked automatically four times a day; each new Sentinel-1 pass is downloaded with matching MODIS/GPM, rendered into 8 previews with plain-language condition sentences, charted with a short forecast, and old scenes are deleted by a retention rule (1–12 scenes).

## Core Differentiator: User Configures Everything

```
Step 1: Region & Dates           → saved location + date range (+ optional quality filters, S1 orbit direction)
Step 2: Select Satellites        → per satellite: Sentinel-1, MODIS, GPM, each RAW and/or PROCESSED
Step 3: Fusion & Preview         → CO_OCCURRENCE | FULL_COVERAGE | HYBRID; GRAYSCALE, COLORED, COMPOSITE
Step 4: Review                   → name, description, create
```

This enables **preprocessing ablation studies** (compare RAW vs PROCESSED inputs to ML models) and **fusion strategy comparison** (co-occurrence vs full-coverage) — neither possible in existing platforms.

## Quick Start

```bash
# 1. Clone and set up Python
git clone <repo-url> && cd trinity-datalab
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Set up PostgreSQL (14+ with PostGIS; TimescaleDB optional)
# Default DB name is sentinel1_flood (see .env.example) — a historical leftover
# from before the project was renamed "Trinity: The DataLab", never reconciled.
psql -U postgres -c "CREATE DATABASE sentinel1_flood;"
psql -U postgres -d sentinel1_flood -f database/schema.sql
for f in database/migrations/*.sql; do psql -U postgres -d sentinel1_flood -f "$f"; done

# 3. Configure credentials
cp .env.example .env
# Edit: DB_*, then ADD COPERNICUS_USER, COPERNICUS_PASSWORD, NASA_EARTHDATA_TOKEN
# (not present in .env.example — see ARCHITECTURE.md "Environment Variables")

# 4. Run
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
# http://localhost:8000      → landing page (web/index.html)
# http://localhost:8000/app  → the application (web/app.html)
# http://localhost:8000/docs → OpenAPI / Swagger
```

## Project Structure (Key Directories)

```
trinity-datalab/
├── api/              # FastAPI REST API + routes (datasets, report, live, merge, ...)
├── etl/              # Pipeline modules (download, calibrate, filter, fuse),
│                     # live_* (Live Monitoring), report_* (PDF report)
├── web/              # Vanilla HTML/JS/CSS + Leaflet: index.html (landing), app.html + app.js (app)
├── database/         # schema.sql + migrations/ (001–026)
├── config/           # config.json (mostly legacy), config_locations.json
├── tests/            # pytest suite
└── data/
    ├── datasets/     # Output: {id}_{slug}/{source}/{RAW|PROCESSED}/, fusion/, preview/, masks/, reports/, live/
    └── merged/       # Cross-dataset merge output
```

## Documentation

| File | Contents |
|---|---|
| `ARCHITECTURE.md` | Tech stack, deployment, environment variables, disk layout, DB schema (ER diagram, tables, constraints) |
| `PIPELINE.md` | Pipeline stages per satellite, fusion, download concurrency/resilience, Live Monitoring cycle, plus implementation breadcrumbs: level rules, known traps, deliberate limits |
| `INTERFACE.md` | REST API (all endpoints, request/response, error codes) and web UI/UX (pages, components, user journey) |
| `DECISIONS.md` | Architecture decisions and rationale, plus what changed from the earlier prototype |
| `REPORT.md` | What the generated PDF/JSON dataset report contains, where every number comes from, and what it deliberately does not claim |

## Hardware Requirements

- **RAM**: 8 GB min, 16 GB recommended (Lee filter + HDF5 fusion are memory-intensive)
- **Disk**: 20–50 GB free for active datasets (much more for island-scale AOIs — see DECISIONS.md D18)
- **Concurrency**: Single machine. At most `MAX_ACTIVE_JOBS=2` dataset jobs at once (others queue FIFO), `PIPELINE_MAX_CONCURRENT_SCENES=2` scene pipelines per job, `S1_PARALLEL_DOWNLOADS=3` Sentinel-1 downloads per job, global per-provider connection caps (see PIPELINE.md "Download Concurrency & Resilience")
- **Network**: Stable connection for live downloads from ESA/NASA

## Running Tests

```bash
pytest tests/ -v --cov=etl --cov=api
```
