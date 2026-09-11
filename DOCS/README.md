# Trinity: The DataLab

**Parametric, user-configurable multi-modal satellite data lake for flood detection research.**

Users select which satellites to ingest, which processing level to apply, which fusion strategy to use, and which previews to generate — all at dataset creation time. No other platform (GEE, openEO, MPC, Sen1Floods11, Kuro Siwo) offers this parametric configurability.

## What It Does

1. Ingests satellite data from three authorized APIs: Sentinel-1 SAR (ESA/CDSE), MODIS optical (NASA LAADS DAAC), GPM IMERG rainfall (NASA/JAXA GES DISC).
2. Processes each source through its own pipeline (calibration, filtering, quality analytics).
3. Fuses selected sources into a single ML-ready HDF5 file per date.
4. Tracks full data lineage with SHA-256 checksums at every stage.

## Core Differentiator: User Configures Everything

```
Step 1: Select Satellites        → multi-select: Sentinel-1, MODIS, GPM
Step 2: Select Processing Level  → multi-select: RAW, PROCESSED
Step 3: Select Fusion Strategy   → CO-OCCURRENCE | FULL_COVERAGE | HYBRID
Step 4: Select Preview Options   → GRAYSCALE, COLORED, COMPOSITE
```

This enables **preprocessing ablation studies** (compare RAW vs PROCESSED inputs to ML models) and **fusion strategy comparison** (co-occurrence vs full-coverage) — neither possible in existing platforms.

## Quick Start

```bash
# 1. Clone and set up Python
git clone <repo-url> && cd trinity-datalab
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 2. Set up PostgreSQL (14+ with PostGIS + TimescaleDB)
psql -U postgres -c "CREATE DATABASE trinity_datalab;"
psql -U postgres -d trinity_datalab -f database/schema.sql
for f in database/migrations/*.sql; do psql -U postgres -d trinity_datalab -f "$f"; done

# 3. Configure credentials
cp .env.example .env
# Edit: DB_*, COPERNICUS_USER, COPERNICUS_PASSWORD, NASA_EARTHDATA_TOKEN

# 4. Run
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
# Open http://localhost:8000
```

## Project Structure (Key Directories)

```
trinity-datalab/
├── api/              # FastAPI REST API + routes
├── etl/              # Pipeline modules (download, calibrate, filter, fuse)
├── web/              # Vanilla HTML/JS/CSS + Leaflet dashboard
├── database/         # schema.sql + migrations/
├── config/           # config.json, config_locations.json
├── tests/            # pytest suite
└── data/datasets/    # Output: {id}_{slug}/{date}/{tier}/{source}/
```

## Documentation

| File | Contents |
|---|---|
| `INFRASTRUCTURE.md` | Tech stack, DB setup, environment variables |
| `DESIGN.md` | ER diagram, schema, tables, constraints |
| `INTERFACE.md` | UI/UX flow, components, user journey |
| `API.md` | All endpoints, request/response, error codes |
| `ETL.md` | Pipeline stages per satellite, config params |
| `DECISIONS.md` | Architecture decisions and rationale |
| `IMPLEMENTATION_NOTES.md` | Breadcrumbs for maintainers: level rules, known traps, deliberate limits |
| `PROTOTYPE_CHANGELOG.md` | What changed from the earlier prototype |

## Hardware Requirements

- **RAM**: 8 GB min, 16 GB recommended (Lee filter + HDF5 fusion are memory-intensive)
- **Disk**: 20–50 GB free for active datasets
- **Concurrency**: Single machine, MAX_CONCURRENT=2 scenes
- **Network**: Stable connection for live downloads from ESA/NASA

## Running Tests

```bash
pytest tests/ -v --cov=etl --cov=api
```
