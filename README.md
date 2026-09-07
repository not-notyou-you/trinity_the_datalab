# THE TRINITY

> **"Sentinel Watches, MODIS Sees, GPM Measures"**
>
> THE TRINITY fuses three independent satellite data sources into one flood
> risk and early-warning pipeline: **Sentinel-1 SAR** watches the ground
> continuously regardless of cloud cover or time of day, **MODIS** optical
> imagery sees visual surface detail, and **GPM** rainfall data measures
> precipitation. Combined, they give a fuller picture of flood risk than any
> single sensor could alone.

## Sentinel-1 Flood Detection Data Pipeline

> **Skripsi:** Perancangan dan Implementasi Data Lake Terstruktur dengan ETL Pipeline untuk Dataset Sentinel-1 Siap Produksi dalam Aplikasi Deteksi Banjir

**Program:** Sistem Informasi — Universitas Multimedia Nusantara  
**Tech Stack:** Python 3.10+ · PostgreSQL 14+ · TimescaleDB · FastAPI · SQLAlchemy · PostGIS

---

## Overview

A complete, standardized ETL data pipeline for Sentinel-1 SAR satellite imagery, purpose-built for flood detection research in the Jabodetabek region. The system implements a **Lakehouse architecture** (RAW → BRONZE → SILVER → GOLD → PREVIEW → FUSION) with full metadata tracking, data lineage, and quality assurance.

```
Sentinel-1 SAR  ──[M1:  Download]───► RAW
                ──[M2:  Crop]───────► BRONZE   (Jabodetabek bbox)
                ──[M3:  Lee Filter]─► SILVER   (speckle reduced)
                ──[M4:  Gold Export]► GOLD     (analysis-ready COG, per source)
                ──[M10: Preview]────► PREVIEW  (PNG grayscale + colored)
                ──[M9:  Fusion]─────► FUSION   (multi-modal HDF5 stack)
                ──[M6:  Analytics]──► Quality metrics + reports
                ──[M5:  Orchestrate]► PostgreSQL checkpoints + retry
```

MODIS and GPM enter at SILVER (daily products) and follow the same
GOLD → PREVIEW → FUSION path. See
[`STORAGE_STRUCTURE.md`](STORAGE_STRUCTURE.md) for the on-disk layout.

---

## Project Structure

```
sentinel1-flood-detection/
│
├── .env.example                  # Environment variable template
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yml
│
├── database/
│   ├── schema.sql                # DDL: 11 master tables, indexes, triggers  ✅
│   ├── seed_data.sql             # Sample SQL inserts for testing
│   ├── indexes.sql               # Additional performance indexes
│   └── migrations/
│       ├── 001_initial_schema.sql
│       └── 002_add_timescaledb.sql
│
├── etl/
│   ├── config.py                 # Configuration management
│   ├── database_client.py        # SQLAlchemy ORM models + connection pool   ✅
│   ├── metadata_manager.py       # CRUD hooks for all pipeline stages         ✅
│   ├── lineage_tracker.py        # SHA-256 hashing + provenance tracking      ✅
│   ├── module1_download.py       # Sentinel-1 discovery & recovery
│   ├── module2_crop.py           # Spatial subsetting to AOI bbox
│   ├── module3_lee_filter.py     # SAR speckle reduction (Lee adaptive filter)
│   ├── module4_gold_export.py    # Per-source Cloud-Optimized GeoTIFF export  ✅
│   ├── module5_orchestrator.py   # Pipeline orchestration + DB checkpoints    ✅
│   ├── module6_analytics.py      # Quality metrics & visualization
│   ├── module7_modis_download.py # MODIS flood / NDVI / NDWI daily products   ✅
│   ├── module8_gpm_download.py   # GPM rainfall accumulation windows          ✅
│   ├── module9_fusion.py         # Multi-modal HDF5 feature stack (FUSION)    ✅
│   ├── module10_generate_preview.py # PNG previews from GOLD (PREVIEW tier)   ✅
│   └── seed_data.py              # Synthetic test data generator              ✅
│
├── api/
│   ├── main.py                   # FastAPI app (lifespan, middleware, routers) ✅
│   ├── schemas.py                # Pydantic v2 request/response models         ✅
│   └── routes/
│       ├── health.py             # GET /api/health                             ✅
│       ├── scenes.py             # GET /api/scenes, /{id}, /{id}/status        ✅
│       ├── products.py           # GET /api/products, /{id}/download, /verify  ✅
│       ├── quality.py            # GET /api/quality/{scene_id}                 ✅
│       └── lineage.py            # GET /api/metadata/lineage/{product_id}      ✅
│
├── tests/
│   ├── conftest.py               # pytest fixtures, test DB setup/teardown
│   ├── test_database.py          # Schema creation, constraints, queries
│   ├── test_etl_pipeline.py      # Integration: Module 1-6 end-to-end
│   ├── test_api_endpoints.py     # API request/response validation
│   └── test_quality_metrics.py   # Quality checks with sample data
│
├── docs/
│   ├── DATABASE_DESIGN.md        # ER diagram (Mermaid), normalization        ✅
│   ├── API_DOCUMENTATION.md      # OpenAPI spec, request/response examples
│   ├── SETUP_GUIDE.md            # Installation and deployment guide
│   ├── DATA_DICTIONARY.md        # Field-by-field reference
│   ├── DEPLOYMENT.md             # Docker, systemd, production setup
│   ├── TROUBLESHOOTING.md        # Common issues & solutions
│   └── THESIS_CHAPTERS.md        # Thesis chapter outline (Bab 1-9)
│
├── config/
│   ├── config.json               # Pipeline runtime settings
│   ├── config_locations.json     # AOI region definitions (Jabodetabek, etc.)
│   └── docker-compose.yml        # Local dev: PostgreSQL + TimescaleDB
│
├── processed/                    # ETL output files (git-ignored)
│   ├── bronze/                   # After Module 2 (cropped TIFFs)
│   ├── silver/                   # After Module 3 (Lee filtered)
│   └── gold/                     # After Module 4 (COG, production-ready)
│
├── recovered_temp/               # Raw SAFE/ZIP archives (git-ignored)
├── analytics/                    # Quality charts & reports (git-ignored)
├── checkpoints_pipeline/         # Failure recovery state (git-ignored)
└── logs_pipeline/                # Execution logs (git-ignored)
```

> ✅ = implemented in Phase 1-2

---

## Quick Start

### Prerequisites
- Python 3.10+
- PostgreSQL 14+ with PostGIS extension
- TimescaleDB extension *(optional — schema falls back gracefully)*

### 1. Clone & install
```bash
git clone https://github.com/<your-username>/sentinel1-flood-detection.git
cd sentinel1-flood-detection

python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 3. Setup database
```bash
createdb sentinel1_flood
psql sentinel1_flood -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql sentinel1_flood -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
psql sentinel1_flood < database/schema.sql
```

### 4. Insert seed data
```bash
python -m etl.seed_data
# Expected:
# ✅ Seed data inserted and verified successfully.
#    Scene ID : 1
#    GOLD VV  : product_id=7
#    GOLD VH  : product_id=8
```

### 5. Start API
```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
# Open: http://localhost:8000/docs
```

### 6. Run tests
```bash
pytest tests/ -v --cov=etl --cov=api --cov-report=term-missing
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/health` | Database health & pool stats |
| `GET` | `/api/scenes` | List scenes (filter: date, region, quality) |
| `GET` | `/api/scenes/{id}` | Scene metadata detail |
| `GET` | `/api/scenes/{id}/status` | Pipeline stage execution status |
| `GET` | `/api/products` | List output products (filter: tier, band) |
| `GET` | `/api/products/{id}/download` | Download COG/TIFF file |
| `GET` | `/api/products/{id}/verify` | SHA-256 integrity check |
| `GET` | `/api/quality/{scene_id}` | Quality metrics per band |
| `GET` | `/api/quality/summary/stats` | Aggregated quality statistics |
| `GET` | `/api/metadata/lineage/{product_id}` | Transformation provenance chain |
| `GET` | `/api/datasets/{id}/preview` | PREVIEW gallery: grayscale + colored PNGs per date, with colormap and interpretation metadata |
| `GET` | `/api/datasets/{id}/preview/{scene}/{kind}/{file}` | One preview PNG (`kind` = `grayscale` \| `colored`) |
| `GET` | `/api/datasets/{id}/storage/summary` | Disk usage per tier and per source |

Interactive docs: **http://localhost:8000/docs**

---

## Database Schema

11 master tables — 3NF compliant, PostgreSQL 14+:

| Table | Purpose | TimescaleDB |
|-------|---------|-------------|
| `regions_of_interest` | AOI master data | — |
| `processing_stages` | Pipeline stage config | — |
| `satellite_scenes` | Scene registry | ✅ hypertable |
| `processing_jobs` | Job execution log | — |
| `data_products` | Output artifact registry | — |
| `quality_metrics` | Radiometric QA results | — |
| `processing_rules` | Configurable QA rules | — |
| `data_lineage` | Transformation DAG | — |
| `api_access_logs` | API audit trail | ✅ hypertable |
| `alert_events` | Monitoring events | ✅ hypertable |
| `dataset_versions` | Semantic versioning | — |

See [`docs/DATABASE_DESIGN.md`](docs/DATABASE_DESIGN.md) for full ER diagram, data dictionary, and normalization walkthrough.

---

## ETL Pipeline Stages

Execution order per scene:

```
DOWNLOAD → CROP → LEE_FILTER → QUALITY_ANALYTICS → GOLD_EXPORT → PREVIEW → FUSION → CLEANUP
```

| Module | Stage | Output Tier |
|--------|-------|-------------|
| `module1_download.py` | DOWNLOAD | RAW |
| `module2_crop.py` | CROP | BRONZE |
| `module3_lee_filter.py` | LEE_FILTER | SILVER |
| `module6_analytics.py` | QUALITY_ANALYTICS | Quality metrics (reads SILVER) |
| `module4_gold_export.py` | GOLD_EXPORT | GOLD |
| `module10_generate_preview.py` | PREVIEW | PREVIEW |
| `module9_fusion.py` | FUSION | FUSION |
| `module5_orchestrator.py` | ORCHESTRATE | DB checkpoints |

### PREVIEW tier

Renders publication- and analysis-ready PNGs from the GOLD COGs of a single
acquisition date, into
`data/datasets/{id}_{slug}/preview/{YYYYMMDD}/`:

- `grayscale/` — percentile 2–98 stretch, no hue. For reading one file's own
  values; contrast is optimised per file, so grey levels are **not** comparable
  across dates.
- `colored/` — per-source colormaps on physically meaningful ranges (NDVI/NDWI
  pinned to −1..1, rainfall starting at 0), plus a false-colour
  `s1_rgb_composite.png` (R=VV, G=VH, B=VV−VH). For figures and for comparing
  dates.
- `preview_metadata.json` + one `*_info.json` per subfolder, describing the
  stretch, the colormap strategy, and how to read each layer.

It runs **after GOLD_EXPORT and before FUSION** on purpose: a dataset that only
requested the FUSION tier deletes `gold/` during cleanup, so this is the one
window in which every GOLD raster for that date still exists on disk. Preview
failures never fail the scene — FUSION, the actual deliverable, does not depend
on them.

PREVIEW is a *derived* tier: it is not part of the RAW→FUSION lineage, is never
registered in `data_products`, and is never removed by tier cleanup.

Turn it off per dataset with the **"Buat Preview (Grayscale + Berwarna)"**
checkbox on the create form, or `generate_preview: false` on
`POST /api/datasets`. It is stored in `datasets.generate_preview`
(`DEFAULT TRUE`, migration 016 — **required**, unlike the optional 015).
Disabling previews does not affect FUSION. Regenerate a single date with:

```bash
python -m etl.module10_generate_preview <dataset_id> <dataset_name> <YYYYMMDD> [s1_scene_key]
```

---

## License

Academic project — Universitas Multimedia Nusantara, 2026. For research and educational use.
