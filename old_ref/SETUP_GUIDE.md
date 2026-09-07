# SETUP GUIDE
## Sentinel-1 Flood Detection Data Pipeline

**Author:** Julius Marselinus (BRONTO) — NIM 00000111989  
**Program:** Sistem Informasi — Universitas Multimedia Nusantara

---

## Prerequisites

| Component | Minimum Version | Install |
|-----------|-----------------|---------|
| Python | 3.10+ | [python.org](https://python.org) |
| PostgreSQL | 14+ | [postgresql.org](https://postgresql.org) |
| PostGIS | 3.0+ | `apt install postgis` / homebrew |
| TimescaleDB | 2.x *(optional)* | [docs.timescale.com](https://docs.timescale.com) |
| Git | any | [git-scm.com](https://git-scm.com) |

---

## 1. Clone & Install

```bash
git clone https://github.com/<your-username>/sentinel1-flood-detection.git
cd sentinel1-flood-detection

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

---

## 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sentinel1_flood
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_POOL_SIZE=5
DB_ECHO=false

API_HOST=0.0.0.0
API_PORT=8000
```

---

## 3. Database Setup

### Option A: Manual (recommended for development)

```bash
# Create database
createdb sentinel1_flood

# Install extensions
psql sentinel1_flood -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql sentinel1_flood -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
psql sentinel1_flood -c "CREATE EXTENSION IF NOT EXISTS uuid-ossp;"

# Run schema
psql sentinel1_flood < database/schema.sql

# (Optional) Run additional performance indexes
psql sentinel1_flood < database/indexes.sql

# (Optional) Insert sample SQL seed data
psql sentinel1_flood < database/seed_data.sql
```

### Option B: Docker Compose (quickest)

```bash
docker-compose up -d db
# Waits for healthy state, then runs schema + seed automatically
```

---

## 4. Insert Python Seed Data

For a complete test dataset (8 products, quality metrics, lineage):

```bash
python -m etl.seed_data
```

Expected output:
```
✅ Seed data inserted and verified successfully.
   Scene ID  : 1
   GOLD VV   : product_id=7
   GOLD VH   : product_id=8
   QA VV     : metric_id=1
```

---

## 5. Start API Server

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

Open: **http://localhost:8000/docs** for interactive Swagger UI.

Available docs:
- Swagger UI: `/docs`
- ReDoc: `/redoc`
- OpenAPI JSON: `/openapi.json`

---

## 6. Run Tests

```bash
# All tests with coverage
pytest tests/ -v --cov=etl --cov=api --cov-report=term-missing

# Specific test files
pytest tests/test_database.py -v
pytest tests/test_etl_pipeline.py -v
pytest tests/test_api_endpoints.py -v
pytest tests/test_quality_metrics.py -v
```

### Test Database Setup

Tests use a separate database to avoid polluting development data. Create it first:

```bash
createdb sentinel1_flood_test
psql sentinel1_flood_test -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql sentinel1_flood_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
psql sentinel1_flood_test -c "CREATE EXTENSION IF NOT EXISTS uuid-ossp;"
psql sentinel1_flood_test < database/schema.sql
```

Set the test DB URL in your environment:
```bash
export TEST_DATABASE_URL="postgresql+psycopg2://postgres:password@localhost:5432/sentinel1_flood_test"
```

---

## 7. Run ETL Pipeline

### Manually (module by module)

```bash
# Run the orchestrator for a specific scene
python -m etl.module5_orchestrator
```

### Via Python API

```python
from etl.database_client import DatabaseClient
from etl.module5_orchestrator import PipelineOrchestrator, SceneContext

db  = DatabaseClient.from_env()
orch = PipelineOrchestrator(db, output_dir="processed")

ctx = SceneContext(
    scene_id=1,
    product_identifier="S1A_IW_GRDH_...",
    region_id=1,
    raw_file_path="/data/raw/S1A_20240115.zip"
)
result = orch.run(ctx)
print(result.completed_stages)
```

---

## 8. Project Directory Reference

```
sentinel1/
├── .env                    ← your credentials (git-ignored)
├── .env.example            ← template
├── database/schema.sql     ← run this first
├── etl/seed_data.py        ← python -m etl.seed_data
├── api/main.py             ← uvicorn api.main:app
└── tests/                  ← pytest tests/
```

---

## Common Issues

**`psycopg2` install fails on Windows:**
```bash
pip install psycopg2-binary
```

**PostGIS not found:**
```bash
# Ubuntu/Debian
sudo apt install postgresql-14-postgis-3

# macOS (homebrew)
brew install postgis
```

**TimescaleDB optional:** If TimescaleDB is not installed, the schema falls back gracefully to plain PostgreSQL. All features work — only automatic time-series partitioning is disabled.

**`uuid_generate_v4()` function missing:**
```bash
psql sentinel1_flood -c "CREATE EXTENSION IF NOT EXISTS uuid-ossp;"
```
