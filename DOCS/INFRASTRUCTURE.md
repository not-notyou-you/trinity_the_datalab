# Infrastructure

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
| Data | numpy, scipy, pandas, h5py, xarray | — | Arrays, interpolation, HDF5 |
| HDF4 | pyhdf 0.11+ | — | Reading MODIS HDF4 granules |
| HTTP | requests, httpx | — | Downloads, async testing |
| Scheduling | APScheduler 3.10+ | — | Daily live ingestion cron |
| Resilience | tenacity 9.0+ | — | Retry with exponential backoff |
| Visualization | matplotlib, Pillow | — | Preview PNG generation |
| Frontend | HTML5, CSS3, vanilla JS, Leaflet.js | — | Dashboard (no build step) |
| Testing | pytest, pytest-asyncio, pytest-cov | — | Test suite |

## Database Setup

```bash
# Create database
psql -U postgres -c "CREATE DATABASE trinity_datalab;"

# Enable extensions
psql -U postgres -d trinity_datalab <<EOF
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;  -- skip if unavailable
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
EOF

# Apply schema + migrations
psql -U postgres -d trinity_datalab -f database/schema.sql
for f in database/migrations/*.sql; do
  psql -U postgres -d trinity_datalab -f "$f"
done
```

**TimescaleDB note**: On PostgreSQL 18 (Windows), TimescaleDB is unavailable. Schema uses `IF EXISTS` guards — affected tables fall back to plain PostgreSQL tables. PostgreSQL 17 has an official TimescaleDB Windows installer if hypertables are needed during development.

## Environment Variables (.env)

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=trinity_datalab
DB_USER=postgres
DB_PASSWORD=<strong-password>

# ESA Copernicus (register at dataspace.copernicus.eu)
COPERNICUS_USER=<email>
COPERNICUS_PASSWORD=<password>
# OAuth2 token endpoint: identity.dataspace.copernicus.eu

# NASA Earthdata (get token at urs.earthdata.nasa.gov)
NASA_EARTHDATA_TOKEN=<bearer-token>
# Used for both MODIS (LAADS DAAC) and GPM (GES DISC)

# Pipeline
PIPELINE_MAX_CONCURRENT_SCENES=2
DATA_DIR=./data/datasets
LOG_DIR=./logs
```

## Data Source Authentication

| Source | Auth Method | Endpoint |
|---|---|---|
| Sentinel-1 (CDSE) | OAuth2 password grant | `identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token` |
| MODIS (LAADS DAAC) | Bearer token | `ladsweb.modaps.eosdis.nasa.gov` |
| GPM IMERG (GES DISC) | Bearer token (same NASA token) | `disc.gsfc.nasa.gov` |

All data access is through authorized APIs — not scraping.

## Disk Layout

```
data/datasets/{dataset_id}_{slug}/
├── metadata.json           # Dataset config (satellites, processing level, fusion strategy)
├── {YYYYMMDD}/             # One folder per acquisition date
│   ├── raw/{source}/       # Original downloads (sentinel1/{scene}/)
│   ├── bronze/{source}/    # Cropped to AOI
│   ├── silver/{source}/    # Filtered + QA metadata
│   ├── gold/{source}/      # Cloud-Optimized GeoTIFF
│   ├── preview/{LEVEL}/    # PNG previews (grayscale/, colored/, composite/)
│   └── fusion/             # HDF5 fusion stacks
├── _granule_cache/{modis,gpm}/  # Raw NASA granules shared across dates
└── _work/{scene}/          # Calibration scratch, removed after CROP

logs/{dataset_id}_{slug}.txt     # One run log per dataset
```

Sentinel-1 keeps a `{scene}` (product identifier) folder under its source
because one date can hold several S1 scenes; MODIS/GPM/fusion/preview are
keyed by the date itself, so their files sit directly in the tier folder.

Storage per Sentinel-1 scene: ~2.4 GB (all tiers) or ~0.25 GB (GOLD+FUSION only).

## Docker (Optional)

```yaml
# docker-compose.yml
services:
  db:
    image: timescale/timescaledb-ha:pg14-latest
    environment:
      POSTGRES_DB: trinity_datalab
      POSTGRES_PASSWORD: ${DB_PASSWORD}
    ports: ["5432:5432"]
    volumes: ["pgdata:/var/lib/postgresql/data"]
  api:
    build: .
    ports: ["8000:8000"]
    env_file: .env
    depends_on: [db]
volumes:
  pgdata:
```
