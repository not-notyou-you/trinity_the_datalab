# The Trinity — Flood Prediction Data Pipeline

## Overview

**The Trinity** adalah sistem ETL (Extract, Transform, Load) otomatis yang mengumpulkan, memproses, dan menyimpan data satelit multi-sensor untuk prediksi banjir dan perutean evakuasi di wilayah DKI Jakarta (Jabodetabek). Sistem ini menggabungkan data dari tiga sumber utama:

- **Sentinel-1**: SAR (Synthetic Aperture Radar) untuk deteksi permukaan air dan genangan
- **MODIS**: Flood detection dan analisis tutupan awan
- **GPM**: Pengukuran presipitasi real-time dari satelit

Arsitektur berbasis database PostgreSQL dengan GIS, memungkinkan data lineage tracking lengkap, quality assurance terukur, dan akses API untuk aplikasi machine learning dan deep learning.

---

## Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────┐
│  PUBLIC DATA SOURCES                                        │
│  • Copernicus CDSE (Sentinel-1)                            │
│  • NASA LAADS NRT (MODIS)                                  │
│  • NASA GES DISC (GPM)                                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  MODULE 1: DISCOVERY & DOWNLOAD                            │
│  • Query scene availability (bbox, date range)             │
│  • Resume-capable download with MD5 validation             │
│  • Band extraction (VV, VH polarizations)                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼ (RAW tier: 400-800 MB per scene)
┌─────────────────────────────────────────────────────────────┐
│  MODULE 1b: CALIBRATION (Sentinel-1 only)                 │
│  • Radiometric calibration using metadata XML              │
│  • Terrain correction via GCP re-projection                │
│  • TIFF output with georeferencing                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼ 
┌─────────────────────────────────────────────────────────────┐
│  MODULE 2: SPATIAL PROCESSING                              │
│  • Crop to Jabodetabek boundary (106.4°-107.2°E, 6.7°-5.9°S)
│  • Nearest neighbor or bilinear resampling                 │
│  • Output: BRONZE tier (40-50 MB per scene)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  MODULE 3: SPECKLE FILTERING                               │
│  • Enhanced Lee filter (7×7 window, 1 look)                │
│  • Preserve edges while reducing noise                     │
│  • Output: SILVER tier (45-50 MB per scene)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  MODULE 4: CLOUD-OPTIMIZED GEOTIFF EXPORT                 │
│  • LZW compression, 512×512 tile blocks                    │
│  • Overviews for multi-scale access (2, 4, 8, 16)          │
│  • Output: GOLD tier (40-45 MB per scene)                  │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  MODULE 6: QUALITY ANALYTICS                               │
│  • Compute band metrics (mean, std, min, max backscatter)  │
│  • Radiometric consistency check (valid range -35 to +5 dB)│
│  • Speckle index & nodata quantification                   │
│  • Quality score (0-100): PASS/FAIL flag                   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  MODULE 10: PREVIEW RENDERING                              │
│  • Render GOLD COGs to PNG (grayscale + colored)           │
│  • Percentile 2-98 stretch; per-source colormaps           │
│  • Runs BEFORE fusion, while gold/ still exists on disk    │
│  • Output: PREVIEW tier (~9-30 MB per acquisition date)    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  MODULE 9: DATA FUSION (optional)                          │
│  • Align Sentinel-1, MODIS, GPM by date/location          │
│  • Create multi-modal feature stacks (HDF5)                │
│  • Output ready for deep learning training                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  POSTGRESQL DATABASE + STORAGE BACKEND                      │
│  • Full data lineage and provenance tracking               │
│  • Satellite scene metadata + quality metrics              │
│  • Processing job logs with error handling                 │
│  • Alert events (arrivals, quality warnings, failures)     │
│  • Tier cleanup & storage optimization policies            │
└─────────────────────────────────────────────────────────────┘
```

---

## Proses Download

### Mekanisme Discovery

Sistem menggunakan **Copernicus CDSE OData API** untuk menemukan scene Sentinel-1:

```
GET https://catalogue.dataspace.copernicus.eu/odata/v1/Products
  ?$filter=Collection/Name eq 'SENTINEL-1'
           and OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(...)')
           and ContentDate/Start gt 2024-01-01T00:00:00Z
           and ContentDate/Start lt 2024-12-31T23:59:59Z
           and Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' 
               and att/OData.CSC.StringAttribute/Value eq 'GRD')
```

**Output Discovery**: List scene metadata (orbit, cloud cover, acquisition time, size)

### Download & Resume

- **Streaming download** ke disk lokal dengan **MD5 checksum validation**
- **Resume capability**: Jika terputus, lanjut dari byte terakhir yang berhasil
- **Error handling**: Retry otomatis hingga 3x dengan exponential backoff
- **Storage**: Disimpan sebagai `.zip` asli (diperlukan untuk metadata kalibrasi)

---

## Penyimpanan File & Organisasi

Struktur direktori dataset setelah processing lengkap:

```
data/
└── datasets/
    └── {dataset_id}/
        ├── raw/
        │   ├── {scene_slug}/
        │   │   ├── S1A_..._VV.tif (original extracted)
        │   │   └── S1A_..._VH.tif
        │   └── S1A_....zip (opsional, jika keep_raw=true)
        │
        ├── bronze/
        │   └── {scene_slug}/
        │       ├── S1A_..._VV_crop.tif (after CROP)
        │       └── S1A_..._VH_crop.tif
        │
        ├── preview/                  # tier turunan: PNG hasil render dari gold/
        │   └── {YYYYMMDD}/
        │       ├── preview_metadata.json
        │       ├── grayscale/        # stretch persentil 2-98, tanpa hue
        │       │   ├── s1_vv.png, s1_vh.png, modis_ndvi.png, ...
        │       │   └── grayscale_info.json
        │       └── colored/          # colormap per-source + komposit RGB
        │           ├── s1_rgb_composite.png, gpm_rain_24h.png, ...
        │           └── colored_info.json
        │
        ├── silver/
        │   └── {scene_slug}/
        │       ├── S1A_..._VV_lee.tif (after LEE_FILTER)
        │       └── S1A_..._VH_lee.tif
        │
        ├── gold/
        │   └── {scene_slug}/
        │       ├── S1A_..._VV_cog.tif (COG, production-ready)
        │       ├── S1A_..._VH_cog.tif
        │       ├── modis_{date}_flood.tif (MODIS jika live aktif)
        │       ├── gpm_rain_24h_{date}.tif (GPM jika live aktif)
        │       └── gpm_rain_72h_{date}.tif
        │
        └── analytics/
            └── scene_{scene_id}_qa.png (quality plot)
```

**Setiap file disimpan dengan:**
- SHA-256 hash untuk integritas
- Georeference metadata (CRS, transform, bounds)
- 10m pixel resolution (native Sentinel-1)

**Tier Deletion Policy** (otomatis berdasarkan `required_tiers`):
- Jika hanya GOLD diminta → RAW, BRONZE, SILVER dihapus setelah COG_EXPORT selesai
- Jika SILVER diminta → RAW, BRONZE dihapus setelah LEE_FILTER selesai
- Cleanup tidak menghapus GOLD tier (always kept)

---

## Pipeline Processing Flow

### Tahap Pemrosesan (Sequential per Scene, Paralel antar Scene)

| Stage | Input | Output | Duration | Storage Change |
|-------|-------|--------|----------|-----------------|
| **DOWNLOAD** | Scene list + bbox + date range | RAW (VV, VH .tif) | 5-15 min | +400-800 MB |
| **CROP** | RAW .tif + geometry | BRONZE (VV, VH cropped) | 2-4 min | +45-50 MB |
| **LEE_FILTER** | BRONZE .tif | SILVER (speckle-filtered) | 3-8 min | +45-50 MB |
| **COG_EXPORT** | SILVER .tif | GOLD (tiled, compressed, overviews) | 2-5 min | +40-45 MB |
| **QUALITY_ANALYTICS** | GOLD .tif | Metrics (CSV + alert if FAIL) | 30-60 sec | negligible |

### Kontrol Alur & Pause/Resume

- **Pause Event**: Setiap scene menunggu thread pause event sebelum step berikutnya
- **Cancel Event**: Jika dataset dibatalkan, semua scene jobs berhenti (graceful shutdown)
- **Retry Logic**: Job gagal → simpan ke scene_job_state, bisa di-retry manual atau otomatis

### Contoh Timeline (3 Scene Paralel, 2 Concurrent Pipelines)

```
Time:  0min          5min          10min         15min          20min
       |---DOWNLOAD--|
T1:                |---CROP--|
T2:                           |---LEE--|
T3:                                    |---COG--|---QA--|
       |---DOWNLOAD--|
T4:                |---CROP--|
T5:                           |---LEE--|
T3:                                    |---COG--|---QA--|
```

Max concurrent scene pipelines: configurable (`PIPELINE_MAX_CONCURRENT_SCENES`, default 2)

---

## Output Format & Hasil Akhir

### GOLD Tier (Production Data)

**Format**: Cloud-Optimized GeoTIFF (COG)
- **Compression**: LZW (lossless)
- **Block size**: 512×512 pixels
- **Overviews**: [2, 4, 8, 16] levels (built-in)
- **CRS**: EPSG:4326 (WGS84)
- **Resolution**: 10m × 10m
- **Data type**: float32 (sigma-0 in linear scale atau dB)

**Bands**: 
- VV (vertical-vertical polarization): Deteksi permukaan air
- VH (vertical-horizontal cross-pol): Tekstur & roughness

### Quality Metrics (dari MODULE 6)

```json
{
  "scene_id": 12345,
  "band_name": "VV",
  "acquisition_datetime": "2024-01-15T22:50:41Z",
  "total_pixels": 31900000,
  "valid_pixels": 31580000,
  "nodata_pixels": 320000,
  "nodata_percent": 1.0,
  "backscatter_mean_db": -12.37,
  "backscatter_std_db": 2.81,
  "backscatter_min_db": -28.44,
  "backscatter_max_db": 1.92,
  "speckle_index": 0.227,
  "radiometric_consistency": true,
  "quality_score": 82.4,
  "quality_flag": "PASS",
  "notes": "Good quality. Low cloud cover. Normal backscatter range for flooded areas."
}
```

### Data Lineage

Setiap GOLD product dapat di-trace kembali ke RAW via data_lineage table:

```
RAW_VV (product_id=1)
  ↓ CROP (job_id=10, transformation_params: bbox, resampling method)
BRONZE_VV (product_id=2, checksum_in=hash_raw, checksum_out=hash_bronze)
  ↓ LEE_FILTER (job_id=11, transformation_params: window_size=7, looks=1)
SILVER_VV (product_id=3)
  ↓ COG_EXPORT (job_id=12, transformation_params: compression=LZW, blocksize=512)
GOLD_VV (product_id=4)
```

Setiap transformation mencatat:
- Input checksum (verifikasi data integrity)
- Output checksum
- Processing parameters (reproducibility)
- Job ID & timestamps

### API Access

Download GOLD produk via:

```bash
# Single product
curl "https://api.example.com/api/products/{product_id}/download"

# Batch by dataset + date range
curl "https://api.example.com/api/datasets/{dataset_id}/download?format=zip"
```

---

## Live Ingestion Mode

Dataset "live" (jika enabled) terus menerima:

- **Sentinel-1**: Daily scene check pukul 02:00 Asia/Jakarta timezone
- **MODIS Flood**: Daily NRT product (1-day latency)
- **GPM Rainfall**: Daily aggregation (24h, 72h, 7-day accumulation)

Backfill otomatis tersedia untuk isi historical data dalam date range tertentu.

---

## Getting Started

### Prerequisites

```bash
pip install requests rasterio geoalchemy2 sqlalchemy psycopg2 apscheduler
export COPERNICUS_USER="your_cdse_email"
export COPERNICUS_PASSWORD="your_cdse_password"
export NASA_EARTHDATA_TOKEN="your_token_from_urs.earthdata.nasa.gov"
export DB_HOST="localhost"
export DB_PORT="5432"
export DB_NAME="sentinel1_flood"
export DB_USER="postgres"
export DB_PASSWORD="..."
```

### Create Dataset via API

```bash
curl -X POST http://localhost:8000/api/datasets \
  -H "Content-Type: application/json" \
  -d '{
    "location": "Jakarta",
    "date_start": "2024-01-01",
    "date_end": "2024-01-31",
    "tiers": ["BRONZE", "SILVER", "GOLD"],
    "name": "Jan 2024 Flood Detection Study",
    "quality_settings": {
      "min_quality_score": 60,
      "min_cloud_cover": 20
    }
  }'
```

### Monitor Progress

```bash
curl http://localhost:8000/api/datasets/{dataset_id}/status
```

---

## Performance Characteristics

- **Throughput**: 3-5 scenes/day per machine (depending on CPU, disk I/O)
- **Storage**: ~130 MB per scene (GOLD + SILVER + BRONZE) or ~45 MB (GOLD only)
- **Database**: PostgreSQL with TimescaleDB extension recommended for > 10k scenes
- **Scalability**: Pipeline is thread-safe; supports multi-machine setup via shared PG database

---

## Troubleshooting

- **Download fails**: Check Copernicus credentials + network connectivity
- **Calibration fails**: Ensure SNAP gpt is in PATH (ESA Sentinel Application Platform)
- **Low quality score**: Verify cloud cover < 20%, min valid pixels > 70%
- **Database connection**: Check DB_HOST, DB_PASSWORD, firewall rules

For detailed logs: `tail -f logs_pipeline/scheduler.log`
